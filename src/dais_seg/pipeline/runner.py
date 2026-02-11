"""End-to-end pipeline runner — orchestrates the full SEG workflow via Databricks Jobs.

Takes source credentials, creates a federation connection, then submits
Databricks notebook jobs for each pipeline stage:
  Connection → Profile → Generate → Conform → Validate

The Streamlit app (which has no SparkSession) uses this to drive the
actual Spark-based pipeline execution on Databricks clusters.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    NotebookTask,
    RunLifeCycleState,
    RunResultState,
    SubmitTask,
)

from dais_seg.config import get_config, SEGConfig
from dais_seg.ingest.connection_manager import ConnectionManager
from dais_seg.ingest.env_parser import SourceCredentials

logger = logging.getLogger(__name__)


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    CONNECTING = "connecting"
    PROFILING = "profiling"
    GENERATING = "generating"
    CONFORMING = "conforming"
    VALIDATING = "validating"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class StageResult:
    """Result of a single pipeline stage."""

    stage: str
    status: str  # "success" or "failed"
    run_id: Optional[int] = None
    duration_seconds: float = 0.0
    output: dict = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class PipelineResult:
    """Complete result of an end-to-end pipeline execution."""

    status: PipelineStatus
    connection_name: str = ""
    foreign_catalog: str = ""
    blueprint_id: str = ""
    workspace_id: str = ""
    confidence_score: Optional[float] = None
    confidence_level: str = ""
    tables_generated: int = 0
    total_duration_seconds: float = 0.0
    stages: list[StageResult] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to serializable dictionary."""
        return {
            "status": self.status.value,
            "connection_name": self.connection_name,
            "foreign_catalog": self.foreign_catalog,
            "blueprint_id": self.blueprint_id,
            "workspace_id": self.workspace_id,
            "confidence_score": self.confidence_score,
            "confidence_level": self.confidence_level,
            "tables_generated": self.tables_generated,
            "total_duration_seconds": round(self.total_duration_seconds, 1),
            "stages": [
                {
                    "stage": s.stage,
                    "status": s.status,
                    "duration_seconds": round(s.duration_seconds, 1),
                    "error": s.error,
                }
                for s in self.stages
            ],
            "error": self.error,
        }


class PipelineRunner:
    """Orchestrates the full SEG pipeline by submitting Databricks notebook jobs.

    Flow:
    1. Create federation connection (via SDK SQL, no Spark needed)
    2. Submit notebook 01 → profile source → get blueprint_id
    3. Submit notebook 02 → generate synthetic Delta Tables
    4. Submit notebook 03 → conform through medallion
    5. Submit notebook 04 → validate → get confidence score

    Each notebook runs on a Databricks cluster with Spark. The runner
    polls for completion and reads output via the Jobs API.
    """

    # Terminal states for Databricks runs
    TERMINAL_STATES = {
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    }

    def __init__(
        self,
        client: Optional[WorkspaceClient] = None,
        config: Optional[SEGConfig] = None,
    ):
        self.client = client or WorkspaceClient()
        self.config = config or get_config()
        self.connection_manager = ConnectionManager(self.client, self.config)

    def run(
        self,
        credentials: SourceCredentials,
        scale_factor: float = 0.1,
        workspace_name: str = "default",
        on_status_change: Optional[Callable[[PipelineStatus, str], None]] = None,
    ) -> PipelineResult:
        """Execute the full end-to-end pipeline.

        Args:
            credentials: Source database credentials.
            scale_factor: Data scale multiplier (e.g., 0.1 = 10%).
            workspace_name: Name for the synthetic workspace.
            on_status_change: Optional callback for status updates.

        Returns:
            PipelineResult with full execution details.
        """
        start_time = time.time()
        result = PipelineResult(status=PipelineStatus.PENDING)

        def update_status(status: PipelineStatus, message: str = ""):
            result.status = status
            logger.info(f"Pipeline [{status.value}]: {message}")
            if on_status_change:
                on_status_change(status, message)

        try:
            # ---- Stage 1: Create Federation Connection ---- #
            update_status(PipelineStatus.CONNECTING, "Creating federation connection")
            stage_start = time.time()

            setup_result = self.connection_manager.setup_source(credentials)
            result.connection_name = setup_result["connection_name"]
            result.foreign_catalog = setup_result["foreign_catalog"]

            test = setup_result["test_result"]
            if not test["success"]:
                raise RuntimeError(f"Connection test failed: {test.get('error', 'unknown')}")

            result.stages.append(StageResult(
                stage="connect",
                status="success",
                duration_seconds=time.time() - stage_start,
                output=setup_result,
            ))

            # ---- Stage 2: Profile Source ---- #
            update_status(PipelineStatus.PROFILING, f"Profiling {result.foreign_catalog}")

            profile_output = self._run_notebook(
                "01_profile_source",
                {
                    "source_catalog": result.foreign_catalog,
                    "catalog": self.config.catalog,
                    "schema": self.config.schema,
                    "sample_size": "50000",
                },
            )
            result.stages.append(profile_output)
            if profile_output.status == "failed":
                raise RuntimeError(f"Profiling failed: {profile_output.error}")

            result.blueprint_id = profile_output.output.get("blueprint_id", "")

            # ---- Stage 3: Generate Synthetic Environment ---- #
            update_status(PipelineStatus.GENERATING, "Generating synthetic Delta Tables")

            target_schema = f"seg_{workspace_name}"
            generate_output = self._run_notebook(
                "02_generate_synthetic",
                {
                    "blueprint_id": result.blueprint_id,
                    "catalog": self.config.catalog,
                    "target_schema": target_schema,
                    "scale_factor": str(scale_factor),
                },
            )
            result.stages.append(generate_output)
            if generate_output.status == "failed":
                raise RuntimeError(f"Generation failed: {generate_output.error}")

            result.workspace_id = f"{self.config.catalog}.{target_schema}"
            result.tables_generated = generate_output.output.get("tables_generated", 0)

            # ---- Stage 4: Conform to Medallion ---- #
            update_status(PipelineStatus.CONFORMING, "Running Bronze → Silver → Gold pipeline")

            conform_output = self._run_notebook(
                "03_conform_medallion",
                {
                    "blueprint_id": result.blueprint_id,
                    "catalog": self.config.catalog,
                    "source_schema": target_schema,
                },
            )
            result.stages.append(conform_output)
            if conform_output.status == "failed":
                raise RuntimeError(f"Conformance failed: {conform_output.error}")

            # ---- Stage 5: Validate ---- #
            update_status(PipelineStatus.VALIDATING, "Running validation and confidence scoring")

            validate_output = self._run_notebook(
                "04_validate_workspace",
                {
                    "blueprint_id": result.blueprint_id,
                    "workspace_id": result.workspace_id,
                    "catalog": self.config.catalog,
                    "gold_schema": self.config.gold_schema,
                    "scale_factor": str(scale_factor),
                },
            )
            result.stages.append(validate_output)
            if validate_output.status == "failed":
                raise RuntimeError(f"Validation failed: {validate_output.error}")

            result.confidence_score = validate_output.output.get("score")
            result.confidence_level = validate_output.output.get("level", "")

            # ---- Complete ---- #
            result.total_duration_seconds = time.time() - start_time
            update_status(PipelineStatus.COMPLETE, f"Done in {result.total_duration_seconds:.0f}s")

        except Exception as e:
            result.status = PipelineStatus.FAILED
            result.error = str(e)
            result.total_duration_seconds = time.time() - start_time
            logger.error(f"Pipeline failed: {e}")

        return result

    def run_from_blueprint(
        self,
        blueprint: dict,
        scale_factor: float = 1.0,
        workspace_name: str = "loader_default",
        on_status_change: Optional[Callable[[PipelineStatus, str], None]] = None,
    ) -> PipelineResult:
        """Execute the pipeline from a pre-assembled blueprint — no live DB needed.

        Skips Connect + Profile stages entirely. Runs:
          Save Blueprint → Generate → Conform → Validate

        Args:
            blueprint: Blueprint dict (from BlueprintAssembler.assemble()).
            scale_factor: Data scale multiplier (1.0 = use row_count as-is).
            workspace_name: Name for the synthetic workspace.
            on_status_change: Optional callback for status updates.

        Returns:
            PipelineResult with full execution details.
        """
        start_time = time.time()
        result = PipelineResult(status=PipelineStatus.PENDING)
        result.blueprint_id = blueprint.get("blueprint_id", "")

        def update_status(status: PipelineStatus, message: str = ""):
            result.status = status
            logger.info(f"Pipeline [{status.value}]: {message}")
            if on_status_change:
                on_status_change(status, message)

        try:
            # ---- Stage 0: Save Blueprint to Delta ---- #
            update_status(PipelineStatus.PROFILING, "Saving blueprint to Delta Table")
            stage_start = time.time()

            blueprint_json = json.dumps(blueprint)
            save_output = self._run_notebook(
                "00_save_blueprint",
                {
                    "blueprint_json": blueprint_json,
                    "catalog": self.config.catalog,
                    "schema": self.config.schema,
                },
            )
            result.stages.append(save_output)
            if save_output.status == "failed":
                raise RuntimeError(f"Blueprint save failed: {save_output.error}")

            # ---- Stage 3: Generate Synthetic Environment ---- #
            update_status(PipelineStatus.GENERATING, "Generating synthetic Delta Tables")

            target_schema = f"seg_{workspace_name}"
            generate_output = self._run_notebook(
                "02_generate_synthetic",
                {
                    "blueprint_id": result.blueprint_id,
                    "catalog": self.config.catalog,
                    "target_schema": target_schema,
                    "scale_factor": str(scale_factor),
                },
            )
            result.stages.append(generate_output)
            if generate_output.status == "failed":
                raise RuntimeError(f"Generation failed: {generate_output.error}")

            result.workspace_id = f"{self.config.catalog}.{target_schema}"
            result.tables_generated = generate_output.output.get("tables_generated", 0)

            # ---- Stage 4: Conform to Medallion ---- #
            update_status(PipelineStatus.CONFORMING, "Running Bronze → Silver → Gold pipeline")

            conform_output = self._run_notebook(
                "03_conform_medallion",
                {
                    "blueprint_id": result.blueprint_id,
                    "catalog": self.config.catalog,
                    "source_schema": target_schema,
                },
            )
            result.stages.append(conform_output)
            if conform_output.status == "failed":
                raise RuntimeError(f"Conformance failed: {conform_output.error}")

            # ---- Stage 5: Validate ---- #
            update_status(PipelineStatus.VALIDATING, "Running validation and confidence scoring")

            validate_output = self._run_notebook(
                "04_validate_workspace",
                {
                    "blueprint_id": result.blueprint_id,
                    "workspace_id": result.workspace_id,
                    "catalog": self.config.catalog,
                    "gold_schema": self.config.gold_schema,
                    "scale_factor": str(scale_factor),
                },
            )
            result.stages.append(validate_output)
            if validate_output.status == "failed":
                raise RuntimeError(f"Validation failed: {validate_output.error}")

            result.confidence_score = validate_output.output.get("score")
            result.confidence_level = validate_output.output.get("level", "")

            # ---- Complete ---- #
            result.total_duration_seconds = time.time() - start_time
            update_status(PipelineStatus.COMPLETE, f"Done in {result.total_duration_seconds:.0f}s")

        except Exception as e:
            result.status = PipelineStatus.FAILED
            result.error = str(e)
            result.total_duration_seconds = time.time() - start_time
            logger.error(f"Pipeline (from blueprint) failed: {e}")

        return result

    def run_single_stage(
        self, stage: str, parameters: dict[str, str]
    ) -> StageResult:
        """Run a single pipeline stage by name.

        Useful for the Genie interface when a user asks to run
        just one step (e.g., 'profile the oracle database').
        """
        notebook_map = {
            "save_blueprint": "00_save_blueprint",
            "profile": "01_profile_source",
            "generate": "02_generate_synthetic",
            "conform": "03_conform_medallion",
            "validate": "04_validate_workspace",
            "orchestrate": "05_orchestrate_parallel",
        }
        notebook = notebook_map.get(stage)
        if not notebook:
            return StageResult(stage=stage, status="failed", error=f"Unknown stage: {stage}")
        return self._run_notebook(notebook, parameters)

    def _run_notebook(
        self, notebook_name: str, parameters: dict[str, str]
    ) -> StageResult:
        """Submit a notebook job, wait for completion, and return results."""
        stage_start = time.time()
        notebook_path = f"{self.config.notebook_base_path}/{notebook_name}"

        logger.info(f"Submitting notebook: {notebook_path}")

        try:
            # Submit the notebook run
            run = self.client.jobs.submit(
                run_name=f"SEG - {notebook_name}",
                tasks=[
                    SubmitTask(
                        task_key=notebook_name.replace("_", "-"),
                        existing_cluster_id=self.config.cluster_id,
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters=parameters,
                        ),
                    )
                ],
            )
            run_id = run.run_id

            # Poll for completion
            output = self._wait_for_run(run_id)
            duration = time.time() - stage_start

            return StageResult(
                stage=notebook_name,
                status="success",
                run_id=run_id,
                duration_seconds=duration,
                output=output,
            )

        except Exception as e:
            duration = time.time() - stage_start
            logger.error(f"Notebook {notebook_name} failed: {e}")
            return StageResult(
                stage=notebook_name,
                status="failed",
                duration_seconds=duration,
                error=str(e),
            )

    def _wait_for_run(self, run_id: int, poll_interval: float = 10.0) -> dict:
        """Poll a Databricks run until it reaches a terminal state."""
        while True:
            run = self.client.jobs.get_run(run_id)
            state = run.state

            if state and state.life_cycle_state in self.TERMINAL_STATES:
                if state.result_state == RunResultState.SUCCESS:
                    return self._get_run_output(run_id)
                else:
                    error = state.state_message or f"Run failed with state: {state.result_state}"
                    raise RuntimeError(error)

            logger.debug(f"Run {run_id}: {state.life_cycle_state if state else 'unknown'}")
            time.sleep(poll_interval)

    def _get_run_output(self, run_id: int) -> dict:
        """Extract notebook output from a completed run."""
        try:
            output = self.client.jobs.get_run_output(run_id)
            if output.notebook_output and output.notebook_output.result:
                return json.loads(output.notebook_output.result)
        except (json.JSONDecodeError, AttributeError):
            pass
        return {}

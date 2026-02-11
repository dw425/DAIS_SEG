"""Parallel orchestrator — manages concurrent synthetic environment pipelines.

Coordinates multiple workstreams running in parallel: each gets its own
synthetic workspace with independent Profile → Generate → Conform → Validate
pipelines. Tracks progress, handles failures, and aggregates results.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession

from dais_seg.config import get_config
from dais_seg.generator.synthetic_delta_tables import SyntheticDeltaTableGenerator
from dais_seg.generator.workspace_provisioner import (
    SyntheticWorkspace,
    WorkspaceProvisioner,
    WorkspaceStatus,
)
from dais_seg.conformer.medallion_pipeline import MedallionPipeline
from dais_seg.validator.workspace_validator import WorkspaceValidator, ValidationReport

logger = logging.getLogger(__name__)


@dataclass
class WorkstreamConfig:
    """Configuration for a single parallel workstream."""

    name: str
    tables: list[str]
    scale_factor: float = 1.0
    priority: int = 0


@dataclass
class WorkstreamResult:
    """Result of a single workstream's pipeline execution."""

    name: str
    workspace_id: str
    status: str  # "success", "failed", "partial"
    tables_generated: int = 0
    tables_validated: int = 0
    validation_report: Optional[ValidationReport] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None


@dataclass
class OrchestrationResult:
    """Result of a full parallel orchestration run."""

    total_workstreams: int
    completed: int
    failed: int
    workstream_results: list[WorkstreamResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0


class ParallelOrchestrator:
    """Orchestrates parallel synthetic environment creation and validation.

    Takes a blueprint and a set of workstream configurations, then:
    1. Provisions isolated schemas for each workstream
    2. Generates synthetic Delta Tables in parallel
    3. Runs medallion conformance per workspace
    4. Validates each workspace independently
    5. Aggregates results into a unified report
    """

    def __init__(
        self,
        spark: SparkSession,
        provisioner: Optional[WorkspaceProvisioner] = None,
    ):
        self.spark = spark
        self.config = get_config()
        self.provisioner = provisioner or WorkspaceProvisioner()

    def orchestrate(
        self,
        blueprint: dict,
        workstreams: list[WorkstreamConfig],
        max_parallel: Optional[int] = None,
    ) -> OrchestrationResult:
        """Execute parallel synthetic environment pipelines.

        Args:
            blueprint: Source Blueprint dictionary.
            workstreams: List of workstream configurations.
            max_parallel: Maximum concurrent workstreams. Defaults to config value.
        """
        max_parallel = max_parallel or self.config.max_parallel_workspaces
        max_parallel = min(max_parallel, len(workstreams))
        start_time = time.time()

        logger.info(
            f"Starting orchestration: {len(workstreams)} workstreams, "
            f"max {max_parallel} parallel"
        )

        # Sort by priority
        workstreams_sorted = sorted(workstreams, key=lambda w: w.priority, reverse=True)

        results: list[WorkstreamResult] = []
        completed = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(
                    self._run_workstream, blueprint, ws
                ): ws
                for ws in workstreams_sorted
            }

            for future in as_completed(futures):
                ws = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result.status == "success":
                        completed += 1
                    else:
                        failed += 1
                    logger.info(
                        f"Workstream {ws.name}: {result.status} "
                        f"({result.tables_generated} tables, {result.duration_seconds:.1f}s)"
                    )
                except Exception as e:
                    failed += 1
                    results.append(WorkstreamResult(
                        name=ws.name,
                        workspace_id="",
                        status="failed",
                        error=str(e),
                    ))
                    logger.error(f"Workstream {ws.name} failed: {e}")

        total_duration = time.time() - start_time

        return OrchestrationResult(
            total_workstreams=len(workstreams),
            completed=completed,
            failed=failed,
            workstream_results=results,
            total_duration_seconds=total_duration,
        )

    def _run_workstream(
        self, blueprint: dict, ws: WorkstreamConfig
    ) -> WorkstreamResult:
        """Execute a single workstream: provision → generate → conform → validate."""
        start_time = time.time()
        workspace = None

        try:
            # 1. Provision isolated schema
            blueprint_id = blueprint.get("blueprint_id", "unknown")
            workspace = self.provisioner.provision(ws.name, blueprint_id)
            workspace_id = workspace.workspace_id

            # 2. Generate synthetic Delta Tables
            self.provisioner.update_status(workspace_id, WorkspaceStatus.GENERATING)
            generator = SyntheticDeltaTableGenerator(
                self.spark,
                target_catalog=workspace.catalog,
                target_schema=workspace.schema,
            )
            generated = generator.generate_from_blueprint(
                blueprint, scale_factor=ws.scale_factor, tables=ws.tables or None
            )

            # 3. Conform through medallion pipeline
            pipeline = MedallionPipeline(
                self.spark,
                source_catalog=workspace.catalog,
                source_schema=workspace.schema,
                target_catalog=workspace.catalog,
            )
            pipeline_result = pipeline.run(tables=list(generated.keys()), blueprint=blueprint)

            # 4. Validate
            self.provisioner.update_status(workspace_id, WorkspaceStatus.VALIDATING)
            validator = WorkspaceValidator(self.spark)
            report = validator.validate(
                workspace_id=workspace_id,
                blueprint=blueprint,
                target_catalog=workspace.catalog,
                target_schema=self.config.gold_schema,
                quality_violations=pipeline_result.quality_violations,
                scale_factor=ws.scale_factor,
            )

            # 5. Update status
            self.provisioner.update_status(
                workspace_id,
                WorkspaceStatus.COMPLETE,
                tables_generated=len(generated),
                validation_score=report.confidence.overall_score,
            )

            duration = time.time() - start_time
            return WorkstreamResult(
                name=ws.name,
                workspace_id=workspace_id,
                status="success",
                tables_generated=len(generated),
                tables_validated=report.validated_tables,
                validation_report=report,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            if workspace:
                self.provisioner.update_status(
                    workspace.workspace_id, WorkspaceStatus.FAILED
                )
            return WorkstreamResult(
                name=ws.name,
                workspace_id=workspace.workspace_id if workspace else "",
                status="failed",
                duration_seconds=duration,
                error=str(e),
            )

    def teardown(self, orchestration_result: OrchestrationResult) -> None:
        """Deprovision all workspaces from an orchestration run."""
        for ws_result in orchestration_result.workstream_results:
            if ws_result.workspace_id:
                try:
                    self.provisioner.deprovision(ws_result.workspace_id)
                except Exception as e:
                    logger.error(
                        f"Failed to teardown {ws_result.workspace_id}: {e}"
                    )

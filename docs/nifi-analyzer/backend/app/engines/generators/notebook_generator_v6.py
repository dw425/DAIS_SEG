"""V6 Notebook Generator -- Produces runnable Databricks notebooks.

Unlike the V5 generator which produces independent code cells, V6:
1. Wires DataFrame chains end-to-end (pipeline_wirer)
2. Generates terminal writes for all sinks (sink_generator)
3. Injects secrets-backed configuration (config_generator)
4. Orders cells by topological sort of the DAG
5. Handles fan-out, fan-in, relationship routing
6. Includes 6-pass deep analysis as documentation cells
"""

from __future__ import annotations

import ast
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.engines.analyzers.deep_analysis_orchestrator import (
    DeepAnalysisResult,
    run_deep_analysis,
)
from app.engines.generators.code_scrubber import scrub_code
from app.engines.generators.config_generator import (
    ConfigCells,
    generate_config_cells,
)
from app.engines.generators.pipeline_wirer import (
    WiredCell,
    WiringPlan,
    build_wiring_plan,
)
from app.engines.generators.sink_generator import (
    SinkConfig,
    generate_sink_code,
)
from app.models.config import DatabricksConfig
from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    MappingEntry,
    ParseResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class NotebookCellV6:
    """A single cell in a V6-generated Databricks notebook."""

    type: str  # "code" or "markdown"
    source: str
    label: str
    metadata: dict = field(default_factory=dict)


@dataclass
class NotebookResultV6:
    """Complete V6 notebook generation result."""

    cells: list[NotebookCellV6]
    workflow: dict
    deep_analysis: Any | None  # DeepAnalysisResult
    validation_summary: dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_var(name: str) -> str:
    """Convert a name to a safe Python variable identifier."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()


def _safe_key(name: str) -> str:
    """Convert a name to a safe key for workflow task identifiers."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:50].strip("_").lower()


def _avg_confidence(assessment: AssessmentResult) -> float:
    """Compute the average mapping confidence across all processors."""
    if not assessment.mappings:
        return 0.0
    return sum(m.confidence for m in assessment.mappings) / len(assessment.mappings)


def _simple_type(proc_type: str) -> str:
    """Extract simple class name from a possibly fully-qualified NiFi type."""
    return proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type


def _count_by_role(assessment: AssessmentResult) -> dict[str, int]:
    """Count processors by their assigned role."""
    counts: dict[str, int] = {}
    for m in assessment.mappings:
        role = m.role or "utility"
        counts[role] = counts.get(role, 0) + 1
    return counts


def _is_sink_processor(mapping: MappingEntry) -> bool:
    """Determine if a mapping entry is a sink processor."""
    simple = _simple_type(mapping.type)
    sink_pattern = re.compile(
        r"(Put|Publish|Send|Insert|Post|Store|Write)",
        re.IGNORECASE,
    )
    return bool(sink_pattern.search(simple)) or mapping.role == "sink"


# ---------------------------------------------------------------------------
# Title cell
# ---------------------------------------------------------------------------

def _build_title_cell(
    parsed: ParseResult,
    assessment: AssessmentResult,
) -> NotebookCellV6:
    """Build the notebook title markdown cell."""
    flow_name = parsed.metadata.get("source_file", "Unknown Flow")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    role_counts = _count_by_role(assessment)
    role_summary = ", ".join(
        f"{role}: {count}" for role, count in sorted(role_counts.items())
    )

    source = (
        f"# Migration: {parsed.platform.upper()} to Databricks\n"
        f"\n"
        f"**Source**: `{flow_name}`\n"
        f"**Platform**: {parsed.platform}\n"
        f"**Processors**: {len(parsed.processors)} "
        f"({role_summary})\n"
        f"**Connections**: {len(parsed.connections)}\n"
        f"**Generated**: {timestamp}\n"
        f"**Confidence**: {_avg_confidence(assessment):.0%}\n"
        f"**Generator**: V6 (wired pipeline)"
    )

    return NotebookCellV6(
        type="markdown",
        source=source,
        label="title",
        metadata={"v6_section": "header"},
    )


# ---------------------------------------------------------------------------
# Analysis summary cell
# ---------------------------------------------------------------------------

def _build_analysis_summary_cell(
    deep_analysis: DeepAnalysisResult,
) -> NotebookCellV6:
    """Build the deep analysis summary as a markdown documentation cell."""
    func = deep_analysis.functional
    proc_report = deep_analysis.processors
    workflow = deep_analysis.workflow
    line_by_line = deep_analysis.line_by_line

    lines: list[str] = [
        "## Flow Analysis Summary",
        "",
        f"**Purpose**: {func.flow_purpose}",
        f"**Pipeline Pattern**: {func.pipeline_pattern}",
        f"**Complexity**: {func.estimated_complexity}",
        f"**SLA Profile**: {func.sla_profile}",
        "",
    ]

    # Data domains
    if func.data_domains:
        lines.append(f"**Data Domains**: {', '.join(func.data_domains)}")
        lines.append("")

    # Functional zones
    if func.functional_zones:
        lines.append("### Functional Zones")
        for zone in func.functional_zones:
            if isinstance(zone, dict):
                lines.append(f"- **{zone.get('name', 'unknown')}**: {zone.get('description', '')}")
            else:
                lines.append(f"- {zone}")
        lines.append("")

    # Processor breakdown
    lines.append("### Processor Breakdown")
    lines.append(f"- **Total**: {proc_report.total_processors}")
    if proc_report.by_role:
        role_parts = [f"{role}: {count}" for role, count in sorted(proc_report.by_role.items())]
        lines.append(f"- **By Role**: {', '.join(role_parts)}")
    if proc_report.by_conversion_complexity:
        cx_parts = [f"{cx}: {count}" for cx, count in sorted(proc_report.by_conversion_complexity.items())]
        lines.append(f"- **Conversion Complexity**: {', '.join(cx_parts)}")
    if proc_report.unknown_processors:
        lines.append(f"- **Unknown Processors**: {len(proc_report.unknown_processors)}")
    lines.append("")

    # Workflow
    lines.append("### Workflow Topology")
    lines.append(
        f"- **Execution Phases**: {workflow.total_execution_phases} | "
        f"**Critical Path**: {workflow.critical_path_length} | "
        f"**Parallelism**: {workflow.parallelism_factor:.1f}x"
    )
    if workflow.cycles_detected:
        lines.append(f"- **Cycles**: {len(workflow.cycles_detected)} detected")
    if workflow.synchronization_points:
        lines.append(f"- **Sync Points**: {len(workflow.synchronization_points)}")
    lines.append("")

    # Line-by-line
    lines.append("### Code Translation Metrics")
    lines.append(f"- **Properties Analyzed**: {line_by_line.total_lines_analyzed}")
    lines.append(f"- **NEL Expressions**: {line_by_line.nel_expressions_count}")
    lines.append(f"- **SQL Statements**: {line_by_line.sql_statements_count}")
    lines.append(f"- **Script Blocks**: {line_by_line.script_blocks_count}")
    lines.append(f"- **Conversion Confidence**: {line_by_line.overall_conversion_confidence:.1%}")
    lines.append("")
    lines.append(f"_Analysis completed in {deep_analysis.duration_ms:.0f} ms_")

    return NotebookCellV6(
        type="markdown",
        source="\n".join(lines),
        label="analysis_summary",
        metadata={"v6_section": "documentation"},
    )


# ---------------------------------------------------------------------------
# Setup cell
# ---------------------------------------------------------------------------

def _build_setup_cell(
    parsed: ParseResult,
    config: dict,
) -> NotebookCellV6:
    """Build the schema/volume initialization code cell."""
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "default")

    code_lines = [
        "# -- Setup: Create schema and checkpoint locations --",
        "",
        "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}\")",
        "",
        "# Create checkpoint volume for streaming writes",
        "spark.sql(f\"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints\")",
        "",
        "# Create landing volume for file-based ingestion",
        "spark.sql(f\"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing\")",
        "",
        "# Create staging volume for intermediate data",
        "spark.sql(f\"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.staging\")",
        "",
        'print(f"[SETUP] Schema {CATALOG}.{SCHEMA} initialized with volumes: '
        'checkpoints, landing, staging")',
    ]

    return NotebookCellV6(
        type="code",
        source="\n".join(code_lines),
        label="setup",
        metadata={"v6_section": "infrastructure"},
    )


# ---------------------------------------------------------------------------
# Teardown cell
# ---------------------------------------------------------------------------

def _build_teardown_cell(
    parsed: ParseResult,
    assessment: AssessmentResult,
    wiring_plan: WiringPlan,
    sink_configs: list[SinkConfig],
) -> NotebookCellV6:
    """Build the teardown cell with summary statistics and cleanup."""
    num_sources = sum(1 for m in assessment.mappings if m.role == "source")
    num_transforms = sum(1 for m in assessment.mappings if m.role in ("transform", "route"))
    num_sinks = len(sink_configs)
    streaming_writes = sum(1 for s in sink_configs if s.is_streaming)

    code_lines = [
        "# -- Teardown: Cleanup and Summary --",
        "",
        "# Clean up temporary views",
        "for _table_info in spark.catalog.listTables():",
        "    if _table_info.name.startswith(\"tmp_\") or _table_info.name.startswith(\"_v6_\"):",
        "        try:",
        "            spark.catalog.dropTempView(_table_info.name)",
        "        except Exception:",
        "            pass",
        "",
    ]

    # Wait for streaming queries if any
    if streaming_writes > 0:
        code_lines.extend([
            "# Wait for streaming queries to complete (availableNow=True finishes quickly)",
            "import time",
            "_active_queries = spark.streams.active",
            "if _active_queries:",
            f'    print(f"[TEARDOWN] Waiting for {{len(_active_queries)}} streaming query(ies)...")',
            "    for _q in _active_queries:",
            "        try:",
            "            _q.awaitTermination(timeout=300)",
            "        except Exception as _e:",
            '            print(f"[WARN] Streaming query wait error: {_e}")',
            "",
        ])

    code_lines.extend([
        "# Display pipeline summary",
        'print("=" * 60)',
        f'print("[DONE] Pipeline execution complete")',
        'print("=" * 60)',
        f'print(f"  Sources:    {num_sources}")',
        f'print(f"  Transforms: {num_transforms}")',
        f'print(f"  Sinks:      {num_sinks}")',
    ])

    if streaming_writes > 0:
        code_lines.append(
            f'print(f"  Streaming:  {streaming_writes} write(s)")'
        )

    if wiring_plan.warnings:
        code_lines.append(f'print(f"  Warnings:   {len(wiring_plan.warnings)}")')

    code_lines.append('print("=" * 60)')

    return NotebookCellV6(
        type="code",
        source="\n".join(code_lines),
        label="teardown",
        metadata={"v6_section": "teardown"},
    )


# ---------------------------------------------------------------------------
# Sink cell generation
# ---------------------------------------------------------------------------

def _build_sink_cells(
    parsed: ParseResult,
    assessment: AssessmentResult,
    wiring_plan: WiringPlan,
    config: dict,
    execution_mode: str,
) -> tuple[list[NotebookCellV6], list[SinkConfig]]:
    """Generate write cells for all sink processors.

    Returns both the notebook cells and the SinkConfig list for downstream use.
    """
    cells: list[NotebookCellV6] = []
    sink_configs: list[SinkConfig] = []

    # Track which processors already have wired sink code from the wiring plan
    wired_sinks: set[str] = set()
    for wired_cell in wiring_plan.cells:
        if wired_cell.role == "sink" and wired_cell.cell_type == "code":
            wired_sinks.add(wired_cell.processor_name)

    # Build processor property lookup
    proc_props: dict[str, dict] = {}
    for proc in parsed.processors:
        proc_props[proc.name] = proc.properties

    for mapping in assessment.mappings:
        if not _is_sink_processor(mapping):
            continue

        proc_name = mapping.name
        safe_name = _safe_var(proc_name)

        # Determine the upstream DataFrame from the wiring plan
        upstream_df = wiring_plan.df_registry.get(proc_name, f"df_{safe_name}")

        # If the wirer already produced code for this sink, check if we need
        # a dedicated write cell (the wirer may have produced the transform
        # portion but not the actual .write call).
        properties = proc_props.get(proc_name, {})

        sink_cfg = generate_sink_code(
            processor_name=proc_name,
            processor_type=mapping.type,
            properties=properties,
            upstream_df_name=upstream_df,
            execution_mode=execution_mode,
            config=config,
        )
        sink_configs.append(sink_cfg)

        # If the wiring plan already emitted code for this sink, we append
        # the dedicated write code as a SEPARATE cell so the sink logic
        # (the .write/.writeStream) is always explicit.
        if proc_name in wired_sinks:
            # The wiring plan cell contains the transform/passthrough code.
            # We add just the write portion.
            pass

        # Markdown header for the sink
        cells.append(NotebookCellV6(
            type="markdown",
            source=(
                f"### Write: {proc_name}\n"
                f"**Sink Type**: `{sink_cfg.sink_type}` | "
                f"**Streaming**: {'Yes' if sink_cfg.is_streaming else 'No'}"
            ),
            label=f"sink_header_{safe_name}",
            metadata={
                "v6_section": "sink",
                "processor_name": proc_name,
                "sink_type": sink_cfg.sink_type,
            },
        ))

        # Code cell with the write logic
        cells.append(NotebookCellV6(
            type="code",
            source=sink_cfg.write_code,
            label=f"sink_{safe_name}",
            metadata={
                "v6_section": "sink",
                "processor_name": proc_name,
                "sink_type": sink_cfg.sink_type,
                "is_streaming": sink_cfg.is_streaming,
                "checkpoint_path": sink_cfg.checkpoint_path,
            },
        ))

    return cells, sink_configs


# ---------------------------------------------------------------------------
# Workflow definition builder
# ---------------------------------------------------------------------------

def _build_workflow_definition(
    parsed: ParseResult,
    assessment: AssessmentResult,
    wiring_plan: WiringPlan,
    config: dict,
) -> dict:
    """Build a Databricks Asset Bundle workflow (job) definition.

    Uses the topological order from the wiring plan to define DAG-based
    task dependencies instead of a linear chain.
    """
    flow_name = _safe_key(
        parsed.metadata.get("source_file", "migration")
    )
    cloud_provider = config.get("cloud_provider", "aws")
    runtime_version = config.get("runtime_version", "15.4")
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "default")

    # Build task-key lookup from topological order
    task_keys: dict[str, str] = {}
    for idx, proc_name in enumerate(wiring_plan.topological_order):
        task_keys[proc_name] = f"step_{idx + 1}_{_safe_key(proc_name)}"

    # Build upstream map from connections
    upstream_map: dict[str, set[str]] = {name: set() for name in task_keys}
    for conn in parsed.connections:
        src = conn.source_name
        dst = conn.destination_name
        if src in task_keys and dst in task_keys:
            upstream_map[dst].add(src)

    # Determine if we should cluster tasks (reduce task count for small flows)
    use_task_clusters = len(wiring_plan.topological_order) > 10
    cluster_map: dict[str, str] = {}  # processor -> cluster_id

    if use_task_clusters:
        for wired_cell in wiring_plan.cells:
            if wired_cell.cluster_id and wired_cell.cell_type == "code":
                cluster_map[wired_cell.processor_name] = wired_cell.cluster_id

    # Build tasks
    tasks: list[dict] = []
    emitted_clusters: set[str] = set()

    for proc_name in wiring_plan.topological_order:
        # If this processor is part of a cluster and we already emitted it, skip
        cluster_id = cluster_map.get(proc_name)
        if cluster_id and cluster_id in emitted_clusters:
            continue

        task_key = task_keys[proc_name]
        if cluster_id:
            task_key = f"cluster_{_safe_key(cluster_id)}"
            emitted_clusters.add(cluster_id)

        task: dict = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"/Workspace/migrations/{flow_name}_v6",
                "base_parameters": {
                    "catalog": catalog,
                    "schema": schema,
                },
            },
            "job_cluster_key": "migration_cluster",
            "max_retries": 2,
            "timeout_seconds": 3600,
        }

        # Compute dependencies
        deps = upstream_map.get(proc_name, set())
        if cluster_id:
            # For clustered tasks, gather all upstream deps from all cluster members
            for wc in wiring_plan.cells:
                if wc.cluster_id == cluster_id and wc.cell_type == "code":
                    deps = deps.union(upstream_map.get(wc.processor_name, set()))

        dep_task_keys: list[str] = []
        for dep_name in sorted(deps):
            dep_cluster = cluster_map.get(dep_name)
            if dep_cluster and dep_cluster != cluster_id:
                dep_key = f"cluster_{_safe_key(dep_cluster)}"
            elif dep_name in task_keys:
                dep_key = task_keys[dep_name]
            else:
                continue
            if dep_key != task_key and dep_key not in dep_task_keys:
                dep_task_keys.append(dep_key)

        if dep_task_keys:
            task["depends_on"] = [{"task_key": dk} for dk in sorted(dep_task_keys)]

        tasks.append(task)

    # Cluster configuration
    node_type = {
        "aws": "i3.xlarge",
        "azure": "Standard_DS3_v2",
        "gcp": "n1-standard-4",
    }.get(cloud_provider, "i3.xlarge")

    # Determine autoscaling from processor count
    num_processors = len(wiring_plan.topological_order)
    max_workers = min(max(2, num_processors // 5), 16)

    cluster_sizing: dict
    if max_workers > 2:
        cluster_sizing = {
            "autoscale": {
                "min_workers": 2,
                "max_workers": max_workers,
            },
        }
    else:
        cluster_sizing = {"num_workers": 2}

    workflow = {
        "name": f"migration_{flow_name}_v6",
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "migration_cluster",
                "new_cluster": {
                    "spark_version": f"{runtime_version}.x-scala2.12",
                    **cluster_sizing,
                    "node_type_id": node_type,
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                    },
                    "custom_tags": {
                        "project": "nifi-migration-v6",
                        "source_platform": parsed.platform,
                    },
                },
            },
        ],
        "schedule": None,
        "email_notifications": {"on_failure": []},
        "tags": {
            "generator": "v6",
            "source_platform": parsed.platform,
            "processor_count": str(num_processors),
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        },
    }

    # Derive schedule from NiFi scheduling properties
    schedule = _derive_schedule(parsed)
    if schedule:
        workflow["schedule"] = schedule

    return workflow


def _derive_schedule(parsed: ParseResult) -> dict | None:
    """Derive job schedule from NiFi processor scheduling properties."""
    for proc in parsed.processors:
        if not proc.scheduling:
            continue
        strategy = (
            proc.scheduling.get("strategy", "")
            or proc.scheduling.get("schedulingStrategy", "")
        ).upper()
        period = (
            proc.scheduling.get("period", "")
            or proc.scheduling.get("schedulingPeriod", "")
        )

        if strategy == "CRON_DRIVEN" and period:
            return {
                "quartz_cron_expression": period,
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED",
            }

        if strategy == "TIMER_DRIVEN" and period:
            cron = _period_to_cron(period)
            if cron:
                return {
                    "quartz_cron_expression": cron,
                    "timezone_id": "UTC",
                    "pause_status": "UNPAUSED",
                }
    return None


def _period_to_cron(period: str) -> str | None:
    """Convert a NiFi scheduling period string to a Quartz cron expression."""
    m = re.match(r"(\d+)\s*(sec|min|hour|day)", period, re.IGNORECASE)
    if not m:
        return None
    val, unit = int(m.group(1)), m.group(2).lower()
    if unit.startswith("sec"):
        minutes = max(1, val // 60)
        return f"0 0/{minutes} * * * ?"
    if unit.startswith("min"):
        return f"0 0/{val} * * * ?"
    if unit.startswith("hour"):
        return f"0 0 0/{val} * * ?"
    if unit.startswith("day"):
        return f"0 0 0 1/{val} * ?"
    return None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_notebook(
    cells: list[NotebookCellV6],
    wiring_plan: WiringPlan,
    sink_configs: list[SinkConfig],
) -> dict:
    """Run quick validation on the generated notebook.

    Checks:
    1. Python syntax validity of all code cells
    2. DataFrame reference resolution (all reads have a preceding write)
    3. All streaming sinks have checkpoint locations
    4. No duplicate cell labels

    Returns a validation summary dict.
    """
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Syntax check all code cells
    syntax_ok = 0
    syntax_fail = 0
    for cell in cells:
        if cell.type != "code":
            continue
        try:
            ast.parse(cell.source)
            syntax_ok += 1
        except SyntaxError as exc:
            syntax_fail += 1
            errors.append(
                f"Syntax error in cell '{cell.label}' "
                f"at line {exc.lineno}: {exc.msg}"
            )

    # 2. DataFrame reference resolution
    # Collect all produced df names from the wiring plan
    produced_dfs: set[str] = set()
    unresolved_refs: list[str] = []

    for wired_cell in wiring_plan.cells:
        if wired_cell.cell_type == "code" and wired_cell.output_df_name:
            produced_dfs.add(wired_cell.output_df_name)

    for wired_cell in wiring_plan.cells:
        if wired_cell.cell_type != "code":
            continue
        for upstream_df in wired_cell.upstream_df_names:
            if upstream_df and upstream_df not in produced_dfs:
                unresolved_refs.append(
                    f"Cell '{wired_cell.processor_name}' references "
                    f"'{upstream_df}' which is not produced by any upstream cell"
                )

    if unresolved_refs:
        for ref in unresolved_refs:
            warnings.append(f"Unresolved DataFrame: {ref}")

    # 3. Streaming checkpoint check
    for sink in sink_configs:
        if sink.is_streaming and not sink.checkpoint_path:
            errors.append(
                f"Streaming sink '{sink.processor_name}' "
                f"has no checkpoint path configured"
            )

    # 4. Duplicate label check
    labels = [c.label for c in cells]
    seen_labels: set[str] = set()
    for label in labels:
        if label in seen_labels:
            warnings.append(f"Duplicate cell label: '{label}'")
        seen_labels.add(label)

    # 5. Wiring plan warnings
    for w in wiring_plan.warnings:
        warnings.append(f"Wiring: {w}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "total_cells": len(cells),
            "code_cells": sum(1 for c in cells if c.type == "code"),
            "markdown_cells": sum(1 for c in cells if c.type == "markdown"),
            "syntax_ok": syntax_ok,
            "syntax_errors": syntax_fail,
            "produced_dataframes": len(produced_dfs),
            "unresolved_references": len(unresolved_refs),
            "sinks": len(sink_configs),
            "streaming_sinks": sum(1 for s in sink_configs if s.is_streaming),
            "fan_out_points": len(wiring_plan.fan_out_points),
            "fan_in_points": len(wiring_plan.fan_in_points),
            "error_branches": len(wiring_plan.error_branches),
        },
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_notebook_v6(
    parsed: ParseResult,
    assessment: AssessmentResult,
    analysis_result: AnalysisResult | None = None,
    config: dict | None = None,
) -> NotebookResultV6:
    """Generate a runnable Databricks notebook from parsed flow and assessment.

    This is the main entry point for V6 notebook generation. It:
    1. Runs 6-pass deep analysis (if not already provided)
    2. Generates config/imports/secrets cells via config_generator
    3. Builds the wiring plan (topological sort + DataFrame chains)
    4. Generates code cells with proper upstream references
    5. Generates sink/write cells for all terminal processors
    6. Adds analysis summary as documentation cells
    7. Runs quick validation on the result

    Args:
        parsed: Normalized parse result from the parser engine.
        assessment: Processor-to-Databricks mapping assessment.
        analysis_result: Optional pre-computed AnalysisResult. If None,
            a minimal AnalysisResult is constructed.
        config: Optional configuration dict with keys:
            catalog, schema, scope, cloud_provider, runtime_version,
            checkpoint_base, volume_path.

    Returns:
        NotebookResultV6 with cells, workflow, deep analysis, and validation.
    """
    t_start = time.monotonic()

    # ---- Defaults ----
    if config is None:
        config = {}
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "default")
    scope = config.get("scope", "nifi_migration")
    checkpoint_base = config.get(
        "checkpoint_base", f"/Volumes/{catalog}/{schema}/checkpoints"
    )
    config.setdefault("catalog", catalog)
    config.setdefault("schema", schema)
    config.setdefault("scope", scope)
    config.setdefault("checkpoint_base", checkpoint_base)
    config.setdefault("cloud_provider", "aws")
    config.setdefault("runtime_version", "15.4")

    if analysis_result is None:
        analysis_result = AnalysisResult()

    logger.info(
        "V6 notebook generation started: %d processors, %d connections",
        len(parsed.processors),
        len(parsed.connections),
    )

    # ---- Step 1: Run 6-pass deep analysis ----
    logger.info("Step 1/7: Running 6-pass deep analysis")
    deep_analysis: DeepAnalysisResult | None = None
    try:
        deep_analysis = run_deep_analysis(parsed, analysis_result)
        logger.info(
            "Deep analysis complete in %.1f ms: %s",
            deep_analysis.duration_ms,
            deep_analysis.functional.pipeline_pattern,
        )
    except Exception as exc:
        logger.exception("Deep analysis failed (continuing without): %s", exc)

    # ---- Step 2: Generate config cells ----
    logger.info("Step 2/7: Generating configuration cells")
    config_cells: ConfigCells | None = None
    try:
        config_cells = generate_config_cells(
            parsed,
            assessment,
            analysis_result,
            deep_analysis=None,  # deep_analysis is a DeepAnalysisResult, not dict
        )
    except Exception as exc:
        logger.exception("Config cell generation failed: %s", exc)

    # ---- Step 3: Build wiring plan ----
    logger.info("Step 3/7: Building wiring plan (topological sort + DataFrame chains)")
    wiring_plan = build_wiring_plan(
        parsed,
        assessment,
        analysis_result,
        deep_analysis=None,
    )
    logger.info(
        "Wiring plan: %d cells, %d topological nodes, streaming=%s",
        len(wiring_plan.cells),
        len(wiring_plan.topological_order),
        wiring_plan.streaming_mode,
    )

    # Determine execution mode
    execution_mode = "streaming" if wiring_plan.streaming_mode else "batch"

    # ---- Step 4: Assemble cells in order ----
    logger.info("Step 4/7: Assembling notebook cells")
    cells: list[NotebookCellV6] = []

    # 4a. Title cell
    cells.append(_build_title_cell(parsed, assessment))

    # 4b. Analysis summary cell (from deep analysis)
    if deep_analysis is not None:
        cells.append(_build_analysis_summary_cell(deep_analysis))

    # 4c. Secrets documentation cell (markdown)
    if config_cells is not None:
        cells.append(NotebookCellV6(
            type="markdown",
            source=config_cells.secrets_doc_cell,
            label="secrets_documentation",
            metadata={"v6_section": "documentation"},
        ))

    # 4d. Imports cell (code)
    if config_cells is not None:
        cells.append(NotebookCellV6(
            type="code",
            source=config_cells.imports_cell,
            label="imports",
            metadata={"v6_section": "imports"},
        ))
    else:
        # Fallback minimal imports
        cells.append(NotebookCellV6(
            type="code",
            source=(
                "# -- Imports --\n"
                "from pyspark.sql import SparkSession\n"
                "from pyspark.sql import functions as F\n"
                "from pyspark.sql.functions import col, lit, when, coalesce, "
                "regexp_replace, from_json, sha2, current_timestamp\n"
                "from pyspark.sql.types import *"
            ),
            label="imports",
            metadata={"v6_section": "imports"},
        ))

    # 4e. Config cell (code)
    if config_cells is not None:
        cells.append(NotebookCellV6(
            type="code",
            source=config_cells.config_cell,
            label="config",
            metadata={"v6_section": "config"},
        ))
    else:
        # Fallback minimal config
        cells.append(NotebookCellV6(
            type="code",
            source=(
                "# -- Configuration --\n"
                "try:\n"
                '    CATALOG = dbutils.widgets.get("catalog")\n'
                "except Exception:\n"
                f'    CATALOG = "{catalog}"\n'
                "try:\n"
                '    SCHEMA = dbutils.widgets.get("schema")\n'
                "except Exception:\n"
                f'    SCHEMA = "{schema}"\n'
                "\n"
                f'SECRETS_SCOPE = "{scope}"\n'
                'CHECKPOINT_BASE = f"/Volumes/{{CATALOG}}/{{SCHEMA}}/checkpoints"\n'
                'VOLUME_BASE = f"/Volumes/{{CATALOG}}/{{SCHEMA}}/landing"\n'
                "\n"
                'spark.sql(f"USE CATALOG {{CATALOG}}")\n'
                'spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}")\n'
                'spark.sql(f"USE SCHEMA {{SCHEMA}}")\n'
                "\n"
                'print(f"[CONFIG] catalog={{CATALOG}}, schema={{SCHEMA}}, '
                'scope={{SECRETS_SCOPE}}")'
            ),
            label="config",
            metadata={"v6_section": "config"},
        ))

    # 4f. Setup cell (code)
    cells.append(_build_setup_cell(parsed, config))

    # 4g. Pipeline header
    cells.append(NotebookCellV6(
        type="markdown",
        source=(
            "## Pipeline Steps\n"
            "\n"
            f"The following {len(wiring_plan.topological_order)} processor(s) "
            f"are wired in topological execution order. Each cell references "
            f"its upstream DataFrame(s) by variable name."
        ),
        label="pipeline_header",
        metadata={"v6_section": "pipeline"},
    ))

    # 4h. Wired pipeline cells (in topological order from wiring plan)
    for wired_cell in wiring_plan.cells:
        # Scrub any hardcoded secrets from the generated code
        source = wired_cell.code
        if wired_cell.cell_type == "code":
            source = scrub_code(source)

        cells.append(NotebookCellV6(
            type=wired_cell.cell_type,
            source=source,
            label=wired_cell.label,
            metadata={
                "v6_section": "pipeline",
                "processor_name": wired_cell.processor_name,
                "processor_type": wired_cell.processor_type,
                "role": wired_cell.role,
                "output_df": wired_cell.output_df_name,
                "upstream_dfs": wired_cell.upstream_df_names,
                "relationship": wired_cell.relationship_from_upstream,
                "is_streaming": wired_cell.is_streaming,
                "cluster_id": wired_cell.cluster_id,
            },
        ))

    # ---- Step 5: Generate sink write cells ----
    logger.info("Step 5/7: Generating sink write cells")
    sink_cells, sink_configs = _build_sink_cells(
        parsed, assessment, wiring_plan, config, execution_mode,
    )

    if sink_cells:
        # Add sinks section header
        cells.append(NotebookCellV6(
            type="markdown",
            source=(
                "## Terminal Writes\n"
                "\n"
                f"The following {len(sink_configs)} sink(s) persist data "
                f"to their target destinations."
            ),
            label="sinks_header",
            metadata={"v6_section": "sinks"},
        ))
        cells.extend(sink_cells)

    # 4i. Teardown cell
    cells.append(_build_teardown_cell(
        parsed, assessment, wiring_plan, sink_configs,
    ))

    # ---- Step 6: Build workflow definition ----
    logger.info("Step 6/7: Building workflow definition")
    workflow = _build_workflow_definition(
        parsed, assessment, wiring_plan, config,
    )

    # ---- Step 7: Validate ----
    logger.info("Step 7/7: Running validation")
    validation_summary = _validate_notebook(cells, wiring_plan, sink_configs)

    elapsed_ms = (time.monotonic() - t_start) * 1000
    logger.info(
        "V6 notebook generation complete in %.1f ms: "
        "%d cells (%d code, %d markdown), %d sinks, valid=%s",
        elapsed_ms,
        validation_summary["stats"]["total_cells"],
        validation_summary["stats"]["code_cells"],
        validation_summary["stats"]["markdown_cells"],
        validation_summary["stats"]["sinks"],
        validation_summary["valid"],
    )

    return NotebookResultV6(
        cells=cells,
        workflow=workflow,
        deep_analysis=deep_analysis,
        validation_summary=validation_summary,
    )


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def export_as_python(result: NotebookResultV6) -> str:
    """Export notebook as a .py file with Databricks cell markers.

    Uses the ``# Databricks notebook source`` header and
    ``# COMMAND ----------`` separators recognized by the Databricks
    Workspace IDE.

    Args:
        result: A NotebookResultV6 from generate_notebook_v6().

    Returns:
        A string containing the full .py notebook content.
    """
    lines: list[str] = ["# Databricks notebook source"]

    for cell in result.cells:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")

        if cell.type == "markdown":
            # Databricks markdown convention: each line prefixed with # MAGIC %md
            # First line gets the %md directive, subsequent lines get # MAGIC
            md_lines = cell.source.split("\n")
            lines.append("# MAGIC %md")
            for md_line in md_lines:
                lines.append(f"# MAGIC {md_line}")
        else:
            # Code cell -- include directly
            lines.append(cell.source)

    lines.append("")
    return "\n".join(lines)


def export_as_ipynb(result: NotebookResultV6) -> dict:
    """Export notebook as Jupyter .ipynb JSON format.

    Produces a valid Jupyter Notebook v4 document that can be imported
    into Databricks or opened in JupyterLab.

    Args:
        result: A NotebookResultV6 from generate_notebook_v6().

    Returns:
        A dict representing the .ipynb JSON structure. Serialize with
        ``json.dumps(result, indent=2)`` to get the file content.
    """
    cells: list[dict] = []

    for idx, cell in enumerate(result.cells):
        cell_type = cell.type
        # Jupyter uses "code" and "markdown" — same as our types
        if cell_type not in ("code", "markdown"):
            cell_type = "code"

        # Jupyter expects source as a list of lines (each ending with \\n)
        source_lines = cell.source.split("\n")
        # Add newlines to all but the last line
        source_with_newlines: list[str] = []
        for i, line in enumerate(source_lines):
            if i < len(source_lines) - 1:
                source_with_newlines.append(line + "\n")
            else:
                source_with_newlines.append(line)

        jupyter_cell: dict = {
            "cell_type": cell_type,
            "source": source_with_newlines,
            "metadata": {
                "label": cell.label,
                **cell.metadata,
            },
        }

        if cell_type == "code":
            jupyter_cell["execution_count"] = None
            jupyter_cell["outputs"] = []
        else:
            # Markdown cells don't have execution_count or outputs
            pass

        # Jupyter 4.5+ uses cell IDs
        jupyter_cell["id"] = f"v6-cell-{idx:04d}"

        cells.append(jupyter_cell)

    # Add validation summary as a final markdown cell
    validation = result.validation_summary
    summary_lines = [
        "---\n",
        "## Generation Report\n",
        "\n",
        f"- **Valid**: {'Yes' if validation.get('valid') else 'No'}\n",
    ]

    stats = validation.get("stats", {})
    if stats:
        summary_lines.extend([
            f"- **Total Cells**: {stats.get('total_cells', 0)}\n",
            f"- **Code Cells**: {stats.get('code_cells', 0)}\n",
            f"- **Markdown Cells**: {stats.get('markdown_cells', 0)}\n",
            f"- **Syntax OK**: {stats.get('syntax_ok', 0)}\n",
            f"- **Syntax Errors**: {stats.get('syntax_errors', 0)}\n",
            f"- **DataFrames Produced**: {stats.get('produced_dataframes', 0)}\n",
            f"- **Sinks**: {stats.get('sinks', 0)}\n",
            f"- **Fan-out Points**: {stats.get('fan_out_points', 0)}\n",
            f"- **Fan-in Points**: {stats.get('fan_in_points', 0)}\n",
        ])

    errs = validation.get("errors", [])
    if errs:
        summary_lines.append("\n")
        summary_lines.append("### Errors\n")
        for e in errs:
            summary_lines.append(f"- {e}\n")

    warns = validation.get("warnings", [])
    if warns:
        summary_lines.append("\n")
        summary_lines.append("### Warnings\n")
        for w in warns:
            summary_lines.append(f"- {w}\n")

    cells.append({
        "cell_type": "markdown",
        "source": summary_lines,
        "metadata": {"label": "generation_report", "v6_section": "report"},
        "id": f"v6-cell-{len(result.cells):04d}",
    })

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0",
                "mimetype": "text/x-python",
                "file_extension": ".py",
            },
            "application/vnd.databricks.v1+notebook": {
                "language": "python",
                "notebookName": "NiFi Migration (V6)",
            },
            "v6_metadata": {
                "generator": "notebook_generator_v6",
                "generated": datetime.now(timezone.utc).isoformat(),
                "workflow": result.workflow.get("name", ""),
                "task_count": len(result.workflow.get("tasks", [])),
                "validation": validation,
            },
        },
        "cells": cells,
    }

    return notebook


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------

__all__ = [
    "NotebookCellV6",
    "NotebookResultV6",
    "generate_notebook_v6",
    "export_as_python",
    "export_as_ipynb",
]

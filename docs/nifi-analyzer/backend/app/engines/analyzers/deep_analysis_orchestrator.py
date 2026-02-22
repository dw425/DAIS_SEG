"""Orchestrator for 7-pass deep analysis engine.

Runs all seven analysis passes in dependency order:
  Passes 1-2 (functional, processor) run first -- no cross-dependencies.
  Passes 3-5 (workflow, upstream, downstream) use the processor report.
  Pass 6 (line-by-line) runs last -- most expensive.
  Pass 7 (lint) runs independently -- configurable checkstyle rules.

Merges everything into a single DeepAnalysisResult with timing.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from app.models.pipeline import AnalysisResult, ParseResult

from app.engines.analyzers.deep_functional_analyzer import (
    FunctionalReport,
    analyze_functional,
)
from app.engines.analyzers.deep_processor_analyzer import (
    ProcessorReport,
    analyze_processors,
)
from app.engines.analyzers.deep_workflow_analyzer import (
    WorkflowReport,
    analyze_workflow,
)
from app.engines.analyzers.deep_upstream_analyzer import (
    UpstreamReport,
    analyze_upstream,
)
from app.engines.analyzers.deep_downstream_analyzer import (
    DownstreamReport,
    analyze_downstream,
)
from app.engines.analyzers.deep_line_by_line_analyzer import (
    LineByLineReport,
    analyze_line_by_line,
)
from app.engines.analyzers.flow_linter import (
    LintReport,
    lint_flow,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class DeepAnalysisResult:
    functional: FunctionalReport
    processors: ProcessorReport
    workflow: WorkflowReport
    upstream: UpstreamReport
    downstream: DownstreamReport
    line_by_line: LineByLineReport
    lint: LintReport | None
    duration_ms: float
    summary: str


# ---------------------------------------------------------------------------
# Safe runner (mirrors existing _safe_run pattern)
# ---------------------------------------------------------------------------

_SENTINEL_FUNCTIONAL = FunctionalReport(
    flow_purpose="Analysis failed",
    functional_zones=[],
    data_domains=[],
    pipeline_pattern="unknown",
    estimated_complexity="unknown",
    sla_profile="unknown",
)

_SENTINEL_PROCESSOR = ProcessorReport(
    total_processors=0,
    by_category={},
    by_role={},
    by_conversion_complexity={},
    unknown_processors=[],
    processors=[],
)

_SENTINEL_WORKFLOW = WorkflowReport(
    execution_phases=[],
    process_groups=[],
    cycles_detected=[],
    synchronization_points=[],
    total_execution_phases=0,
    critical_path_length=0,
    parallelism_factor=0.0,
    topological_order=[],
)

_SENTINEL_UPSTREAM = UpstreamReport(
    data_sources=[],
    attribute_lineage={},
    content_transformations=[],
    external_dependencies=[],
    parameter_injections=[],
)

_SENTINEL_DOWNSTREAM = DownstreamReport(
    data_sinks=[],
    data_flow_map={},
    error_routing={},
    data_volume_estimates={},
)

_SENTINEL_LINE_BY_LINE = LineByLineReport(
    processor_analyses=[],
    total_lines_analyzed=0,
    needs_conversion_count=0,
    nel_expressions_count=0,
    sql_statements_count=0,
    script_blocks_count=0,
    overall_conversion_confidence=0.0,
)

_SENTINEL_LINT = LintReport(
    findings=[],
    rules_checked=0,
    rules_triggered=0,
    summary="Lint analysis failed",
)


def _safe_pass(name: str, func, sentinel, *args, **kwargs):
    """Run an analysis pass safely, returning a sentinel on failure."""
    try:
        t0 = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = (time.monotonic() - t0) * 1000
        logger.info("Deep analysis pass '%s' completed in %.1f ms", name, elapsed)
        return result
    except Exception as exc:
        logger.exception("Deep analysis pass '%s' failed: %s", name, exc)
        return sentinel


# ---------------------------------------------------------------------------
# Summary generation
# ---------------------------------------------------------------------------

def _generate_summary(
    functional: FunctionalReport,
    proc_report: ProcessorReport,
    workflow: WorkflowReport,
    upstream: UpstreamReport,
    downstream: DownstreamReport,
    line_by_line: LineByLineReport,
    lint_report: LintReport | None,
    duration_ms: float,
) -> str:
    """Generate a human-readable summary of the deep analysis."""
    lines: list[str] = []
    lines.append("=== Deep Analysis Summary ===")
    lines.append("")

    # Functional overview
    lines.append(f"Purpose: {functional.flow_purpose}")
    lines.append(f"Pattern: {functional.pipeline_pattern} | "
                 f"Complexity: {functional.estimated_complexity} | "
                 f"SLA: {functional.sla_profile}")
    if functional.data_domains:
        lines.append(f"Data Domains: {', '.join(functional.data_domains)}")
    lines.append("")

    # Processor overview
    lines.append(f"Processors: {proc_report.total_processors} total")
    if proc_report.by_role:
        role_parts = [f"{role}: {count}" for role, count in sorted(proc_report.by_role.items())]
        lines.append(f"  Roles: {', '.join(role_parts)}")
    if proc_report.by_conversion_complexity:
        cx_parts = [f"{cx}: {count}" for cx, count in sorted(proc_report.by_conversion_complexity.items())]
        lines.append(f"  Complexity: {', '.join(cx_parts)}")
    if proc_report.unknown_processors:
        lines.append(f"  Unknown: {len(proc_report.unknown_processors)} processor(s) not in KB")
    lines.append("")

    # Workflow overview
    lines.append(f"Workflow: {workflow.total_execution_phases} phase(s), "
                 f"critical path = {workflow.critical_path_length}, "
                 f"parallelism = {workflow.parallelism_factor:.1f}x")
    if workflow.cycles_detected:
        lines.append(f"  Cycles: {len(workflow.cycles_detected)} detected")
    if workflow.synchronization_points:
        lines.append(f"  Sync Points: {len(workflow.synchronization_points)}")
    lines.append("")

    # Data sources
    lines.append(f"Upstream: {len(upstream.data_sources)} data source(s)")
    for src in upstream.data_sources[:5]:
        lines.append(f"  - {src.processor}: {src.system} ({src.source_type}, {src.format})")
    if len(upstream.data_sources) > 5:
        lines.append(f"  ... and {len(upstream.data_sources) - 5} more")
    lines.append("")

    # Data sinks
    lines.append(f"Downstream: {len(downstream.data_sinks)} data sink(s)")
    for sink in downstream.data_sinks[:5]:
        lines.append(f"  - {sink.processor}: {sink.system} ({sink.sink_type}, {sink.format})")
    if len(downstream.data_sinks) > 5:
        lines.append(f"  ... and {len(downstream.data_sinks) - 5} more")

    # Data flow highlights
    flow_map = downstream.data_flow_map
    if isinstance(flow_map, dict):
        mult = flow_map.get("multiplication_points", [])
        loss = flow_map.get("loss_points", [])
        if mult:
            lines.append(f"  Data Multiplication: {len(mult)} point(s)")
        if loss:
            lines.append(f"  Data Loss Risk: {len(loss)} point(s)")
    lines.append("")

    # Error routing
    error = downstream.error_routing
    if isinstance(error, dict):
        unhandled = error.get("unhandled_failures", [])
        if unhandled:
            lines.append(f"Error Routing: {len(unhandled)} UNHANDLED failure path(s)")
        else:
            handled = error.get("handled_failures", [])
            lines.append(f"Error Routing: All {len(handled)} failure path(s) handled")
    lines.append("")

    # Line-by-line stats
    lines.append(f"Line-by-Line: {line_by_line.total_lines_analyzed} properties analyzed")
    lines.append(f"  Needs Conversion: {line_by_line.needs_conversion_count}")
    lines.append(f"  NEL Expressions: {line_by_line.nel_expressions_count}")
    lines.append(f"  SQL Statements: {line_by_line.sql_statements_count}")
    lines.append(f"  Script Blocks: {line_by_line.script_blocks_count}")
    lines.append(f"  Conversion Confidence: {line_by_line.overall_conversion_confidence:.1%}")
    lines.append("")

    # Lint summary
    if lint_report and lint_report.findings:
        lines.append(f"Flow Lint: {len(lint_report.findings)} finding(s)")
        lines.append(f"  Critical: {lint_report.critical_count}, High: {lint_report.high_count}, "
                     f"Medium: {lint_report.medium_count}, Low: {lint_report.low_count}")
    elif lint_report:
        lines.append("Flow Lint: No issues found")
    lines.append("")

    lines.append(f"Total Duration: {duration_ms:.0f} ms")
    lines.append("=" * 40)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_deep_analysis(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None = None,
) -> DeepAnalysisResult:
    """Run all 7 analysis passes and return unified result.

    Execution order:
      1. Functional analysis (pass 1) -- no dependencies
      2. Processor analysis (pass 2) -- no dependencies
      3. Workflow analysis (pass 3) -- uses analysis_result
      4. Upstream analysis (pass 4) -- uses analysis_result + processor report
      5. Downstream analysis (pass 5) -- uses analysis_result + processor report
      6. Line-by-line analysis (pass 6) -- most expensive, runs last
      7. Flow lint (pass 7) -- configurable checkstyle rules

    Args:
        parsed: Normalized parse result from the parser engine.
        analysis_result: Optional existing AnalysisResult from run_analysis().

    Returns:
        DeepAnalysisResult with all 7 reports, timing, and summary.
    """
    t_start = time.monotonic()

    logger.info(
        "Starting deep analysis: %d processors, %d connections",
        len(parsed.processors), len(parsed.connections),
    )

    # --- Phase A: Independent passes (1 & 2) ---
    functional: FunctionalReport = _safe_pass(
        "functional",
        analyze_functional,
        _SENTINEL_FUNCTIONAL,
        parsed,
        analysis_result,
    )

    proc_report: ProcessorReport = _safe_pass(
        "processor",
        analyze_processors,
        _SENTINEL_PROCESSOR,
        parsed,
    )

    # --- Phase B: Dependent passes (3, 4, 5) — use processor report ---
    workflow: WorkflowReport = _safe_pass(
        "workflow",
        analyze_workflow,
        _SENTINEL_WORKFLOW,
        parsed,
        analysis_result,
    )

    upstream: UpstreamReport = _safe_pass(
        "upstream",
        analyze_upstream,
        _SENTINEL_UPSTREAM,
        parsed,
        analysis_result,
        proc_report,
    )

    downstream: DownstreamReport = _safe_pass(
        "downstream",
        analyze_downstream,
        _SENTINEL_DOWNSTREAM,
        parsed,
        analysis_result,
        proc_report,
    )

    # --- Phase C: Most expensive pass (6) ---
    line_by_line: LineByLineReport = _safe_pass(
        "line_by_line",
        analyze_line_by_line,
        _SENTINEL_LINE_BY_LINE,
        parsed,
    )

    # --- Phase D: Flow lint (pass 7) ---
    lint_report: LintReport = _safe_pass(
        "lint",
        lint_flow,
        _SENTINEL_LINT,
        parsed,
    )

    # --- Merge and summarize ---
    duration_ms = (time.monotonic() - t_start) * 1000

    summary = _generate_summary(
        functional, proc_report, workflow,
        upstream, downstream, line_by_line,
        lint_report, duration_ms,
    )

    result = DeepAnalysisResult(
        functional=functional,
        processors=proc_report,
        workflow=workflow,
        upstream=upstream,
        downstream=downstream,
        line_by_line=line_by_line,
        lint=lint_report,
        duration_ms=round(duration_ms, 1),
        summary=summary,
    )

    logger.info("Deep analysis complete in %.1f ms", duration_ms)
    return result


__all__ = [
    "DeepAnalysisResult",
    "run_deep_analysis",
]

"""Analysis engine — dependency graph, cycles, external systems, security, stages, lineage, stats.

Phase 2 additions: cycle classification, task clustering, backpressure translation.
Phase 3 additions: transaction boundaries, state management, schema evolution,
    execution mode, site-to-site, process groups, attribute translation,
    connection generation, CI/CD generation.
"""

import logging

from app.engines.analyzers.attribute_flow import analyze_attribute_flow
from app.engines.analyzers.backpressure import extract_backpressure_configs
from app.engines.analyzers.cycle_detection import classify_cycles, detect_cycles
from app.engines.analyzers.dependency_graph import build_dependency_graph
from app.engines.analyzers.execution_mode_analyzer import analyze_execution_mode
from app.engines.analyzers.external_systems import detect_external_systems
from app.engines.analyzers.flow_metrics import compute_flow_metrics
from app.engines.analyzers.lineage_tracker import track_lineage
from app.engines.analyzers.process_group_analyzer import analyze_process_groups
from app.engines.analyzers.processor_stats import aggregate_stats
from app.engines.analyzers.schema_analyzer import analyze_schemas
from app.engines.analyzers.security_scanner_v2 import scan_v2 as scan_security
from app.engines.analyzers.site_to_site_analyzer import analyze_site_to_site
from app.engines.analyzers.stage_classifier import classify_stages
from app.engines.analyzers.state_analyzer import analyze_state
from app.engines.analyzers.task_clusterer import cluster_tasks
from app.engines.analyzers.transaction_analyzer import analyze_transactions
from app.engines.generators.attribute_translator import translate_attributes
from app.engines.generators.cicd_generator import generate_cicd
from app.engines.generators.connection_generator import generate_connections
from app.engines.analyzers.sql_pattern_analyzer import analyze_sql_patterns
from app.models.pipeline import AnalysisResult, ParseResult

logger = logging.getLogger(__name__)


def run_analysis(parse_result: ParseResult) -> AnalysisResult:
    """Run all analyzers and return a combined AnalysisResult."""
    graph = build_dependency_graph(parse_result.processors, parse_result.connections)
    cycles = detect_cycles(graph["downstream"])
    ext_systems = detect_external_systems(parse_result.processors)
    security = scan_security(parse_result.processors)
    stages = classify_stages(parse_result.processors)
    metrics = compute_flow_metrics(parse_result, graph)
    _attr_flow = analyze_attribute_flow(parse_result.processors, parse_result.connections)

    # Phase 2: Cycle classification
    cycle_classifications = classify_cycles(
        cycles, parse_result.processors, parse_result.connections
    )

    # Phase 2: Task clustering
    task_clusters = cluster_tasks(parse_result.processors, parse_result.connections)

    # Phase 2: Backpressure translation
    backpressure_configs = extract_backpressure_configs(parse_result.connections)

    # Enrich metrics with lineage and stats
    lineage = track_lineage(parse_result)
    stats = aggregate_stats(parse_result)
    metrics["lineage_depth"] = lineage["maxDepth"]
    metrics["source_count"] = len(lineage["sources"])
    metrics["sink_count"] = len(lineage["sinks"])
    metrics["orphan_count"] = len(lineage["orphans"])
    metrics["critical_path_length"] = len(lineage["criticalPath"])
    metrics["property_coverage_pct"] = stats.get("property_coverage_pct", 0)

    # Convert v2 findings back to simple format for backward compat
    simple_security = [
        {
            "processor": f["processor"],
            "type": f["processor_type"],
            "finding": f["category"],
            "severity": f["severity"],
            "snippet": f["evidence"],
        }
        for f in security
    ]

    # Phase 3: Advanced conversion analyzers
    transaction_analysis = _safe_run("transaction_analyzer", analyze_transactions, parse_result)
    state_analysis = _safe_run("state_analyzer", analyze_state, parse_result)
    schema_analysis = _safe_run("schema_analyzer", analyze_schemas, parse_result)
    execution_mode_analysis = _safe_run("execution_mode_analyzer", analyze_execution_mode, parse_result)
    site_to_site_analysis = _safe_run("site_to_site_analyzer", analyze_site_to_site, parse_result)
    process_group_analysis = _safe_run("process_group_analyzer", analyze_process_groups, parse_result)

    # Phase 3: Advanced generators (run during analysis for availability)
    attribute_translation = _safe_run("attribute_translator", translate_attributes, parse_result)
    connection_generation = _safe_run("connection_generator", generate_connections, parse_result)
    cicd_generation = _safe_run("cicd_generator", generate_cicd, parse_result)

    # SQL pattern analysis with dialect detection and transpilation
    sql_transpilation = _safe_run("sql_pattern_analyzer", lambda pr: analyze_sql_patterns(pr).to_dict(), parse_result)

    return AnalysisResult(
        dependency_graph=graph,
        external_systems=ext_systems,
        cycles=cycles,
        cycle_classifications=cycle_classifications,
        task_clusters=task_clusters,
        backpressure_configs=backpressure_configs,
        flow_metrics=metrics,
        security_findings=simple_security,
        stages=stages,
        transaction_analysis=transaction_analysis,
        state_analysis=state_analysis,
        schema_analysis=schema_analysis,
        execution_mode_analysis=execution_mode_analysis,
        site_to_site_analysis=site_to_site_analysis,
        process_group_analysis=process_group_analysis,
        attribute_translation=attribute_translation,
        connection_generation=connection_generation,
        cicd_generation=cicd_generation,
        sql_transpilation=sql_transpilation,
    )


def _safe_run(name: str, func, *args, **kwargs) -> dict:
    """Run an analyzer safely, returning empty dict on failure for backward compat."""
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        logger.exception("Analyzer '%s' failed — returning error result", name)
        return {"_error": f"Analyzer '{name}' failed: {exc}"}


__all__ = ["run_analysis"]

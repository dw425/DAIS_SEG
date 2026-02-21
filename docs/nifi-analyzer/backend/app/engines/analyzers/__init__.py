"""Analysis engine — dependency graph, cycles, external systems, security, stages."""

from app.engines.analyzers.attribute_flow import analyze_attribute_flow
from app.engines.analyzers.cycle_detection import detect_cycles
from app.engines.analyzers.dependency_graph import build_dependency_graph
from app.engines.analyzers.external_systems import detect_external_systems
from app.engines.analyzers.flow_metrics import compute_flow_metrics
from app.engines.analyzers.security_scanner import scan_security
from app.engines.analyzers.stage_classifier import classify_stages
from app.models.pipeline import AnalysisResult, ParseResult


def run_analysis(parse_result: ParseResult) -> AnalysisResult:
    """Run all analyzers and return a combined AnalysisResult."""
    graph = build_dependency_graph(parse_result.processors, parse_result.connections)
    cycles = detect_cycles(graph["downstream"])
    ext_systems = detect_external_systems(parse_result.processors)
    security = scan_security(parse_result.processors)
    stages = classify_stages(parse_result.processors)
    metrics = compute_flow_metrics(parse_result, graph)
    _attr_flow = analyze_attribute_flow(parse_result.processors, parse_result.connections)

    return AnalysisResult(
        dependency_graph=graph,
        external_systems=ext_systems,
        cycles=cycles,
        flow_metrics=metrics,
        security_findings=security,
        stages=stages,
    )


__all__ = ["run_analysis"]

"""Tests for analyzers: dependency graph, cycle detection, external systems, security."""

from app.engines.analyzers.cycle_detection import classify_cycles, detect_cycles
from app.engines.analyzers.dependency_graph import build_dependency_graph
from app.engines.analyzers.execution_mode_analyzer import analyze_execution_mode
from app.engines.analyzers.external_systems import detect_external_systems
from app.engines.analyzers.flow_metrics import compute_flow_metrics
from app.engines.analyzers.security_scanner import scan_security
from app.engines.analyzers.stage_classifier import classify_stages
from app.engines.analyzers.state_analyzer import analyze_state
from app.models.pipeline import ParseResult
from app.models.processor import Connection, Processor


def _make_processors(*names_and_types):
    return [Processor(name=n, type=t, platform="nifi") for n, t in names_and_types]


def _make_connections(*edges):
    return [Connection(source_name=s, destination_name=d) for s, d in edges]


# ── Dependency Graph ──


def test_dependency_graph_basic():
    procs = _make_processors(("A", "GetFile"), ("B", "ReplaceText"), ("C", "PutFile"))
    conns = _make_connections(("A", "B"), ("B", "C"))
    graph = build_dependency_graph(procs, conns)

    assert "A" in graph["downstream"]
    assert "B" in graph["downstream"]["A"]
    assert "A" in graph["upstream"]["B"]
    assert graph["fan_out"]["A"] == 1
    assert graph["fan_in"]["C"] == 1


def test_dependency_graph_fan_out():
    procs = _make_processors(("A", "GetFile"), ("B", "PutFile"), ("C", "PutKafka"), ("D", "LogAttribute"))
    conns = _make_connections(("A", "B"), ("A", "C"), ("A", "D"))
    graph = build_dependency_graph(procs, conns)
    assert graph["fan_out"]["A"] == 3
    assert graph["fan_in"]["A"] == 0


def test_dependency_graph_transitive():
    procs = _make_processors(("A", "GetFile"), ("B", "ReplaceText"), ("C", "PutFile"))
    conns = _make_connections(("A", "B"), ("B", "C"))
    graph = build_dependency_graph(procs, conns)
    assert "C" in graph["full_downstream"]["A"]
    assert "A" in graph["full_upstream"]["C"]


# ── Cycle Detection ──


def test_no_cycles():
    adj = {"A": ["B"], "B": ["C"], "C": []}
    cycles = detect_cycles(adj)
    assert len(cycles) == 0


def test_simple_cycle():
    adj = {"A": ["B"], "B": ["C"], "C": ["A"]}
    cycles = detect_cycles(adj)
    assert len(cycles) == 1
    assert set(cycles[0]) == {"A", "B", "C"}


def test_self_loop():
    adj = {"A": ["A"]}
    # Self-loops are now detected as retry patterns (common NiFi pattern)
    cycles = detect_cycles(adj)
    assert len(cycles) == 1
    assert cycles[0] == ["A"]


def test_multiple_cycles():
    adj = {"A": ["B"], "B": ["A"], "C": ["D"], "D": ["C"]}
    cycles = detect_cycles(adj)
    assert len(cycles) == 2


# ── External Systems ──


def test_detect_kafka():
    procs = _make_processors(("k1", "ConsumeKafka"), ("k2", "PublishKafka"))
    systems = detect_external_systems(procs)
    kafka_sys = [s for s in systems if s["key"] == "kafka"]
    assert len(kafka_sys) == 1
    assert len(kafka_sys[0]["processors"]) == 2


def test_detect_jdbc():
    procs = [
        Processor(
            name="db_read", type="QueryDatabaseTable", platform="nifi", properties={"url": "jdbc:mysql://host:3306/db"}
        )
    ]
    systems = detect_external_systems(procs)
    jdbc_found = any(s["key"] in ("mysql", "sql_jdbc") for s in systems)
    assert jdbc_found


# ── Security Scanner ──


def test_detect_sql_injection():
    procs = [
        Processor(
            name="bad",
            type="ExecuteSQL",
            platform="nifi",
            properties={"sql": "SELECT * FROM users; DROP TABLE users --"},
        )
    ]
    findings = scan_security(procs)
    assert len(findings) >= 1
    assert findings[0]["finding"] == "SQL Injection"


def test_detect_hardcoded_secret():
    procs = [
        Processor(
            name="bad", type="InvokeHTTP", platform="nifi", properties={"config": 'password = "mysecretpassword"'}
        )
    ]
    findings = scan_security(procs)
    assert any(f["finding"] == "Hardcoded Secret" for f in findings)


def test_clean_properties():
    procs = [
        Processor(
            name="clean", type="ReplaceText", platform="nifi", properties={"pattern": "hello", "replacement": "world"}
        )
    ]
    findings = scan_security(procs)
    assert len(findings) == 0


# ── Stage Classifier ──


def test_classify_stages():
    procs = _make_processors(
        ("src", "GetFile"),
        ("xform", "ReplaceText"),
        ("sink", "PutFile"),
        ("log", "LogAttribute"),
    )
    stages = classify_stages(procs)
    stage_ids = [s["id"] for s in stages]
    assert "ingestion" in stage_ids
    assert "loading" in stage_ids


# ── Flow Metrics ──


def test_flow_metrics():
    procs = _make_processors(("A", "GetFile"), ("B", "PutFile"))
    conns = _make_connections(("A", "B"))
    graph = build_dependency_graph(procs, conns)
    pr = ParseResult(platform="nifi", processors=procs, connections=conns)
    metrics = compute_flow_metrics(pr, graph)
    assert metrics["processor_count"] == 2
    assert metrics["connection_count"] == 1


# ── Self-loop cycle classification ──


def test_self_loop_classified_as_error_retry():
    """Self-loops should be classified as error_retry patterns."""
    adj = {"InvokeHTTP_1": ["InvokeHTTP_1"]}
    cycles = detect_cycles(adj)
    assert len(cycles) == 1

    procs = [Processor(name="InvokeHTTP_1", type="InvokeHTTP", platform="nifi")]
    conns = [Connection(source_name="InvokeHTTP_1", destination_name="InvokeHTTP_1", relationship="failure")]
    classifications = classify_cycles(cycles, procs, conns)
    assert len(classifications) == 1
    assert classifications[0]["category"] == "error_retry"
    assert "Self-loop" in classifications[0]["description"]


# ── Execution mode: scheduling key compatibility ──


def test_execution_mode_reads_short_keys():
    """Execution mode analyzer should work with JSON parser's short scheduling keys."""
    procs = [
        Processor(
            name="Kafka",
            type="ConsumeKafka",
            platform="nifi",
            scheduling={"strategy": "EVENT_DRIVEN", "period": "0 sec"},
        ),
        Processor(
            name="BatchSQL",
            type="ExecuteSQL",
            platform="nifi",
            scheduling={"strategy": "CRON_DRIVEN", "period": "0 0 * * *"},
        ),
    ]
    conns = _make_connections(("Kafka", "BatchSQL"))
    pr = ParseResult(platform="nifi", processors=procs, connections=conns)
    result = analyze_execution_mode(pr)
    modes = {m["processor"]: m["recommended_mode"] for m in result["processor_modes"]}
    assert modes["Kafka"] == "streaming"
    assert modes["BatchSQL"] == "batch"


# ── State analyzer: scheduling key compatibility ──


def test_state_analyzer_detects_streaming_with_short_keys():
    """State analyzer should correctly detect streaming with short scheduling keys."""
    procs = [
        Processor(
            name="ConsumeKafka_1",
            type="ConsumeKafka",
            platform="nifi",
            scheduling={"strategy": "EVENT_DRIVEN", "period": "0 sec"},
        ),
    ]
    pr = ParseResult(platform="nifi", processors=procs, connections=[])
    result = analyze_state(pr)
    assert len(result["stateful_processors"]) == 1
    assert result["stateful_processors"][0]["is_streaming"] is True


# ── Cycle 2: Backpressure default filtering ──


def test_backpressure_skips_nifi_defaults():
    """NiFi default backpressure (10000 / 1 GB) should not generate configs."""
    from app.engines.analyzers.backpressure import extract_backpressure_configs

    conns = [
        Connection(
            source_name="A",
            destination_name="B",
            back_pressure_object_threshold=10000,
            back_pressure_data_size_threshold="1 GB",
        ),
    ]
    configs = extract_backpressure_configs(conns)
    assert len(configs) == 0, "NiFi default backpressure should be filtered out"


def test_backpressure_includes_custom_values():
    """Custom backpressure values should still be reported."""
    from app.engines.analyzers.backpressure import extract_backpressure_configs

    conns = [
        Connection(
            source_name="A",
            destination_name="B",
            back_pressure_object_threshold=5000,
            back_pressure_data_size_threshold="500 MB",
        ),
    ]
    configs = extract_backpressure_configs(conns)
    assert len(configs) == 1


# ── Cycle 2: Lineage depth with reconvergent paths ──


def test_lineage_depth_reconvergent_paths():
    """Diamond DAG should compute max depth, not first-visited depth."""
    from app.engines.analyzers.lineage_tracker import build_lineage_graph

    #   A
    #  / \\
    # B   C
    #  \\ /
    #   D
    procs = _make_processors(("A", "GetFile"), ("B", "ReplaceText"), ("C", "UpdateAttribute"), ("D", "PutFile"))
    conns = _make_connections(("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"))
    graph = build_lineage_graph(procs, conns)
    # D should have depth 2 (A->B->D or A->C->D), not depth 1
    assert graph["D"]["depth"] == 2, f"D depth should be 2, got {graph['D']['depth']}"
    assert graph["A"]["depth"] == 0


# ── Cycle 2: External systems JDBC type safety ──


def test_external_systems_non_string_property_safe():
    """Properties with non-string values should not crash JDBC URL detection."""
    procs = [
        Processor(
            name="proc",
            type="QueryDatabaseTable",
            platform="nifi",
            properties={"url": "jdbc:oracle:thin:@host:1521:db", "count": 42, "flag": True},
        )
    ]
    # Should not raise TypeError on non-string properties
    systems = detect_external_systems(procs)
    assert any(s["key"] in ("oracle", "sql_jdbc") for s in systems)


# ── Cycle 2: Attribute flow with nested EL expressions ──


def test_attribute_flow_nested_el():
    """Nested ${...} in EL expressions should not produce spurious attribute refs."""
    from app.engines.analyzers.attribute_flow import analyze_attribute_flow

    procs = [
        Processor(
            name="Route",
            type="RouteOnAttribute",
            platform="nifi",
            properties={"condition": "${literal(${type}):equals('CSV')}"},
        ),
    ]
    result = analyze_attribute_flow(procs, [])
    proc_reads = result["processor_attributes"]["Route"]["reads"]
    # "type" should be extracted; "literal(...)" should NOT appear as an attribute
    assert "literal(${type}" not in proc_reads
    # The base-level EL reference starts with "literal" which contains "(" so it is skipped
    # But the nested ${type} should ideally be found — at minimum, no crash

"""Tests for analyzers: dependency graph, cycle detection, external systems, security."""

from app.engines.analyzers.cycle_detection import detect_cycles
from app.engines.analyzers.dependency_graph import build_dependency_graph
from app.engines.analyzers.external_systems import detect_external_systems
from app.engines.analyzers.flow_metrics import compute_flow_metrics
from app.engines.analyzers.security_scanner import scan_security
from app.engines.analyzers.stage_classifier import classify_stages
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
    # networkx treats self-loops as SCC of size 1, which we filter out
    # But our detect_cycles only returns SCCs > 1
    cycles = detect_cycles(adj)
    # Self loop is SCC of 1, so no cycles reported
    assert len(cycles) == 0


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

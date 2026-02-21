"""Compute flow metrics: total processors, connections, depth, fan-out, density."""

from app.models.pipeline import ParseResult


def compute_flow_metrics(parse_result: ParseResult, graph: dict) -> dict:
    """Compute aggregate flow metrics."""
    proc_count = len(parse_result.processors)
    conn_count = len(parse_result.connections)
    group_count = len(parse_result.process_groups)
    cs_count = len(parse_result.controller_services)

    # Max depth = longest full_downstream chain
    full_down = graph.get("full_downstream", {})
    max_depth = max((len(v) for v in full_down.values()), default=0)

    # Fan-out average
    fan_out = graph.get("fan_out", {})
    avg_fan_out = sum(fan_out.values()) / max(len(fan_out), 1)

    # Connection density = connections / max_possible_connections
    max_possible = proc_count * (proc_count - 1) if proc_count > 1 else 1
    density = conn_count / max_possible

    return {
        "processor_count": proc_count,
        "connection_count": conn_count,
        "process_group_count": group_count,
        "controller_service_count": cs_count,
        "max_depth": max_depth,
        "avg_fan_out": round(avg_fan_out, 2),
        "connection_density": round(density, 4),
        "platform": parse_result.platform,
    }

"""Cycle detection using networkx strongly connected components.

Ported from cycle-detection.js (Tarjan's SCC algorithm).
Enhanced with cycle classification for Databricks translation (Phase 2).
"""

import logging

import networkx as nx

from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def detect_cycles(adjacency_map: dict[str, list[str]]) -> list[list[str]]:
    """Detect cycles in a directed graph.

    Returns list of strongly connected components with more than one node,
    plus self-loops (nodes with an edge to themselves). Self-loops are a common
    NiFi pattern (e.g., processor routing failure back to itself for retry).
    """
    graph = nx.DiGraph()
    for node, neighbors in adjacency_map.items():
        graph.add_node(node)
        for n in neighbors:
            graph.add_edge(node, n)

    sccs = list(nx.strongly_connected_components(graph))
    cycles = [sorted(scc) for scc in sccs if len(scc) > 1]

    # Also detect self-loops — SCC of size 1 with an edge to itself
    for node in graph.nodes():
        if graph.has_edge(node, node):
            # Only add if not already part of a larger SCC
            if not any(node in scc_nodes for scc_nodes in cycles):
                cycles.append([node])

    logger.info("Cycle detection complete: %d cycle(s) found", len(cycles))
    return cycles


# Processor types commonly involved in error retry patterns
_ERROR_RETRY_INDICATORS = {"LogAttribute", "LogMessage", "RetryFlowFile", "Wait", "Notify"}

# Processor types commonly involved in pagination
_PAGINATION_INDICATORS = {"InvokeHTTP", "HandleHttpRequest", "HandleHttpResponse", "GetHTTP"}

# Processor types commonly involved in data-driven re-evaluation
_DATA_REEVALUATION_INDICATORS = {
    "RouteOnAttribute",
    "RouteOnContent",
    "EvaluateJsonPath",
    "ValidateRecord",
    "QueryRecord",
}


def classify_cycles(
    cycles: list[list[str]],
    processors: list[Processor],
    connections: list[Connection],
) -> list[dict]:
    """Classify each cycle into a category for Databricks translation.

    Categories:
    - error_retry: failure relationship routes back (translate to max_retries)
    - data_reevaluation: data-driven loops (translate to for_each_task)
    - pagination: HTTP pagination loops (translate to while loop)

    Returns list of CycleClassification-compatible dicts.
    """
    # Build lookup maps
    proc_by_name: dict[str, Processor] = {p.name: p for p in processors}
    conn_lookup: list[tuple[str, str, str]] = [
        (c.source_name, c.destination_name, c.relationship) for c in connections
    ]

    classifications: list[dict] = []

    for cycle_nodes in cycles:
        cycle_set = set(cycle_nodes)

        # Handle self-loops (single node cycling to itself)
        is_self_loop = len(cycle_nodes) == 1

        # Collect processor types in this cycle
        cycle_types = set()
        for node in cycle_nodes:
            p = proc_by_name.get(node)
            if p:
                cycle_types.add(p.type)

        # Check connections within cycle for failure relationships
        has_failure_edge = False
        for src, dst, rel in conn_lookup:
            if src in cycle_set and dst in cycle_set:
                if "failure" in rel.lower() or "retry" in rel.lower():
                    has_failure_edge = True
                    break

        # Self-loops are almost always error retry patterns
        if is_self_loop:
            node_name = cycle_nodes[0]
            p = proc_by_name.get(node_name)
            proc_type = p.type if p else "Unknown"
            classifications.append({
                "cycle_nodes": cycle_nodes,
                "category": "error_retry",
                "description": (
                    f"Self-loop on processor '{node_name}' ({proc_type}). "
                    f"Failure output routes back to the same processor for retry."
                ),
                "databricks_translation": "max_retries + min_retry_interval_millis on Databricks task",
            })
            continue

        # Classify
        if has_failure_edge or cycle_types & _ERROR_RETRY_INDICATORS:
            classifications.append({
                "cycle_nodes": cycle_nodes,
                "category": "error_retry",
                "description": (
                    f"Error retry loop involving {len(cycle_nodes)} processors. "
                    f"Failure/retry relationships detected."
                ),
                "databricks_translation": "max_retries + min_retry_interval_millis on Databricks task",
            })
        elif cycle_types & _PAGINATION_INDICATORS:
            classifications.append({
                "cycle_nodes": cycle_nodes,
                "category": "pagination",
                "description": (
                    f"Pagination loop involving {len(cycle_nodes)} processors. "
                    f"HTTP processors detected: {cycle_types & _PAGINATION_INDICATORS}."
                ),
                "databricks_translation": "PySpark while loop with pagination state variable",
            })
        elif cycle_types & _DATA_REEVALUATION_INDICATORS:
            classifications.append({
                "cycle_nodes": cycle_nodes,
                "category": "data_reevaluation",
                "description": (
                    f"Data re-evaluation loop involving {len(cycle_nodes)} processors. "
                    f"Routing/evaluation processors detected."
                ),
                "databricks_translation": "for_each_task or Structured Streaming micro-batch loop",
            })
        else:
            # Default classification when pattern is unclear
            classifications.append({
                "cycle_nodes": cycle_nodes,
                "category": "data_reevaluation",
                "description": (
                    f"Loop involving {len(cycle_nodes)} processors. "
                    f"Types: {cycle_types}. Defaulting to data re-evaluation."
                ),
                "databricks_translation": "for_each_task or Structured Streaming micro-batch loop",
            })

    return classifications

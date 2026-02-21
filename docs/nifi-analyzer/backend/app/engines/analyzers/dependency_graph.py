"""Build processor DAG using networkx — ported from dependency-graph.js."""

import logging
from collections import deque

import networkx as nx

from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def build_dependency_graph(
    processors: list[Processor],
    connections: list[Connection],
) -> dict:
    """Build upstream/downstream dependency graph with transitive closures.

    Returns:
        {
            "downstream": {name: [downstream_names]},
            "upstream": {name: [upstream_names]},
            "full_downstream": {name: [all reachable downstream]},
            "full_upstream": {name: [all reachable upstream]},
            "fan_in": {name: int},
            "fan_out": {name: int},
        }
    """
    graph = nx.DiGraph()

    for p in processors:
        graph.add_node(p.name)

    for c in connections:
        if c.source_name and c.destination_name:
            graph.add_edge(c.source_name, c.destination_name)

    downstream: dict[str, list[str]] = {}
    upstream: dict[str, list[str]] = {}

    for p in processors:
        downstream[p.name] = list(graph.successors(p.name)) if p.name in graph else []
        upstream[p.name] = list(graph.predecessors(p.name)) if p.name in graph else []

    # Transitive closures via BFS
    def _get_chain(graph: dict[str, list[str]], start: str) -> list[str]:
        result: list[str] = []
        visited: set[str] = set()
        queue = deque(graph.get(start, []))
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            result.append(node)
            for n in graph.get(node, []):
                if n not in visited:
                    queue.append(n)
        return result

    full_downstream: dict[str, list[str]] = {}
    full_upstream: dict[str, list[str]] = {}
    fan_in: dict[str, int] = {}
    fan_out: dict[str, int] = {}

    for p in processors:
        full_downstream[p.name] = _get_chain(downstream, p.name)
        full_upstream[p.name] = _get_chain(upstream, p.name)
        fan_in[p.name] = len(upstream.get(p.name, []))
        fan_out[p.name] = len(downstream.get(p.name, []))

    logger.info("Dependency graph: %d nodes, %d edges", graph.number_of_nodes(), graph.number_of_edges())
    return {
        "downstream": downstream,
        "upstream": upstream,
        "full_downstream": full_downstream,
        "full_upstream": full_upstream,
        "fan_in": fan_in,
        "fan_out": fan_out,
    }

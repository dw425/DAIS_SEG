"""Cycle detection using networkx strongly connected components.

Ported from cycle-detection.js (Tarjan's SCC algorithm).
"""

import networkx as nx


def detect_cycles(adjacency_map: dict[str, list[str]]) -> list[list[str]]:
    """Detect cycles in a directed graph.

    Returns list of strongly connected components with more than one node.
    """
    graph = nx.DiGraph()
    for node, neighbors in adjacency_map.items():
        graph.add_node(node)
        for n in neighbors:
            graph.add_edge(node, n)

    sccs = list(nx.strongly_connected_components(graph))
    return [sorted(scc) for scc in sccs if len(scc) > 1]

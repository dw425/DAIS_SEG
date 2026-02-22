"""Data lineage tracker — builds lineage graph from parsed flow.

Computes upstream/downstream relationships, critical paths,
source/sink identification, lineage depth, and orphan detection.

All algorithms are O(V+E) to handle flows with 10,000+ processors.
"""

from __future__ import annotations

import logging
from collections import deque

from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def build_lineage_graph(
    processors: list[Processor],
    connections: list[Connection],
) -> dict[str, dict]:
    """Build lineage graph with upstream/downstream lists and depth.

    Returns {processor_name: {upstream: [], downstream: [], depth: int}}.
    Handles cycles safely by capping depth propagation.
    """
    graph: dict[str, dict] = {}
    for p in processors:
        graph[p.name] = {"upstream": [], "downstream": [], "depth": 0}

    for c in connections:
        src = c.source_name
        dst = c.destination_name
        if src in graph and dst in graph:
            if dst not in graph[src]["downstream"]:
                graph[src]["downstream"].append(dst)
            if src not in graph[dst]["upstream"]:
                graph[dst]["upstream"].append(src)

    # Compute depth via BFS from sources with cycle-safe cap.
    # For DAGs with reconvergent paths, depth = max distance from any source.
    # Cap iterations at V*2 to prevent infinite loops from cycles.
    max_iterations = len(graph) * 2
    sources = [name for name, info in graph.items() if not info["upstream"]]
    queue: deque[tuple[str, int]] = deque((s, 0) for s in sources)
    iterations = 0
    while queue and iterations < max_iterations:
        iterations += 1
        node, depth = queue.popleft()
        if depth <= graph[node]["depth"] and depth > 0:
            continue
        graph[node]["depth"] = depth
        for ds in graph[node]["downstream"]:
            if depth + 1 > graph[ds]["depth"]:
                queue.append((ds, depth + 1))

    return graph


def get_upstream(graph: dict[str, dict], processor_name: str) -> list[str]:
    """Get all transitive upstream ancestors."""
    if processor_name not in graph:
        return []
    result: list[str] = []
    visited: set[str] = set()
    queue: deque[str] = deque(graph[processor_name]["upstream"])
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        result.append(node)
        for up in graph.get(node, {}).get("upstream", []):
            if up not in visited:
                queue.append(up)
    return result


def get_downstream(graph: dict[str, dict], processor_name: str) -> list[str]:
    """Get all transitive downstream descendants."""
    if processor_name not in graph:
        return []
    result: list[str] = []
    visited: set[str] = set()
    queue: deque[str] = deque(graph[processor_name]["downstream"])
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        result.append(node)
        for ds in graph.get(node, {}).get("downstream", []):
            if ds not in visited:
                queue.append(ds)
    return result


def _topological_sort(graph: dict[str, dict]) -> list[str]:
    """Kahn's algorithm — returns topological order, ignoring back-edges from cycles."""
    in_degree: dict[str, int] = {name: len(info["upstream"]) for name, info in graph.items()}
    queue: deque[str] = deque(name for name, deg in in_degree.items() if deg == 0)
    order: list[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for ds in graph[node]["downstream"]:
            in_degree[ds] -= 1
            if in_degree[ds] == 0:
                queue.append(ds)
    # Nodes in cycles won't appear in order — that's fine, we skip them.
    return order


def get_critical_path(graph: dict[str, dict]) -> list[str]:
    """Find the longest path from any source to any sink.

    Uses O(V+E) dynamic programming on topological order instead of
    exponential DFS enumeration.
    """
    if not graph:
        return []

    topo = _topological_sort(graph)
    if not topo:
        return []

    # DP: dist[node] = length of longest path ending at node
    dist: dict[str, int] = {}
    pred: dict[str, str | None] = {}
    for name in topo:
        dist[name] = 0
        pred[name] = None

    for node in topo:
        for ds in graph[node]["downstream"]:
            if ds in dist and dist[node] + 1 > dist[ds]:
                dist[ds] = dist[node] + 1
                pred[ds] = node

    # Find the sink with longest path
    sinks = {name for name, info in graph.items() if not info["downstream"]}
    # Consider all nodes in topo order (some sinks may be in cycles and missing)
    best_node: str | None = None
    best_dist = -1
    for name in topo:
        if name in sinks and dist[name] > best_dist:
            best_dist = dist[name]
            best_node = name

    # If no sinks found (all in cycles), pick the longest path endpoint
    if best_node is None:
        for name in topo:
            if dist[name] > best_dist:
                best_dist = dist[name]
                best_node = name

    if best_node is None:
        return []

    # Reconstruct path
    path: list[str] = []
    current: str | None = best_node
    while current is not None:
        path.append(current)
        current = pred[current]
    path.reverse()
    return path


def get_lineage_depth(graph: dict[str, dict]) -> int:
    """Get maximum lineage depth."""
    if not graph:
        return 0
    return max(info["depth"] for info in graph.values())


def track_lineage(
    parse_result: ParseResult,
    analysis: AnalysisResult | None = None,
) -> dict:
    """Build complete lineage tracking result.

    Returns {graph, sources, sinks, criticalPath, maxDepth, orphans}.
    """
    graph = build_lineage_graph(parse_result.processors, parse_result.connections)

    sources = [name for name, info in graph.items() if not info["upstream"]]
    sinks = [name for name, info in graph.items() if not info["downstream"]]
    orphans = [
        name for name, info in graph.items()
        if not info["upstream"] and not info["downstream"]
    ]
    critical_path = get_critical_path(graph)
    max_depth = get_lineage_depth(graph)

    # Generate Mermaid markdown for visualization
    mermaid_md = _generate_mermaid(graph, sources, sinks, critical_path)

    # Build lineage nodes and edges for frontend visualization
    lineage_nodes = _build_lineage_nodes(parse_result.processors, sources, sinks)
    lineage_edges = _build_lineage_edges(parse_result.connections)

    logger.info("Lineage tracking: %d nodes, critical_path=%d, max_depth=%d, orphans=%d", len(graph), len(critical_path), max_depth, len(orphans))
    return {
        "graph": graph,
        "sources": sources,
        "sinks": sinks,
        "criticalPath": critical_path,
        "maxDepth": max_depth,
        "orphans": orphans,
        "mermaidMarkdown": mermaid_md,
        "lineageNodes": lineage_nodes,
        "lineageEdges": lineage_edges,
    }


def _generate_mermaid(
    graph: dict[str, dict],
    sources: list[str],
    sinks: list[str],
    critical_path: list[str],
) -> str:
    """Generate Mermaid flowchart markdown from lineage graph."""
    lines = ["graph LR"]
    node_ids: dict[str, str] = {}

    # Assign safe IDs
    for i, name in enumerate(graph.keys()):
        safe_id = f"N{i}"
        node_ids[name] = safe_id

        # Style sources, sinks, and critical path nodes differently
        short_name = name[:20] + ".." if len(name) > 20 else name
        if name in sources:
            lines.append(f"    {safe_id}[/{short_name}/]")
        elif name in sinks:
            lines.append(f"    {safe_id}[\\{short_name}\\]")
        else:
            lines.append(f"    {safe_id}[{short_name}]")

    # Add edges
    for name, info in graph.items():
        src_id = node_ids.get(name, "")
        for ds in info.get("downstream", []):
            dst_id = node_ids.get(ds, "")
            if src_id and dst_id:
                lines.append(f"    {src_id} --> {dst_id}")

    # Style classes
    lines.append("")
    if sources:
        src_ids = " & ".join(node_ids.get(s, "") for s in sources if s in node_ids)
        if src_ids:
            lines.append(f"    classDef source fill:#22c55e20,stroke:#22c55e,color:#22c55e")
            lines.append(f"    class {','.join(node_ids[s] for s in sources if s in node_ids)} source")
    if sinks:
        lines.append(f"    classDef sink fill:#f9731620,stroke:#f97316,color:#f97316")
        lines.append(f"    class {','.join(node_ids[s] for s in sinks if s in node_ids)} sink")
    if critical_path:
        cp_ids = [node_ids[n] for n in critical_path if n in node_ids]
        if cp_ids:
            lines.append(f"    classDef critical fill:#ef444420,stroke:#ef4444,color:#ef4444")
            lines.append(f"    class {','.join(cp_ids)} critical")

    return "\n".join(lines)


def _build_lineage_nodes(
    processors: list[Processor],
    sources: list[str],
    sinks: list[str],
) -> list[dict]:
    """Build lineage nodes for the frontend LineageGraph component."""
    # Classify node types by role
    source_types = {"GetFile", "ConsumeKafka", "ConsumeKafka_2_6", "QueryDatabaseTable",
                    "ListenHTTP", "GetSFTP", "GetFTP", "GetHTTP", "GenerateFlowFile",
                    "ConsumeKafkaRecord_2_6", "GetS3Object", "FetchS3Object", "ListS3"}
    sink_types = {"PutFile", "PublishKafka", "PublishKafka_2_6", "PutSQL",
                  "PutDatabaseRecord", "PutS3Object", "PutSFTP", "PutHDFS",
                  "PutKinesisStream", "PutElasticsearch5"}

    nodes: list[dict] = []
    for proc in processors:
        if proc.name in sources or proc.type in source_types:
            node_type = "source"
        elif proc.name in sinks or proc.type in sink_types:
            node_type = "sink"
        else:
            node_type = "transform"

        nodes.append({
            "id": proc.name,
            "name": proc.name,
            "type": node_type,
            "processorType": proc.type,
            "confidence": 0.0,  # Will be enriched by frontend
        })
    return nodes


def _build_lineage_edges(connections: list[Connection]) -> list[dict]:
    """Build lineage edges for the frontend LineageGraph component."""
    edges: list[dict] = []
    for conn in connections:
        edges.append({
            "source": conn.source_name,
            "target": conn.destination_name,
            "relationship": conn.relationship,
        })
    return edges

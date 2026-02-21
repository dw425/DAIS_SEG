"""Task clustering: merge contiguous sequential processors into single execution units.

Identifies chains of processors that perform sequential transformations
(e.g., EvaluateJsonPath -> UpdateAttribute -> RouteOnAttribute) and merges
them into a single logical TaskCluster that maps to one PySpark script,
eliminating intermediate file I/O between micro-steps.
"""

import logging

from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)

# Processor types that are natural cluster boundaries (I/O operations)
_BOUNDARY_TYPES = {
    # Source processors
    "GetFile",
    "GetSFTP",
    "GetHTTP",
    "GetHDFS",
    "GetS3Object",
    "GetKafka",
    "ConsumeKafka",
    "ConsumeKafkaRecord",
    "GenerateFlowFile",
    "ListFile",
    "ListSFTP",
    "ListS3",
    "ListHDFS",
    "QueryDatabaseTable",
    "QueryDatabaseTableRecord",
    "ExecuteSQL",
    "ExecuteSQLRecord",
    # Sink processors
    "PutFile",
    "PutSFTP",
    "PutHDFS",
    "PutS3Object",
    "PutKafka",
    "PublishKafka",
    "PublishKafkaRecord",
    "PutDatabaseRecord",
    "PutSQL",
    "PutEmail",
    "PutSlack",
    # I/O boundary types
    "InputPort",
    "OutputPort",
    "Funnel",
    "InvokeHTTP",
    "HandleHttpRequest",
    "HandleHttpResponse",
}

# Processor types that are good candidates for clustering (transformations)
_TRANSFORM_TYPES = {
    "EvaluateJsonPath",
    "EvaluateXPath",
    "EvaluateXQuery",
    "UpdateAttribute",
    "ReplaceText",
    "RouteOnAttribute",
    "RouteOnContent",
    "SplitJson",
    "SplitXml",
    "SplitText",
    "SplitRecord",
    "MergeContent",
    "MergeRecord",
    "JoltTransformJSON",
    "TransformXml",
    "ConvertRecord",
    "ValidateRecord",
    "QueryRecord",
    "LookupRecord",
    "ExtractText",
    "ExtractGrok",
    "LogAttribute",
    "LogMessage",
    "AttributesToJSON",
    "ConvertJSONToSQL",
}


def cluster_tasks(
    processors: list[Processor],
    connections: list[Connection],
) -> list[dict]:
    """Cluster contiguous sequential processors into logical execution units.

    Algorithm:
    1. Build adjacency (downstream/upstream) maps
    2. Identify boundary processors (sources, sinks, I/O) that cannot be merged
    3. Walk chains of non-boundary processors with fan-in=1 and fan-out=1
    4. Group them into TaskCluster objects

    Returns list of TaskCluster-compatible dicts with keys:
        id, processors, entry_processor, exit_processors, connections
    """
    if not processors:
        return []

    # Build adjacency maps
    proc_names = {p.name for p in processors}
    downstream: dict[str, list[str]] = {p.name: [] for p in processors}
    upstream: dict[str, list[str]] = {p.name: [] for p in processors}
    conn_map: dict[str, list[Connection]] = {p.name: [] for p in processors}

    for c in connections:
        if c.source_name in proc_names and c.destination_name in proc_names:
            downstream.setdefault(c.source_name, []).append(c.destination_name)
            upstream.setdefault(c.destination_name, []).append(c.source_name)
            conn_map.setdefault(c.source_name, []).append(c)

    # Build type lookup
    type_by_name: dict[str, str] = {p.name: p.type for p in processors}

    # Determine which processors are boundary nodes
    def is_boundary(name: str) -> bool:
        return type_by_name.get(name, "") in _BOUNDARY_TYPES

    # Find clusters by walking chains of non-boundary, single-path processors
    visited: set[str] = set()
    clusters: list[dict] = []
    cluster_id = 0

    for proc in processors:
        if proc.name in visited:
            continue

        # Start a new potential cluster from this processor
        chain: list[str] = []
        current = proc.name

        # Walk backward to find the start of the chain
        while (
            current not in visited
            and len(upstream.get(current, [])) == 1
            and not is_boundary(current)
        ):
            prev = upstream[current][0]
            if (
                prev not in visited
                and len(downstream.get(prev, [])) == 1
                and not is_boundary(prev)
            ):
                current = prev
            else:
                break

        # Walk forward from chain start
        while current not in visited:
            visited.add(current)
            chain.append(current)

            successors = downstream.get(current, [])
            if (
                len(successors) == 1
                and successors[0] not in visited
                and len(upstream.get(successors[0], [])) == 1
                and not is_boundary(current)
                and not is_boundary(successors[0])
            ):
                current = successors[0]
            else:
                break

        # Only create a cluster if there are 2+ processors
        if len(chain) >= 2:
            entry = chain[0]
            exits = [chain[-1]]

            # Collect internal connections
            internal_conns: list[str] = []
            chain_set = set(chain)
            for node in chain:
                for c in conn_map.get(node, []):
                    if c.destination_name in chain_set:
                        internal_conns.append(f"{c.source_name} -> {c.destination_name} [{c.relationship}]")

            clusters.append({
                "id": f"cluster_{cluster_id}",
                "processors": chain,
                "entry_processor": entry,
                "exit_processors": exits,
                "connections": internal_conns,
            })
            cluster_id += 1

    logger.info("Task clustering: %d cluster(s) from %d processors", len(clusters), len(processors))
    return clusters

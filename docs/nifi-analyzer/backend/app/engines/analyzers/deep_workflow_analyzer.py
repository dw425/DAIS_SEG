"""Pass 3: Workflow Analysis -- Order of operations, parallel vs sequential.

Uses the dependency graph from the existing analysis engine to compute
topological sort, execution phases, cycle detection, synchronization
points, critical path, and parallelism factor.
"""

from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import dataclass, field

from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExecutionPhase:
    phase: int
    name: str
    parallel_processors: list[str]
    sequential_after: list[str]
    execution_mode: str  # streaming, batch, hybrid
    fan_out: dict[str, list[str]] | None = None


@dataclass
class WorkflowReport:
    execution_phases: list[ExecutionPhase]
    process_groups: list[dict]
    cycles_detected: list[dict]
    synchronization_points: list[dict]
    total_execution_phases: int
    critical_path_length: int
    parallelism_factor: float
    topological_order: list[str]


# ---------------------------------------------------------------------------
# Topological sort (Kahn's algorithm)
# ---------------------------------------------------------------------------

def _topological_sort(
    proc_names: set[str],
    downstream: dict[str, list[str]],
    upstream: dict[str, list[str]],
) -> list[str]:
    """Kahn's algorithm. Nodes in cycles are appended at the end."""
    in_degree: dict[str, int] = {
        name: len([u for u in upstream.get(name, []) if u in proc_names])
        for name in proc_names
    }
    queue: deque[str] = deque(
        name for name, deg in in_degree.items() if deg == 0
    )
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for ds in downstream.get(node, []):
            if ds not in proc_names:
                continue
            in_degree[ds] -= 1
            if in_degree[ds] == 0:
                queue.append(ds)

    # Nodes still with in_degree > 0 are in cycles -- append them
    remaining = [n for n in proc_names if n not in set(order)]
    order.extend(sorted(remaining))
    return order


# ---------------------------------------------------------------------------
# Execution phase grouping (longest-path / depth-based)
# ---------------------------------------------------------------------------

def _compute_depth_map(
    proc_names: set[str],
    downstream: dict[str, list[str]],
    upstream: dict[str, list[str]],
    topo_order: list[str],
) -> dict[str, int]:
    """Compute depth for each node (longest path from any source).

    Processors at the same depth can execute in parallel.
    """
    depth: dict[str, int] = {name: 0 for name in proc_names}
    order_set = set(topo_order)

    for node in topo_order:
        for ds in downstream.get(node, []):
            if ds in order_set:
                new_depth = depth[node] + 1
                if new_depth > depth.get(ds, 0):
                    depth[ds] = new_depth

    return depth


def _build_execution_phases(
    proc_names: set[str],
    depth_map: dict[str, int],
    downstream: dict[str, list[str]],
    upstream: dict[str, list[str]],
    proc_mode_map: dict[str, str],
) -> list[ExecutionPhase]:
    """Group processors into execution phases by depth."""
    # Bucket by depth
    depth_buckets: dict[int, list[str]] = {}
    for name, d in depth_map.items():
        if name in proc_names:
            depth_buckets.setdefault(d, []).append(name)

    phases: list[ExecutionPhase] = []
    for phase_idx in sorted(depth_buckets.keys()):
        members = sorted(depth_buckets[phase_idx])
        # Determine sequential_after: the phases that must complete first
        predecessors: set[str] = set()
        for m in members:
            for u in upstream.get(m, []):
                if u in proc_names and depth_map.get(u, 0) < phase_idx:
                    predecessors.add(u)

        # Determine execution mode for this phase
        modes = [proc_mode_map.get(m, "batch") for m in members]
        if all(m == "streaming" for m in modes):
            exec_mode = "streaming"
        elif any(m == "streaming" for m in modes):
            exec_mode = "hybrid"
        else:
            exec_mode = "batch"

        # Detect fan-out within this phase
        fan_out_map: dict[str, list[str]] | None = None
        for m in members:
            ds_list = [d for d in downstream.get(m, []) if d in proc_names]
            if len(ds_list) > 1:
                if fan_out_map is None:
                    fan_out_map = {}
                fan_out_map[m] = ds_list

        phase_name = _generate_phase_name(phase_idx, members, exec_mode)

        phases.append(ExecutionPhase(
            phase=phase_idx,
            name=phase_name,
            parallel_processors=members,
            sequential_after=sorted(predecessors),
            execution_mode=exec_mode,
            fan_out=fan_out_map,
        ))

    return phases


def _generate_phase_name(idx: int, members: list[str], mode: str) -> str:
    """Generate a human-readable phase name."""
    if idx == 0:
        prefix = "Ingestion"
    elif mode == "streaming":
        prefix = "Streaming"
    else:
        prefix = "Processing"
    return f"Phase {idx + 1}: {prefix} ({len(members)} processor{'s' if len(members) != 1 else ''})"


# ---------------------------------------------------------------------------
# Streaming / batch classification per processor
# ---------------------------------------------------------------------------

_STREAMING_TYPES = {
    "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
    "ListenHTTP", "ListenTCP", "ListenUDP", "ListenSyslog",
    "ListenRELP", "ConsumeMQTT", "ConsumeJMS", "ConsumeAMQP",
    "TailFile",
}

_DURATION_RE = re.compile(r"(\d+)\s*(sec|second|min|minute|hour|hr|ms|millisecond)s?", re.I)


def _classify_processor_mode(p: Processor) -> str:
    """Classify a processor as streaming or batch."""
    short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
    if short in _STREAMING_TYPES:
        return "streaming"

    sched = p.scheduling or {}
    strategy = (sched.get("strategy", "") or sched.get("schedulingStrategy", "")).lower()
    period = sched.get("period", "") or sched.get("schedulingPeriod", "")

    if strategy == "event_driven":
        return "streaming"

    if period:
        m = _DURATION_RE.search(period)
        if m:
            val = float(m.group(1))
            unit = m.group(2).lower()
            secs = val
            if unit.startswith("ms") or unit.startswith("millisecond"):
                secs = val / 1000
            elif unit.startswith("min") or unit.startswith("minute"):
                secs = val * 60
            elif unit.startswith("hour") or unit.startswith("hr"):
                secs = val * 3600
            if secs < 30:
                return "streaming"

    return "batch"


# ---------------------------------------------------------------------------
# Synchronization point detection (Wait/Notify and merge/barrier patterns)
# ---------------------------------------------------------------------------

_SYNC_TYPES = {"Wait", "Notify", "MergeContent", "MergeRecord"}


def _detect_sync_points(
    processors: list[Processor],
    connections: list[Connection],
) -> list[dict]:
    """Detect synchronization points in the flow."""
    sync_points: list[dict] = []
    proc_by_name = {p.name: p for p in processors}

    downstream: dict[str, list[str]] = {}
    upstream: dict[str, list[str]] = {}
    for c in connections:
        downstream.setdefault(c.source_name, []).append(c.destination_name)
        upstream.setdefault(c.destination_name, []).append(c.source_name)

    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type

        if short == "Wait":
            # Find corresponding Notify
            signal_id = p.properties.get("Signal Counter Name",
                        p.properties.get("Release Signal Identifier", ""))
            notify_candidates = [
                pp for pp in processors
                if (pp.type.rsplit(".", 1)[-1] if "." in pp.type else pp.type) == "Notify"
            ]
            sync_points.append({
                "type": "wait_notify",
                "waiter": p.name,
                "signal_id": signal_id,
                "notify_candidates": [n.name for n in notify_candidates],
                "databricks_equivalent": "dbutils.jobs.taskValues (signal passing between tasks)",
            })

        elif short == "Notify":
            signal_id = p.properties.get("Signal Counter Name",
                        p.properties.get("Release Signal Identifier", ""))
            sync_points.append({
                "type": "notify_signal",
                "notifier": p.name,
                "signal_id": signal_id,
                "databricks_equivalent": "dbutils.jobs.taskValues.set()",
            })

        elif short in ("MergeContent", "MergeRecord"):
            # Merge is a barrier: waits for N inputs before emitting
            ups = upstream.get(p.name, [])
            sync_points.append({
                "type": "merge_barrier",
                "processor": p.name,
                "input_count": len(ups),
                "inputs": ups,
                "merge_strategy": p.properties.get("Merge Strategy",
                                  p.properties.get("Merge Format", "unknown")),
                "databricks_equivalent": (
                    "DataFrame union + groupBy aggregation or "
                    "Structured Streaming foreachBatch with merge"
                ),
            })

    return sync_points


# ---------------------------------------------------------------------------
# Process group hierarchy
# ---------------------------------------------------------------------------

def _map_process_groups(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None,
) -> list[dict]:
    """Map process group hierarchy from parse result or analysis."""
    groups = parsed.process_groups
    if not groups:
        return [{"name": "(root)", "processors": [p.name for p in parsed.processors]}]

    result: list[dict] = []
    proc_by_group: dict[str, list[str]] = {}
    for p in parsed.processors:
        grp = p.group if p.group and p.group != "(root)" else "(root)"
        proc_by_group.setdefault(grp, []).append(p.name)

    for g in groups:
        procs = proc_by_group.get(g.name, g.processors)
        result.append({
            "name": g.name,
            "processor_count": len(procs),
            "processors": procs,
        })

    # Add root group if processors exist outside named groups
    root_procs = proc_by_group.get("(root)", [])
    if root_procs and not any(g["name"] == "(root)" for g in result):
        result.insert(0, {
            "name": "(root)",
            "processor_count": len(root_procs),
            "processors": root_procs,
        })

    return result


# ---------------------------------------------------------------------------
# Critical path computation (reuses lineage_tracker algorithm inline)
# ---------------------------------------------------------------------------

def _compute_critical_path(
    proc_names: set[str],
    downstream: dict[str, list[str]],
    topo_order: list[str],
) -> list[str]:
    """Find the longest path from any source to any sink (O(V+E) DP)."""
    if not topo_order:
        return []

    dist: dict[str, int] = {n: 0 for n in topo_order}
    pred: dict[str, str | None] = {n: None for n in topo_order}

    for node in topo_order:
        for ds in downstream.get(node, []):
            if ds in dist and dist[node] + 1 > dist[ds]:
                dist[ds] = dist[node] + 1
                pred[ds] = node

    # Find endpoint with longest path
    best_node = max(topo_order, key=lambda n: dist.get(n, 0))
    if dist.get(best_node, 0) == 0 and len(topo_order) > 1:
        return [topo_order[0]]

    path: list[str] = []
    current: str | None = best_node
    while current is not None:
        path.append(current)
        current = pred.get(current)
    path.reverse()
    return path


# ---------------------------------------------------------------------------
# Cycle enrichment from existing analysis
# ---------------------------------------------------------------------------

def _enrich_cycles(analysis_result: AnalysisResult | None) -> list[dict]:
    """Extract and enrich cycle information from existing analysis."""
    if not analysis_result:
        return []

    result: list[dict] = []

    # Use cycle_classifications if available, else raw cycles
    if analysis_result.cycle_classifications:
        for cc in analysis_result.cycle_classifications:
            entry = {
                "nodes": cc.cycle_nodes if hasattr(cc, "cycle_nodes") else cc.get("cycle_nodes", []),
                "category": cc.category if hasattr(cc, "category") else cc.get("category", ""),
                "description": cc.description if hasattr(cc, "description") else cc.get("description", ""),
                "databricks_translation": (
                    cc.databricks_translation if hasattr(cc, "databricks_translation")
                    else cc.get("databricks_translation", "")
                ),
            }
            result.append(entry)
    elif analysis_result.cycles:
        for cycle_nodes in analysis_result.cycles:
            result.append({
                "nodes": cycle_nodes,
                "category": "unclassified",
                "description": f"Cycle involving {len(cycle_nodes)} processor(s)",
                "databricks_translation": "",
            })

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_workflow(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None = None,
) -> WorkflowReport:
    """Analyze execution order, parallelism, and workflow structure.

    Args:
        parsed: Normalized parse result.
        analysis_result: Existing analysis result (dependency_graph, cycles).

    Returns:
        WorkflowReport with phases, topology, cycles, sync points, etc.
    """
    processors = parsed.processors
    connections = parsed.connections
    proc_names = {p.name for p in processors}

    logger.info(
        "Pass 3 (Workflow): analyzing %d processors, %d connections",
        len(processors), len(connections),
    )

    # Build adjacency from existing analysis or from scratch
    if analysis_result and analysis_result.dependency_graph:
        graph = analysis_result.dependency_graph
        downstream = graph.get("downstream", {})
        upstream = graph.get("upstream", {})
    else:
        downstream: dict[str, list[str]] = {p.name: [] for p in processors}
        upstream: dict[str, list[str]] = {p.name: [] for p in processors}
        for c in connections:
            if c.source_name in proc_names and c.destination_name in proc_names:
                downstream.setdefault(c.source_name, []).append(c.destination_name)
                upstream.setdefault(c.destination_name, []).append(c.source_name)

    # 1. Topological sort
    topo_order = _topological_sort(proc_names, downstream, upstream)

    # 2. Compute depth map
    depth_map = _compute_depth_map(proc_names, downstream, upstream, topo_order)

    # 3. Classify each processor as streaming/batch
    proc_mode_map = {p.name: _classify_processor_mode(p) for p in processors}

    # 4. Build execution phases
    phases = _build_execution_phases(
        proc_names, depth_map, downstream, upstream, proc_mode_map,
    )

    # 5. Process group hierarchy
    process_groups = _map_process_groups(parsed, analysis_result)

    # 6. Cycle detection from existing analysis
    cycles = _enrich_cycles(analysis_result)

    # 7. Synchronization points
    sync_points = _detect_sync_points(processors, connections)

    # 8. Critical path
    critical_path = _compute_critical_path(proc_names, downstream, topo_order)
    crit_len = len(critical_path)

    # 9. Parallelism factor
    parallelism = (
        len(processors) / max(crit_len, 1)
        if processors else 0.0
    )

    report = WorkflowReport(
        execution_phases=phases,
        process_groups=process_groups,
        cycles_detected=cycles,
        synchronization_points=sync_points,
        total_execution_phases=len(phases),
        critical_path_length=crit_len,
        parallelism_factor=round(parallelism, 2),
        topological_order=topo_order,
    )

    logger.info(
        "Pass 3 complete: %d phases, critical_path=%d, parallelism=%.2f, "
        "cycles=%d, sync_points=%d",
        len(phases), crit_len, parallelism, len(cycles), len(sync_points),
    )
    return report

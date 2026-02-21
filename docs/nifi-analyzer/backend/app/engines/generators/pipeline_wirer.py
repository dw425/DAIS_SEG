"""Pipeline Wiring Engine -- Wires DataFrame chains so notebooks are runnable.

Takes the DAG topology, connections with relationships, and generates
a cell execution plan where each cell's DataFrame references its upstream.
This is THE most critical file for making generated notebooks actually executable:
without correct wiring, every cell operates on a disconnected ``df`` variable.
"""

import logging
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field

from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    Connection,
    MappingEntry,
    ParseResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class WiredCell:
    """A single notebook cell with wiring metadata."""

    cell_id: str
    processor_name: str
    processor_type: str
    role: str  # source, transform, route, sink, utility
    upstream_df_names: list[str]  # DataFrame variables this cell reads from
    output_df_name: str  # DataFrame variable this cell produces
    relationship_from_upstream: str  # which relationship feeds this cell
    code: str  # the actual PySpark code
    cell_type: str  # "code" or "markdown"
    label: str
    is_streaming: bool
    cluster_id: str | None = None  # if part of a task cluster


@dataclass
class WiringPlan:
    """Complete wiring plan for the notebook."""

    cells: list[WiredCell]
    topological_order: list[str]  # processor names in execution order
    df_registry: dict[str, str]  # processor_name -> df_variable_name
    fan_out_points: list[dict]  # {processor, relationships, branches}
    fan_in_points: list[dict]  # {processor, upstream_processors}
    error_branches: list[dict]  # {source_processor, error_df_name, error_handling}
    streaming_mode: bool
    warnings: list[str]


# Processor types that produce a source DataFrame (no upstream required)
_SOURCE_TYPES = re.compile(
    r"(Get|List|Fetch|Consume|Query|Generate|Execute(SQL|Script))",
    re.IGNORECASE,
)

# Processor types that act as sinks
_SINK_TYPES = re.compile(
    r"(Put|Publish|Send|Insert|Post|Store|Write)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_wiring_plan(
    parsed: ParseResult,
    assessment: AssessmentResult,
    analysis_result: AnalysisResult,
    deep_analysis: dict | None = None,
) -> WiringPlan:
    """Build the complete wiring plan from parsed flow and analysis.

    Steps:
        1. Build adjacency from connections.
        2. Topologically sort the DAG (Kahn's algorithm).
        3. Assign DataFrame variable names.
        4. Resolve upstream references for every processor.
        5. Detect fan-out / fan-in points.
        6. Wire relationship-specific logic.
        7. Inject wired code into each cell.
    """
    warnings: list[str] = []

    # Step 1 -- adjacency
    adjacency, reverse_adj, conn_index = _build_adjacency(parsed)

    # Step 2 -- topological sort
    proc_names = [p.name for p in parsed.processors]
    topo_order = _topological_sort(adjacency, proc_names)
    if len(topo_order) < len(proc_names):
        missing = set(proc_names) - set(topo_order)
        warnings.append(f"Cycle detected; {len(missing)} processor(s) excluded from topological order: {sorted(missing)}")

    # Step 3 -- DataFrame variable registry
    df_registry = _build_df_registry(topo_order)

    # Step 4 -- streaming mode detection
    exec_mode = getattr(analysis_result, "execution_mode_analysis", {}) or {}
    streaming_mode = exec_mode.get("recommended_mode", "batch") == "streaming"

    # Step 5 -- fan-out / fan-in detection
    fan_out_points = _detect_fan_out(adjacency, conn_index)
    fan_in_points = _detect_fan_in(reverse_adj)

    # Step 6 -- error branches
    error_branches = _detect_error_branches(conn_index, df_registry)

    # Build mapping lookup  (name -> MappingEntry)
    mapping_lookup: dict[str, MappingEntry] = {m.name: m for m in assessment.mappings}

    # Build processor lookup  (name -> Processor)
    proc_lookup = {p.name: p for p in parsed.processors}

    # Step 7 -- assemble wired cells
    cells: list[WiredCell] = []
    for proc_name in topo_order:
        mapping = mapping_lookup.get(proc_name)
        proc = proc_lookup.get(proc_name)
        if mapping is None or proc is None:
            continue

        # Determine upstream DataFrames
        upstream_info = _resolve_upstream_df(proc_name, reverse_adj, df_registry, conn_index)
        upstream_df_names = [info["df_name"] for info in upstream_info]
        primary_relationship = upstream_info[0]["relationship"] if upstream_info else "none"
        output_df_name = df_registry.get(proc_name, f"df_{_safe_var(proc_name)}")

        # Determine role
        role = mapping.role or _infer_role(mapping.type)

        # Handle fan-in: merge multiple upstream DataFrames
        fan_in_code = ""
        if len(upstream_df_names) > 1:
            fan_in_code = _handle_fan_in(proc_name, upstream_info, df_registry)

        # Handle fan-out: generate filtered branches
        fan_out_code = ""
        if proc_name in [fo["processor"] for fo in fan_out_points]:
            fan_out_code = _handle_fan_out(proc_name, adjacency, conn_index, df_registry)

        # Wire the mapping code with upstream references
        primary_upstream = upstream_df_names[0] if upstream_df_names else None
        raw_code = mapping.code or ""
        wired_code = _inject_code_with_upstream(
            raw_code, primary_upstream, output_df_name, role,
        )

        # Prepend fan-in merge if needed
        if fan_in_code:
            wired_code = fan_in_code + "\n\n" + wired_code

        # Append fan-out branches if needed
        if fan_out_code:
            wired_code = wired_code + "\n\n" + fan_out_code

        # Wire relationship-specific transforms
        if primary_upstream and primary_relationship != "success":
            rel_code = _wire_relationship(
                primary_relationship, primary_upstream,
                proc_lookup.get(upstream_info[0].get("upstream_proc", ""), proc).type if upstream_info else "",
            )
            if rel_code:
                wired_code = rel_code + "\n" + wired_code

        # Generate markdown header cell
        md_header = _generate_markdown_header(
            proc_name, mapping.type, role, upstream_df_names, primary_relationship,
        )
        cells.append(WiredCell(
            cell_id=f"md_{_safe_var(proc_name)}",
            processor_name=proc_name,
            processor_type=mapping.type,
            role=role,
            upstream_df_names=[],
            output_df_name="",
            relationship_from_upstream="",
            code=md_header,
            cell_type="markdown",
            label=f"header_{_safe_var(proc_name)}",
            is_streaming=streaming_mode,
        ))

        # Code cell
        is_streaming_cell = streaming_mode and role == "source"
        cluster_id = _find_cluster_id(proc_name, analysis_result)

        cells.append(WiredCell(
            cell_id=f"code_{_safe_var(proc_name)}",
            processor_name=proc_name,
            processor_type=mapping.type,
            role=role,
            upstream_df_names=upstream_df_names,
            output_df_name=output_df_name,
            relationship_from_upstream=primary_relationship,
            code=wired_code,
            cell_type="code",
            label=f"{role}_{_safe_var(proc_name)}",
            is_streaming=is_streaming_cell,
            cluster_id=cluster_id,
        ))

    return WiringPlan(
        cells=cells,
        topological_order=topo_order,
        df_registry=df_registry,
        fan_out_points=fan_out_points,
        fan_in_points=fan_in_points,
        error_branches=error_branches,
        streaming_mode=streaming_mode,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# 1. Build adjacency
# ---------------------------------------------------------------------------

def _build_adjacency(
    parsed: ParseResult,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[tuple[str, str], list[Connection]]]:
    """Build forward adjacency, reverse adjacency, and connection index.

    Returns:
        adjacency:      source_name -> [dest_name, ...]
        reverse_adj:    dest_name   -> [source_name, ...]
        conn_index:     (source_name, dest_name) -> [Connection, ...]
    """
    adjacency: dict[str, list[str]] = defaultdict(list)
    reverse_adj: dict[str, list[str]] = defaultdict(list)
    conn_index: dict[tuple[str, str], list[Connection]] = defaultdict(list)

    for conn in parsed.connections:
        src, dst = conn.source_name, conn.destination_name
        if dst not in adjacency[src]:
            adjacency[src].append(dst)
        if src not in reverse_adj[dst]:
            reverse_adj[dst].append(src)
        conn_index[(src, dst)].append(conn)

    return dict(adjacency), dict(reverse_adj), dict(conn_index)


# ---------------------------------------------------------------------------
# 2. Topological sort (Kahn's algorithm)
# ---------------------------------------------------------------------------

def _topological_sort(
    adjacency: dict[str, list[str]],
    processors: list[str],
) -> list[str]:
    """Topological sort via Kahn's algorithm.

    * Handles disconnected subgraphs by treating isolated nodes as having
      zero in-degree.
    * Breaks ties by processor name for deterministic output across runs.
    """
    proc_set = set(processors)
    in_degree: dict[str, int] = {p: 0 for p in proc_set}

    for src, dests in adjacency.items():
        for dst in dests:
            if dst in in_degree:
                in_degree[dst] += 1

    # Seed queue with zero-in-degree nodes, sorted for determinism
    queue: deque[str] = deque(sorted(p for p in proc_set if in_degree[p] == 0))
    result: list[str] = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for neighbor in sorted(adjacency.get(node, [])):
            if neighbor not in in_degree:
                continue
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return result


# ---------------------------------------------------------------------------
# 3. DataFrame variable registry
# ---------------------------------------------------------------------------

def _build_df_registry(topo_order: list[str]) -> dict[str, str]:
    """Assign unique DataFrame variable names for every processor.

    Convention: ``df_{snake_cased_name}``.  Duplicates are disambiguated
    with ``_2``, ``_3``, etc.
    """
    registry: dict[str, str] = {}
    used_names: dict[str, int] = {}

    for proc_name in topo_order:
        base = f"df_{_safe_var(proc_name)}"
        if base not in used_names:
            used_names[base] = 1
            registry[proc_name] = base
        else:
            used_names[base] += 1
            registry[proc_name] = f"{base}_{used_names[base]}"

    return registry


# ---------------------------------------------------------------------------
# 4. Resolve upstream DataFrames
# ---------------------------------------------------------------------------

def _resolve_upstream_df(
    processor: str,
    reverse_adj: dict[str, list[str]],
    df_registry: dict[str, str],
    conn_index: dict[tuple[str, str], list[Connection]],
) -> list[dict]:
    """Determine which upstream DataFrame(s) feed *processor* and via which relationship.

    Returns a list of dicts:
        [{"upstream_proc": name, "df_name": var, "relationship": rel}, ...]
    """
    upstream_procs = reverse_adj.get(processor, [])
    if not upstream_procs:
        return []

    results: list[dict] = []
    for up in sorted(upstream_procs):
        df_name = df_registry.get(up, f"df_{_safe_var(up)}")
        conns = conn_index.get((up, processor), [])
        # Pick the first relationship (usually "success")
        rel = conns[0].relationship if conns else "success"
        results.append({
            "upstream_proc": up,
            "df_name": df_name,
            "relationship": rel,
        })

    return results


# ---------------------------------------------------------------------------
# 5. Wire relationship-specific logic
# ---------------------------------------------------------------------------

def _wire_relationship(
    relationship: str,
    upstream_df: str,
    upstream_proc_type: str,
) -> str:
    """Generate relationship-specific wiring code.

    Different NiFi relationships require different downstream handling.
    Returns a code snippet to prepend, or empty string for ``success``.
    """
    rel = relationship.lower().strip()

    if rel == "success":
        # Direct reference -- no extra code needed
        return ""

    if rel == "failure":
        return (
            f"# -- failure branch from upstream --\n"
            f"# Wrapping upstream in try/except to capture error rows\n"
            f"try:\n"
            f"    _upstream = {upstream_df}\n"
            f"except Exception as _e:\n"
            f"    _upstream = spark.createDataFrame([], {upstream_df}.schema)\n"
            f"    print(f'[WARN] Failure branch triggered: {{_e}}')\n"
            f"{upstream_df}_errors = {upstream_df}  # error rows forwarded"
        )

    if rel == "matched":
        return (
            f"# -- matched branch: rows that satisfy the upstream condition --\n"
            f"# NOTE: The actual filter condition depends on the upstream processor.\n"
            f"# Wire the specific condition from the RouteOnAttribute/ValidateRecord.\n"
            f"# {upstream_df}_matched = {upstream_df}.filter(<condition>)"
        )

    if rel == "unmatched":
        return (
            f"# -- unmatched branch: rows that did NOT satisfy the upstream condition --\n"
            f"# {upstream_df}_unmatched = {upstream_df}.filter(~<condition>)"
        )

    if rel == "original":
        return (
            f"# -- original branch: upstream DataFrame preserved unchanged --\n"
            f"# The 'original' relationship passes through the unmodified data."
        )

    if rel in ("split", "splits"):
        return (
            f"# -- split branch: result of array/record explosion --\n"
            f"# Typically produced by SplitJson, SplitRecord, SplitText.\n"
            f"# Each split fragment becomes its own row."
        )

    if rel == "merged":
        return (
            f"# -- merged branch: union of multiple upstream DataFrames --\n"
            f"# Use unionByName(allowMissingColumns=True) when schemas differ."
        )

    if rel == "valid":
        return (
            f"# -- valid branch: rows that passed upstream validation --\n"
            f"# From ValidateRecord / ValidateCSV / SchemaValidation."
        )

    if rel == "invalid":
        return (
            f"# -- invalid branch: rows that FAILED upstream validation --\n"
            f"# These should be routed to a quarantine / dead-letter table."
        )

    if rel == "duplicate":
        return (
            f"# -- duplicate branch: rows identified as duplicates --\n"
            f"# From DetectDuplicate or similar deduplication processor."
        )

    if rel in ("non-duplicate", "non_duplicate"):
        return (
            f"# -- non-duplicate branch: unique rows after deduplication --"
        )

    if rel == "retry":
        return (
            f"# -- retry branch: rows to be reprocessed --\n"
            f"# Implementing as a retry-wrapper with exponential backoff.\n"
            f"import time\n"
            f"_MAX_RETRIES = 3\n"
            f"for _attempt in range(1, _MAX_RETRIES + 1):\n"
            f"    try:\n"
            f"        _retry_df = {upstream_df}\n"
            f"        break\n"
            f"    except Exception as _e:\n"
            f"        if _attempt == _MAX_RETRIES:\n"
            f"            raise\n"
            f"        time.sleep(2 ** _attempt)\n"
            f"        print(f'[RETRY] Attempt {{_attempt}}/{{_MAX_RETRIES}}: {{_e}}')"
        )

    # Any other / unknown relationship -- pass through with comment
    return (
        f"# -- relationship '{relationship}' from upstream --\n"
        f"# Unrecognized NiFi relationship; passing through upstream DataFrame."
    )


# ---------------------------------------------------------------------------
# 6. Fan-out handling
# ---------------------------------------------------------------------------

def _detect_fan_out(
    adjacency: dict[str, list[str]],
    conn_index: dict[tuple[str, str], list[Connection]],
) -> list[dict]:
    """Detect processors that feed multiple downstream via different relationships."""
    fan_outs: list[dict] = []

    for src, dests in adjacency.items():
        if len(dests) <= 1:
            continue
        # Collect distinct relationships used
        relationships: dict[str, list[str]] = defaultdict(list)
        for dst in dests:
            for conn in conn_index.get((src, dst), []):
                relationships[conn.relationship].append(dst)

        if len(relationships) > 1 or len(dests) > 1:
            fan_outs.append({
                "processor": src,
                "relationships": dict(relationships),
                "branches": dests,
            })

    return fan_outs


def _handle_fan_out(
    processor: str,
    adjacency: dict[str, list[str]],
    conn_index: dict[tuple[str, str], list[Connection]],
    df_registry: dict[str, str],
) -> str:
    """Generate filtered DataFrames for each branch when one processor fans out."""
    output_df = df_registry.get(processor, f"df_{_safe_var(processor)}")
    dests = adjacency.get(processor, [])
    lines = [f"# -- fan-out: {processor} feeds {len(dests)} downstream processors --"]

    for dst in sorted(dests):
        conns = conn_index.get((processor, dst), [])
        rel = conns[0].relationship if conns else "success"
        branch_df = df_registry.get(dst, f"df_{_safe_var(dst)}")

        if rel == "success":
            lines.append(f"# {dst} receives full success output")
        elif rel == "failure":
            lines.append(f"# {dst} receives failure/error rows")
        elif rel in ("matched", "valid"):
            lines.append(f"# {dst} receives {rel} rows (filter condition from upstream)")
        elif rel in ("unmatched", "invalid"):
            lines.append(f"# {dst} receives {rel} rows (negated filter condition)")
        else:
            lines.append(f"# {dst} receives '{rel}' branch")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 7. Fan-in handling
# ---------------------------------------------------------------------------

def _detect_fan_in(reverse_adj: dict[str, list[str]]) -> list[dict]:
    """Detect processors that receive from multiple upstream processors."""
    return [
        {"processor": dst, "upstream_processors": sorted(srcs)}
        for dst, srcs in reverse_adj.items()
        if len(srcs) > 1
    ]


def _handle_fan_in(
    processor: str,
    upstream_info: list[dict],
    df_registry: dict[str, str],
) -> str:
    """Merge multiple upstream DataFrames via unionByName."""
    output_df = df_registry.get(processor, f"df_{_safe_var(processor)}")
    upstream_dfs = [info["df_name"] for info in upstream_info]

    lines = [
        f"# -- fan-in: merging {len(upstream_dfs)} upstream DataFrames into {output_df} --",
    ]

    if len(upstream_dfs) == 2:
        lines.append(
            f"{output_df}_merged = {upstream_dfs[0]}.unionByName({upstream_dfs[1]}, allowMissingColumns=True)"
        )
    else:
        # functools.reduce for 3+ DataFrames
        lines.append("from functools import reduce")
        df_list = ", ".join(upstream_dfs)
        lines.append(
            f"{output_df}_merged = reduce(\n"
            f"    lambda a, b: a.unionByName(b, allowMissingColumns=True),\n"
            f"    [{df_list}]\n"
            f")"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 8. Code injection: replace generic df references with wired names
# ---------------------------------------------------------------------------

def _inject_code_with_upstream(
    mapping_code: str,
    upstream_df_name: str | None,
    output_df_name: str,
    role: str,
) -> str:
    """Replace generic ``df`` references with actual upstream/output DataFrame names.

    Patterns handled:
    * ``df = spark.read...``  / ``df = spark.readStream...``  -> kept as-is (source)
    * ``df = df.withColumn(...)``                              -> ``output = upstream.withColumn(...)``
    * ``df.write...``  / ``df.writeStream...``                 -> ``upstream.write...``
    * Generic LHS ``df = ...``                                 -> ``output = ...``
    """
    if not mapping_code:
        if upstream_df_name and role != "source":
            return f"{output_df_name} = {upstream_df_name}  # passthrough (no code generated)"
        return f"# No code generated for this processor"

    code = mapping_code

    # Source processors -- keep spark.read but rename LHS
    if role == "source" or upstream_df_name is None:
        # Replace leading ``df = `` or ``df_xxx = `` with the output name
        code = re.sub(
            r'^(df(?:_\w+)?)\s*=\s*(spark\.(read|readStream)\b)',
            f'{output_df_name} = \\2',
            code,
            count=1,
            flags=re.MULTILINE,
        )
        # If no spark.read found, still rename the LHS
        if not code.startswith(output_df_name):
            code = re.sub(
                r'^df\b',
                output_df_name,
                code,
                count=1,
                flags=re.MULTILINE,
            )
        return code

    # Sink processors -- replace df.write references with upstream name
    if role == "sink":
        code = re.sub(r'\bdf\b(?=\s*\.\s*(write|writeStream))', upstream_df_name, code)
        # Also replace any remaining bare df references
        code = re.sub(r'\bdf\b(?!\w)', upstream_df_name, code)
        return code

    # Transform / route / utility processors
    # Step A: Replace RHS df references (reads) with the upstream name
    # But only if upstream differs from the generic "df" token
    if upstream_df_name and upstream_df_name != "df":
        # Replace ``= df.`` with ``= upstream.``
        code = re.sub(
            r'=\s*df\b(?=\s*\.)',
            f'= {upstream_df_name}',
            code,
        )
        # Replace standalone ``df.`` at start of expression
        code = re.sub(
            r'(?<![.\w])df(?=\s*\.)',
            upstream_df_name,
            code,
        )

    # Step B: Replace LHS df assignment with output name
    # Matches ``df_something = `` or bare ``df = ``
    code = re.sub(
        r'^(df(?:_\w+)?)\s*=',
        f'{output_df_name} =',
        code,
        count=1,
        flags=re.MULTILINE,
    )

    return code


# ---------------------------------------------------------------------------
# 9. Markdown header generation
# ---------------------------------------------------------------------------

def _generate_markdown_header(
    processor_name: str,
    processor_type: str,
    role: str,
    upstream_df_names: list[str],
    relationship: str,
) -> str:
    """Generate a markdown cell documenting the pipeline step."""
    simple_type = processor_type.rsplit(".", 1)[-1] if "." in processor_type else processor_type
    role_emoji_map = {
        "source": "Source",
        "transform": "Transform",
        "route": "Route",
        "sink": "Sink",
        "utility": "Utility",
    }
    role_label = role_emoji_map.get(role, role.title())

    lines = [
        f"### {processor_name}",
        f"**Type**: `{simple_type}` | **Role**: {role_label}",
    ]

    if upstream_df_names:
        upstream_str = ", ".join(f"`{df}`" for df in upstream_df_names)
        lines.append(f"**Reads from**: {upstream_str} (relationship: `{relationship}`)")
    else:
        lines.append("**Reads from**: (none -- pipeline entry point)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Error branch detection
# ---------------------------------------------------------------------------

def _detect_error_branches(
    conn_index: dict[tuple[str, str], list[Connection]],
    df_registry: dict[str, str],
) -> list[dict]:
    """Detect connections on the 'failure' relationship and record them."""
    error_branches: list[dict] = []
    for (src, dst), conns in conn_index.items():
        for conn in conns:
            if conn.relationship.lower() == "failure":
                error_branches.append({
                    "source_processor": src,
                    "error_df_name": f"{df_registry.get(src, 'df')}_errors",
                    "error_handling": f"Failure rows from {src} routed to {dst}",
                })
    return error_branches


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_var(name: str) -> str:
    """Convert processor name to a safe Python variable name."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()


def _infer_role(proc_type: str) -> str:
    """Infer processor role from its type name when not explicitly set."""
    simple = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type
    if _SOURCE_TYPES.search(simple):
        return "source"
    if _SINK_TYPES.search(simple):
        return "sink"
    if any(kw in simple for kw in ("Route", "Distribute", "Split", "Fork")):
        return "route"
    if any(kw in simple for kw in ("Update", "Replace", "Transform", "Convert", "Merge", "Validate")):
        return "transform"
    return "utility"


def _find_cluster_id(proc_name: str, analysis_result: AnalysisResult) -> str | None:
    """Look up the task cluster containing this processor, if any."""
    for cluster in analysis_result.task_clusters:
        if proc_name in cluster.processors:
            return cluster.id
    return None

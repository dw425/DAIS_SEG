"""Flow comparator — diff two ParseResults to find structural changes.

Compares processors and connections between two flow versions,
identifying additions, removals, and modifications.
"""

from __future__ import annotations

import logging

from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)


def compare_flows(flow_a: ParseResult, flow_b: ParseResult) -> dict:
    """Compare two ParseResults and return a structural diff.

    Returns:
        {
            added: list[dict],
            removed: list[dict],
            modified: list[dict],
            addedConnections: int,
            removedConnections: int,
            summary: str,
        }
    """
    # Index processors by name
    procs_a = {p.name: p for p in flow_a.processors}
    procs_b = {p.name: p for p in flow_b.processors}

    names_a = set(procs_a.keys())
    names_b = set(procs_b.keys())

    # Added processors (in B but not A)
    added = [
        {"name": name, "type": procs_b[name].type, "group": procs_b[name].group}
        for name in sorted(names_b - names_a)
    ]

    # Removed processors (in A but not B)
    removed = [
        {"name": name, "type": procs_a[name].type, "group": procs_a[name].group}
        for name in sorted(names_a - names_b)
    ]

    # Modified processors (same name, different type or properties)
    modified = []
    for name in sorted(names_a & names_b):
        pa = procs_a[name]
        pb = procs_b[name]
        changes: list[str] = []
        if pa.type != pb.type:
            changes.append(f"type: {pa.type} -> {pb.type}")
        # Compare properties
        props_a = pa.properties
        props_b = pb.properties
        added_props = set(props_b.keys()) - set(props_a.keys())
        removed_props = set(props_a.keys()) - set(props_b.keys())
        changed_props = [
            k for k in set(props_a.keys()) & set(props_b.keys())
            if str(props_a.get(k, "")) != str(props_b.get(k, ""))
        ]
        if added_props:
            changes.append(f"added properties: {', '.join(sorted(added_props)[:5])}")
        if removed_props:
            changes.append(f"removed properties: {', '.join(sorted(removed_props)[:5])}")
        if changed_props:
            changes.append(f"changed properties: {', '.join(sorted(changed_props)[:5])}")
        if pa.group != pb.group:
            changes.append(f"group: {pa.group} -> {pb.group}")
        if changes:
            modified.append({
                "name": name,
                "type": pb.type,
                "changes": changes,
            })

    # Connections diff
    conns_a = {(c.source_name, c.destination_name, c.relationship) for c in flow_a.connections}
    conns_b = {(c.source_name, c.destination_name, c.relationship) for c in flow_b.connections}
    added_conns = len(conns_b - conns_a)
    removed_conns = len(conns_a - conns_b)

    # Summary
    parts = []
    if added:
        parts.append(f"{len(added)} processor(s) added")
    if removed:
        parts.append(f"{len(removed)} processor(s) removed")
    if modified:
        parts.append(f"{len(modified)} processor(s) modified")
    if added_conns:
        parts.append(f"{added_conns} connection(s) added")
    if removed_conns:
        parts.append(f"{removed_conns} connection(s) removed")
    summary = "; ".join(parts) if parts else "No differences found"

    logger.info("Flow comparison: %s", summary)
    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "addedConnections": added_conns,
        "removedConnections": removed_conns,
        "summary": summary,
    }

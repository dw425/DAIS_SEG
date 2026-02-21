"""NiFi Registry JSON parser — ported from nifi-registry-json.js.

Parses NiFi Registry JSON flow exports (versioned process group snapshots).
"""

import json
import logging
import re

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)

_NIFI_PKG_RE = re.compile(r"^org\.apache\.nifi\.processors?\.\w+\.")


def parse_nifi_json(content: bytes, filename: str) -> ParseResult:
    """Parse a NiFi Registry JSON export."""
    data = json.loads(content)

    # Unwrap common wrappers
    if "versionedFlowSnapshot" in data:
        data = data["versionedFlowSnapshot"]
    if "flowContents" in data:
        data = data["flowContents"]
    # If top level has 'flow' key (processGroupFlow format)
    if "flow" in data and isinstance(data["flow"], dict):
        data = data["flow"]

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    controller_services: list[ControllerService] = []
    warnings: list[Warning] = []

    def walk(group: dict, parent_name: str) -> None:
        g_name = group.get("name") or parent_name or "root"
        if group.get("name"):
            process_groups.append(ProcessGroup(name=g_name))

        for p in group.get("processors", []):
            props: dict[str, str] = {}
            raw_props = p.get("properties", {})
            if isinstance(raw_props, list):
                for pr in raw_props:
                    if pr.get("name") is not None:
                        props[pr["name"]] = str(pr.get("value", ""))
            elif isinstance(raw_props, dict):
                for k, v in raw_props.items():
                    if v is not None:
                        props[k] = str(v)

            full_type = p.get("type", "")
            short_type = _NIFI_PKG_RE.sub("", full_type) if full_type else "Unknown"
            _proc_id = p.get("identifier") or p.get("id") or f"p_{len(processors)}"  # noqa: F841

            sched_strategy = p.get("schedulingStrategy", "TIMER_DRIVEN")
            sched_period = p.get("schedulingPeriod", "0 sec")

            processors.append(
                Processor(
                    name=p.get("name") or short_type,
                    type=short_type,
                    platform="nifi",
                    properties=props,
                    group=g_name,
                    state=p.get("scheduledState") or p.get("state") or "STOPPED",
                    scheduling={"strategy": sched_strategy, "period": sched_period},
                )
            )

        for c in group.get("connections", []):
            src = c.get("source", {})
            dst = c.get("destination", {})
            src_id = src.get("id") or c.get("sourceId", "")
            dst_id = dst.get("id") or c.get("destinationId", "")
            src_name = src.get("name") or c.get("sourceName", "")
            dst_name = dst.get("name") or c.get("destinationName", "")
            rels = c.get("selectedRelationships", [])
            connections.append(
                Connection(
                    source_name=src_name or src_id,
                    destination_name=dst_name or dst_id,
                    relationship=",".join(rels) if rels else "success",
                )
            )

        for cs in group.get("controllerServices", []):
            cs_type = cs.get("type", "")
            controller_services.append(
                ControllerService(
                    name=cs.get("name") or cs_type.rsplit(".", 1)[-1],
                    type=cs_type.rsplit(".", 1)[-1] if cs_type else "Unknown",
                    properties=cs.get("properties", {}),
                )
            )

        for pg in group.get("processGroups", []):
            walk(pg, pg.get("name") or g_name)

    walk(data, "root")

    # Resolve connection names from processor list
    id_to_name: dict[str, str] = {}
    for p in processors:
        if hasattr(p, "_id"):
            id_to_name[p._id] = p.name
    resolved: list[Connection] = []
    for c in connections:
        src = id_to_name.get(c.source_name, c.source_name)
        dst = id_to_name.get(c.destination_name, c.destination_name)
        resolved.append(Connection(source_name=src, destination_name=dst, relationship=c.relationship))

    # Populate process group processor lists
    group_map: dict[str, list[str]] = {}
    for p in processors:
        group_map.setdefault(p.group, []).append(p.name)
    for pg in process_groups:
        pg.processors = group_map.get(pg.name, [])

    if not processors:
        warnings.append(Warning(severity="warning", message="No processors found in JSON", source=filename))

    return ParseResult(
        platform="nifi",
        version="",
        processors=processors,
        connections=resolved,
        process_groups=process_groups,
        controller_services=controller_services,
        metadata={"source_file": filename, "format": "nifi_registry_json"},
        warnings=warnings,
    )

"""NiFi Registry JSON parser — ported from nifi-registry-json.js.

Parses NiFi Registry JSON flow exports (versioned process group snapshots).
Includes Phase 1 enhancements: parameter contexts, controller service resolution,
FlowFile attribute tracking, and backpressure extraction.
"""

import json
import logging
import re

from app.models.pipeline import ParameterContext, ParameterEntry, ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)

_NIFI_PKG_RE = re.compile(r"^org\.apache\.nifi\.processors?\.\w+\.")

# Controller service types that represent JDBC connection pools
_DBCP_SERVICE_TYPES = {
    "DBCPConnectionPool",
    "DBCPConnectionPoolLookup",
    "HikariCPConnectionPool",
}

# JDBC property keys commonly found in DBCPConnectionPool
_JDBC_PROPERTY_KEYS = {
    "Database Connection URL",
    "Database Driver Class Name",
    "Database User",
    "Password",
    "database-connection-url",
    "database-driver-class-name",
    "db-user",
}


def _infer_parameter_type(key: str, value: str, sensitive: bool) -> str:
    """Infer parameter type: 'secret', 'numeric', or 'string'."""
    if sensitive:
        return "secret"
    lower_key = key.lower()
    if any(kw in lower_key for kw in ("password", "secret", "token", "key", "credential")):
        return "secret"
    # Numeric detection
    stripped = value.strip()
    if stripped:
        try:
            float(stripped)
            return "numeric"
        except ValueError:
            pass
    return "string"


def _make_databricks_variable_name(context_name: str, param_key: str) -> str:
    """Generate a Databricks Asset Bundle variable name from a NiFi parameter."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", f"{context_name}_{param_key}").strip("_").lower()
    return slug


def _extract_parameter_contexts(data: dict, root_data: dict) -> list[ParameterContext]:
    """Extract parameterContexts from NiFi flow JSON.

    Looks in multiple locations:
    - data["parameterContexts"] (versioned flow snapshot)
    - root_data["parameterContexts"] (top-level)
    - root_data["parameterProviders"] (NiFi 1.15+)
    """
    contexts: list[ParameterContext] = []
    raw_contexts = []

    # Try multiple locations
    for source in [data, root_data]:
        if isinstance(source, dict) and "parameterContexts" in source:
            raw = source["parameterContexts"]
            if isinstance(raw, list):
                raw_contexts.extend(raw)
            elif isinstance(raw, dict):
                raw_contexts.append(raw)

    seen_names: set[str] = set()
    for ctx in raw_contexts:
        ctx_name = ctx.get("name", "")
        if not ctx_name or ctx_name in seen_names:
            continue
        seen_names.add(ctx_name)

        params: list[ParameterEntry] = []
        for p in ctx.get("parameters", []):
            # Parameters can be nested under a "parameter" key
            param_data = p.get("parameter", p)
            key = param_data.get("name", param_data.get("key", ""))
            value = str(param_data.get("value", ""))
            sensitive = param_data.get("sensitive", False)
            if not key:
                continue
            inferred = _infer_parameter_type(key, value, sensitive)
            params.append(
                ParameterEntry(
                    key=key,
                    value="" if sensitive else value,
                    sensitive=sensitive,
                    inferred_type=inferred,
                    databricks_variable=_make_databricks_variable_name(ctx_name, key),
                )
            )

        contexts.append(ParameterContext(name=ctx_name, parameters=params))

    return contexts


def _build_controller_service_index(
    controller_services: list[ControllerService],
) -> dict[str, dict]:
    """Build an index mapping controller service names to their properties.

    Returns: {service_name: {"type": ..., "properties": {...}}}
    """
    index: dict[str, dict] = {}
    for cs in controller_services:
        index[cs.name] = {
            "type": cs.type,
            "properties": dict(cs.properties),
        }
    return index


def _resolve_processor_services(
    processor: Processor,
    cs_index: dict[str, dict],
) -> dict | None:
    """If a processor references a controller service (e.g. DBCPConnectionPool),
    resolve the actual service properties and return them.

    Returns resolved service dict or None.
    """
    resolved: dict[str, dict] = {}

    for prop_key, prop_val in processor.properties.items():
        if not prop_val or not isinstance(prop_val, str):
            continue
        # Check if the property value matches a known controller service name
        if prop_val in cs_index:
            cs_entry = cs_index[prop_val]
            service_info: dict = {"service_name": prop_val, "service_type": cs_entry["type"]}
            # For DBCP services, extract JDBC-specific properties
            if cs_entry["type"] in _DBCP_SERVICE_TYPES:
                for jdbc_key in _JDBC_PROPERTY_KEYS:
                    if jdbc_key in cs_entry["properties"]:
                        safe_key = jdbc_key.lower().replace(" ", "_").replace("-", "_")
                        service_info[safe_key] = cs_entry["properties"][jdbc_key]
            else:
                # Include all non-sensitive properties
                for sk, sv in cs_entry["properties"].items():
                    if sv and "password" not in sk.lower() and "secret" not in sk.lower():
                        service_info[sk] = sv
            resolved[prop_key] = service_info

    return resolved if resolved else None


def parse_nifi_json(content: bytes, filename: str) -> ParseResult:
    """Parse a NiFi Registry JSON export."""
    try:
        raw_data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {filename}: {exc}") from exc
    root_data = raw_data  # keep reference for parameter context search

    data = raw_data
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
                    # Include all properties: None becomes "" to match list-format
                    # behavior and preserve property keys for attribute flow analysis
                    props[k] = str(v) if v is not None else ""

            full_type = p.get("type", "")
            short_type = _NIFI_PKG_RE.sub("", full_type) if full_type else "Unknown"
            proc_id = p.get("identifier") or p.get("id") or f"p_{len(processors)}"

            sched_strategy = p.get("schedulingStrategy", "TIMER_DRIVEN")
            sched_period = p.get("schedulingPeriod", "0 sec")

            proc = Processor(
                name=p.get("name") or short_type,
                type=short_type,
                platform="nifi",
                properties=props,
                group=g_name,
                state=p.get("scheduledState") or p.get("state") or "STOPPED",
                scheduling={"strategy": sched_strategy, "period": sched_period},
            )
            proc._internal_id = proc_id  # stash for connection resolution
            processors.append(proc)

        for c in group.get("connections", []):
            src = c.get("source", {})
            dst = c.get("destination", {})
            src_id = src.get("id") or c.get("sourceId", "")
            dst_id = dst.get("id") or c.get("destinationId", "")
            src_name = src.get("name") or c.get("sourceName", "")
            dst_name = dst.get("name") or c.get("destinationName", "")
            rels = c.get("selectedRelationships", [])

            # Extract backpressure configuration
            bp_obj = c.get("backPressureObjectThreshold", 0)
            bp_data = c.get("backPressureDataSizeThreshold", "")

            # Safely coerce backpressure object threshold (NiFi API may return string or int)
            try:
                bp_obj_int = int(bp_obj) if bp_obj else 0
            except (ValueError, TypeError):
                bp_obj_int = 0

            connections.append(
                Connection(
                    source_name=src_name or src_id,
                    destination_name=dst_name or dst_id,
                    relationship=",".join(rels) if rels else "success",
                    back_pressure_object_threshold=bp_obj_int,
                    back_pressure_data_size_threshold=str(bp_data) if bp_data else "",
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
        if hasattr(p, "_internal_id"):
            id_to_name[p._internal_id] = p.name
    resolved: list[Connection] = []
    for c in connections:
        src = id_to_name.get(c.source_name, c.source_name)
        dst = id_to_name.get(c.destination_name, c.destination_name)
        resolved.append(
            Connection(
                source_name=src,
                destination_name=dst,
                relationship=c.relationship,
                back_pressure_object_threshold=c.back_pressure_object_threshold,
                back_pressure_data_size_threshold=c.back_pressure_data_size_threshold,
            )
        )

    # Populate process group processor lists
    group_map: dict[str, list[str]] = {}
    for p in processors:
        group_map.setdefault(p.group, []).append(p.name)
    for pg in process_groups:
        pg.processors = group_map.get(pg.name, [])

    # Phase 1: Extract parameter contexts
    parameter_contexts = _extract_parameter_contexts(data, root_data)

    # Phase 1: Resolve controller service references on processors
    cs_index = _build_controller_service_index(controller_services)
    for proc in processors:
        resolved_svc = _resolve_processor_services(proc, cs_index)
        if resolved_svc:
            proc.resolved_services = resolved_svc

    if not processors:
        warnings.append(Warning(severity="warning", message="No processors found in JSON", source=filename))

    return ParseResult(
        platform="nifi",
        version="",
        processors=processors,
        connections=resolved,
        process_groups=process_groups,
        controller_services=controller_services,
        parameter_contexts=parameter_contexts,
        metadata={"source_file": filename, "format": "nifi_registry_json"},
        warnings=warnings,
    )

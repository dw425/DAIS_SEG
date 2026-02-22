"""NiFi 2.x JSON parser — handles flow.json.gz (gzip-compressed JSON).

NiFi 2.x uses a new JSON-based flow definition format with:
- rootGroup containing nested processGroups
- Component IDs instead of names in connections
- Parameter providers (new in 2.x)
- Property descriptors with explicit type info
"""

import gzip
import json
import logging
import re
from io import BytesIO

from app.models.pipeline import ParameterContext, ParameterEntry, ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)

_NIFI_PKG_RE = re.compile(r"^org\.apache\.nifi\.processors?\.\w+\.")


def _short_type(full_type: str) -> str:
    """Extract short processor type from fully-qualified Java class name."""
    if not full_type:
        return "Unknown"
    return _NIFI_PKG_RE.sub("", full_type) if "." in full_type else full_type


def _decompress_gz(content: bytes) -> bytes:
    """Decompress gzip content. Returns original bytes if not gzip."""
    if content[:2] == b"\x1f\x8b":  # gzip magic bytes
        try:
            return gzip.decompress(content)
        except (gzip.BadGzipFile, OSError) as exc:
            logger.warning("Failed to decompress gzip: %s", exc)
    return content


def _extract_parameter_contexts_v2(data: dict) -> list[ParameterContext]:
    """Extract parameter contexts from NiFi 2.x flow JSON.

    NiFi 2.x stores parameter contexts at the top level or within
    the rootGroup. Also supports parameter providers.
    """
    contexts: list[ParameterContext] = []
    raw_contexts = data.get("parameterContexts", [])
    if not isinstance(raw_contexts, list):
        raw_contexts = [raw_contexts] if raw_contexts else []

    seen_names: set[str] = set()
    for ctx in raw_contexts:
        if not isinstance(ctx, dict):
            continue
        ctx_name = ctx.get("name", "")
        if not ctx_name or ctx_name in seen_names:
            continue
        seen_names.add(ctx_name)

        params: list[ParameterEntry] = []
        for p in ctx.get("parameters", []):
            param_data = p.get("parameter", p)
            key = param_data.get("name", param_data.get("key", ""))
            value = str(param_data.get("value", ""))
            sensitive = param_data.get("sensitive", False)
            if not key:
                continue

            # Infer type
            inferred = "secret" if sensitive else "string"
            lower_key = key.lower()
            if any(kw in lower_key for kw in ("password", "secret", "token", "key", "credential")):
                inferred = "secret"
            elif value.strip():
                try:
                    float(value.strip())
                    inferred = "numeric"
                except ValueError:
                    pass

            slug = re.sub(r"[^a-zA-Z0-9]+", "_", f"{ctx_name}_{key}").strip("_").lower()
            params.append(
                ParameterEntry(
                    key=key,
                    value="" if sensitive else value,
                    sensitive=sensitive,
                    inferred_type=inferred,
                    databricks_variable=slug,
                )
            )

        contexts.append(ParameterContext(name=ctx_name, parameters=params))

    # Also check for parameter providers (NiFi 2.x feature)
    providers = data.get("parameterProviders", [])
    if isinstance(providers, list):
        for provider in providers:
            if not isinstance(provider, dict):
                continue
            prov_name = provider.get("name", "provider")
            if prov_name in seen_names:
                continue
            seen_names.add(prov_name)
            # Parameter providers don't have inline params, just record them
            contexts.append(ParameterContext(name=f"[provider] {prov_name}", parameters=[]))

    return contexts


def _resolve_properties_v2(raw_props: dict | list | None) -> dict[str, str]:
    """Resolve NiFi 2.x property descriptors to flat key-value pairs.

    NiFi 2.x can store properties as:
    - dict of {key: value} (simple)
    - dict of {key: {value: ..., descriptor: ...}} (with descriptors)
    - list of {name: ..., value: ...}
    """
    props: dict[str, str] = {}
    if not raw_props:
        return props

    if isinstance(raw_props, list):
        for item in raw_props:
            if isinstance(item, dict) and "name" in item:
                props[item["name"]] = str(item.get("value", ""))
        return props

    if isinstance(raw_props, dict):
        for key, val in raw_props.items():
            if isinstance(val, dict):
                # NiFi 2.x property descriptor format
                props[key] = str(val.get("value", val.get("rawValue", "")))
            elif val is not None:
                props[key] = str(val)
            else:
                props[key] = ""
    return props


def parse_nifi_v2_json(content: bytes, filename: str) -> ParseResult:
    """Parse a NiFi 2.x JSON flow definition (flow.json or flow.json.gz)."""
    # Decompress if gzip
    raw_bytes = _decompress_gz(content)

    try:
        data = json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {filename}: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    controller_services: list[ControllerService] = []
    warnings: list[Warning] = []

    # Detect NiFi version from metadata
    nifi_version = ""
    if "encodingVersion" in data:
        nifi_version = str(data["encodingVersion"])
    elif "header" in data and isinstance(data["header"], dict):
        nifi_version = data["header"].get("niFiVersion", "")

    # Navigate to root group — NiFi 2.x structure
    root_group = data
    if "rootGroup" in data:
        root_group = data["rootGroup"]
    elif "flowContents" in data:
        root_group = data["flowContents"]

    def walk(group: dict, parent_path: str = "") -> None:
        g_name = group.get("name", "") or parent_path or "root"
        group_path = f"{parent_path}/{g_name}" if parent_path else g_name

        if group.get("name"):
            process_groups.append(ProcessGroup(name=g_name))

        # Process processors
        for p in group.get("processors", []):
            full_type = p.get("type", "")
            short = _short_type(full_type)
            proc_id = p.get("identifier") or p.get("id") or f"p_{len(processors)}"

            # NiFi 2.x may store properties in a nested structure
            raw_props = p.get("properties", {})
            props = _resolve_properties_v2(raw_props)

            # Also check propertyDescriptors for additional info
            descriptors = p.get("propertyDescriptors", {})
            if isinstance(descriptors, dict):
                for desc_key, desc_val in descriptors.items():
                    if isinstance(desc_val, dict) and desc_key not in props:
                        default_val = desc_val.get("defaultValue", "")
                        if default_val:
                            props[desc_key] = str(default_val)

            # Scheduling
            sched_strategy = p.get("schedulingStrategy", "TIMER_DRIVEN")
            sched_period = p.get("schedulingPeriod", "0 sec")
            # NiFi 2.x may nest scheduling config
            sched_config = p.get("config", {})
            if isinstance(sched_config, dict):
                sched_strategy = sched_config.get("schedulingStrategy", sched_strategy)
                sched_period = sched_config.get("schedulingPeriod", sched_period)

            proc = Processor(
                name=p.get("name") or short,
                type=short,
                platform="nifi",
                properties=props,
                group=g_name,
                state=p.get("scheduledState") or p.get("state") or "STOPPED",
                scheduling={"strategy": sched_strategy, "period": sched_period},
            )
            proc._internal_id = proc_id
            processors.append(proc)

        # Process connections — NiFi 2.x uses source/destination with component IDs
        for c in group.get("connections", []):
            src = c.get("source", {})
            dst = c.get("destination", {})

            # NiFi 2.x connection format
            if isinstance(src, dict):
                src_id = src.get("id", src.get("componentId", ""))
                src_name = src.get("name", "")
                src_group = src.get("groupId", "")
            else:
                src_id = str(src)
                src_name = ""
                src_group = ""

            if isinstance(dst, dict):
                dst_id = dst.get("id", dst.get("componentId", ""))
                dst_name = dst.get("name", "")
                dst_group = dst.get("groupId", "")
            else:
                dst_id = str(dst)
                dst_name = ""
                dst_group = ""

            # Fallback to direct ID fields
            src_id = src_id or c.get("sourceId", "")
            dst_id = dst_id or c.get("destinationId", "")

            rels = c.get("selectedRelationships", [])
            if isinstance(rels, str):
                rels = [rels]

            # Backpressure
            bp_obj = c.get("backPressureObjectThreshold", 0)
            bp_data = c.get("backPressureDataSizeThreshold", "")
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

        # Process controller services
        for cs in group.get("controllerServices", []):
            cs_type = cs.get("type", "")
            cs_props = _resolve_properties_v2(cs.get("properties", {}))
            controller_services.append(
                ControllerService(
                    name=cs.get("name") or cs_type.rsplit(".", 1)[-1],
                    type=cs_type.rsplit(".", 1)[-1] if cs_type else "Unknown",
                    properties=cs_props,
                )
            )

        # Recurse into nested process groups
        for pg in group.get("processGroups", []):
            walk(pg, group_path)

    walk(root_group)

    # Resolve connection names from processor IDs
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

    # Extract parameter contexts
    parameter_contexts = _extract_parameter_contexts_v2(data)

    if not processors:
        warnings.append(Warning(severity="warning", message="No processors found in NiFi 2.x JSON", source=filename))

    return ParseResult(
        platform="nifi",
        version=nifi_version,
        processors=processors,
        connections=resolved,
        process_groups=process_groups,
        controller_services=controller_services,
        parameter_contexts=parameter_contexts,
        metadata={
            "source_file": filename,
            "format": "nifi_v2_json",
            "nifi_version": nifi_version,
        },
        warnings=warnings,
    )

"""NiFi XML parser — ported from nifi-xml-parser.js + nifi-xml-helpers.js.

Parses NiFi templates, flowController exports, and registry XML exports.
Uses lxml.etree for XML DOM traversal.
"""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def _get_child_text(el: etree._Element, tag: str) -> str:
    """Get trimmed text of a direct child element (port of getChildText)."""
    child = el.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _extract_properties(el: etree._Element) -> dict:
    """Extract key-value properties from a processor/service element.

    Handles multiple NiFi XML formats:
      - Template: config/properties/entry/key + value
      - Snippet: properties/entry/key + value
      - flowController: property/name + value
    """
    props: dict[str, str] = {}

    # Template format: config > properties > entry > key + value
    config = el.find("config")
    if config is not None:
        properties = config.find("properties")
        if properties is not None:
            for entry in properties.findall("entry"):
                key_el = entry.find("key")
                val_el = entry.find("value")
                if key_el is not None and key_el.text and val_el is not None:
                    props[key_el.text] = val_el.text or ""

    # Direct properties > entry (snippet level)
    if not props:
        properties = el.find("properties")
        if properties is not None:
            for entry in properties.findall("entry"):
                key_el = entry.find("key")
                val_el = entry.find("value")
                if key_el is not None and key_el.text and val_el is not None:
                    props[key_el.text] = val_el.text or ""

    # flowController format: direct property/name + value children
    if not props:
        for prop in el.findall("property"):
            name_el = prop.find("name")
            val_el = prop.find("value")
            if name_el is not None and name_el.text:
                props[name_el.text] = (val_el.text or "") if val_el is not None else ""

    return props


def parse_nifi_xml(content: bytes, filename: str) -> ParseResult:
    """Parse a NiFi XML document into a normalized ParseResult."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []
    id_to_name: dict[str, str] = {}

    def _find_children(parent: etree._Element, plural: str, singular: str) -> list[etree._Element]:
        """Find child elements handling both plural wrapper and singular tags."""
        children = list(parent.findall(plural))
        if not children:
            children = list(parent.findall(singular))
        return children

    def _extract_from_group(group_el: etree._Element, group_name: str) -> None:
        contents = group_el.find("contents")
        if contents is None:
            contents = group_el

        # Processors
        proc_els = _find_children(contents, "processors", "processor")
        for proc in proc_els:
            name = _get_child_text(proc, "name")
            full_type = _get_child_text(proc, "type") or _get_child_text(proc, "class")
            short_type = full_type.rsplit(".", 1)[-1] if full_type else ""
            state = _get_child_text(proc, "state")
            props = _extract_properties(proc)
            proc_id = _get_child_text(proc, "id")

            # Scheduling
            config_el = proc.find("config")
            sched_period = ""
            sched_strategy = ""
            if config_el is not None:
                sp = config_el.find("schedulingPeriod")
                ss = config_el.find("schedulingStrategy")
                sched_period = sp.text.strip() if sp is not None and sp.text else ""
                sched_strategy = ss.text.strip() if ss is not None and ss.text else ""
            if not sched_period:
                sched_period = _get_child_text(proc, "schedulingPeriod")
            if not sched_strategy:
                sched_strategy = _get_child_text(proc, "schedulingStrategy")

            if proc_id:
                id_to_name[proc_id] = name or short_type

            scheduling = None
            if sched_period or sched_strategy:
                scheduling = {"period": sched_period, "strategy": sched_strategy}

            processors.append(
                Processor(
                    name=name or short_type,
                    type=short_type,
                    platform="nifi",
                    properties=props,
                    group=group_name,
                    state=state or "RUNNING",
                    scheduling=scheduling,
                )
            )

        # Connections
        conn_els = _find_children(contents, "connections", "connection")
        for conn in conn_els:
            src_el = conn.find("source")
            dst_el = conn.find("destination")
            src_id = ""
            dst_id = ""
            if src_el is not None:
                sid = src_el.find("id")
                src_id = sid.text.strip() if sid is not None and sid.text else ""
            if not src_id:
                src_id = _get_child_text(conn, "sourceId")
            if dst_el is not None:
                did = dst_el.find("id")
                dst_id = did.text.strip() if did is not None and did.text else ""
            if not dst_id:
                dst_id = _get_child_text(conn, "destinationId")

            rels: list[str] = []
            for r in conn.findall("selectedRelationships"):
                if r.text:
                    rels.append(r.text.strip())
            if not rels:
                for r in conn.findall("relationship"):
                    if r.text:
                        rels.append(r.text.strip())

            connections.append(
                Connection(
                    source_name=src_id,
                    destination_name=dst_id,
                    relationship=",".join(rels) if rels else "success",
                )
            )

        # Input ports
        for port_tag in ["inputPorts/inputPort", "inputPort"]:
            for p in contents.findall(port_tag):
                pid = _get_child_text(p, "id")
                pname = _get_child_text(p, "name") or "InputPort"
                if pid:
                    id_to_name[pid] = pname
                    processors.append(
                        Processor(
                            name=pname,
                            type="InputPort",
                            platform="nifi",
                            group=group_name,
                            properties={},
                        )
                    )

        # Output ports
        for port_tag in ["outputPorts/outputPort", "outputPort"]:
            for p in contents.findall(port_tag):
                pid = _get_child_text(p, "id")
                pname = _get_child_text(p, "name") or "OutputPort"
                if pid:
                    id_to_name[pid] = pname
                    processors.append(
                        Processor(
                            name=pname,
                            type="OutputPort",
                            platform="nifi",
                            group=group_name,
                            properties={},
                        )
                    )

        # Funnels
        for funnel_tag in ["funnels/funnel", "funnel"]:
            for f in contents.findall(funnel_tag):
                fid = _get_child_text(f, "id")
                if fid:
                    id_to_name[fid] = "Funnel"
                    processors.append(
                        Processor(
                            name="Funnel",
                            type="Funnel",
                            platform="nifi",
                            group=group_name,
                            properties={},
                        )
                    )

        # Nested processGroups
        pg_els = _find_children(contents, "processGroups", "processGroup")
        for pg in pg_els:
            pg_name = _get_child_text(pg, "name")
            pg_id = _get_child_text(pg, "id")
            if pg_id:
                id_to_name[pg_id] = pg_name
            process_groups.append(ProcessGroup(name=pg_name))
            _extract_from_group(pg, pg_name)

    # Find the root entry point
    _candidates = [
        doc.find(".//template/snippet"),
        doc.find(".//snippet"),
        doc.find(".//flowController/rootGroup"),
        doc.find(".//rootGroup"),
        doc.find(".//processGroupFlow/flow"),
    ]
    snippet = next((c for c in _candidates if c is not None), doc)

    # Controller services
    def _parse_cs_elements(cs_els: list[etree._Element]) -> None:
        for cs in cs_els:
            name = _get_child_text(cs, "name")
            cs_type = _get_child_text(cs, "type") or _get_child_text(cs, "class")
            short_type = cs_type.rsplit(".", 1)[-1] if cs_type else ""
            cs_props: dict[str, str] = {}
            # entry format
            props_el = cs.find("properties")
            if props_el is not None:
                for entry in props_el.findall("entry"):
                    key_el = entry.find("key")
                    val_el = entry.find("value")
                    if key_el is not None and key_el.text and val_el is not None:
                        cs_props[key_el.text] = val_el.text or ""
            # property format
            if not cs_props:
                for prop in cs.findall("property"):
                    n = prop.find("name")
                    v = prop.find("value")
                    if n is not None and n.text:
                        cs_props[n.text] = (v.text or "") if v is not None else ""
            controller_services.append(
                ControllerService(
                    name=name or short_type,
                    type=short_type,
                    properties=cs_props,
                )
            )

    # Try multiple CS locations
    cs_wrapper = snippet.find("controllerServices")
    if cs_wrapper is not None:
        _parse_cs_elements(list(cs_wrapper.findall("controllerService")))
    else:
        _parse_cs_elements(list(snippet.findall("controllerService")))
    # Global CS container outside snippet
    if not controller_services:
        global_cs = doc.find(".//controllerServices")
        if global_cs is not None:
            _parse_cs_elements(list(global_cs.findall("controllerService")))

    # Top-level processGroups
    pg_els = _find_children(snippet, "processGroups", "processGroup")
    for pg in pg_els:
        pg_name = _get_child_text(pg, "name")
        pg_id = _get_child_text(pg, "id")
        if pg_id:
            id_to_name[pg_id] = pg_name
        process_groups.append(ProcessGroup(name=pg_name))
        _extract_from_group(pg, pg_name)

    # Top-level processors directly in snippet
    proc_els = _find_children(snippet, "processors", "processor")
    for proc in proc_els:
        name = _get_child_text(proc, "name")
        full_type = _get_child_text(proc, "type") or _get_child_text(proc, "class")
        short_type = full_type.rsplit(".", 1)[-1] if full_type else ""
        proc_id = _get_child_text(proc, "id")
        if proc_id:
            id_to_name[proc_id] = name or short_type
        props = _extract_properties(proc)

        config_el = proc.find("config")
        sched_period = ""
        sched_strategy = ""
        if config_el is not None:
            sp = config_el.find("schedulingPeriod")
            ss = config_el.find("schedulingStrategy")
            sched_period = sp.text.strip() if sp is not None and sp.text else ""
            sched_strategy = ss.text.strip() if ss is not None and ss.text else ""
        if not sched_period:
            sched_period = _get_child_text(proc, "schedulingPeriod")
        if not sched_strategy:
            sched_strategy = _get_child_text(proc, "schedulingStrategy")

        scheduling = None
        if sched_period or sched_strategy:
            scheduling = {"period": sched_period, "strategy": sched_strategy}

        processors.append(
            Processor(
                name=name or short_type,
                type=short_type,
                platform="nifi",
                properties=props,
                group="(root)",
                state=_get_child_text(proc, "state") or "RUNNING",
                scheduling=scheduling,
            )
        )

    # Top-level connections in snippet
    conn_keys: set[str] = set()
    for c in connections:
        conn_keys.add(f"{c.source_name}|{c.destination_name}|{c.relationship}")

    conn_els = _find_children(snippet, "connections", "connection")
    for conn in conn_els:
        src_el = conn.find("source")
        dst_el = conn.find("destination")
        src_id = ""
        dst_id = ""
        if src_el is not None:
            sid = src_el.find("id")
            src_id = sid.text.strip() if sid is not None and sid.text else ""
        if not src_id:
            src_id = _get_child_text(conn, "sourceId")
        if dst_el is not None:
            did = dst_el.find("id")
            dst_id = did.text.strip() if did is not None and did.text else ""
        if not dst_id:
            dst_id = _get_child_text(conn, "destinationId")

        rels: list[str] = []
        for r in conn.findall("selectedRelationships"):
            if r.text:
                rels.append(r.text.strip())
        if not rels:
            for r in conn.findall("relationship"):
                if r.text:
                    rels.append(r.text.strip())

        rel_str = ",".join(sorted(rels)) if rels else "success"
        key = f"{src_id}|{dst_id}|{rel_str}"
        if key not in conn_keys:
            conn_keys.add(key)
            connections.append(
                Connection(
                    source_name=src_id,
                    destination_name=dst_id,
                    relationship=rel_str,
                )
            )

    # Resolve connection IDs to processor names
    resolved_connections: list[Connection] = []
    for c in connections:
        src_fallback = c.source_name[:12] + "..." if len(c.source_name) > 12 else c.source_name
        dst_fallback = c.destination_name[:12] + "..." if len(c.destination_name) > 12 else c.destination_name
        src_name = id_to_name.get(c.source_name, src_fallback)
        dst_name = id_to_name.get(c.destination_name, dst_fallback)
        resolved_connections.append(
            Connection(
                source_name=src_name,
                destination_name=dst_name,
                relationship=c.relationship,
            )
        )

    # Populate process group processor lists
    group_map: dict[str, list[str]] = {}
    for p in processors:
        group_map.setdefault(p.group, []).append(p.name)
    for pg in process_groups:
        pg.processors = group_map.get(pg.name, [])

    # Detect version
    version = ""
    encoding_el = doc.find(".//encodingVersion")
    if encoding_el is not None and encoding_el.text:
        version = encoding_el.text.strip()

    if not processors:
        warnings.append(Warning(severity="warning", message="No processors found in XML", source=filename))

    return ParseResult(
        platform="nifi",
        version=version,
        processors=processors,
        connections=resolved_connections,
        process_groups=process_groups,
        controller_services=controller_services,
        metadata={"source_file": filename, "id_count": len(id_to_name)},
        warnings=warnings,
    )

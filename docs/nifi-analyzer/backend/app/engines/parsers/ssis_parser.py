"""SSIS .dtsx parser — extracts Data Flow tasks, connection managers, precedence constraints."""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)

# SSIS uses DTS namespace
_NS = {
    "dts": "www.microsoft.com/SqlServer/Dts",
    "DTS": "www.microsoft.com/SqlServer/Dts",
}


def _strip_ns(tag: str) -> str:
    """Strip namespace prefix from element tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _dts_attr(el: etree._Element, name: str) -> str:
    """Get a DTS-namespaced attribute value."""
    for ns_uri in ["www.microsoft.com/SqlServer/Dts"]:
        val = el.get(f"{{{ns_uri}}}{name}")
        if val:
            return val
    # Fallback: try un-namespaced
    return el.get(name, "")


def parse_ssis(content: bytes, filename: str) -> ParseResult:
    """Parse an SSIS .dtsx package."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid SSIS XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    # Extract connection managers
    for cm in doc.iter():
        tag = _strip_ns(cm.tag)
        if tag == "ConnectionManager":
            name = _dts_attr(cm, "ObjectName") or _dts_attr(cm, "DTSID") or "Unknown"
            creation_name = _dts_attr(cm, "CreationName") or ""
            conn_str = ""
            for prop in cm.iter():
                if _strip_ns(prop.tag) == "Property":
                    pname = _dts_attr(prop, "Name") or prop.get("Name", "")
                    if pname == "ConnectionString" and prop.text:
                        conn_str = prop.text
            controller_services.append(
                ControllerService(
                    name=name,
                    type=creation_name or "ConnectionManager",
                    properties={"ConnectionString": conn_str} if conn_str else {},
                )
            )

    # Extract executables (tasks)
    task_names: dict[str, str] = {}

    def _walk_executables(parent: etree._Element, group_name: str) -> None:
        for el in parent:
            tag = _strip_ns(el.tag)
            if tag in ("Executable", "Executables"):
                if tag == "Executables":
                    _walk_executables(el, group_name)
                    continue

                obj_name = _dts_attr(el, "ObjectName") or ""
                creation = _dts_attr(el, "CreationName") or ""
                dts_id = _dts_attr(el, "DTSID") or ""

                if "Data Flow" in creation or "PIPELINE" in creation.upper() or "DFT" in creation:
                    # Data Flow Task — extract components
                    pg = ProcessGroup(name=obj_name or "DataFlowTask")
                    process_groups.append(pg)
                    _extract_data_flow(el, obj_name or "DataFlowTask")
                elif creation:
                    proc_type = creation.replace("Microsoft.SqlServer.Dts.Tasks.", "").replace(",", " ").split(".")[0]
                    processors.append(
                        Processor(
                            name=obj_name or proc_type,
                            type=proc_type,
                            platform="ssis",
                            group=group_name,
                            properties={},
                        )
                    )

                if dts_id:
                    task_names[dts_id] = obj_name
                if obj_name:
                    task_names[obj_name] = obj_name

                # Recurse into nested executables
                for child in el:
                    if _strip_ns(child.tag) == "Executables":
                        _walk_executables(child, obj_name or group_name)

    def _extract_data_flow(dft_el: etree._Element, group_name: str) -> None:
        """Extract components from a Data Flow Task pipeline layout."""
        component_names: dict[str, str] = {}
        for el in dft_el.iter():
            tag = _strip_ns(el.tag)
            if tag == "component":
                comp_name = el.get("name", "")
                comp_type = el.get("componentClassID", el.get("contactInfo", ""))
                comp_id = el.get("id", "")
                if comp_name:
                    component_names[comp_id] = comp_name
                    # Extract properties
                    props: dict[str, str] = {}
                    for prop_el in el.iter():
                        ptag = _strip_ns(prop_el.tag)
                        if ptag == "property":
                            pname = prop_el.get("name", "")
                            if pname and prop_el.text:
                                props[pname] = prop_el.text
                    processors.append(
                        Processor(
                            name=comp_name,
                            type=comp_type.split(".")[-1] if comp_type else "Unknown",
                            platform="ssis",
                            group=group_name,
                            properties=props,
                        )
                    )
            elif tag == "path":
                src_id = el.get("startId", "")
                dst_id = el.get("endId", "")
                if src_id and dst_id:
                    connections.append(
                        Connection(
                            source_name=src_id,
                            destination_name=dst_id,
                            relationship="success",
                        )
                    )

    _walk_executables(doc, "(root)")

    # Precedence constraints
    for el in doc.iter():
        tag = _strip_ns(el.tag)
        if tag == "PrecedenceConstraint":
            from_id = _dts_attr(el, "From") or ""
            to_id = _dts_attr(el, "To") or ""
            value = _dts_attr(el, "Value") or "Success"
            constraint_map = {"0": "success", "1": "failure", "2": "completion"}
            rel = constraint_map.get(value, value.lower())
            src = task_names.get(from_id, from_id)
            dst = task_names.get(to_id, to_id)
            if src and dst:
                connections.append(Connection(source_name=src, destination_name=dst, relationship=rel))

    if not processors:
        warnings.append(Warning(severity="warning", message="No tasks found in SSIS package", source=filename))

    return ParseResult(
        platform="ssis",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

"""Oracle ODI XML export parser.

Extracts interfaces, packages, scenarios, and mappings.
"""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_oracle_odi(content: bytes, filename: str) -> ParseResult:
    """Parse an Oracle ODI XML export."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid ODI XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    # Extract Folders as process groups
    for folder in doc.iter():
        tag = folder.tag.split("}")[-1] if "}" in folder.tag else folder.tag
        if tag.lower() in ("folder", "odiproject"):
            name = folder.get("Name") or folder.get("name") or ""
            if name:
                process_groups.append(ProcessGroup(name=name))

    # Extract interfaces/mappings
    for el in doc.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag

        if tag.lower() in ("interface", "mapping", "odiinterface", "odimapping"):
            name = el.get("Name") or el.get("name") or "Unknown"
            itype = el.get("Type") or el.get("type") or tag
            props: dict[str, str] = {}
            for attr_name, attr_val in el.attrib.items():
                if attr_name.lower() not in ("name", "type"):
                    props[attr_name] = attr_val
            processors.append(
                Processor(
                    name=name,
                    type=itype,
                    platform="oracle_odi",
                    properties=props,
                )
            )

        elif tag.lower() in ("package", "odipackage"):
            name = el.get("Name") or el.get("name") or "Unknown"
            processors.append(
                Processor(
                    name=name,
                    type="Package",
                    platform="oracle_odi",
                )
            )

        elif tag.lower() in ("scenario", "odiscenario"):
            name = el.get("Name") or el.get("name") or "Unknown"
            processors.append(
                Processor(
                    name=name,
                    type="Scenario",
                    platform="oracle_odi",
                )
            )

        elif tag.lower() in ("datastoreset", "datasource", "source"):
            name = el.get("Name") or el.get("name") or ""
            if name:
                processors.append(
                    Processor(
                        name=name,
                        type="DataSource",
                        platform="oracle_odi",
                    )
                )

        elif tag.lower() in ("targetdatastore", "target"):
            name = el.get("Name") or el.get("name") or ""
            if name:
                processors.append(
                    Processor(
                        name=name,
                        type="Target",
                        platform="oracle_odi",
                    )
                )

    # Extract step sequences for connections
    prev_step = ""
    for el in doc.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag.lower() in ("step", "odistep"):
            step_name = el.get("Name") or el.get("name") or ""
            if prev_step and step_name:
                connections.append(Connection(source_name=prev_step, destination_name=step_name))
            prev_step = step_name

    if not processors:
        warnings.append(Warning(severity="warning", message="No components found in ODI export", source=filename))

    return ParseResult(
        platform="oracle_odi",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename},
        warnings=warnings,
    )

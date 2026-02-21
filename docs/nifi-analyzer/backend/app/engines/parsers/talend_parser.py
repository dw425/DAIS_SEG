"""Talend .item XML / ZIP parser.

Extracts tMysqlInput, tMap, tFileOutputDelimited, etc. from Talend job definitions.
"""

import logging
import zipfile
from io import BytesIO

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_talend(content: bytes, filename: str) -> ParseResult:
    """Parse a Talend .item XML file or ZIP archive containing .item files."""
    warnings: list[Warning] = []
    xml_contents: list[tuple[str, bytes]] = []

    if filename.endswith(".zip"):
        try:
            with zipfile.ZipFile(BytesIO(content)) as zf:
                for name in zf.namelist():
                    if name.endswith(".item"):
                        xml_contents.append((name, zf.read(name)))
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP: {exc}") from exc
    else:
        xml_contents.append((filename, content))

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []

    for item_name, item_content in xml_contents:
        try:
            doc = etree.fromstring(item_content)
        except etree.XMLSyntaxError:
            warnings.append(Warning(severity="warning", message=f"Invalid XML in {item_name}", source=item_name))
            continue

        job_name = doc.get("name", item_name)
        process_groups.append(ProcessGroup(name=job_name))

        # Extract nodes (components)
        for node in doc.iter("node"):
            comp_name = node.get("componentName", "")
            unique_name = node.get("componentVersion", node.get("uniqueName", ""))
            label = node.get("label", "") or unique_name or comp_name

            props: dict[str, str] = {}
            for param in node.iter("elementParameter"):
                pname = param.get("name", "")
                pval = param.get("value", "")
                if pname:
                    props[pname] = pval

            if comp_name:
                processors.append(
                    Processor(
                        name=label or comp_name,
                        type=comp_name,
                        platform="talend",
                        group=job_name,
                        properties=props,
                    )
                )

        # Extract connections
        for conn in doc.iter("connection"):
            src = conn.get("source", "")
            tgt = conn.get("target", "")
            conn_type = conn.get("connectorName", conn.get("lineStyle", "success"))
            if src and tgt:
                connections.append(
                    Connection(
                        source_name=src,
                        destination_name=tgt,
                        relationship=conn_type,
                    )
                )

    if not processors:
        warnings.append(Warning(severity="warning", message="No components found in Talend job", source=filename))

    return ParseResult(
        platform="talend",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename, "items_parsed": len(xml_contents)},
        warnings=warnings,
    )

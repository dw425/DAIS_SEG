"""IBM DataStage .dsx XML parser.

Extracts stages (Sequential File, Transformer, DB2 Connector, etc.) and links.
"""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_datastage(content: bytes, filename: str) -> ParseResult:
    """Parse an IBM DataStage .dsx XML export."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid DataStage XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    # DataStage DSX format uses DSJOB and DSRECORD elements
    for el in doc.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag

        if tag == "DSJOB":
            job_name = el.get("Name") or el.get("Identifier", "")
            if job_name:
                process_groups.append(ProcessGroup(name=job_name))

        elif tag in ("DSRECORD", "Record"):
            rec_type = ""
            name = ""
            stage_type = ""
            props: dict[str, str] = {}

            for child in el:
                ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                text = (child.text or "").strip()
                if ctag == "Identifier":
                    name = text
                elif ctag == "OLEType":
                    rec_type = text
                elif ctag == "StageType" or ctag == "StageTypeName":
                    stage_type = text
                elif text:
                    props[ctag] = text[:200]

            # CTransformerStage, CCustomStage, CHashedFileStage, etc.
            if rec_type in ("CTransformerStage", "CCustomStage") or stage_type:
                if name:
                    processors.append(
                        Processor(
                            name=name,
                            type=stage_type or rec_type,
                            platform="datastage",
                            properties=props,
                        )
                    )

        elif tag in ("DSLINK", "Link"):
            src = el.get("SourceStage") or ""
            dst = el.get("TargetStage") or ""
            if not src:
                for child in el:
                    ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    text = (child.text or "").strip()
                    if ctag == "SourceStage":
                        src = text
                    elif ctag == "TargetStage":
                        dst = text
            if src and dst:
                connections.append(Connection(source_name=src, destination_name=dst))

    # Also try stage/link extraction from flat RECORD elements
    for record in doc.findall(".//Record"):
        props_map: dict[str, str] = {}
        for prop in record.findall("Property"):
            pname = prop.get("Name", "")
            if pname and prop.text:
                props_map[pname] = prop.text.strip()
        name = props_map.get("Name", "")
        stype = props_map.get("StageType", "")
        if name and stype:
            processors.append(
                Processor(
                    name=name,
                    type=stype,
                    platform="datastage",
                    properties=props_map,
                )
            )

    if not processors:
        warnings.append(Warning(severity="warning", message="No stages found in DataStage export", source=filename))

    return ParseResult(
        platform="datastage",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename},
        warnings=warnings,
    )

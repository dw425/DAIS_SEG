"""Pentaho .ktr (transformation) and .kjb (job) XML parser.

Extracts steps (Table Input, Sort Rows, Select Values, Table Output) and hops.
"""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_pentaho(content: bytes, filename: str) -> ParseResult:
    """Parse a Pentaho .ktr or .kjb file."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid Pentaho XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    root_tag = doc.tag.lower()
    is_job = root_tag == "job"

    trans_name = ""
    name_el = doc.find("name")
    if name_el is not None and name_el.text:
        trans_name = name_el.text.strip()

    if trans_name:
        process_groups.append(ProcessGroup(name=trans_name))

    if is_job:
        # Job entries
        for entry in doc.findall(".//entry"):
            entry_name = ""
            entry_type = ""
            props: dict[str, str] = {}
            for child in entry:
                if child.tag == "name" and child.text:
                    entry_name = child.text.strip()
                elif child.tag == "type" and child.text:
                    entry_type = child.text.strip()
                elif child.text:
                    props[child.tag] = child.text.strip()

            if entry_name:
                processors.append(
                    Processor(
                        name=entry_name,
                        type=entry_type or "JobEntry",
                        platform="pentaho",
                        group=trans_name,
                        properties=props,
                    )
                )

        # Job hops
        for hop in doc.findall(".//hop"):
            from_el = hop.find("from")
            to_el = hop.find("to")
            if from_el is not None and from_el.text and to_el is not None and to_el.text:
                enabled = hop.find("enabled")
                if enabled is None or enabled.text != "N":
                    connections.append(
                        Connection(
                            source_name=from_el.text.strip(),
                            destination_name=to_el.text.strip(),
                        )
                    )
    else:
        # Transformation steps
        for step in doc.findall(".//step"):
            step_name = ""
            step_type = ""
            props: dict[str, str] = {}
            for child in step:
                if child.tag == "name" and child.text:
                    step_name = child.text.strip()
                elif child.tag == "type" and child.text:
                    step_type = child.text.strip()
                elif child.text:
                    props[child.tag] = child.text.strip()[:200]

            if step_name:
                processors.append(
                    Processor(
                        name=step_name,
                        type=step_type or "Step",
                        platform="pentaho",
                        group=trans_name,
                        properties=props,
                    )
                )

        # Transformation hops (order)
        for hop in doc.findall(".//hop") + doc.findall(".//order/hop"):
            from_el = hop.find("from")
            to_el = hop.find("to")
            if from_el is not None and from_el.text and to_el is not None and to_el.text:
                enabled = hop.find("enabled")
                if enabled is None or enabled.text != "N":
                    connections.append(
                        Connection(
                            source_name=from_el.text.strip(),
                            destination_name=to_el.text.strip(),
                        )
                    )

    if not processors:
        warnings.append(Warning(severity="warning", message="No steps/entries found", source=filename))

    return ParseResult(
        platform="pentaho",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename, "type": "job" if is_job else "transformation"},
        warnings=warnings,
    )

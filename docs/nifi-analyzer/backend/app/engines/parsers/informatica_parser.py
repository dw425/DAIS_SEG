"""Informatica PowerCenter XML parser.

Extracts sessions, mappings, transformations (Source Qualifier, Expression,
Filter, Joiner, Router, Target), sources, and targets.
"""

import logging

from lxml import etree

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_informatica(content: bytes, filename: str) -> ParseResult:
    """Parse an Informatica PowerCenter XML export."""
    try:
        doc = etree.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Invalid Informatica XML: {exc}") from exc

    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    # Extract FOLDER as process groups
    for folder in doc.iter("FOLDER"):
        name = folder.get("NAME", "")
        if name:
            process_groups.append(ProcessGroup(name=name))

    # Extract SOURCE definitions
    for src in doc.iter("SOURCE"):
        name = src.get("NAME", "")
        db_type = src.get("DATABASETYPE", "")
        owner = src.get("OWNERNAME", "")
        processors.append(
            Processor(
                name=name or "Source",
                type="Source",
                platform="informatica",
                properties={"DATABASETYPE": db_type, "OWNERNAME": owner},
            )
        )

    # Extract TARGET definitions
    for tgt in doc.iter("TARGET"):
        name = tgt.get("NAME", "")
        db_type = tgt.get("DATABASETYPE", "")
        processors.append(
            Processor(
                name=name or "Target",
                type="Target",
                platform="informatica",
                properties={"DATABASETYPE": db_type},
            )
        )

    # Extract MAPPING transformations
    for mapping in doc.iter("MAPPING"):
        map_name = mapping.get("NAME", "")
        process_groups.append(ProcessGroup(name=map_name))

        for xform in mapping.iter("TRANSFORMATION"):
            xf_name = xform.get("NAME", "")
            xf_type = xform.get("TYPE", "")
            props: dict[str, str] = {}
            for prop in xform.iter("TABLEATTRIBUTE"):
                pname = prop.get("NAME", "")
                pval = prop.get("VALUE", "")
                if pname:
                    props[pname] = pval
            processors.append(
                Processor(
                    name=xf_name or xf_type,
                    type=xf_type,
                    platform="informatica",
                    group=map_name,
                    properties=props,
                )
            )

        # Extract CONNECTOR (data flow edges)
        for conn in mapping.iter("CONNECTOR"):
            from_inst = conn.get("FROMINSTANCE", "")
            to_inst = conn.get("TOINSTANCE", "")
            from_field = conn.get("FROMFIELD", "")
            to_field = conn.get("TOFIELD", "")
            if from_inst and to_inst:
                connections.append(
                    Connection(
                        source_name=from_inst,
                        destination_name=to_inst,
                        relationship=f"{from_field}->{to_field}" if from_field else "success",
                    )
                )

    # Extract SESSION as controller services (they hold connection info)
    for session in doc.iter("SESSION"):
        sess_name = session.get("NAME", "")
        map_name = session.get("MAPPINGNAME", "")
        controller_services.append(
            ControllerService(
                name=sess_name or "Session",
                type="Session",
                properties={"MAPPINGNAME": map_name},
            )
        )

    if not processors:
        warnings.append(Warning(severity="warning", message="No components found", source=filename))

    return ParseResult(
        platform="informatica",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

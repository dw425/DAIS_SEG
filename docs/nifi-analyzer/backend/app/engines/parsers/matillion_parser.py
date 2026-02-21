"""Matillion JSON export parser.

Extracts jobs, components (Table Input, Table Output, Calculator, etc.), connections.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_matillion(content: bytes, filename: str) -> ParseResult:
    """Parse a Matillion JSON export."""
    data = json.loads(content)
    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    jobs = []
    if isinstance(data, list):
        jobs = data
    elif isinstance(data, dict):
        if "jobs" in data:
            jobs = data["jobs"] if isinstance(data["jobs"], list) else [data["jobs"]]
        else:
            jobs = [data]

    for job in jobs:
        job_name = job.get("name") or job.get("jobName", "MatillionJob")
        process_groups.append(ProcessGroup(name=job_name))

        components = job.get("components", [])
        if isinstance(components, dict):
            components = list(components.values())

        for comp in components:
            if not isinstance(comp, dict):
                continue
            comp_name = comp.get("name") or comp.get("componentName", "")
            comp_type = comp.get("type") or comp.get("componentType", "Unknown")
            parameters = comp.get("parameters", {})
            props: dict[str, str] = {}
            if isinstance(parameters, dict):
                for k, v in parameters.items():
                    props[k] = str(v) if v is not None else ""
            elif isinstance(parameters, list):
                for p in parameters:
                    if isinstance(p, dict):
                        props[p.get("name", "")] = str(p.get("value", ""))

            if comp_name:
                processors.append(
                    Processor(
                        name=comp_name,
                        type=comp_type,
                        platform="matillion",
                        group=job_name,
                        properties=props,
                    )
                )

        # Connections / links
        links = job.get("connections", job.get("links", []))
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict):
                    src = link.get("source") or link.get("from", "")
                    dst = link.get("target") or link.get("to", "")
                    if src and dst:
                        connections.append(Connection(source_name=src, destination_name=dst))

    if not processors:
        warnings.append(Warning(severity="warning", message="No components found", source=filename))

    return ParseResult(
        platform="matillion",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename},
        warnings=warnings,
    )

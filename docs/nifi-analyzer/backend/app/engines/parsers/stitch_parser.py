"""Stitch Data config JSON parser.

Extracts integrations, tables, and replication keys.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_stitch(content: bytes, filename: str) -> ParseResult:
    """Parse a Stitch Data config JSON."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {filename}: {exc}") from exc
    processors: list[Processor] = []
    connections: list[Connection] = []
    warnings: list[Warning] = []

    integrations = []
    if isinstance(data, list):
        integrations = data
    elif isinstance(data, dict):
        if "integrations" in data:
            integrations = data["integrations"]
        else:
            integrations = [data]

    for integ in integrations:
        if not isinstance(integ, dict):
            continue

        name = integ.get("name", integ.get("integration_type", "stitch_source"))
        itype = integ.get("integration_type") or integ.get("type", "unknown")
        frequency = integ.get("frequency_in_minutes", "")

        processors.append(
            Processor(
                name=name,
                type=f"Stitch_{itype}",
                platform="stitch",
                properties={
                    "integration_type": itype,
                    "frequency_in_minutes": str(frequency),
                    "paused": str(integ.get("paused", False)),
                },
            )
        )

        # Tables / streams
        tables = integ.get("tables", integ.get("streams", []))
        for table in tables:
            if isinstance(table, dict):
                tname = table.get("name") or table.get("stream_name", "")
                rep_key = table.get("replication_key", "")
                rep_method = table.get("replication_method", "")
                if tname:
                    processors.append(
                        Processor(
                            name=tname,
                            type="Stitch_Table",
                            platform="stitch",
                            properties={
                                "replication_key": rep_key,
                                "replication_method": rep_method,
                            },
                        )
                    )
                    connections.append(Connection(source_name=name, destination_name=tname))
            elif isinstance(table, str):
                processors.append(Processor(name=table, type="Stitch_Table", platform="stitch"))
                connections.append(Connection(source_name=name, destination_name=table))

    if not processors:
        warnings.append(Warning(severity="warning", message="No integrations found", source=filename))

    return ParseResult(
        platform="stitch",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename},
        warnings=warnings,
    )

"""Fivetran connector JSON config parser.

Extracts connector type, schema, tables, sync mode.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, Processor

logger = logging.getLogger(__name__)


def parse_fivetran(content: bytes, filename: str) -> ParseResult:
    """Parse a Fivetran connector JSON config."""
    data = json.loads(content)
    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    warnings: list[Warning] = []

    connectors = data.get("connectors", [data]) if isinstance(data, dict) else data

    for conn in connectors:
        if not isinstance(conn, dict):
            continue
        name = conn.get("schema", conn.get("service", "fivetran_connector"))
        service = conn.get("service", "unknown")
        sync_mode = conn.get("sync_mode", "")
        status = conn.get("status", {})

        props: dict[str, str] = {
            "service": service,
            "sync_mode": sync_mode,
            "sync_frequency": str(conn.get("sync_frequency", "")),
            "paused": str(conn.get("paused", False)),
        }

        if isinstance(status, dict):
            props["setup_state"] = status.get("setup_state", "")

        # Source connector
        processors.append(
            Processor(
                name=f"{name}_source",
                type=f"Fivetran_{service}",
                platform="fivetran",
                properties=props,
            )
        )

        # Destination
        dest_schema = conn.get("schema", name)
        processors.append(
            Processor(
                name=f"{dest_schema}_dest",
                type="Fivetran_Destination",
                platform="fivetran",
                properties={"schema": dest_schema},
            )
        )

        connections.append(
            Connection(
                source_name=f"{name}_source",
                destination_name=f"{dest_schema}_dest",
            )
        )

        # Tables
        for table_name, table_cfg in conn.get("schema_status", {}).items():
            if isinstance(table_cfg, dict):
                enabled = table_cfg.get("enabled", True)
                if enabled:
                    processors.append(
                        Processor(
                            name=table_name,
                            type="Fivetran_Table",
                            platform="fivetran",
                            properties={"enabled": str(enabled)},
                        )
                    )

    if not processors:
        warnings.append(Warning(severity="warning", message="No connectors found", source=filename))

    return ParseResult(
        platform="fivetran",
        processors=processors,
        connections=connections,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

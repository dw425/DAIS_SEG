"""Airbyte catalog JSON / connection config parser.

Extracts streams, sync modes, destination configuration.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, Processor

logger = logging.getLogger(__name__)


def parse_airbyte(content: bytes, filename: str) -> ParseResult:
    """Parse an Airbyte catalog or connection config JSON."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {filename}: {exc}") from exc
    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    warnings: list[Warning] = []

    # Extract catalog streams
    catalog = data.get("catalog") or data.get("syncCatalog") or data.get("streams", [])
    if isinstance(catalog, dict):
        catalog = catalog.get("streams", [])

    source_name = data.get("sourceName") or data.get("source", {}).get("name", "airbyte_source")
    dest_name = data.get("destinationName") or data.get("destination", {}).get("name", "airbyte_dest")

    # Source and destination as controller services
    source_cfg = data.get("source", {}).get("connectionConfiguration", {})
    dest_cfg = data.get("destination", {}).get("connectionConfiguration", {})
    if source_cfg:
        controller_services.append(
            ControllerService(
                name=source_name,
                type="AirbyteSource",
                properties={k: str(v)[:200] for k, v in source_cfg.items() if isinstance(v, (str, int, bool))},
            )
        )
    if dest_cfg:
        controller_services.append(
            ControllerService(
                name=dest_name,
                type="AirbyteDestination",
                properties={k: str(v)[:200] for k, v in dest_cfg.items() if isinstance(v, (str, int, bool))},
            )
        )

    for stream in catalog:
        if not isinstance(stream, dict):
            continue
        stream_cfg = stream.get("config", stream)
        stream_def = stream.get("stream", stream)

        stream_name = stream_def.get("name", "")
        namespace = stream_def.get("namespace", "")
        sync_mode = stream_cfg.get("syncMode", stream_cfg.get("sync_mode", ""))
        dest_sync_mode = stream_cfg.get("destinationSyncMode", "")

        if stream_name:
            processors.append(
                Processor(
                    name=stream_name,
                    type="Airbyte_Stream",
                    platform="airbyte",
                    properties={
                        "namespace": namespace,
                        "sync_mode": sync_mode,
                        "destination_sync_mode": dest_sync_mode,
                    },
                )
            )
            connections.append(
                Connection(
                    source_name=source_name,
                    destination_name=stream_name,
                )
            )
            connections.append(
                Connection(
                    source_name=stream_name,
                    destination_name=dest_name,
                )
            )

    if not processors:
        warnings.append(Warning(severity="warning", message="No streams found", source=filename))

    return ParseResult(
        platform="airbyte",
        processors=processors,
        connections=connections,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

"""dbt manifest.json + SQL model parser.

Extracts models, sources, tests, macros, and ref() dependencies.
"""

import json
import logging
import re

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_dbt(content: bytes, filename: str) -> ParseResult:
    """Parse a dbt manifest.json or SQL model file."""
    text = content.decode("utf-8", errors="replace")

    # Try JSON first (manifest.json)
    if filename.endswith(".json"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON: {exc}") from exc
        return _parse_manifest(data, filename)

    # SQL model file
    return _parse_sql_model(text, filename)


def _parse_manifest(data: dict, filename: str) -> ParseResult:
    """Parse a dbt manifest.json."""
    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    nodes = data.get("nodes", {})
    sources = data.get("sources", {})
    # Track schemas as process groups
    schemas: set[str] = set()

    for node_id, node in nodes.items():
        resource_type = node.get("resource_type", "")
        name = node.get("name", node_id)
        schema = node.get("schema", "")
        if schema:
            schemas.add(schema)

        props = {
            "materialized": node.get("config", {}).get("materialized", ""),
            "schema": schema,
            "database": node.get("database", ""),
            "package_name": node.get("package_name", ""),
        }

        if resource_type in ("model", "seed", "snapshot"):
            processors.append(
                Processor(
                    name=name,
                    type=f"dbt_{resource_type}",
                    platform="dbt",
                    group=schema,
                    properties=props,
                )
            )
        elif resource_type == "test":
            processors.append(
                Processor(
                    name=name,
                    type="dbt_test",
                    platform="dbt",
                    group=schema,
                    properties=props,
                )
            )

        # Dependencies via depends_on
        deps = node.get("depends_on", {}).get("nodes", [])
        for dep_id in deps:
            dep_name = dep_id.split(".")[-1] if "." in dep_id else dep_id
            connections.append(
                Connection(
                    source_name=dep_name,
                    destination_name=name,
                )
            )

    # Sources
    for src_id, src in sources.items():
        name = src.get("name", src_id)
        processors.append(
            Processor(
                name=name,
                type="dbt_source",
                platform="dbt",
                properties={"source_name": src.get("source_name", ""), "loader": src.get("loader", "")},
            )
        )

    for schema in schemas:
        process_groups.append(ProcessGroup(name=schema))

    return ParseResult(
        platform="dbt",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        metadata={"source_file": filename, "node_count": len(nodes), "source_count": len(sources)},
        warnings=warnings,
    )


def _parse_sql_model(text: str, filename: str) -> ParseResult:
    """Parse a single dbt SQL model file."""
    processors: list[Processor] = []
    connections: list[Connection] = []
    warnings: list[Warning] = []

    # Extract model name from filename
    model_name = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]

    # Extract config block
    config_match = re.search(r"\{\{\s*config\(([^)]+)\)\s*\}\}", text)
    props: dict[str, str] = {}
    if config_match:
        config_text = config_match.group(1)
        for kv in re.finditer(r"(\w+)\s*=\s*['\"]?([^'\",$]+)", config_text):
            props[kv.group(1)] = kv.group(2).strip()

    processors.append(
        Processor(
            name=model_name,
            type="dbt_model",
            platform="dbt",
            properties=props,
        )
    )

    # Extract ref() dependencies
    for ref_match in re.finditer(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}", text):
        ref_name = ref_match.group(1)
        connections.append(Connection(source_name=ref_name, destination_name=model_name))

    # Extract source() dependencies
    for src_match in re.finditer(r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}", text):
        src_name = f"{src_match.group(1)}.{src_match.group(2)}"
        connections.append(Connection(source_name=src_name, destination_name=model_name))

    return ParseResult(
        platform="dbt",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename},
        warnings=warnings,
    )

"""Direct schema definition parser — handles JSON and YAML input.

Accepts two flavors:
1. Full blueprint format (passthrough with validation)
2. Simplified format: { "tables": [{ "name": "t", "columns": [{"name":"c","type":"int"}] }] }
"""

from __future__ import annotations

import json
from typing import Any

from dais_seg.source_loader.base import (
    BaseParser,
    InputFormat,
    ParsedColumn,
    ParsedForeignKey,
    ParsedSchema,
    ParsedTable,
)


class SchemaDefinitionParser(BaseParser):
    """Parses JSON or YAML schema definitions."""

    def can_parse(self, content: str) -> bool:
        stripped = content.strip()
        if stripped.startswith("{"):
            try:
                data = json.loads(stripped)
                return "tables" in data
            except json.JSONDecodeError:
                return False
        return "tables:" in content

    def parse(self, content: str, **kwargs) -> ParsedSchema:
        source_name = kwargs.get("source_name", "Schema Definition")
        data = self._load(content)

        # Check if this is already a full blueprint
        if "blueprint_id" in data and "source_system" in data:
            return self._parse_blueprint_format(data, source_name)

        return self._parse_simplified(data, source_name, content)

    def _load(self, content: str) -> dict:
        stripped = content.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            return json.loads(stripped)
        # Try YAML
        try:
            import yaml
            return yaml.safe_load(stripped)
        except ImportError:
            raise ImportError("PyYAML is required for YAML input: pip install pyyaml")

    def _parse_blueprint_format(self, data: dict, source_name: str) -> ParsedSchema:
        """Handle input that is already in blueprint format."""
        tables = []
        for t in data.get("tables", []):
            columns = [
                ParsedColumn(
                    name=c["name"],
                    data_type=c.get("data_type", "varchar"),
                    nullable=c.get("nullable", True),
                    is_primary_key=c.get("is_primary_key", False),
                )
                for c in t.get("columns", [])
            ]
            fks = [
                ParsedForeignKey(
                    fk_column=fk.get("column", fk.get("fk_column", "")),
                    referenced_table=fk.get("references_table", fk.get("referenced_table", "")),
                    referenced_column=fk.get("references_column", fk.get("referenced_column", "")),
                )
                for fk in t.get("foreign_keys", [])
            ]
            tables.append(ParsedTable(
                name=t["name"],
                schema=t.get("schema", "dbo"),
                columns=columns,
                foreign_keys=fks,
                row_count=t.get("row_count", 1000),
            ))

        return ParsedSchema(
            source_name=data.get("source_system", {}).get("name", source_name),
            source_type=data.get("source_system", {}).get("type", "parsed"),
            tables=tables,
            input_format=InputFormat.SCHEMA_JSON,
        )

    def _parse_simplified(self, data: dict, source_name: str, raw: str) -> ParsedSchema:
        """Handle simplified schema format."""
        tables = []
        for t in data.get("tables", []):
            columns: list[ParsedColumn] = []
            for c in t.get("columns", []):
                if isinstance(c, str):
                    # "column_name:type" format
                    parts = c.split(":")
                    columns.append(ParsedColumn(
                        name=parts[0].strip(),
                        data_type=parts[1].strip() if len(parts) > 1 else "varchar",
                    ))
                elif isinstance(c, dict):
                    columns.append(ParsedColumn(
                        name=c.get("name", c.get("column_name", "")),
                        data_type=c.get("type", c.get("data_type", "varchar")),
                        nullable=c.get("nullable", True),
                        is_primary_key=c.get("pk", c.get("primary_key", c.get("is_primary_key", False))),
                        max_length=c.get("length", c.get("max_length")),
                        check_constraints=c.get("values", c.get("allowed_values", [])),
                    ))

            fks = [
                ParsedForeignKey(
                    fk_column=fk.get("column", fk.get("fk_column", "")),
                    referenced_table=fk.get("references", fk.get("referenced_table", "")),
                    referenced_column=fk.get("references_column", fk.get("referenced_column", "id")),
                )
                for fk in t.get("foreign_keys", t.get("fks", []))
            ]

            tables.append(ParsedTable(
                name=t.get("name", t.get("table_name", "")),
                schema=t.get("schema", "dbo"),
                columns=columns,
                foreign_keys=fks,
            ))

        is_json = raw.strip().startswith("{")
        return ParsedSchema(
            source_name=data.get("name", data.get("source_name", source_name)),
            source_type=data.get("type", data.get("source_type", "parsed")),
            tables=tables,
            input_format=InputFormat.SCHEMA_JSON if is_json else InputFormat.SCHEMA_YAML,
            raw_input=raw[:5000],
        )

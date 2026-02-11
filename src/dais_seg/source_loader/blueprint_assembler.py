"""Blueprint assembler — converts ParsedSchema into a blueprint dict.

Produces a blueprint dict with the exact same structure as
BlueprintGenerator.generate(), so the downstream generation pipeline
(SyntheticDeltaTableGenerator, RelationshipPreserver, MedallionPipeline,
WorkspaceValidator) works identically regardless of whether the blueprint
came from a live profiled source or from a parsed DDL file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from dais_seg.source_loader.base import ParsedSchema
from dais_seg.source_loader.default_stats import generate_default_stats


class BlueprintAssembler:
    """Assembles a standard blueprint dict from a ParsedSchema."""

    def assemble(
        self,
        parsed: ParsedSchema,
        row_count: int = 1000,
    ) -> dict:
        """Convert a ParsedSchema into a full blueprint dict.

        Args:
            parsed: The output of any parser.
            row_count: Default rows per table (overrides table.row_count if different from 1000).

        Returns:
            Blueprint dict matching the BlueprintGenerator.generate() output.
        """
        blueprint_id = str(uuid4())

        tables = []
        for pt in parsed.tables:
            effective_rows = pt.row_count if pt.row_count != 1000 else row_count

            columns = []
            for pc in pt.columns:
                stats = generate_default_stats(pc, effective_rows)
                columns.append({
                    "name": pc.name,
                    "data_type": pc.data_type,
                    "nullable": pc.nullable,
                    "is_primary_key": pc.is_primary_key,
                    "stats": stats,
                })

            # FK key names match RelationshipPreserver.get_fk_constraints()
            # which reads fk.get("column"), fk.get("references_table"), fk.get("references_column")
            foreign_keys = [
                {
                    "column": fk.fk_column,
                    "references_table": fk.referenced_table,
                    "references_column": fk.referenced_column,
                }
                for fk in pt.foreign_keys
            ]

            tables.append({
                "name": pt.name,
                "schema": pt.schema,
                "row_count": effective_rows,
                "size_bytes": 0,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "business_rules": [],
            })

        # Build relationship graph from FK metadata
        relationships = []
        for pt in parsed.tables:
            for fk in pt.foreign_keys:
                relationships.append({
                    "from_table": f"{pt.schema}.{pt.name}",
                    "to_table": f"{pt.schema}.{fk.referenced_table}",
                    "relationship_type": "one_to_many",
                    "join_columns": [{
                        "from_column": fk.fk_column,
                        "to_column": fk.referenced_column,
                    }],
                })

        return {
            "blueprint_id": blueprint_id,
            "source_system": {
                "name": parsed.source_name,
                "type": parsed.source_type,
            },
            "profiled_at": datetime.now(timezone.utc).isoformat(),
            "profiled_by": "source_loader",
            "tables": tables,
            "relationships": relationships,
            "security_model": {
                "rbac_roles": [],
                "row_level_security": False,
                "column_masking": [],
            },
        }

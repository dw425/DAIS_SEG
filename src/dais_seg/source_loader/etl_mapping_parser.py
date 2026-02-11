"""ETL source-to-target mapping parser — extracts schema from CSV/Excel mapping files.

Expects columns like: source_table, source_column, source_type, target_table,
target_column, target_type, nullable, pk, fk_table, fk_column.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Optional

from dais_seg.source_loader.base import (
    BaseParser,
    InputFormat,
    ParsedColumn,
    ParsedForeignKey,
    ParsedSchema,
    ParsedTable,
)

# Header synonyms for flexible column detection
HEADER_SYNONYMS: dict[str, list[str]] = {
    "source_table": ["source_table", "src_table", "source_tbl", "from_table"],
    "source_column": ["source_column", "src_column", "src_col", "from_column"],
    "source_type": ["source_type", "src_type", "source_data_type", "src_data_type"],
    "target_table": ["target_table", "tgt_table", "target_tbl", "to_table"],
    "target_column": ["target_column", "tgt_column", "tgt_col", "to_column"],
    "target_type": ["target_type", "tgt_type", "target_data_type", "tgt_data_type"],
    "nullable": ["nullable", "null", "is_nullable", "allow_null"],
    "pk": ["pk", "primary_key", "is_pk", "is_primary_key"],
    "fk_table": ["fk_table", "ref_table", "references_table", "fk_ref_table"],
    "fk_column": ["fk_column", "ref_column", "references_column", "fk_ref_column"],
    "transformation": ["transformation", "transform", "rule", "mapping_rule"],
}


class ETLMappingParser(BaseParser):
    """Parses ETL source-to-target mapping files (CSV/TSV)."""

    def can_parse(self, content: str) -> bool:
        first_line = content.strip().split("\n")[0].lower()
        return any(
            h in first_line
            for synonyms in HEADER_SYNONYMS.values()
            for h in synonyms
        )

    def parse(self, content: str, **kwargs) -> ParsedSchema:
        source_name = kwargs.get("source_name", "ETL Mapping Import")
        use_target = kwargs.get("use_target_side", True)

        reader = csv.DictReader(io.StringIO(content))
        header_map = self._map_headers(reader.fieldnames or [])

        # Group rows by table
        table_rows: dict[str, list[dict]] = {}
        for row in reader:
            table_key = "target_table" if use_target else "source_table"
            table_name = self._get_mapped_value(row, header_map, table_key)
            if table_name:
                table_rows.setdefault(table_name, []).append(row)

        tables = []
        warnings: list[str] = []

        for table_name, rows in table_rows.items():
            columns: list[ParsedColumn] = []
            fks: list[ParsedForeignKey] = []

            for row in rows:
                col_key = "target_column" if use_target else "source_column"
                type_key = "target_type" if use_target else "source_type"

                col_name = self._get_mapped_value(row, header_map, col_key)
                col_type = self._get_mapped_value(row, header_map, type_key) or "varchar"

                if not col_name:
                    continue

                is_nullable_str = self._get_mapped_value(row, header_map, "nullable")
                is_pk_str = self._get_mapped_value(row, header_map, "pk")
                fk_table = self._get_mapped_value(row, header_map, "fk_table")
                fk_col = self._get_mapped_value(row, header_map, "fk_column")

                columns.append(ParsedColumn(
                    name=col_name,
                    data_type=col_type.lower().split("(")[0].strip(),
                    raw_type=col_type,
                    nullable=is_nullable_str.lower() in ("y", "yes", "true", "1", "")
                    if is_nullable_str
                    else True,
                    is_primary_key=is_pk_str.lower() in ("y", "yes", "true", "1")
                    if is_pk_str
                    else False,
                    max_length=self._extract_length(col_type),
                ))

                if fk_table and fk_col:
                    fks.append(ParsedForeignKey(
                        fk_column=col_name,
                        referenced_table=fk_table,
                        referenced_column=fk_col,
                    ))

            tables.append(ParsedTable(
                name=table_name,
                columns=columns,
                foreign_keys=fks,
            ))

        return ParsedSchema(
            source_name=source_name,
            source_type="etl_mapping",
            tables=tables,
            input_format=InputFormat.ETL_MAPPING,
            raw_input=content[:5000],
            parse_warnings=warnings,
        )

    def parse_excel(self, file_bytes: bytes, **kwargs) -> ParsedSchema:
        """Parse an Excel file. Requires openpyxl."""
        import openpyxl

        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return ParsedSchema(source_name="Empty Excel", tables=[])

        headers = [str(h).strip() if h else "" for h in rows[0]]
        csv_lines = [",".join(headers)]
        for row in rows[1:]:
            csv_lines.append(",".join(str(v).strip() if v else "" for v in row))

        csv_content = "\n".join(csv_lines)
        return self.parse(csv_content, **kwargs)

    def _map_headers(self, fieldnames: list[str]) -> dict[str, str]:
        """Map actual CSV headers to canonical header names using synonyms."""
        mapping: dict[str, str] = {}
        for canonical, synonyms in HEADER_SYNONYMS.items():
            for field_name in fieldnames:
                if field_name.lower().strip() in synonyms:
                    mapping[canonical] = field_name
                    break
        return mapping

    def _get_mapped_value(self, row: dict, header_map: dict, canonical: str) -> str:
        actual_header = header_map.get(canonical, "")
        return row.get(actual_header, "").strip() if actual_header else ""

    @staticmethod
    def _extract_length(type_str: str) -> Optional[int]:
        m = re.search(r"\((\d+)", type_str)
        return int(m.group(1)) if m else None

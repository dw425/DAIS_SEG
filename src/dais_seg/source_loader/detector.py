"""Auto-detection of input format for the Universal Source Loader."""

from __future__ import annotations

import re
from typing import Optional

from dais_seg.source_loader.base import InputFormat


class FormatDetector:
    """Detects the format of uploaded or pasted input."""

    @classmethod
    def detect(cls, content: str, filename: Optional[str] = None) -> InputFormat:
        """Detect input format from content and optional filename.

        Priority:
        1. File extension (if filename provided)
        2. Content-based heuristics
        """
        if filename:
            fmt = cls._detect_from_extension(filename)
            if fmt != InputFormat.UNKNOWN:
                return fmt

        return cls._detect_from_content(content)

    @classmethod
    def _detect_from_extension(cls, filename: str) -> InputFormat:
        ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
        mapping = {
            "sql": InputFormat.DDL,
            "ddl": InputFormat.DDL,
            "csv": InputFormat.ETL_MAPPING,
            "tsv": InputFormat.ETL_MAPPING,
            "xlsx": InputFormat.ETL_MAPPING,
            "xls": InputFormat.ETL_MAPPING,
            "json": InputFormat.SCHEMA_JSON,
            "yaml": InputFormat.SCHEMA_YAML,
            "yml": InputFormat.SCHEMA_YAML,
            "env": InputFormat.ENV_FILE,
        }
        return mapping.get(ext, InputFormat.UNKNOWN)

    @classmethod
    def _detect_from_content(cls, content: str) -> InputFormat:
        stripped = content.strip()
        if not stripped:
            return InputFormat.UNKNOWN

        # DDL: contains CREATE TABLE
        if re.search(r"\bCREATE\s+TABLE\b", stripped, re.IGNORECASE):
            return InputFormat.DDL

        # JSON: starts with { and contains "tables"
        if stripped.startswith("{") and '"tables"' in stripped:
            return InputFormat.SCHEMA_JSON

        # YAML: has tables: key on its own line
        if re.search(r"^tables:\s*$", stripped, re.MULTILINE):
            return InputFormat.SCHEMA_YAML

        # ETL mapping: CSV/TSV with known header keywords
        first_line = stripped.split("\n")[0].lower()
        etl_headers = [
            "source_table", "target_table", "src_table", "tgt_table",
            "source_column", "target_column",
        ]
        if any(h in first_line for h in etl_headers):
            return InputFormat.ETL_MAPPING

        # .env: contains SOURCE_TYPE= or DB_TYPE= or SOURCE_HOST=
        if re.search(r"\b(SOURCE_TYPE|DB_TYPE|SOURCE_HOST)\s*=", stripped):
            return InputFormat.ENV_FILE

        # If it has SQL type keywords in column-definition-like context, treat as raw text
        if re.search(
            r"\b(INT|VARCHAR|DECIMAL|TIMESTAMP|BOOLEAN|BIGINT|SMALLINT|FLOAT|DOUBLE)\b",
            stripped,
            re.IGNORECASE,
        ):
            return InputFormat.RAW_TEXT

        return InputFormat.UNKNOWN

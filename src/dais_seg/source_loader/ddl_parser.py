"""DDL parser — extracts schema from CREATE TABLE statements.

Handles Oracle, SQL Server, PostgreSQL, MySQL syntax variations:
- Oracle: NUMBER(p,s), VARCHAR2(n), CLOB, REFERENCES inline
- SQL Server: [brackets], NVARCHAR, BIT for boolean, IDENTITY
- PostgreSQL: SERIAL, TEXT, BYTEA, standard SQL
- MySQL: backtick-quoted names, AUTO_INCREMENT, ENGINE=InnoDB

Parsing strategy: regex-based extraction designed for the 95% case of
straightforward CREATE TABLE statements.
"""

from __future__ import annotations

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

# Data type normalization: dialect-specific types → canonical types
TYPE_NORMALIZATION = {
    # Oracle
    "number": "decimal",
    "varchar2": "varchar",
    "nvarchar2": "varchar",
    "clob": "text",
    "nclob": "text",
    "blob": "binary",
    "raw": "binary",
    # SQL Server
    "nvarchar": "varchar",
    "nchar": "char",
    "bit": "boolean",
    "money": "decimal",
    "smallmoney": "decimal",
    "datetime2": "timestamp",
    "datetimeoffset": "timestamp",
    "uniqueidentifier": "varchar",
    "image": "binary",
    "ntext": "text",
    # PostgreSQL
    "serial": "int",
    "bigserial": "bigint",
    "smallserial": "smallint",
    "real": "float",
    "double precision": "double",
    "character varying": "varchar",
    "character": "char",
    "bytea": "binary",
    "json": "text",
    "jsonb": "text",
    "uuid": "varchar",
    "interval": "varchar",
    # MySQL
    "mediumint": "int",
    "mediumtext": "text",
    "longtext": "text",
    "tinytext": "text",
    "enum": "varchar",
    "set": "varchar",
    "year": "int",
}


class DDLParser(BaseParser):
    """Parses SQL DDL CREATE TABLE statements into ParsedSchema."""

    def can_parse(self, content: str) -> bool:
        return bool(re.search(r"\bCREATE\s+TABLE\b", content, re.IGNORECASE))

    def parse(self, content: str, **kwargs) -> ParsedSchema:
        source_name = kwargs.get("source_name", "DDL Import")
        dialect = kwargs.get("dialect") or self._detect_dialect(content)

        tables = []
        warnings = []

        # Find CREATE TABLE statements and extract name + body using paren-depth matching
        for raw_name, body in self._extract_create_tables(content):
            table_name, schema_name = self._parse_table_name(raw_name)
            columns, fks, table_warnings = self._parse_table_body(body)

            tables.append(ParsedTable(
                name=table_name,
                schema=schema_name,
                columns=columns,
                foreign_keys=fks,
            ))
            warnings.extend(table_warnings)

        # Also extract ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
        alter_fks = self._parse_alter_table_fks(content)
        for table_name, fk in alter_fks:
            for table in tables:
                if table.name.lower() == table_name.lower():
                    table.foreign_keys.append(fk)

        return ParsedSchema(
            source_name=source_name,
            source_type=dialect or "unknown",
            tables=tables,
            input_format=InputFormat.DDL,
            raw_input=content,
            parse_warnings=warnings,
        )

    def _detect_dialect(self, content: str) -> str:
        upper = content.upper()
        if "VARCHAR2" in upper or re.search(r"\bNUMBER\s*\(", upper):
            return "oracle"
        if "[DBO]" in content or "NVARCHAR" in upper or "IDENTITY" in upper:
            return "sqlserver"
        if re.search(r"\bSERIAL\b", upper) or "BYTEA" in upper:
            return "postgresql"
        if "AUTO_INCREMENT" in upper or "ENGINE=" in upper:
            return "mysql"
        return "unknown"

    def _extract_create_tables(self, content: str) -> list[tuple[str, str]]:
        """Extract (table_name, body) pairs from CREATE TABLE statements.

        Uses paren-depth matching instead of regex to correctly handle
        nested parentheses in type definitions like NUMBER(10,2) and
        CHECK constraints like CHECK (status IN ('a','b')).
        """
        results = []
        # Find each CREATE TABLE header
        header_pattern = re.compile(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*\(",
            re.IGNORECASE,
        )
        for header in header_pattern.finditer(content):
            table_name = header.group(1)
            start = header.end()  # position right after the opening (
            depth = 1
            i = start
            while i < len(content) and depth > 0:
                if content[i] == "(":
                    depth += 1
                elif content[i] == ")":
                    depth -= 1
                i += 1
            if depth == 0:
                body = content[start:i - 1]  # exclude the closing )
                results.append((table_name, body))
        return results

    def _parse_table_name(self, raw_name: str) -> tuple[str, str]:
        """Extract table name and schema from qualified name."""
        clean = re.sub(r'[\[\]`"]', "", raw_name)
        parts = clean.split(".")
        if len(parts) >= 2:
            return parts[-1], parts[-2]
        return parts[0], "dbo"

    def _parse_table_body(
        self, body: str
    ) -> tuple[list[ParsedColumn], list[ParsedForeignKey], list[str]]:
        """Parse column definitions and constraints inside CREATE TABLE (...)."""
        columns: list[ParsedColumn] = []
        fks: list[ParsedForeignKey] = []
        warnings: list[str] = []

        elements = self._split_column_definitions(body)

        for element in elements:
            element = element.strip()
            if not element:
                continue

            upper = element.upper().strip()

            # Table-level PRIMARY KEY
            if upper.startswith("PRIMARY KEY"):
                pk_cols = self._extract_parens_list(element)
                for col in columns:
                    if col.name.lower() in [c.lower() for c in pk_cols]:
                        col.is_primary_key = True
                continue

            # Table-level FOREIGN KEY
            if upper.startswith("FOREIGN KEY"):
                fk = self._parse_table_level_fk(element)
                if fk:
                    fks.append(fk)
                continue

            # Table-level UNIQUE, CHECK, CONSTRAINT, INDEX — skip
            if upper.startswith(("UNIQUE", "CHECK", "CONSTRAINT", "INDEX")):
                continue

            # Column definition
            col, inline_fk = self._parse_column_definition(element, warnings)
            if col:
                columns.append(col)
                if inline_fk:
                    fks.append(inline_fk)

        return columns, fks, warnings

    # Multi-word SQL types that should be captured as a single type token
    MULTI_WORD_TYPES = {
        "double precision", "character varying", "long raw",
        "timestamp with time zone", "timestamp without time zone",
        "time with time zone", "time without time zone",
    }

    def _parse_column_definition(
        self, element: str, warnings: list[str]
    ) -> tuple[Optional[ParsedColumn], Optional[ParsedForeignKey]]:
        """Parse a single column definition line."""
        stripped = element.strip()
        # Extract column name (possibly quoted)
        name_match = re.match(r'^([`"\[\]]?\w+[`"\]\]]?)\s+', stripped)
        if not name_match:
            return None, None

        raw_name = re.sub(r'[\[\]`"]', "", name_match.group(1))
        rest = stripped[name_match.end():]

        # Extract type — check for multi-word types first
        raw_type = ""
        type_args = None
        rest_lower = rest.lower()
        for mwt in sorted(self.MULTI_WORD_TYPES, key=len, reverse=True):
            if rest_lower.startswith(mwt):
                raw_type = mwt
                rest = rest[len(mwt):].strip()
                break

        if not raw_type:
            # Single-word type
            type_match = re.match(r'([A-Za-z_]\w*)', rest)
            if not type_match:
                return None, None
            raw_type = type_match.group(1).lower()
            rest = rest[type_match.end():].strip()

        # Extract optional (precision,scale) or (length)
        if rest.startswith("("):
            paren_end = rest.find(")")
            if paren_end > 0:
                type_args = rest[1:paren_end]
                rest = rest[paren_end + 1:].strip()

        modifiers_original = rest        # preserve case for value extraction
        modifiers = rest.upper()          # uppercase for keyword detection

        # Normalize type
        canonical_type = TYPE_NORMALIZATION.get(raw_type, raw_type)
        precision = None
        scale = None
        max_length = None

        if type_args:
            parts = [p.strip() for p in type_args.split(",")]
            if canonical_type in ("decimal", "numeric"):
                precision = int(parts[0]) if parts[0].isdigit() else None
                scale = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            elif canonical_type in ("varchar", "char", "text"):
                if parts[0].isdigit():
                    max_length = int(parts[0])
            # INT(11) in MySQL is display width — ignore

        # Parse modifiers (keyword detection on uppercase)
        nullable = "NOT NULL" not in modifiers
        is_pk = "PRIMARY KEY" in modifiers
        is_unique = "UNIQUE" in modifiers

        # CHECK constraints: CHECK (status IN ('a','b','c')) — extract from original case
        check_values: list[str] = []
        check_match = re.search(r"CHECK\s*\(.*?IN\s*\(([^)]+)\)", modifiers_original, re.IGNORECASE)
        if check_match:
            raw_values = check_match.group(1)
            check_values = [v.strip().strip("'\"") for v in raw_values.split(",")]

        # DEFAULT value — extract from original case
        default_value = None
        default_match = re.search(r"DEFAULT\s+(\S+)", modifiers_original, re.IGNORECASE)
        if default_match:
            default_value = default_match.group(1).strip("'\"")

        # Inline REFERENCES — extract from original case
        inline_fk = None
        ref_match = re.search(
            r"REFERENCES\s+([`\"\[\]]?\w+[`\"\]\]]?)(?:\s*\(\s*(\w+)\s*\))?",
            modifiers_original,
            re.IGNORECASE,
        )
        if ref_match:
            ref_table = re.sub(r'[\[\]`"]', "", ref_match.group(1))
            ref_col = ref_match.group(2) or "id"
            inline_fk = ParsedForeignKey(
                fk_column=raw_name,
                referenced_table=ref_table,
                referenced_column=ref_col,
            )

        # IDENTITY / AUTO_INCREMENT / SERIAL → mark as PK
        if any(kw in modifiers for kw in ("IDENTITY", "AUTO_INCREMENT")):
            is_pk = True
        if raw_type in ("serial", "bigserial", "smallserial"):
            is_pk = True

        col = ParsedColumn(
            name=raw_name,
            data_type=canonical_type,
            raw_type=f"{raw_type}({type_args})" if type_args else raw_type,
            nullable=nullable,
            is_primary_key=is_pk,
            is_unique=is_unique,
            default_value=default_value,
            check_constraints=check_values,
            precision=precision,
            scale=scale,
            max_length=max_length,
        )
        return col, inline_fk

    def _split_column_definitions(self, body: str) -> list[str]:
        """Split CREATE TABLE body on commas, respecting parenthesized sub-expressions."""
        elements = []
        current: list[str] = []
        depth = 0
        for char in body:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                elements.append("".join(current))
                current = []
            else:
                current.append(char)
        if current:
            elements.append("".join(current))
        return elements

    def _extract_parens_list(self, element: str) -> list[str]:
        """Extract comma-separated names from (...) in a constraint."""
        m = re.search(r"\(([^)]+)\)", element)
        if m:
            return [re.sub(r'[\[\]`"]', "", n.strip()) for n in m.group(1).split(",")]
        return []

    def _parse_table_level_fk(self, element: str) -> Optional[ParsedForeignKey]:
        """Parse: FOREIGN KEY (col) REFERENCES table(col)."""
        m = re.search(
            r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\S+)\s*\(\s*(\w+)\s*\)",
            element,
            re.IGNORECASE,
        )
        if m:
            return ParsedForeignKey(
                fk_column=m.group(1),
                referenced_table=re.sub(r'[\[\]`"]', "", m.group(2)).split(".")[-1],
                referenced_column=m.group(3),
            )
        return None

    def _parse_alter_table_fks(self, content: str) -> list[tuple[str, ParsedForeignKey]]:
        """Parse ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ..."""
        results = []
        pattern = re.compile(
            r"ALTER\s+TABLE\s+(\S+)\s+ADD\s+(?:CONSTRAINT\s+\S+\s+)?"
            r"FOREIGN\s+KEY\s*\(\s*[`\"\[\]]?(\w+)[`\"\]\]]?\s*\)\s*"
            r"REFERENCES\s+(\S+)\s*\(\s*[`\"\[\]]?(\w+)[`\"\]\]]?\s*\)",
            re.IGNORECASE,
        )
        for m in pattern.finditer(content):
            table_name = re.sub(r'[\[\]`"]', "", m.group(1)).split(".")[-1]
            fk = ParsedForeignKey(
                fk_column=m.group(2),
                referenced_table=re.sub(r'[\[\]`"]', "", m.group(3)).split(".")[-1],
                referenced_column=m.group(4),
            )
            results.append((table_name, fk))
        return results

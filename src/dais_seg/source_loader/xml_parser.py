"""XML schema definition parser — handles XML-based table/column definitions.

Accepts XML in the form:
  <tables>
    <table name="customers" schema="dbo">
      <column name="id" type="int" primaryKey="true" nullable="false"/>
      <column name="name" type="varchar" length="100" nullable="false"/>
      <column name="status" type="varchar" check="active,inactive,pending"/>
      <foreignKey column="dept_id" references="departments(id)"/>
    </table>
  </tables>

Also supports:
- <schema> as root element (alias for <tables>)
- Inline references: <column name="x" type="int" references="other(id)"/>
- Attributes: pk, primaryKey, unique, default, precision, scale, length
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Optional

from dais_seg.source_loader.base import (
    BaseParser,
    InputFormat,
    ParsedColumn,
    ParsedForeignKey,
    ParsedSchema,
    ParsedTable,
)


class XMLParser(BaseParser):
    """Parses XML schema definitions into ParsedSchema."""

    def can_parse(self, content: str) -> bool:
        stripped = content.strip()
        if not stripped.startswith("<"):
            return False
        try:
            root = ET.fromstring(stripped)
            return root.tag.lower() in ("tables", "schema", "database") or any(
                child.tag.lower() == "table" for child in root
            )
        except ET.ParseError:
            return False

    def parse(self, content: str, **kwargs) -> ParsedSchema:
        source_name = kwargs.get("source_name", "XML Import")

        root = ET.fromstring(content.strip())
        tables = []
        warnings = []

        # Find <table> elements — either direct children or nested under <tables>
        table_elements = []
        if root.tag.lower() in ("tables", "schema", "database"):
            table_elements = [c for c in root if c.tag.lower() == "table"]
        elif root.tag.lower() == "table":
            table_elements = [root]
        else:
            # Search recursively
            table_elements = list(root.iter("table")) + list(root.iter("Table"))

        for table_el in table_elements:
            table = self._parse_table(table_el, warnings)
            if table:
                tables.append(table)

        return ParsedSchema(
            source_name=source_name,
            source_type="xml",
            tables=tables,
            input_format=InputFormat.SCHEMA_XML,
            raw_input=content[:5000],
            parse_warnings=warnings,
        )

    def _parse_table(self, el: ET.Element, warnings: list[str]) -> Optional[ParsedTable]:
        """Parse a <table> element."""
        name = el.get("name", el.get("Name", ""))
        if not name:
            warnings.append("Found <table> element without a name attribute")
            return None

        schema = el.get("schema", el.get("Schema", "dbo"))
        row_count = int(el.get("rowCount", el.get("row_count", "1000")))

        columns = []
        fks = []

        for child in el:
            tag = child.tag.lower()

            if tag == "column":
                col, inline_fk = self._parse_column(child)
                if col:
                    columns.append(col)
                if inline_fk:
                    fks.append(inline_fk)

            elif tag in ("foreignkey", "foreign_key", "fk"):
                fk = self._parse_fk(child)
                if fk:
                    fks.append(fk)

            elif tag in ("primarykey", "primary_key", "pk"):
                # Table-level PK: <primaryKey columns="id,name"/>
                pk_cols_str = child.get("columns", child.get("column", ""))
                pk_cols = [c.strip() for c in pk_cols_str.split(",") if c.strip()]
                for col in columns:
                    if col.name.lower() in [p.lower() for p in pk_cols]:
                        col.is_primary_key = True

        return ParsedTable(
            name=name,
            schema=schema,
            columns=columns,
            foreign_keys=fks,
            row_count=row_count,
        )

    def _parse_column(self, el: ET.Element) -> tuple[Optional[ParsedColumn], Optional[ParsedForeignKey]]:
        """Parse a <column> element."""
        name = el.get("name", el.get("Name", ""))
        if not name:
            return None, None

        raw_type = el.get("type", el.get("Type", el.get("dataType", "varchar")))
        data_type = raw_type.lower().split("(")[0]

        nullable_str = el.get("nullable", el.get("Nullable", "true")).lower()
        nullable = nullable_str not in ("false", "no", "0")

        is_pk_str = el.get("primaryKey", el.get("pk", el.get("PrimaryKey", "false"))).lower()
        is_pk = is_pk_str in ("true", "yes", "1")

        is_unique_str = el.get("unique", el.get("Unique", "false")).lower()
        is_unique = is_unique_str in ("true", "yes", "1")

        default_value = el.get("default", el.get("Default"))

        # Length, precision, scale
        max_length = None
        precision = None
        scale = None

        length_str = el.get("length", el.get("maxLength", el.get("Length")))
        if length_str and length_str.isdigit():
            max_length = int(length_str)

        prec_str = el.get("precision", el.get("Precision"))
        if prec_str and prec_str.isdigit():
            precision = int(prec_str)

        scale_str = el.get("scale", el.get("Scale"))
        if scale_str and scale_str.isdigit():
            scale = int(scale_str)

        # Check constraints
        check_str = el.get("check", el.get("Check", el.get("values", "")))
        check_constraints = [v.strip() for v in check_str.split(",") if v.strip()] if check_str else []

        # Inline references
        inline_fk = None
        ref_str = el.get("references", el.get("References"))
        if ref_str:
            inline_fk = self._parse_ref_string(name, ref_str)

        col = ParsedColumn(
            name=name,
            data_type=data_type,
            raw_type=raw_type,
            nullable=nullable,
            is_primary_key=is_pk,
            is_unique=is_unique,
            default_value=default_value,
            check_constraints=check_constraints,
            precision=precision,
            scale=scale,
            max_length=max_length,
        )
        return col, inline_fk

    def _parse_fk(self, el: ET.Element) -> Optional[ParsedForeignKey]:
        """Parse a <foreignKey> element."""
        column = el.get("column", el.get("Column", ""))
        ref_str = el.get("references", el.get("References", ""))

        if not column or not ref_str:
            return None

        return self._parse_ref_string(column, ref_str)

    def _parse_ref_string(self, fk_column: str, ref_str: str) -> Optional[ParsedForeignKey]:
        """Parse a references string like 'table(column)' or 'table.column'."""
        # Format: table(column)
        m = re.match(r"(\w+)\((\w+)\)", ref_str)
        if m:
            return ParsedForeignKey(
                fk_column=fk_column,
                referenced_table=m.group(1),
                referenced_column=m.group(2),
            )
        # Format: table.column
        if "." in ref_str:
            parts = ref_str.split(".")
            return ParsedForeignKey(
                fk_column=fk_column,
                referenced_table=parts[0],
                referenced_column=parts[1],
            )
        # Just table name — assume "id"
        if ref_str.strip():
            return ParsedForeignKey(
                fk_column=fk_column,
                referenced_table=ref_str.strip(),
                referenced_column="id",
            )
        return None

"""Generic SQL DDL/DML parser using sqlparse.

Extracts CREATE TABLE, INSERT, SELECT, stored procedures.
"""

import logging
import re

import sqlparse

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_sql(content: bytes, filename: str) -> ParseResult:
    """Parse a generic SQL file."""
    text = content.decode("utf-8", errors="replace")
    processors: list[Processor] = []
    connections: list[Connection] = []
    warnings: list[Warning] = []

    statements = sqlparse.parse(text)

    for stmt in statements:
        stmt_type = stmt.get_type()
        raw = str(stmt).strip()
        if not raw:
            continue

        if stmt_type == "CREATE":
            _handle_create(raw, processors)
        elif stmt_type == "INSERT":
            _handle_insert(raw, processors, connections)
        elif stmt_type == "SELECT":
            _handle_select(raw, processors, connections)
        elif stmt_type == "DROP":
            _handle_drop(raw, processors)
        elif raw.upper().startswith(
            (
                "CREATE PROCEDURE",
                "CREATE FUNCTION",
                "CREATE OR REPLACE PROCEDURE",
                "CREATE OR REPLACE FUNCTION",
            )
        ):
            _handle_procedure(raw, processors)
        elif raw.upper().startswith("MERGE"):
            _handle_merge(raw, processors, connections)
        else:
            # Try to detect by regex
            if re.match(r"CREATE\s+", raw, re.IGNORECASE):
                _handle_create(raw, processors)

    if not processors:
        warnings.append(Warning(severity="info", message="No SQL objects found", source=filename))

    return ParseResult(
        platform="sql",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename, "statement_count": len(statements)},
        warnings=warnings,
    )


def _handle_create(raw: str, processors: list[Processor]) -> None:
    match = re.match(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?(\w+)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        raw,
        re.IGNORECASE,
    )
    if match:
        obj_type = match.group(1).upper()
        obj_name = match.group(2).strip(";").strip('"').strip("'").strip("`")

        # Extract columns for TABLE
        props: dict[str, str] = {}
        if obj_type == "TABLE":
            col_match = re.search(r"\((.+)\)", raw, re.DOTALL)
            if col_match:
                skip_prefixes = (
                    "PRIMARY",
                    "FOREIGN",
                    "CONSTRAINT",
                    "INDEX",
                    "UNIQUE",
                    "CHECK",
                )
                cols = [
                    c.strip().split()[0]
                    for c in col_match.group(1).split(",")
                    if c.strip() and not c.strip().upper().startswith(skip_prefixes)
                ]
                props["columns"] = ",".join(cols[:50])
                props["column_count"] = str(len(cols))

        processors.append(
            Processor(
                name=obj_name,
                type=f"SQL_CREATE_{obj_type}",
                platform="sql",
                properties=props,
            )
        )


def _handle_insert(raw: str, processors: list[Processor], connections: list[Connection]) -> None:
    match = re.match(r"INSERT\s+(?:INTO\s+)?(\S+)", raw, re.IGNORECASE)
    if match:
        table = match.group(1).strip(";").strip('"').strip("'").strip("`")
        processors.append(
            Processor(
                name=f"INSERT_{table}",
                type="SQL_INSERT",
                platform="sql",
                properties={"table": table},
            )
        )

        # Find SELECT FROM sources
        for src in re.findall(r"FROM\s+(\S+)", raw, re.IGNORECASE):
            src = src.strip(";").strip('"').strip("(")
            if src.upper() not in ("SELECT", "VALUES", "(", "DUAL"):
                connections.append(Connection(source_name=src, destination_name=f"INSERT_{table}"))


def _handle_select(raw: str, processors: list[Processor], connections: list[Connection]) -> None:
    tables = re.findall(r"FROM\s+(\S+)", raw, re.IGNORECASE)
    joins = re.findall(r"JOIN\s+(\S+)", raw, re.IGNORECASE)
    all_tables = tables + joins
    if all_tables:
        query_name = f"SELECT_{'_'.join(t.split('.')[-1] for t in all_tables[:3])}"
        processors.append(
            Processor(
                name=query_name,
                type="SQL_SELECT",
                platform="sql",
                properties={"tables": ",".join(all_tables)},
            )
        )


def _handle_drop(raw: str, processors: list[Processor]) -> None:
    match = re.match(r"DROP\s+(\w+)\s+(?:IF\s+EXISTS\s+)?(\S+)", raw, re.IGNORECASE)
    if match:
        processors.append(
            Processor(
                name=match.group(2).strip(";"),
                type=f"SQL_DROP_{match.group(1).upper()}",
                platform="sql",
            )
        )


def _handle_procedure(raw: str, processors: list[Processor]) -> None:
    match = re.match(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(\S+)",
        raw,
        re.IGNORECASE,
    )
    if match:
        processors.append(
            Processor(
                name=match.group(1).strip("("),
                type="SQL_PROCEDURE",
                platform="sql",
                properties={"body_length": str(len(raw))},
            )
        )


def _handle_merge(raw: str, processors: list[Processor], connections: list[Connection]) -> None:
    match = re.match(r"MERGE\s+(?:INTO\s+)?(\S+)\s+", raw, re.IGNORECASE)
    using_match = re.search(r"USING\s+(\S+)", raw, re.IGNORECASE)
    if match:
        target = match.group(1).strip(";")
        processors.append(
            Processor(
                name=f"MERGE_{target}",
                type="SQL_MERGE",
                platform="sql",
                properties={"target": target},
            )
        )
        if using_match:
            source = using_match.group(1).strip("(")
            connections.append(Connection(source_name=source, destination_name=f"MERGE_{target}"))

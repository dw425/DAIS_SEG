"""Snowflake SQL file parser.

Extracts CREATE TABLE, CREATE PIPE, CREATE TASK, CREATE STREAM definitions using sqlparse.
"""

import logging
import re

import sqlparse

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)

# Patterns for Snowflake-specific objects
_CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+)?(\w+)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
    re.IGNORECASE,
)

_TASK_AFTER_RE = re.compile(r"AFTER\s+(\S+)", re.IGNORECASE)
_TASK_SCHEDULE_RE = re.compile(r"SCHEDULE\s*=\s*'([^']+)'", re.IGNORECASE)
_COPY_INTO_RE = re.compile(r"COPY\s+INTO\s+(\S+)\s+FROM\s+(\S+)", re.IGNORECASE)
_INSERT_INTO_RE = re.compile(r"INSERT\s+(?:INTO|OVERWRITE\s+INTO)\s+(\S+)", re.IGNORECASE)
_SELECT_FROM_RE = re.compile(r"FROM\s+(\S+)", re.IGNORECASE)


def parse_snowflake(content: bytes, filename: str) -> ParseResult:
    """Parse a Snowflake SQL file."""
    text = content.decode("utf-8", errors="replace")
    processors: list[Processor] = []
    connections: list[Connection] = []
    warnings: list[Warning] = []

    statements = sqlparse.split(text)

    for stmt_text in statements:
        stmt_text = stmt_text.strip()
        if not stmt_text:
            continue

        match = _CREATE_RE.search(stmt_text)
        if not match:
            continue

        obj_type = match.group(1).upper()
        obj_name = match.group(2).strip(";").strip('"').strip("'")

        props: dict[str, str] = {"raw_sql": stmt_text[:500]}

        if obj_type == "TASK":
            # Extract AFTER dependency
            after_match = _TASK_AFTER_RE.search(stmt_text)
            if after_match:
                dep = after_match.group(1)
                props["after"] = dep
                connections.append(Connection(source_name=dep, destination_name=obj_name))
            # Extract schedule
            sched_match = _TASK_SCHEDULE_RE.search(stmt_text)
            if sched_match:
                props["schedule"] = sched_match.group(1)

        elif obj_type == "PIPE":
            # Extract COPY INTO source/dest
            copy_match = _COPY_INTO_RE.search(stmt_text)
            if copy_match:
                props["copy_dest"] = copy_match.group(1)
                props["copy_source"] = copy_match.group(2)

        elif obj_type == "STREAM":
            # Extract ON TABLE
            on_match = re.search(r"ON\s+TABLE\s+(\S+)", stmt_text, re.IGNORECASE)
            if on_match:
                props["on_table"] = on_match.group(1)

        elif obj_type == "TABLE":
            # Extract column definitions count
            col_type_re = (
                r"(\w+)\s+(VARCHAR|NUMBER|INT|FLOAT|DATE"
                r"|TIMESTAMP|BOOLEAN|VARIANT|ARRAY|OBJECT)"
            )
            cols = re.findall(col_type_re, stmt_text, re.IGNORECASE)
            props["column_count"] = str(len(cols))

        elif obj_type in ("VIEW", "MATERIALIZED"):
            # Extract the SQL body (AS SELECT ... or AS ...)
            as_match = re.search(r"\bAS\b\s+(.*)", stmt_text, re.IGNORECASE | re.DOTALL)
            if as_match:
                props["sql_body"] = as_match.group(1).strip()[:500]

        elif obj_type in ("PROCEDURE", "FUNCTION"):
            # Extract the SQL body
            as_match = re.search(r"\bAS\b\s+(.*)", stmt_text, re.IGNORECASE | re.DOTALL)
            if as_match:
                props["sql_body"] = as_match.group(1).strip()[:500]

        else:
            continue  # Skip other CREATE types

        processors.append(
            Processor(
                name=obj_name,
                type=f"Snowflake_{obj_type}",
                platform="snowflake",
                properties=props,
            )
        )

    # Detect COPY INTO / INSERT INTO / SELECT FROM relationships
    for stmt_text in statements:
        insert_match = _INSERT_INTO_RE.search(stmt_text)
        select_matches = _SELECT_FROM_RE.findall(stmt_text)
        if insert_match and select_matches:
            dest = insert_match.group(1)
            for src in select_matches:
                src = src.strip(";").strip("(")
                if src.upper() not in ("SELECT", "VALUES", "("):
                    connections.append(Connection(source_name=src, destination_name=dest))

    if not processors:
        warnings.append(Warning(severity="warning", message="No Snowflake objects found", source=filename))

    return ParseResult(
        platform="snowflake",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename},
        warnings=warnings,
    )

"""Spark .py file parser using ast module.

Extracts SparkSession usage, read/write operations, transformations.
"""

import ast
import logging
import re

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_spark(content: bytes, filename: str) -> ParseResult:
    """Parse an existing Spark Python file."""
    text = content.decode("utf-8", errors="replace")
    warnings: list[Warning] = []
    processors: list[Processor] = []
    connections: list[Connection] = []

    try:
        tree = ast.parse(text, filename=filename)
    except SyntaxError as exc:
        raise ValueError(f"Python syntax error: {exc}") from exc

    # Extract read operations
    for match in re.finditer(
        r'spark\.read(?:Stream)?\.format\(["\'](\w+)["\']\).*?\.(?:load|table)\(["\']([^"\']+)["\']\)',
        text,
        re.DOTALL,
    ):
        fmt = match.group(1)
        path = match.group(2)
        name = f"read_{fmt}_{path.split('/')[-1].split('.')[0]}"
        processors.append(
            Processor(
                name=name,
                type=f"Spark_Read_{fmt}",
                platform="spark",
                properties={"format": fmt, "path": path},
            )
        )

    # Extract spark.table() reads
    for match in re.finditer(r'spark\.table\(["\']([^"\']+)["\']\)', text):
        table = match.group(1)
        processors.append(
            Processor(
                name=f"read_{table}",
                type="Spark_Read_Table",
                platform="spark",
                properties={"table": table},
            )
        )

    # Extract spark.sql() calls
    for match in re.finditer(r'spark\.sql\(\s*(?:"""(.+?)"""|"([^"]+)"|f"""(.+?)""")', text, re.DOTALL):
        sql = match.group(1) or match.group(2) or match.group(3) or ""
        sql_preview = sql.strip()[:100]
        processors.append(
            Processor(
                name=f"sql_{sql_preview[:30].replace(' ', '_')}",
                type="Spark_SQL",
                platform="spark",
                properties={"sql": sql_preview},
            )
        )

    # Extract write operations
    for match in re.finditer(
        r'\.write(?:Stream)?\.format\(["\'](\w+)["\']\).*?\.(?:save|saveAsTable)\(["\']([^"\']+)["\']\)',
        text,
        re.DOTALL,
    ):
        fmt = match.group(1)
        path = match.group(2)
        name = f"write_{fmt}_{path.split('/')[-1].split('.')[0]}"
        processors.append(
            Processor(
                name=name,
                type=f"Spark_Write_{fmt}",
                platform="spark",
                properties={"format": fmt, "path": path},
            )
        )

    # Extract UDF definitions
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                dec_name = ""
                if isinstance(dec, ast.Name):
                    dec_name = dec.id
                elif isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name):
                    dec_name = dec.func.id
                if dec_name in ("udf", "pandas_udf"):
                    processors.append(
                        Processor(
                            name=node.name,
                            type=f"Spark_{dec_name}",
                            platform="spark",
                            properties={"function": node.name},
                        )
                    )

    # Build simple linear connections between reads and writes
    reads = [p for p in processors if "Read" in p.type or p.type == "Spark_SQL"]
    writes = [p for p in processors if "Write" in p.type]
    for r in reads:
        for w in writes:
            connections.append(Connection(source_name=r.name, destination_name=w.name))

    if not processors:
        warnings.append(Warning(severity="info", message="No Spark operations found", source=filename))

    return ParseResult(
        platform="spark",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename},
        warnings=warnings,
    )

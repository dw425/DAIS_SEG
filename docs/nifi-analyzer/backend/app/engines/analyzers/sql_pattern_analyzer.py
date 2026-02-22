"""SQL pattern analyzer — detects and analyzes SQL in processor properties.

Uses SQLGlot (when available) for AST-based analysis, with regex fallback.
Identifies SQL-containing processors, detects dialects, and provides
transpilation results for migration to Spark SQL.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from app.models.pipeline import ParseResult
from app.engines.transformers.sql_transpiler import (
    TranspileResult,
    detect_dialect,
    transpile_to_spark,
)

logger = logging.getLogger(__name__)

# Processor types known to contain SQL
_SQL_PROCESSOR_TYPES = {
    "ExecuteSQL", "ExecuteSQLRecord",
    "SelectHiveQL",
    "PutSQL",
    "QueryDatabaseTable", "QueryDatabaseTableRecord",
    "ConvertJSONToSQL",
    "GenerateTableFetch",
    "PutDatabaseRecord",
    "ListDatabaseTables",
    "ExecuteProcess",  # may contain SQL via command
}

# Property keys that commonly hold SQL statements
_SQL_PROPERTY_KEYS = {
    "SQL select query", "sql-select-query",
    "SQL pre-query", "sql-pre-query",
    "SQL post-query", "sql-post-query",
    "HiveQL Select Query", "hiveql-select-query",
    "Statement", "statement",
    "SQL Statement", "sql-statement",
    "Hive Query", "hive-query",
    "Custom Query", "custom-query",
    "Where Clause", "where-clause",
}

# Regex to detect SQL-like content in any property value
_SQL_DETECT_RE = re.compile(
    r"\b(SELECT|INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE\s+(TABLE|VIEW|INDEX)|"
    r"DROP\s+(TABLE|VIEW)|ALTER\s+TABLE|MERGE\s+INTO|WITH\s+\w+\s+AS)\b",
    re.I,
)


@dataclass
class SqlAnalysisEntry:
    """Analysis of a single SQL statement found in the flow."""
    processor_name: str
    processor_type: str
    property_key: str
    original_sql: str
    detected_dialect: str
    transpile_result: TranspileResult | None = None
    is_sql: bool = True


@dataclass
class SqlPatternReport:
    """Report of all SQL patterns found in the flow."""
    entries: list[SqlAnalysisEntry] = field(default_factory=list)
    total_sql_statements: int = 0
    dialects_found: dict[str, int] = field(default_factory=dict)
    transpilation_summary: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        dialect_counts = {}
        transpiled_count = 0
        failed_count = 0

        for entry in self.entries:
            dialect_counts[entry.detected_dialect] = dialect_counts.get(entry.detected_dialect, 0) + 1
            if entry.transpile_result and entry.transpile_result.is_transpiled:
                transpiled_count += 1
            elif entry.transpile_result and entry.transpile_result.warnings:
                failed_count += 1

        return {
            "totalSqlStatements": self.total_sql_statements,
            "dialectsFound": dialect_counts,
            "transpilationSummary": {
                "transpiled": transpiled_count,
                "failed": failed_count,
                "passthrough": self.total_sql_statements - transpiled_count - failed_count,
            },
            "entries": [
                {
                    "processorName": e.processor_name,
                    "processorType": e.processor_type,
                    "propertyKey": e.property_key,
                    "originalSql": e.original_sql[:200],
                    "detectedDialect": e.detected_dialect,
                    "transpileResult": e.transpile_result.to_dict() if e.transpile_result else None,
                }
                for e in self.entries
            ],
        }


def analyze_sql_patterns(parsed: ParseResult) -> SqlPatternReport:
    """Scan all processors for SQL content, detect dialects, and transpile.

    Returns a SqlPatternReport with all findings.
    """
    report = SqlPatternReport()

    for proc in parsed.processors:
        # Check known SQL processor types
        is_sql_processor = proc.type in _SQL_PROCESSOR_TYPES

        for key, value in proc.properties.items():
            if not value or not isinstance(value, str) or len(value.strip()) < 10:
                continue

            # Check if this property likely contains SQL
            is_sql_property = key in _SQL_PROPERTY_KEYS or key.lower() in {
                k.lower() for k in _SQL_PROPERTY_KEYS
            }
            has_sql_content = bool(_SQL_DETECT_RE.search(value))

            if not (is_sql_processor and (is_sql_property or has_sql_content)) and not has_sql_content:
                continue

            # Found SQL — analyze it
            sql = value.strip()
            dialect = detect_dialect(sql)

            # Transpile to Spark SQL
            transpile_result = transpile_to_spark(sql, dialect)

            entry = SqlAnalysisEntry(
                processor_name=proc.name,
                processor_type=proc.type,
                property_key=key,
                original_sql=sql,
                detected_dialect=dialect,
                transpile_result=transpile_result,
            )
            report.entries.append(entry)

    report.total_sql_statements = len(report.entries)

    # Compute dialect distribution
    for entry in report.entries:
        d = entry.detected_dialect
        report.dialects_found[d] = report.dialects_found.get(d, 0) + 1

    return report


__all__ = ["SqlAnalysisEntry", "SqlPatternReport", "analyze_sql_patterns"]

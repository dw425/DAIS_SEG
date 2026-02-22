"""SQL dialect transpilation module — converts SQL between dialects using SQLGlot.

Provides:
- detect_dialect(sql) -> detected source dialect
- transpile_to_spark(sql, source_dialect) -> TranspileResult with converted SQL
- validate_spark_sql(sql) -> list of validation issues
- extract_column_lineage(sql) -> column-level lineage from SQL AST

Requires: sqlglot>=26.0.0 (optional dependency)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Try to import sqlglot — graceful degradation if not installed
try:
    import sqlglot
    from sqlglot import exp, errors as sqlglot_errors
    from sqlglot.dialects import dialect as sqlglot_dialect
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    sqlglot = None  # type: ignore


# ── Data classes ──

@dataclass
class TranspileResult:
    """Result of SQL transpilation."""
    original_sql: str
    transpiled_sql: str
    source_dialect: str
    target_dialect: str = "spark"
    confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)
    column_lineage: dict[str, list[str]] = field(default_factory=dict)
    is_transpiled: bool = False

    def to_dict(self) -> dict:
        return {
            "originalSql": self.original_sql,
            "transpiledSql": self.transpiled_sql,
            "sourceDialect": self.source_dialect,
            "targetDialect": self.target_dialect,
            "confidence": self.confidence,
            "warnings": self.warnings,
            "columnLineage": self.column_lineage,
            "isTranspiled": self.is_transpiled,
        }


# ── Dialect detection heuristics ──

# Dialect-specific keywords and syntax patterns
_DIALECT_PATTERNS: list[tuple[str, list[str]]] = [
    ("hive", [
        r"\bLATERAL\s+VIEW\b", r"\bEXPLODE\b", r"\bCOLLECT_SET\b",
        r"\bCOLLECT_LIST\b", r"\bMAP_KEYS\b", r"\bSTR_TO_MAP\b",
        r"\bSPLIT\b.*\bLATERAL\b", r"\bSERDE\b", r"\bPARTITIONED\s+BY\b",
        r"\bCLUSTERED\s+BY\b", r"\bSTORED\s+AS\b",
    ]),
    ("mysql", [
        r"\bIFNULL\b", r"\bGROUP_CONCAT\b", r"\bLIMIT\s+\d+\s*,\s*\d+\b",
        r"\bAUTO_INCREMENT\b", r"\bBACKTICK", r"`\w+`",
        r"\bSTR_TO_DATE\b", r"\bDATE_FORMAT\b.*'%",
        r"\bINTO\s+OUTFILE\b", r"\bLOCK\s+TABLES\b",
    ]),
    ("oracle", [
        r"\bROWNUM\b", r"\bDECODE\b", r"\bNVL\b", r"\bNVL2\b",
        r"\bSYSDATE\b", r"\bROWID\b", r"\bCONNECT\s+BY\b",
        r"\bSTART\s+WITH\b", r"\bMINUS\b",
        r"\bTO_CHAR\b", r"\bTO_DATE\b", r"\bTO_NUMBER\b",
    ]),
    ("postgres", [
        r"\bILIKE\b", r"\bSERIAL\b", r"\bBIGSERIAL\b",
        r"\bRETURNING\b", r"\b::\w+", r"\bLATERAL\b.*\bJOIN\b",
        r"\bGENERATE_SERIES\b", r"\bARRAY_AGG\b",
        r"\bNOW\(\)\b", r"\bCURRENT_TIMESTAMP\b.*AT\s+TIME\s+ZONE\b",
    ]),
    ("tsql", [
        r"\bISNULL\b", r"\bTOP\s+\d+\b", r"\bGETDATE\(\)",
        r"\bDATEADD\b", r"\bDATEDIFF\b", r"\bCONVERT\b",
        r"\bNOLOCK\b", r"\bIDENTITY\b.*\bPRIMARY\s+KEY\b",
        r"\bOUTER\s+APPLY\b", r"\bCROSS\s+APPLY\b",
    ]),
    ("sqlite", [
        r"\bAUTOINCREMENT\b", r"\bGLOB\b", r"\bDATETIME\('now'\)",
        r"\bJULIANDAY\b", r"\bTYPEOF\b",
    ]),
    ("bigquery", [
        r"\bSAFE_DIVIDE\b", r"\bSTRUCT\b.*\bAS\b", r"\bUNNEST\b",
        r"\bARRAY\b.*\bSELECT\b", r"\bFORMAT_DATE\b",
        r"\bDATE_TRUNC\b.*\bDAY\b", r"\bFARM_FINGERPRINT\b",
    ]),
    ("spark", [
        r"\bUSING\s+DELTA\b", r"\bTBLPROPERTIES\b",
        r"\bDIST\s*RIBUTE\s+BY\b", r"\bCREATE\s+TEMPORARY\s+VIEW\b",
        r"\bSPARK_PARTITION_ID\b", r"\bINPUT_FILE_NAME\b",
    ]),
]


def detect_dialect(sql: str) -> str:
    """Detect the SQL dialect using heuristic pattern matching.

    Returns one of: 'hive', 'mysql', 'oracle', 'postgres', 'tsql',
    'sqlite', 'bigquery', 'spark', or 'unknown'.
    """
    if not sql or not sql.strip():
        return "unknown"

    sql_upper = sql.upper()
    scores: dict[str, int] = {}

    for dialect, patterns in _DIALECT_PATTERNS:
        score = 0
        for pattern in patterns:
            if re.search(pattern, sql_upper if "BACKTICK" not in pattern else sql, re.IGNORECASE):
                score += 1
        if score > 0:
            scores[dialect] = score

    if not scores:
        return "unknown"

    # If SQLGlot is available, try trial parsing with top candidates
    if SQLGLOT_AVAILABLE and scores:
        top = sorted(scores.items(), key=lambda x: -x[1])[:3]
        for dialect, _ in top:
            try:
                sqlglot.transpile(sql, read=dialect, write="spark")
                return dialect
            except Exception:
                continue

    # Return highest-scoring dialect
    return max(scores, key=scores.get)  # type: ignore


def transpile_to_spark(
    sql: str,
    source_dialect: str | None = None,
) -> TranspileResult:
    """Transpile SQL from source dialect to Spark SQL.

    If source_dialect is None, auto-detects it.
    Falls back to regex-based transpilation if SQLGlot is unavailable.
    """
    if not sql or not sql.strip():
        return TranspileResult(
            original_sql=sql,
            transpiled_sql=sql,
            source_dialect=source_dialect or "unknown",
            confidence=1.0,
            is_transpiled=False,
        )

    # Auto-detect dialect if not specified
    if not source_dialect or source_dialect == "unknown":
        source_dialect = detect_dialect(sql)

    # If already Spark SQL, return as-is
    if source_dialect == "spark":
        return TranspileResult(
            original_sql=sql,
            transpiled_sql=sql,
            source_dialect="spark",
            confidence=1.0,
            is_transpiled=False,
        )

    # Try SQLGlot transpilation
    if SQLGLOT_AVAILABLE:
        return _transpile_with_sqlglot(sql, source_dialect)

    # Fallback: regex-based transpilation for common patterns
    return _transpile_with_regex(sql, source_dialect)


def _transpile_with_sqlglot(sql: str, source_dialect: str) -> TranspileResult:
    """Transpile using SQLGlot AST-based transformation."""
    warnings: list[str] = []
    confidence = 0.95  # High confidence for AST-based transpilation

    # Map dialect names to SQLGlot dialect identifiers
    dialect_map = {
        "hive": "hive",
        "mysql": "mysql",
        "oracle": "oracle",
        "postgres": "postgres",
        "tsql": "tsql",
        "sqlite": "sqlite",
        "bigquery": "bigquery",
        "unknown": "",  # Let SQLGlot try to parse generically
    }
    read_dialect = dialect_map.get(source_dialect, "")

    try:
        result = sqlglot.transpile(
            sql,
            read=read_dialect if read_dialect else None,
            write="spark",
            pretty=True,
        )
        transpiled = result[0] if result else sql

        # Check if anything actually changed
        is_transpiled = transpiled.strip().lower() != sql.strip().lower()

        # Extract column lineage
        lineage = _extract_column_lineage_sqlglot(sql, read_dialect)

        return TranspileResult(
            original_sql=sql,
            transpiled_sql=transpiled,
            source_dialect=source_dialect,
            confidence=confidence,
            warnings=warnings,
            column_lineage=lineage,
            is_transpiled=is_transpiled,
        )
    except Exception as exc:
        warnings.append(f"SQLGlot transpilation error: {str(exc)[:200]}")
        # Fall back to regex
        fallback = _transpile_with_regex(sql, source_dialect)
        fallback.warnings.extend(warnings)
        return fallback


def _transpile_with_regex(sql: str, source_dialect: str) -> TranspileResult:
    """Regex-based transpilation for common cross-dialect patterns."""
    warnings: list[str] = []
    transpiled = sql
    confidence = 0.6  # Lower confidence for regex-based

    # Common transformations per dialect
    if source_dialect in ("oracle", "unknown"):
        # NVL -> COALESCE
        transpiled = re.sub(r"\bNVL\s*\(", "COALESCE(", transpiled, flags=re.I)
        # NVL2 -> CASE WHEN
        transpiled = re.sub(
            r"\bNVL2\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)",
            r"CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END",
            transpiled, flags=re.I
        )
        # SYSDATE -> CURRENT_TIMESTAMP
        transpiled = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP()", transpiled, flags=re.I)
        # DECODE -> CASE
        if re.search(r"\bDECODE\b", transpiled, re.I):
            warnings.append("DECODE function detected — manual CASE WHEN conversion may be needed")
        # ROWNUM -> ROW_NUMBER()
        if re.search(r"\bROWNUM\b", transpiled, re.I):
            warnings.append("ROWNUM detected — replace with ROW_NUMBER() OVER (...)")
        # TO_CHAR date formatting
        transpiled = re.sub(r"\bTO_CHAR\s*\(", "DATE_FORMAT(", transpiled, flags=re.I)
        # MINUS -> EXCEPT
        transpiled = re.sub(r"\bMINUS\b", "EXCEPT", transpiled, flags=re.I)

    if source_dialect in ("mysql", "unknown"):
        # IFNULL -> COALESCE
        transpiled = re.sub(r"\bIFNULL\s*\(", "COALESCE(", transpiled, flags=re.I)
        # GROUP_CONCAT -> COLLECT_LIST + CONCAT_WS
        if re.search(r"\bGROUP_CONCAT\b", transpiled, re.I):
            warnings.append("GROUP_CONCAT detected — use CONCAT_WS(',', COLLECT_LIST(col)) in Spark")
        # LIMIT offset,count -> LIMIT count OFFSET offset
        transpiled = re.sub(
            r"\bLIMIT\s+(\d+)\s*,\s*(\d+)\b",
            r"LIMIT \2 OFFSET \1",
            transpiled, flags=re.I
        )
        # Backtick quoting -> no quoting (Spark supports backticks too, but clean up)
        # Keep backticks as Spark supports them

    if source_dialect in ("tsql", "unknown"):
        # ISNULL -> COALESCE
        transpiled = re.sub(r"\bISNULL\s*\(", "COALESCE(", transpiled, flags=re.I)
        # GETDATE() -> CURRENT_TIMESTAMP()
        transpiled = re.sub(r"\bGETDATE\s*\(\)", "CURRENT_TIMESTAMP()", transpiled, flags=re.I)
        # TOP N -> LIMIT N
        top_match = re.search(r"\bSELECT\s+TOP\s+(\d+)\b", transpiled, re.I)
        if top_match:
            n = top_match.group(1)
            transpiled = re.sub(r"\bTOP\s+\d+\b", "", transpiled, flags=re.I)
            transpiled = transpiled.rstrip().rstrip(";") + f" LIMIT {n}"
        # CONVERT -> CAST
        transpiled = re.sub(
            r"\bCONVERT\s*\(\s*(\w+)\s*,\s*([^)]+)\)",
            r"CAST(\2 AS \1)",
            transpiled, flags=re.I
        )

    if source_dialect in ("postgres", "unknown"):
        # :: type casting -> CAST
        transpiled = re.sub(
            r"(\w+)::(\w+)",
            r"CAST(\1 AS \2)",
            transpiled,
        )
        # ILIKE -> LOWER() LIKE LOWER()
        if re.search(r"\bILIKE\b", transpiled, re.I):
            warnings.append("ILIKE detected — Spark supports ILIKE in Spark 3.3+")

    if source_dialect in ("hive",):
        # Hive is close to Spark SQL, mostly compatible
        confidence = 0.9
        # LATERAL VIEW EXPLODE is supported in Spark
        # COLLECT_SET/LIST is supported in Spark

    is_transpiled = transpiled.strip().lower() != sql.strip().lower()
    if is_transpiled:
        confidence = max(confidence, 0.5)

    return TranspileResult(
        original_sql=sql,
        transpiled_sql=transpiled,
        source_dialect=source_dialect,
        confidence=confidence,
        warnings=warnings,
        is_transpiled=is_transpiled,
    )


def _extract_column_lineage_sqlglot(sql: str, dialect: str) -> dict[str, list[str]]:
    """Extract column-level lineage from SQL using SQLGlot AST.

    Returns {output_column: [source_columns]}.
    """
    if not SQLGLOT_AVAILABLE:
        return {}

    lineage: dict[str, list[str]] = {}
    try:
        parsed = sqlglot.parse_one(sql, read=dialect if dialect else None)

        # Extract SELECT expressions to find output -> source mappings
        for select_expr in parsed.find_all(exp.Select):
            for col_expr in select_expr.expressions:
                # Get the output alias (or column name)
                output_name = ""
                sources: list[str] = []

                if isinstance(col_expr, exp.Alias):
                    output_name = col_expr.alias
                    # Find all column references in the aliased expression
                    for col in col_expr.find_all(exp.Column):
                        col_name = col.name
                        table = col.table
                        sources.append(f"{table}.{col_name}" if table else col_name)
                elif isinstance(col_expr, exp.Column):
                    output_name = col_expr.name
                    table = col_expr.table
                    sources.append(f"{table}.{output_name}" if table else output_name)
                elif isinstance(col_expr, exp.Star):
                    output_name = "*"
                    sources.append("*")

                if output_name and sources:
                    lineage[output_name] = sources

    except Exception as exc:
        logger.debug("Column lineage extraction failed: %s", exc)

    return lineage


def extract_column_lineage(sql: str) -> dict[str, list[str]]:
    """Public API for column lineage extraction."""
    dialect = detect_dialect(sql)
    return _extract_column_lineage_sqlglot(sql, dialect)


def validate_spark_sql(sql: str) -> list[str]:
    """Validate SQL syntax for Spark SQL compatibility.

    Returns a list of issues found (empty = valid).
    """
    issues: list[str] = []

    if not sql or not sql.strip():
        return issues

    if SQLGLOT_AVAILABLE:
        try:
            sqlglot.parse_one(sql, read="spark")
        except Exception as exc:
            issues.append(f"Spark SQL parse error: {str(exc)[:200]}")
    else:
        # Basic regex validation
        sql_upper = sql.upper().strip()
        if not any(sql_upper.startswith(kw) for kw in [
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
            "ALTER", "WITH", "MERGE", "SET", "SHOW", "DESCRIBE", "EXPLAIN",
        ]):
            issues.append(f"SQL does not start with a recognized keyword")

    # Check for known incompatible patterns
    incompatible = [
        (r"\bROWNUM\b", "ROWNUM is not supported in Spark SQL — use ROW_NUMBER() OVER (...)"),
        (r"\bCONNECT\s+BY\b", "CONNECT BY (hierarchical queries) is not supported in Spark SQL"),
        (r"\bOUTER\s+APPLY\b", "OUTER APPLY is not supported — use LEFT JOIN LATERAL"),
        (r"\bTOP\s+\d+\b", "TOP N is not Spark SQL — use LIMIT N"),
    ]
    for pattern, msg in incompatible:
        if re.search(pattern, sql, re.I):
            issues.append(msg)

    return issues


__all__ = [
    "SQLGLOT_AVAILABLE",
    "TranspileResult",
    "detect_dialect",
    "transpile_to_spark",
    "validate_spark_sql",
    "extract_column_lineage",
]

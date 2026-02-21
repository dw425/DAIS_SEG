"""Pass 6: Line-by-Line Analysis -- Every property, expression, script line.

Iterates every property of every processor, classifying each value as
configuration, NEL expression, SQL statement, script code, JSON path,
XPath, regex, Jolt spec, Avro schema, URL, credential, file path, or
static value. Provides PySpark equivalents and conversion confidence.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from app.models.pipeline import ParseResult
from app.models.processor import Processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LineAnalysis:
    line_id: int
    property_key: str
    value: str
    line_type: str  # configuration, nel_expression, nel_expression_compound,
                     # sql_statement, script_code, json_path, xpath, regex,
                     # jolt_spec, avro_schema, static_value, url, credential,
                     # file_path
    what_it_does: str
    why_its_there: str
    needs_conversion: bool | str  # True, False, "partial", "dialect"
    pyspark_equivalent: str | None
    conversion_confidence: float
    conversion_notes: str


@dataclass
class ProcessorLineAnalysis:
    processor: str
    type: str
    lines: list[LineAnalysis]
    script_summary: dict | None  # for ExecuteScript processors


@dataclass
class LineByLineReport:
    processor_analyses: list[ProcessorLineAnalysis]
    total_lines_analyzed: int
    needs_conversion_count: int
    nel_expressions_count: int
    sql_statements_count: int
    script_blocks_count: int
    overall_conversion_confidence: float


# ---------------------------------------------------------------------------
# Pattern matchers
# ---------------------------------------------------------------------------

_NEL_RE = re.compile(r"\$\{")
_NESTED_NEL_RE = re.compile(r"\$\{[^}]*\$\{")  # nested ${..${..}..}
_JSON_PATH_RE = re.compile(r"^\$\.[\w\[\].*]+")
_XPATH_RE = re.compile(r"^/[\w\-]+(/[\w\-\[\]@*]+)+")
_URL_RE = re.compile(
    r"^(https?://|jdbc:|s3://|s3a://|gs://|abfss?://|wasbs?://|hdfs://|ftp://|sftp://)",
    re.I,
)
_CRON_RE = re.compile(r"^\d+\s+[\d*]+\s+[\d*]+\s+[\d*]+\s+[\d*]+")
_JOLT_KEYWORDS = {"shift", "default", "remove", "modify-default-beta",
                  "modify-overwrite-beta", "cardinality", "sort"}

# Property key patterns for classification
_SQL_KEY_RE = re.compile(
    r"(sql|query|statement|select|where|insert|delete|update|merge|dml)",
    re.I,
)
_SCRIPT_KEY_RE = re.compile(r"(script[\s._-]?body|script[\s._-]?file|code)", re.I)
_CRED_KEY_RE = re.compile(
    r"(password|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|credentials|passphrase)",
    re.I,
)
_PATH_KEY_RE = re.compile(
    r"(directory|path|location|remote[\s._-]?path|output[\s._-]?directory|"
    r"input[\s._-]?directory|file[\s._-]?path)",
    re.I,
)

# Regex metacharacters suggesting a regex value
_REGEX_META_RE = re.compile(r"[\[\](){}|\\+*?^$]")

# NiFi internal/meta properties to skip or mark as configuration
_INTERNAL_PROPS = {
    "Scheduling Strategy", "Scheduling Period", "Concurrent Tasks",
    "Run Duration", "Yield Duration", "Penalty Duration",
    "Bulletin Level", "Auto-Terminated Relationships",
    "auto-terminated-relationships-list",
}

# ---------------------------------------------------------------------------
# NEL expression parsing and PySpark mapping
# ---------------------------------------------------------------------------

# Common NEL function -> PySpark mapping
_NEL_FUNC_MAP: dict[str, str] = {
    "toUpper": ".upper()  # or F.upper(col)",
    "toLower": ".lower()  # or F.lower(col)",
    "trim": ".strip()  # or F.trim(col)",
    "substring": "[start:end]  # or F.substring(col, pos, len)",
    "replace": ".replace(old, new)  # or F.regexp_replace(col, pattern, replacement)",
    "replaceAll": "re.sub(pattern, replacement, val)  # or F.regexp_replace()",
    "replaceFirst": "re.sub(pattern, replacement, val, count=1)",
    "matches": "re.match(pattern, val)  # or col.rlike(pattern)",
    "find": "re.search(pattern, val)",
    "indexOf": "val.index(substr)  # or F.instr(col, substr)",
    "length": "len(val)  # or F.length(col)",
    "plus": "+ operator  # or F.expr('col + N')",
    "minus": "- operator",
    "multiply": "* operator",
    "divide": "/ operator",
    "mod": "% operator",
    "toNumber": "int(val) or float(val)  # or col.cast('int')",
    "toString": "str(val)  # or col.cast('string')",
    "toDate": "datetime.strptime(val, fmt)  # or F.to_date(col, fmt)",
    "now": "datetime.now()  # or F.current_timestamp()",
    "format": "val.format() or f-string  # or F.date_format(col, fmt)",
    "UUID": "str(uuid.uuid4())  # or F.expr('uuid()')",
    "uuid": "str(uuid.uuid4())  # or F.expr('uuid()')",
    "nextInt": "random.randint(0, N)  # or F.expr('rand()')",
    "random": "random.random()  # or F.rand()",
    "hostname": "socket.gethostname()  # or spark.conf.get('spark.driver.host')",
    "ip": "socket.gethostbyname(socket.gethostname())",
    "literal": "literal value (pass-through)",
    "isEmpty": "not val or val.strip() == ''  # or F.length(F.trim(col)) == 0",
    "isNull": "val is None  # or col.isNull()",
    "notNull": "val is not None  # or col.isNotNull()",
    "equals": "== operator",
    "equalsIgnoreCase": ".lower() == .lower()",
    "gt": "> operator",
    "ge": ">= operator",
    "lt": "< operator",
    "le": "<= operator",
    "and": "and operator  # or & for columns",
    "or": "or operator  # or | for columns",
    "not": "not operator  # or ~ for columns",
    "ifElse": "val_a if cond else val_b  # or F.when().otherwise()",
    "prepend": "prefix + val  # or F.concat(F.lit(prefix), col)",
    "append": "val + suffix  # or F.concat(col, F.lit(suffix))",
    "padLeft": "val.zfill(n) or val.rjust(n, ch)  # or F.lpad(col, n, ch)",
    "padRight": "val.ljust(n, ch)  # or F.rpad(col, n, ch)",
    "getDelimitedField": "val.split(delim)[idx]  # or F.split(col, delim)[idx]",
    "count": "collection.count()  # or F.size(col)",
    "jsonPath": "json.loads(val)[path]  # or F.get_json_object(col, path)",
    "jsonPathAdd": "update JSON at path  # or manual JSON manipulation",
    "jsonPathSet": "set JSON at path  # or manual JSON manipulation",
    "jsonPathDelete": "delete JSON at path  # or manual JSON manipulation",
    "evaluateELString": "f-string interpolation",
    "base64Encode": "base64.b64encode(val.encode())  # or F.base64(col)",
    "base64Decode": "base64.b64decode(val).decode()  # or F.unbase64(col)",
    "urlEncode": "urllib.parse.quote(val)",
    "urlDecode": "urllib.parse.unquote(val)",
    "escapeJson": "json.dumps(val)  # escapes for JSON embedding",
    "unescapeJson": "json.loads(val)",
    "hash": "hashlib.sha256(val.encode()).hexdigest()  # or F.sha2(col, 256)",
    "math": "math expression  # or F.expr()",
}


def _map_nel_to_pyspark(expression: str) -> tuple[str, float]:
    """Map a NEL expression to PySpark equivalent. Returns (code, confidence)."""
    inner = expression.strip()
    if inner.startswith("${"):
        inner = inner[2:]
    if inner.endswith("}"):
        inner = inner[:-1]

    # Simple attribute reference: ${attr_name}
    if ":" not in inner and "(" not in inner:
        return f'col("{inner}")', 0.95

    # Function chain: ${attr:func1():func2()}
    parts = inner.split(":", 1)
    attr_name = parts[0].strip()
    if len(parts) < 2:
        return f'col("{attr_name}")', 0.90

    func_chain = parts[1]
    # Find first function name
    func_match = re.match(r"(\w+)\s*\(", func_chain)
    if func_match:
        func_name = func_match.group(1)
        pyspark_hint = _NEL_FUNC_MAP.get(func_name, "")
        if pyspark_hint:
            return f'col("{attr_name}")  # NEL :{func_name}() -> {pyspark_hint}', 0.80
        return f'col("{attr_name}")  # NEL :{func_name}() -> MANUAL TRANSLATION', 0.50

    return f'# NEL: {expression[:80]}  -> MANUAL TRANSLATION', 0.40


# ---------------------------------------------------------------------------
# SQL dialect detection
# ---------------------------------------------------------------------------

_SQL_RE = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE|TRUNCATE|"
    r"WITH\s+\w+\s+AS)\b",
    re.I,
)

_SQL_DIALECT_HINTS = {
    "nvl(": "Oracle",
    "nvl2(": "Oracle",
    "decode(": "Oracle",
    "connect by": "Oracle",
    "rownum": "Oracle",
    "sysdate": "Oracle",
    "isnull(": "SQL Server",
    "getdate(": "SQL Server",
    "top ": "SQL Server",
    "dateadd(": "SQL Server",
    "datediff(": "SQL Server",
    "ifnull(": "MySQL",
    "limit ": "MySQL/PostgreSQL",
    "now()": "MySQL/PostgreSQL",
    "::": "PostgreSQL",
    "ilike": "PostgreSQL",
}


def _detect_sql_dialect(sql: str) -> str:
    """Detect SQL dialect from function usage."""
    sql_lower = sql.lower()
    for hint, dialect in _SQL_DIALECT_HINTS.items():
        if hint in sql_lower:
            return dialect
    return "ANSI SQL"


# ---------------------------------------------------------------------------
# Line type classification
# ---------------------------------------------------------------------------

def _classify_line(key: str, value: str) -> str:
    """Classify the type of a property value."""
    if not isinstance(value, str) or not value.strip():
        return "static_value"

    val = value.strip()

    # Internal/scheduling properties
    if key in _INTERNAL_PROPS:
        return "configuration"

    # Credential properties
    if _CRED_KEY_RE.search(key):
        return "credential"

    # Script body
    if _SCRIPT_KEY_RE.search(key):
        return "script_code"

    # SQL-related key with SQL content
    if _SQL_KEY_RE.search(key) and _SQL_RE.search(val):
        return "sql_statement"

    # NEL expressions
    if "${" in val:
        if _NESTED_NEL_RE.search(val):
            return "nel_expression_compound"
        return "nel_expression"

    # JSON path
    if _JSON_PATH_RE.match(val):
        return "json_path"

    # XPath
    if _XPATH_RE.match(val):
        return "xpath"

    # URL
    if _URL_RE.match(val):
        return "url"

    # File path
    if _PATH_KEY_RE.search(key):
        return "file_path"

    # Jolt spec (JSON with known Jolt operation keys)
    if val.startswith("{") or val.startswith("["):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                # Array of Jolt operations
                if parsed and isinstance(parsed[0], dict):
                    if any(k.lower() in _JOLT_KEYWORDS for op in parsed
                           if isinstance(op, dict) for k in op):
                        return "jolt_spec"
                    # Check for Avro schema
                    if any("fields" in op for op in parsed if isinstance(op, dict)):
                        return "avro_schema"
            elif isinstance(parsed, dict):
                if any(k.lower() in _JOLT_KEYWORDS for k in parsed):
                    return "jolt_spec"
                if "type" in parsed and "fields" in parsed:
                    return "avro_schema"
                if "name" in parsed and "type" in parsed:
                    return "avro_schema"
        except (json.JSONDecodeError, TypeError):
            pass

    # Regex (heuristic: value contains regex metacharacters and key suggests pattern)
    if ("regex" in key.lower() or "pattern" in key.lower() or "expression" in key.lower()):
        if _REGEX_META_RE.search(val) and len(val) > 3:
            return "regex"

    # Cron expression
    if _CRON_RE.match(val):
        return "configuration"

    # Bare SQL (no key hint but has SQL keywords)
    if _SQL_RE.search(val) and len(val) > 20:
        return "sql_statement"

    # Everything else
    if key in _INTERNAL_PROPS or val.lower() in ("true", "false", "0", "1"):
        return "configuration"

    return "static_value"


# ---------------------------------------------------------------------------
# Description generators
# ---------------------------------------------------------------------------

def _describe_what(key: str, value: str, line_type: str, proc_type: str) -> str:
    """Generate a 'what it does' description."""
    short_val = value[:80] + "..." if len(value) > 80 else value

    if line_type == "nel_expression" or line_type == "nel_expression_compound":
        return f"Evaluates NiFi Expression Language: {short_val}"
    if line_type == "sql_statement":
        return f"Executes SQL query against the configured database"
    if line_type == "script_code":
        lines = value.strip().split("\n")
        return f"Custom script ({len(lines)} lines of code)"
    if line_type == "json_path":
        return f"Extracts JSON value at path: {short_val}"
    if line_type == "xpath":
        return f"Extracts XML value at XPath: {short_val}"
    if line_type == "regex":
        return f"Matches content against regex pattern: {short_val}"
    if line_type == "jolt_spec":
        return "Defines Jolt JSON transformation specification"
    if line_type == "avro_schema":
        return "Defines Avro schema for record serialization"
    if line_type == "url":
        return f"References external endpoint: {short_val}"
    if line_type == "credential":
        return f"Credential/secret value for authentication"
    if line_type == "file_path":
        return f"File system path: {short_val}"
    if line_type == "configuration":
        return f"Processor configuration: {key} = {short_val}"
    return f"Static value for {key}: {short_val}"


def _describe_why(key: str, value: str, line_type: str, proc_type: str) -> str:
    """Generate a 'why it is there' description."""
    short = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type

    if line_type == "nel_expression" or line_type == "nel_expression_compound":
        return "Dynamic property resolution using NiFi FlowFile attributes"
    if line_type == "sql_statement":
        return f"Data query/manipulation required by {short}"
    if line_type == "script_code":
        return f"Custom business logic that cannot be expressed via {short} configuration"
    if line_type == "json_path":
        return "Field extraction from JSON content into FlowFile attributes"
    if line_type == "xpath":
        return "Field extraction from XML content into FlowFile attributes"
    if line_type == "regex":
        return "Pattern matching for content extraction or routing"
    if line_type == "jolt_spec":
        return "JSON structure transformation (reshape, rename, filter)"
    if line_type == "avro_schema":
        return "Schema definition for record-based processing"
    if line_type == "url":
        return "External system endpoint for data exchange"
    if line_type == "credential":
        return "Authentication required for external system access"
    if line_type == "file_path":
        return "Data location on file system or cloud storage"
    return f"Configuration parameter for {short} behavior"


# ---------------------------------------------------------------------------
# Conversion assessment
# ---------------------------------------------------------------------------

def _assess_conversion(
    key: str,
    value: str,
    line_type: str,
    proc_type: str,
) -> tuple[bool | str, str | None, float, str]:
    """Assess conversion needs for a property line.

    Returns: (needs_conversion, pyspark_equivalent, confidence, notes)
    """
    if line_type == "configuration":
        return False, None, 1.0, "Configuration only -- no code conversion needed"

    if line_type == "static_value":
        return False, None, 1.0, "Static value -- pass through as configuration"

    if line_type == "credential":
        safe_key = re.sub(r"[^a-zA-Z0-9_-]", "-", key).lower()
        equiv = f'dbutils.secrets.get(scope="{{scope}}", key="{safe_key}")'
        return True, equiv, 0.95, "Map to Databricks secret scope"

    if line_type == "nel_expression" or line_type == "nel_expression_compound":
        equiv, conf = _map_nel_to_pyspark(value)
        notes = "Compound NEL -- review carefully" if line_type == "nel_expression_compound" else ""
        return True, equiv, conf, notes

    if line_type == "sql_statement":
        dialect = _detect_sql_dialect(value)
        if dialect == "ANSI SQL":
            return "dialect", f"spark.sql('''{value[:200]}''')", 0.85, "ANSI SQL -- likely compatible with Spark SQL"
        return "dialect", f"spark.sql('''{value[:200]}''')  # {dialect} dialect", 0.60, f"{dialect} dialect -- review for Spark SQL compatibility"

    if line_type == "script_code":
        lines = value.strip().split("\n")
        line_count = len(lines)
        # Classify script complexity
        has_nifi_api = any("session" in l.lower() or "flowFile" in l.lower()
                          or "ProcessSession" in l for l in lines)
        if has_nifi_api:
            return True, "# Manual translation required -- uses NiFi API", 0.30, f"Script ({line_count} lines) uses NiFi session API"
        return True, "# Script requires review for PySpark compatibility", 0.50, f"Script ({line_count} lines) -- review for Python/PySpark porting"

    if line_type == "json_path":
        parts = value.strip().split(".")
        if len(parts) >= 2:
            col_path = ".".join(parts[1:]).replace("[*]", "")
            equiv = f'F.get_json_object(col("content"), \'{value}\')'
            return True, equiv, 0.90, "JSON path -> get_json_object or from_json"
        return True, f'F.get_json_object(col("content"), \'{value}\')', 0.85, ""

    if line_type == "xpath":
        return True, f'# XPath: {value[:80]}\n# Use xpath() UDF or xml parsing library', 0.60, "XPath requires spark-xml library or custom UDF"

    if line_type == "regex":
        return "partial", f'F.regexp_extract(col("content"), r\'{value[:80]}\', 1)', 0.75, "Verify regex syntax compatibility with Java/Python"

    if line_type == "jolt_spec":
        return True, "# Jolt spec -> manual DataFrame transformation chain\n# See: withColumn, select, explode", 0.40, "Jolt specifications require manual translation to PySpark"

    if line_type == "avro_schema":
        return True, "# Avro schema -> StructType definition\n# Use spark.read.format('avro')", 0.80, "Convert Avro schema to PySpark StructType"

    if line_type == "url":
        return "partial", None, 0.90, "Update URL to Databricks-accessible endpoint"

    if line_type == "file_path":
        return "partial", None, 0.85, "Convert to Unity Catalog Volumes path: /Volumes/{catalog}/{schema}/..."

    return False, None, 0.90, ""


# ---------------------------------------------------------------------------
# Script analysis
# ---------------------------------------------------------------------------

_NIFI_BOILERPLATE_RE = re.compile(
    r"(session\.get|session\.transfer|session\.remove|session\.commit|"
    r"session\.rollback|FlowFile|IOUtils\.toString|IOUtils\.write|"
    r"session\.putAttribute|session\.getAttribute|"
    r"ProcessSession|PropertyValue|"
    r"import org\.apache\.nifi|from org\.apache\.nifi)",
    re.I,
)


def _analyze_script(key: str, value: str) -> dict | None:
    """Analyze a script body, classifying lines as boilerplate vs business logic."""
    if not isinstance(value, str) or len(value) < 20:
        return None

    lines = value.strip().split("\n")
    boilerplate_count = 0
    business_count = 0
    import_count = 0
    comment_count = 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") or stripped.startswith("//") or stripped.startswith("/*"):
            comment_count += 1
        elif stripped.startswith("import ") or stripped.startswith("from "):
            import_count += 1
        elif _NIFI_BOILERPLATE_RE.search(stripped):
            boilerplate_count += 1
        else:
            business_count += 1

    total = boilerplate_count + business_count + import_count + comment_count
    business_pct = (business_count / max(total, 1)) * 100

    # Detect language
    language = "unknown"
    if "def " in value or "import " in value and "from " in value:
        language = "python"
    elif "class " in value and "{" in value:
        language = "groovy/java"
    elif "function " in value or "var " in value:
        language = "javascript"

    return {
        "total_lines": len(lines),
        "boilerplate_lines": boilerplate_count,
        "business_logic_lines": business_count,
        "import_lines": import_count,
        "comment_lines": comment_count,
        "business_logic_pct": round(business_pct, 1),
        "language": language,
        "has_nifi_api": boilerplate_count > 0,
        "conversion_effort": (
            "high" if boilerplate_count > business_count
            else "medium" if boilerplate_count > 0
            else "low"
        ),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_line_by_line(
    parsed: ParseResult,
    nel_kb: dict | None = None,
) -> LineByLineReport:
    """Analyze every property of every processor line by line.

    Args:
        parsed: Normalized parse result.
        nel_kb: Optional NEL knowledge base for enhanced expression mapping.

    Returns:
        LineByLineReport with per-processor, per-property analysis.
    """
    processors = parsed.processors

    logger.info("Pass 6 (Line-by-Line): analyzing %d processors", len(processors))

    processor_analyses: list[ProcessorLineAnalysis] = []
    total_lines = 0
    total_needs_conversion = 0
    total_nel = 0
    total_sql = 0
    total_scripts = 0
    confidence_sum = 0.0
    confidence_count = 0

    for p in processors:
        short_type = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        lines: list[LineAnalysis] = []
        script_summary: dict | None = None

        line_id = 0
        for key, val in p.properties.items():
            if val is None:
                continue
            val_str = str(val)
            if not val_str.strip():
                continue

            line_type = _classify_line(key, val_str)
            what = _describe_what(key, val_str, line_type, p.type)
            why = _describe_why(key, val_str, line_type, p.type)
            needs_conv, equiv, confidence, notes = _assess_conversion(
                key, val_str, line_type, p.type,
            )

            la = LineAnalysis(
                line_id=line_id,
                property_key=key,
                value=val_str if len(val_str) <= 500 else val_str[:500] + "...",
                line_type=line_type,
                what_it_does=what,
                why_its_there=why,
                needs_conversion=needs_conv,
                pyspark_equivalent=equiv,
                conversion_confidence=confidence,
                conversion_notes=notes,
            )
            lines.append(la)
            line_id += 1
            total_lines += 1

            if needs_conv and needs_conv is not False:
                total_needs_conversion += 1
            if line_type in ("nel_expression", "nel_expression_compound"):
                total_nel += 1
            if line_type == "sql_statement":
                total_sql += 1
            if line_type == "script_code":
                total_scripts += 1
                script_summary = _analyze_script(key, val_str)

            confidence_sum += confidence
            confidence_count += 1

        processor_analyses.append(ProcessorLineAnalysis(
            processor=p.name,
            type=p.type,
            lines=lines,
            script_summary=script_summary,
        ))

    overall_confidence = (
        round(confidence_sum / max(confidence_count, 1), 3)
    )

    report = LineByLineReport(
        processor_analyses=processor_analyses,
        total_lines_analyzed=total_lines,
        needs_conversion_count=total_needs_conversion,
        nel_expressions_count=total_nel,
        sql_statements_count=total_sql,
        script_blocks_count=total_scripts,
        overall_conversion_confidence=overall_confidence,
    )

    logger.info(
        "Pass 6 complete: %d lines analyzed, %d need conversion, "
        "%d NEL, %d SQL, %d scripts, confidence=%.2f",
        total_lines, total_needs_conversion,
        total_nel, total_sql, total_scripts, overall_confidence,
    )
    return report

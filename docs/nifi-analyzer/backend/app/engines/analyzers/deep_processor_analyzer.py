"""Pass 2: Processor Analysis -- Identify and categorize EVERY processor.

Matches every processor against the YAML knowledge base (or heuristics),
extracts dynamic properties, parses NEL expressions, identifies controller
service references, and flags unknown processors.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from app.models.pipeline import ParseResult
from app.models.processor import Processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ProcessorDetail:
    id: str
    name: str
    type: str
    short_type: str
    category: str
    role: str
    conversion_complexity: str
    databricks_equivalent: str
    scheduling: dict
    properties: dict
    dynamic_properties: dict          # properties containing ${...} NEL expressions
    nel_expressions: list[dict]       # parsed NEL from properties
    controller_services_referenced: list[str]
    auto_terminate_relationships: list[str]
    warnings: list[str]


@dataclass
class ProcessorReport:
    total_processors: int
    by_category: dict[str, int]
    by_role: dict[str, int]
    by_conversion_complexity: dict[str, int]
    unknown_processors: list[dict]
    processors: list[ProcessorDetail]


# ---------------------------------------------------------------------------
# Knowledge-base loader (lazy singleton)
# ---------------------------------------------------------------------------

_YAML_PATH = Path(__file__).parent.parent.parent / "constants" / "processor_maps" / "nifi_databricks.yaml"
_kb_cache: dict | None = None


def _load_kb() -> dict:
    """Load the NiFi-to-Databricks YAML knowledge base.

    Returns a dict keyed by short processor type (e.g. 'GetFile').
    """
    global _kb_cache
    if _kb_cache is not None:
        return _kb_cache

    _kb_cache = {}
    if not _YAML_PATH.exists():
        logger.warning("NiFi KB YAML not found at %s", _YAML_PATH)
        return _kb_cache

    with open(_YAML_PATH) as f:
        raw = yaml.safe_load(f) or {}

    if "mappings" in raw and isinstance(raw["mappings"], list):
        for entry in raw["mappings"]:
            if isinstance(entry, dict) and "type" in entry:
                _kb_cache[entry["type"]] = entry
    elif isinstance(raw, dict):
        _kb_cache = {k: v for k, v in raw.items() if isinstance(v, dict)}

    logger.info("NiFi KB loaded: %d processor type(s)", len(_kb_cache))
    return _kb_cache


# ---------------------------------------------------------------------------
# Role classification (mirrored from nifi_mapper for independence)
# ---------------------------------------------------------------------------

_SOURCE_RE = re.compile(
    r"^(Get|List|Consume|Listen|Fetch|Query|Scan|Select|Generate|Tail)", re.I
)
_SINK_RE = re.compile(r"^(Put|Publish|Send|Post|Insert|Write|Delete)", re.I)
_ROUTE_RE = re.compile(r"^(Route|Distribute|Detect)", re.I)
_TRANSFORM_RE = re.compile(
    r"(Convert|Replace|Update|Jolt|Extract|Split|Merge|Transform|"
    r"Validate|Fork|Partition|Lookup|Compress|Encrypt|Hash|Attribute)",
    re.I,
)
_UTILITY_RE = re.compile(
    r"(Log|Debug|Count|Control|Wait|Notify|ControlRate|Monitor|Sample|Limit)",
    re.I,
)
_EXECUTE_RE = re.compile(r"(Execute|Invoke)", re.I)


def _classify_role(proc_type: str) -> str:
    short = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type
    if _SOURCE_RE.match(short):
        return "source"
    if _SINK_RE.match(short):
        return "sink"
    if _ROUTE_RE.match(short):
        return "route"
    if _UTILITY_RE.search(short):
        return "utility"
    if _TRANSFORM_RE.search(short):
        return "transform"
    if _EXECUTE_RE.search(short):
        return "process"
    return "transform"


# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------

_CATEGORY_MAP: dict[str, str] = {
    "source": "Data Ingestion",
    "sink": "Data Output",
    "transform": "Data Transformation",
    "route": "Data Routing",
    "utility": "Monitoring / Utility",
    "process": "Custom Processing",
}


# ---------------------------------------------------------------------------
# Conversion complexity classification
# ---------------------------------------------------------------------------

_SIMPLE_TYPES = {
    "LogAttribute", "LogMessage", "GenerateFlowFile", "UpdateAttribute",
    "ReplaceText", "AttributesToJSON", "CountText", "DebugFlow",
    "Funnel", "InputPort", "OutputPort",
}

_MODERATE_TYPES = {
    "EvaluateJsonPath", "EvaluateXPath", "RouteOnAttribute", "RouteOnContent",
    "SplitJson", "SplitXml", "SplitText", "SplitRecord", "MergeContent",
    "MergeRecord", "ConvertRecord", "ValidateRecord", "QueryRecord",
    "LookupRecord", "ExtractText", "ExtractGrok", "JoltTransformJSON",
    "ConvertJSONToSQL", "CompressContent", "EncryptContent", "HashContent",
    "ForkRecord", "PartitionRecord",
}

_COMPLEX_TYPES = {
    "ExecuteScript", "ExecuteGroovyScript", "ExecutePython",
    "ExecuteStreamCommand", "ExecuteProcess",
    "InvokeHTTP", "HandleHttpRequest", "HandleHttpResponse",
    "ExecuteSQL", "ExecuteSQLRecord",
}

_ENTERPRISE_TYPES = {
    "PublishKafkaRecord_2_6", "ConsumeKafkaRecord_2_6",
    "PutDatabaseRecord", "QueryDatabaseTableRecord",
    "ListenHTTP", "HandleHttpRequest",
}


def _classify_conversion_complexity(short_type: str, properties: dict) -> str:
    """Classify how difficult it is to convert this processor to Databricks."""
    if short_type in _SIMPLE_TYPES:
        return "trivial"
    if short_type in _MODERATE_TYPES:
        return "moderate"
    if short_type in _ENTERPRISE_TYPES:
        return "enterprise"
    if short_type in _COMPLEX_TYPES:
        return "complex"

    # Heuristic: check for script bodies or many NEL expressions
    nel_count = sum(
        1 for v in properties.values()
        if isinstance(v, str) and "${" in v
    )
    has_script = any(
        "script" in k.lower() and isinstance(v, str) and len(v) > 100
        for k, v in properties.items()
    )
    if has_script:
        return "complex"
    if nel_count > 5:
        return "moderate"
    return "moderate"


# ---------------------------------------------------------------------------
# NEL expression extraction
# ---------------------------------------------------------------------------

_NEL_OUTER_RE = re.compile(r"\$\{")


def _extract_nel_expressions(properties: dict) -> list[dict]:
    """Extract all NiFi Expression Language expressions from properties."""
    expressions: list[dict] = []
    for key, val in properties.items():
        if not isinstance(val, str) or "${" not in val:
            continue
        # Find all top-level ${...} expressions
        pos = 0
        while pos < len(val):
            idx = val.find("${", pos)
            if idx < 0:
                break
            # Track brace depth to find matching close
            depth = 0
            end = idx + 2
            while end < len(val):
                ch = val[end]
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    if depth == 0:
                        expr_text = val[idx:end + 1]
                        # Determine if compound (nested ${...})
                        inner = expr_text[2:-1]
                        is_compound = "${" in inner
                        expressions.append({
                            "property": key,
                            "expression": expr_text,
                            "is_compound": is_compound,
                            "raw_inner": inner,
                        })
                        pos = end + 1
                        break
                    depth -= 1
                end += 1
            else:
                # Unclosed expression
                expressions.append({
                    "property": key,
                    "expression": val[idx:],
                    "is_compound": False,
                    "raw_inner": val[idx + 2:],
                    "warning": "unclosed_expression",
                })
                break
            if end >= len(val):
                break
            if pos <= idx:
                pos = end + 1

    return expressions


# ---------------------------------------------------------------------------
# Dynamic property extraction
# ---------------------------------------------------------------------------

def _extract_dynamic_properties(properties: dict) -> dict:
    """Return subset of properties that contain NEL ${...} expressions."""
    return {
        k: v for k, v in properties.items()
        if isinstance(v, str) and "${" in v
    }


# ---------------------------------------------------------------------------
# Controller service reference detection
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)

# Common property keys that reference controller services
_CS_PROP_KEYS = re.compile(
    r"(record[-.\s_]?reader|record[-.\s_]?writer|"
    r"controller[-.\s_]?service|"
    r"ssl[-.\s_]?context[-.\s_]?service|"
    r"connection[-.\s_]?pool|"
    r"database[-.\s_]?connection|"
    r"distributed[-.\s_]?map[-.\s_]?cache|"
    r"cache[-.\s_]?service|"
    r"lookup[-.\s_]?service|"
    r"registry|"
    r"kerberos[-.\s_]?credentials|"
    r"aws[-.\s_]?credentials|"
    r"gcp[-.\s_]?credentials|"
    r"proxy[-.\s_]?configuration)",
    re.I,
)


def _detect_controller_service_refs(properties: dict) -> list[str]:
    """Detect controller service references from property values."""
    refs: list[str] = []
    for key, val in properties.items():
        if not isinstance(val, str) or not val.strip():
            continue
        # UUID pattern = definite controller service reference
        if _UUID_RE.match(val.strip()):
            refs.append(val.strip())
        # Key pattern match + non-empty value that looks like a service name
        elif _CS_PROP_KEYS.search(key) and len(val.strip()) > 2:
            refs.append(val.strip())
    return refs


# ---------------------------------------------------------------------------
# Auto-terminate relationships
# ---------------------------------------------------------------------------

def _detect_auto_terminate(properties: dict) -> list[str]:
    """Extract auto-terminated relationships from processor properties."""
    auto_term = properties.get("Auto-Terminated Relationships", "")
    if not auto_term:
        auto_term = properties.get("auto-terminated-relationships-list", "")
    if not auto_term or not isinstance(auto_term, str):
        return []
    return [r.strip() for r in auto_term.split(",") if r.strip()]


# ---------------------------------------------------------------------------
# Best-guess categorization for unknown processors
# ---------------------------------------------------------------------------

def _guess_unknown_processor(short_type: str) -> dict:
    """Best-guess categorization for a processor type not in the KB."""
    role = _classify_role(short_type)
    category = _CATEGORY_MAP.get(role, "Unknown")

    # Try to guess Databricks equivalent from name patterns
    lower = short_type.lower()
    equivalent = "# MANUAL: No known Databricks equivalent"
    if "kafka" in lower:
        equivalent = "Structured Streaming Kafka connector"
    elif "s3" in lower or "azure" in lower or "gcs" in lower:
        equivalent = "Auto Loader / Unity Catalog external location"
    elif "database" in lower or "sql" in lower or "jdbc" in lower:
        equivalent = "JDBC connector or Lakehouse Federation"
    elif "http" in lower or "rest" in lower:
        equivalent = "requests library or Model Serving endpoint"
    elif "file" in lower or "hdfs" in lower:
        equivalent = "Auto Loader for file-based ingestion"
    elif "json" in lower or "xml" in lower or "csv" in lower:
        equivalent = "PySpark DataFrame transformations"

    return {
        "short_type": short_type,
        "guessed_role": role,
        "guessed_category": category,
        "guessed_equivalent": equivalent,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_processors(
    parsed: ParseResult,
    knowledge_base: dict | None = None,
) -> ProcessorReport:
    """Analyze every processor in the flow.

    Args:
        parsed: Normalized parse result.
        knowledge_base: Optional pre-loaded KB dict. If None, loads from YAML.

    Returns:
        ProcessorReport with detailed per-processor analysis.
    """
    kb = knowledge_base if knowledge_base is not None else _load_kb()
    processors = parsed.processors

    logger.info("Pass 2 (Processor): analyzing %d processors against %d KB entries",
                len(processors), len(kb))

    details: list[ProcessorDetail] = []
    unknown_list: list[dict] = []

    by_category: dict[str, int] = {}
    by_role: dict[str, int] = {}
    by_complexity: dict[str, int] = {}

    for i, p in enumerate(processors):
        short_type = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type

        # KB lookup: try exact type, then short type, then case-insensitive
        kb_entry = kb.get(p.type) or kb.get(short_type)
        if not kb_entry:
            lower_map = {k.lower(): v for k, v in kb.items()}
            kb_entry = lower_map.get(short_type.lower())

        # Determine category and role
        if kb_entry:
            category = kb_entry.get("category", _CATEGORY_MAP.get(_classify_role(p.type), ""))
            role = kb_entry.get("role", _classify_role(p.type))
            databricks_eq = kb_entry.get("description", kb_entry.get("template", "")[:120])
        else:
            role = _classify_role(p.type)
            category = _CATEGORY_MAP.get(role, "Unknown")
            databricks_eq = ""
            guess = _guess_unknown_processor(short_type)
            unknown_list.append({
                "name": p.name,
                "type": p.type,
                "short_type": short_type,
                **guess,
            })

        # Conversion complexity
        complexity = _classify_conversion_complexity(short_type, p.properties)

        # Dynamic properties
        dynamic_props = _extract_dynamic_properties(p.properties)

        # NEL expressions
        nel_exprs = _extract_nel_expressions(p.properties)

        # Controller service references
        cs_refs = _detect_controller_service_refs(p.properties)

        # Auto-terminate relationships
        auto_term = _detect_auto_terminate(p.properties)

        # Warnings
        warnings: list[str] = []
        if not kb_entry:
            warnings.append(f"Processor type '{short_type}' not found in knowledge base")
        if len(nel_exprs) > 10:
            warnings.append(f"High NEL complexity: {len(nel_exprs)} expressions")
        if short_type.lower() in ("executescript", "executepython", "executegroovyscript"):
            script_body = p.properties.get("Script Body", p.properties.get("script-body", ""))
            if isinstance(script_body, str) and len(script_body) > 500:
                warnings.append(f"Large script body ({len(script_body)} chars) requires manual review")
        if any("password" in k.lower() or "secret" in k.lower() for k in p.properties):
            warnings.append("Contains credential-related properties -- verify secret management")

        detail = ProcessorDetail(
            id=str(i),
            name=p.name,
            type=p.type,
            short_type=short_type,
            category=category,
            role=role,
            conversion_complexity=complexity,
            databricks_equivalent=databricks_eq,
            scheduling=p.scheduling or {},
            properties=p.properties,
            dynamic_properties=dynamic_props,
            nel_expressions=nel_exprs,
            controller_services_referenced=cs_refs,
            auto_terminate_relationships=auto_term,
            warnings=warnings,
        )
        details.append(detail)

        # Aggregate counters
        by_category[category] = by_category.get(category, 0) + 1
        by_role[role] = by_role.get(role, 0) + 1
        by_complexity[complexity] = by_complexity.get(complexity, 0) + 1

    report = ProcessorReport(
        total_processors=len(processors),
        by_category=by_category,
        by_role=by_role,
        by_conversion_complexity=by_complexity,
        unknown_processors=unknown_list,
        processors=details,
    )

    logger.info(
        "Pass 2 complete: %d processors, %d unknown, categories=%s, complexity=%s",
        len(processors), len(unknown_list), by_category, by_complexity,
    )
    return report

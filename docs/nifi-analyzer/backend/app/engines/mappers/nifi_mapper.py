"""NiFi processor mapper — maps 250+ NiFi processor types to Databricks equivalents.

Loads mapping from YAML config file (nifi_databricks.yaml).
Resolves NiFi processor properties into code templates.
"""

import logging
import re
from pathlib import Path

import yaml

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)

_YAML_PATH = Path(__file__).parent.parent.parent / "constants" / "processor_maps" / "nifi_databricks.yaml"

# Lazy-loaded mapping cache
_mapping_cache: dict | None = None

# Regex for unresolved template placeholders (e.g., {input_directory}, {table_name}).
# Uses negative lookbehind to exclude NiFi Expression Language ${...} patterns.
_UNRESOLVED_PLACEHOLDER_RE = re.compile(r"(?<!\$)\{[a-z][a-z0-9_]*\}")

# Environment-level placeholders resolved at pipeline init, NOT per-processor.
# These should NOT penalize confidence since they are set once globally.
_ENV_PLACEHOLDERS = frozenset({
    "catalog", "schema", "scope", "checkpoint_base", "volume",
    "warehouse", "cluster_id", "workspace_url",
})

# NiFi property name → template placeholder mapping.
# Handles the mismatch between NiFi's property names and our template placeholders.
_NIFI_PROPERTY_ALIASES: dict[str, list[str]] = {
    # File/path properties
    "input directory": ["path", "directory", "input_directory"],
    "directory": ["path", "directory"],
    "output directory": ["output_path", "path"],
    "remote path": ["remote_path", "path"],
    "filename": ["filename", "file"],
    "file filter": ["file_filter", "filter"],
    # Command properties
    "command": ["command", "cmd"],
    "command path": ["command", "cmd"],
    "command arguments": ["arg1", "args"],
    "argument delimiter": ["delimiter"],
    # Text/regex properties
    "regular expression": ["pattern", "regex"],
    "replacement value": ["replacement"],
    "replacement strategy": ["strategy"],
    "search value": ["pattern"],
    "character set": ["charset"],
    # Column/field properties
    "column name": ["col", "column", "field"],
    "attribute name": ["field", "attribute"],
    "jsonpath expression": ["jsonpath", "path"],
    "jolt specification": ["jolt_spec", "spec"],
    # Rate/interval properties
    "rate control criteria": ["criteria"],
    "maximum rate": ["interval", "rate"],
    "time duration": ["interval", "duration"],
    "time period": ["interval", "period"],
    "scheduling period": ["interval"],
    "run schedule": ["interval"],
    # Database/connection properties
    "database connection url": ["jdbc_url", "url"],
    "database user": ["user", "username"],
    "table name": ["table", "table_name"],
    "sql select query": ["query", "sql"],
    "sql statement": ["query", "sql"],
    # Kafka properties
    "topic": ["topic"],
    "topic name": ["topic"],
    "kafka brokers": ["brokers", "bootstrap_servers"],
    "group id": ["group_id", "consumer_group"],
    # Format properties
    "input format": ["format", "input_format"],
    "output format": ["format", "output_format"],
    "file format": ["format"],
    "record reader": ["format"],
    # Extract properties
    "group": ["group"],
    "capture group": ["group"],
    # HTTP properties
    "remote url": ["url"],
    "http url": ["url"],
    "http method": ["method"],
    # S3/cloud properties
    "bucket": ["bucket"],
    "region": ["region"],
    "prefix": ["prefix"],
    "object key": ["key", "path"],
    "container name": ["container"],
    # HDFS properties
    "hadoop configuration resources": ["hadoop_conf"],
    "kerberos principal": ["principal"],
    # Generic properties
    "batch size": ["batch_size"],
    "max records": ["max_records"],
    "delimiter": ["delimiter"],
    "header": ["header"],
    "compression format": ["compression"],
}


def _load_mapping() -> dict:
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache
    if _YAML_PATH.exists():
        with open(_YAML_PATH) as f:
            raw = yaml.safe_load(f) or {}
        # Handle both formats:
        #   1. Direct dict keyed by type: {GetFile: {...}, ...}
        #   2. List under 'mappings' key: {mappings: [{type: "GetFile", ...}, ...]}
        if "mappings" in raw and isinstance(raw["mappings"], list):
            _mapping_cache = {}
            for entry in raw["mappings"]:
                if isinstance(entry, dict) and "type" in entry:
                    _mapping_cache[entry["type"]] = entry
        elif isinstance(raw, dict):
            _mapping_cache = raw
        else:
            _mapping_cache = {}
    else:
        logger.warning("NiFi mapping YAML not found at %s", _YAML_PATH)
        _mapping_cache = {}
    return _mapping_cache


def _classify_role(proc_type: str) -> str:
    """Classify NiFi processor into a role."""
    t = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type
    _source_prefixes = ("Get", "List", "Consume", "Listen", "Fetch", "Query", "Scan", "Select", "Generate")
    if any(t.startswith(p) for p in _source_prefixes):
        return "source"
    if any(t.startswith(p) for p in ("Put", "Publish", "Send", "Post", "Insert", "Write", "Delete")):
        return "sink"
    if any(t.startswith(p) for p in ("Route", "Distribute", "Detect")):
        return "route"
    if any(w in t for w in ("Convert", "Replace", "Update", "Jolt", "Extract", "Split", "Merge", "Transform")):
        return "transform"
    if any(w in t for w in ("Log", "Debug", "Count", "Wait", "Notify", "ControlRate")):
        return "utility"
    if any(w in t for w in ("Execute", "Invoke")):
        return "process"
    return "transform"


def _resolve_properties(code: str, properties: dict[str, str], proc_name: str) -> str:
    """Resolve NiFi processor properties into code template placeholders.

    Two-pass resolution:
      1. Direct property name match (normalized to snake_case)
      2. Alias-based matching using _NIFI_PROPERTY_ALIASES lookup
    """
    safe_name = _safe_var(proc_name)

    # Pass 1: Direct property name → placeholder substitution
    for prop_key, prop_val in properties.items():
        normalized = prop_key.lower().replace(" ", "_").replace("-", "_")
        placeholder = "{" + normalized + "}"
        code = code.replace(placeholder, str(prop_val))

    # Pass 2: Alias-based resolution — NiFi property names map to template placeholders
    for prop_key, prop_val in properties.items():
        normalized_key = prop_key.lower().strip()
        aliases = _NIFI_PROPERTY_ALIASES.get(normalized_key, [])
        for alias in aliases:
            placeholder = "{" + alias + "}"
            if placeholder in code:
                code = code.replace(placeholder, str(prop_val))

    # Pass 3: Generic aliases (longest first to avoid partial matches)
    code = code.replace("{input}", safe_name)
    code = code.replace("{name}", safe_name)
    code = code.replace("{in}", safe_name)
    code = code.replace("{v}", safe_name)

    return code


def _count_unresolved_placeholders(code: str) -> int:
    """Count unresolved processor-level placeholders (excludes env-level ones)."""
    all_placeholders = _UNRESOLVED_PLACEHOLDER_RE.findall(code)
    return sum(1 for p in all_placeholders if p.strip("{}") not in _ENV_PLACEHOLDERS)


def _annotate_unresolved(code: str) -> str:
    """Add TODO comments for each unresolved placeholder in generated code."""
    all_placeholders = _UNRESOLVED_PLACEHOLDER_RE.findall(code)
    processor_level = [p for p in all_placeholders if p.strip("{}") not in _ENV_PLACEHOLDERS]
    env_level = [p for p in all_placeholders if p.strip("{}") in _ENV_PLACEHOLDERS]

    lines = []
    if processor_level:
        lines.append(f"# TODO: {len(processor_level)} processor placeholder(s): {', '.join(processor_level)}")
    if env_level:
        lines.append(f"# NOTE: {len(env_level)} env placeholder(s) resolved at pipeline init: {', '.join(env_level)}")
    if lines:
        return "\n".join(lines) + "\n" + code
    return code


def map_nifi(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    """Map NiFi processors to Databricks equivalents."""
    mapping_table = _load_mapping()
    mappings: list[MappingEntry] = []
    packages: set[str] = set()
    unmapped = 0

    for p in parse_result.processors:
        simple_type = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        entry = mapping_table.get(p.type) or mapping_table.get(simple_type)
        role = _classify_role(p.type)

        if entry:
            code = entry.get("template", "")

            # Resolve processor properties into template placeholders
            code = _resolve_properties(code, p.properties, p.name)

            for pkg in entry.get("imports", []):
                if pkg:
                    packages.add(pkg)

            # Confidence: penalize only processor-level unresolved placeholders
            # Environment placeholders (catalog, schema, scope) are resolved at
            # pipeline init and do NOT reduce confidence.
            base_confidence = entry.get("confidence", 0.9)
            unresolved_count = _count_unresolved_placeholders(code)
            if unresolved_count > 0:
                # -5% per unresolved processor placeholder, floor at 0.5
                confidence = max(0.5, base_confidence - (unresolved_count * 0.05))
                code = _annotate_unresolved(code)
                notes = (
                    entry.get("notes", "")
                    + f" [{unresolved_count} unresolved placeholder(s)]"
                )
            else:
                confidence = base_confidence
                notes = entry.get("notes", "")

            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=entry.get("role", role),
                    category=entry.get("category", ""),
                    mapped=True,
                    confidence=confidence,
                    code=code,
                    notes=notes,
                )
            )
        else:
            unmapped += 1
            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=role,
                    category="Unmapped",
                    mapped=False,
                    confidence=0.0,
                    code=f"# UNMAPPED: NiFi {p.type} ({p.name}) -- manual migration required",
                    notes=f"No Databricks mapping found for {p.type}",
                )
            )

    return AssessmentResult(
        mappings=mappings,
        packages=sorted(packages),
        unmapped_count=unmapped,
    )


def _safe_var(name: str) -> str:
    """Convert a processor name to a safe Python variable name."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()

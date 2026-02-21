"""
Property Resolver for NiFi → Databricks Mapping Templates.

Resolves template placeholders using processor properties and selects
template variants based on detected cloud provider, protocol, or format.
"""

import logging
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ── Cloud-path detection patterns ──────────────────────────────────
_CLOUD_PATTERNS = {
    "s3":    re.compile(r"s3[an]?://", re.IGNORECASE),
    "azure": re.compile(r"(wasbs?|abfss?)://", re.IGNORECASE),
    "gcs":   re.compile(r"gs://", re.IGNORECASE),
    "dbfs":  re.compile(r"dbfs:/", re.IGNORECASE),
}

# ── NiFi property → placeholder name mapping ──────────────────────
_PROPERTY_ALIASES: Dict[str, list] = {
    "brokers":          ["Kafka Brokers", "bootstrap.servers", "kafka.bootstrap.servers"],
    "topic":            ["Topic Name", "topic", "Topic"],
    "table":            ["Table Name", "table-name", "dbtable", "Table"],
    "path":             ["Directory", "Remote Path", "directory", "Input Directory"],
    "host":             ["Hostname", "host", "Host", "SFTP Hostname", "FTP Hostname"],
    "scope":            ["Secret Scope", "secret-scope"],
    "driver":           ["Database Driver Class Name", "driver-class"],
    "format":           ["Input Format", "format", "File Format"],
    "container":        ["Container Name", "container", "Container"],
    "account":          ["Storage Account Name", "account-name", "Account Name"],
    "bucket":           ["Bucket", "bucket-name", "Bucket Name", "S3 Bucket"],
    "key":              ["Object Key", "key", "S3 Object Key"],
    "prefix":           ["Prefix", "prefix", "Search Prefix"],
    "database":         ["Database Name", "database", "Mongo Database"],
    "collection":       ["Collection Name", "collection", "Mongo Collection"],
    "queue":            ["Queue Name", "queue", "Queue"],
    "queue_url":        ["Queue URL", "queue-url"],
    "region":           ["Region", "region", "AWS Region"],
    "url":              ["URL", "Remote URL", "HTTP URL", "url"],
    "remote_path":      ["Remote Path", "remote-path"],
    "filename":         ["Filename", "filename", "File Name"],
    "pattern":          ["Regular Expression", "pattern", "Regex"],
    "replacement":      ["Replacement Value", "replacement", "Replacement"],
    "col":              ["Column Name", "column-name", "Column"],
    "attr":             ["Attribute Name", "attribute-name"],
    "value":            ["Value", "value", "Attribute Value"],
    "condition":        ["Routing Condition", "condition"],
    "sql":              ["SQL Statement", "SQL select query", "sql-query"],
    "catalog":          ["Unity Catalog", "catalog"],
    "schema":           ["Unity Schema", "schema", "Schema Name"],
    "count":            ["Number of Records", "count", "Batch Size"],
    "partitions":       ["Number of Partitions", "partitions"],
    "interval":         ["Trigger Interval", "interval", "Run Schedule"],
    "timeout":          ["Timeout", "timeout"],
    "key_col":          ["Key Column", "key-column"],
    "value_col":        ["Value Column", "value-column"],
}

# ── Default placeholder values ────────────────────────────────────
# DEFAULT VALUE: These are injected when no matching processor property is found.
_DEFAULTS: Dict[str, str] = {
    "catalog":    "main",
    "schema":     "default",
    "format":     "csv",
    "driver":     "com.mysql.cj.jdbc.Driver",
    "scope":      "nifi-migration",
    "partitions": "8",
    "count":      "100",
    "region":     "us-east-1",
    "timeout":    "3600",
    "port":       "22",
    "bits":       "256",
    "mode":       "append",
    "ascending":  "True",
}


def _detect_cloud_provider(properties: Dict[str, Any]) -> Optional[str]:
    """Detect cloud provider from property values containing cloud paths."""
    for _prop_val in properties.values():
        val = str(_prop_val) if _prop_val is not None else ""
        for provider, pattern in _CLOUD_PATTERNS.items():
            if pattern.search(val):
                return provider
    return None


def _normalise_name(raw_name: str) -> str:
    """Convert a NiFi processor name to a valid Python identifier."""
    name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if not name or name[0].isdigit():
        name = "proc_" + name
    return name


def _resolve_property(placeholder: str, properties: Dict[str, Any]) -> Optional[str]:
    """Resolve a single placeholder from processor properties."""
    # Direct match
    if placeholder in properties:
        return str(properties[placeholder])

    # Check aliases
    aliases = _PROPERTY_ALIASES.get(placeholder, [])
    for alias in aliases:
        if alias in properties:
            return str(properties[alias])

    # Case-insensitive search
    lower_key = placeholder.lower().replace("_", " ")
    for prop_name, prop_val in properties.items():
        if prop_name.lower().replace("-", " ").replace("_", " ") == lower_key:
            return str(prop_val)

    return None


def resolve_template(
    proc_type: str,
    properties: Dict[str, Any],
    mapping_entry: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Select the best template variant based on processor properties.

    For certain processor types, the cloud provider or protocol detected
    in the properties may influence which template or options to use.

    Returns
    -------
    dict
        A copy of *mapping_entry* with the ``template`` field potentially
        adjusted for the detected cloud variant.
    """
    result = dict(mapping_entry)
    template = result.get("template", "")

    provider = _detect_cloud_provider(properties)

    # ── Cloud-specific template overrides ──────────────────────────
    if proc_type in ("GetFile", "PutFile", "FetchFile", "ListFile", "TailFile"):
        if provider == "s3":
            template = template.replace(
                "/Volumes/{catalog}/{schema}/{path}",
                "s3a://{bucket}/{path}",
            )
            result["category"] = "Cloud Storage"
        elif provider == "azure":
            template = template.replace(
                "/Volumes/{catalog}/{schema}/{path}",
                "abfss://{container}@{account}.dfs.core.windows.net/{path}",
            )
            result["category"] = "Azure ADLS"
        elif provider == "gcs":
            template = template.replace(
                "/Volumes/{catalog}/{schema}/{path}",
                "gs://{bucket}/{path}",
            )
            result["category"] = "Cloud Storage"

    # ── Format-aware adjustments ──────────────────────────────────
    fmt = _resolve_property("format", properties)
    if fmt and fmt.lower() in ("parquet", "orc", "avro", "delta", "json", "csv"):
        template = template.replace(
            '.option("cloudFiles.format", "csv")',
            f'.option("cloudFiles.format", "{fmt.lower()}")',
        )

    result["template"] = template
    return result


def resolve_placeholders(
    template: str,
    processor: Dict[str, Any],
) -> str:
    """
    Resolve all ``{placeholder}`` tokens in *template* from processor
    properties, falling back to sensible defaults.

    Parameters
    ----------
    template : str
        The code template with ``{placeholder}`` tokens.
    processor : dict
        A processor dict with at least ``name`` and ``properties`` keys.

    Returns
    -------
    str
        The template with all resolvable placeholders filled in.
    """
    if template is None:
        return ""

    properties = processor.get("properties", {})
    proc_name = processor.get("name", processor.get("type", "unnamed"))
    safe_name = _normalise_name(proc_name)

    # Find all placeholders
    placeholders = set(re.findall(r"\{(\w+)\}", template))

    replacements: Dict[str, str] = {}
    for ph in placeholders:
        # Special built-in placeholders
        if ph == "name":
            replacements[ph] = safe_name
            continue
        if ph == "input":
            # Try to resolve from upstream connection (caller must set this)
            replacements[ph] = properties.get("_input_ref", "upstream")
            continue
        if ph in ("input1", "input2"):
            replacements[ph] = properties.get(f"_input_ref_{ph[-1]}", f"upstream_{ph[-1]}")
            continue

        # Try property resolution
        resolved = _resolve_property(ph, properties)
        if resolved is not None:
            replacements[ph] = resolved
            continue

        # Fallback to defaults
        if ph in _DEFAULTS:
            replacements[ph] = _DEFAULTS[ph]
            continue

        # Leave unresolved placeholders as a safe string literal
        logger.warning("Unresolved placeholder '%s' for processor '%s' — using UNSET_%s", ph, proc_name, ph)
        replacements[ph] = f"UNSET_{ph}"

    unresolved = sum(1 for v in replacements.values() if v.startswith("UNSET_"))
    if unresolved:
        logger.info("resolve_placeholders: %d/%d placeholders unresolved for '%s'", unresolved, len(placeholders), proc_name)

    # Apply all replacements
    result = template
    for ph, val in replacements.items():
        result = result.replace("{" + ph + "}", val)

    return result


def resolve_full(
    proc_type: str,
    processor: Dict[str, Any],
    mapping_entry: Dict[str, Any],
) -> str:
    """
    Convenience: select template variant *and* resolve placeholders.

    Returns the fully resolved PySpark code string.
    """
    properties = processor.get("properties", {})
    adjusted = resolve_template(proc_type, properties, mapping_entry)
    return resolve_placeholders(adjusted["template"], processor)

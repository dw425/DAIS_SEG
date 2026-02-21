"""Pass 4: Upstream Analysis -- Where is data coming from?

Identifies all data sources, detects formats from controller services,
infers schemas from processor properties, builds attribute lineage,
maps credentials to dbutils.secrets, and tracks parameter injection points.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import Connection, ControllerService, Processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DataSource:
    processor: str
    source_type: str  # file, message_queue, database, api, generated, network
    system: str
    location: str
    format: str
    inferred_schema: dict
    databricks_equivalent: str
    credentials_required: list[str]
    secrets_mapping: dict


@dataclass
class UpstreamReport:
    data_sources: list[DataSource]
    attribute_lineage: dict[str, dict]
    content_transformations: list[dict]
    external_dependencies: list[dict]
    parameter_injections: list[dict]


# ---------------------------------------------------------------------------
# Source type detection
# ---------------------------------------------------------------------------

_SOURCE_RE = re.compile(
    r"^(Get|List|Consume|Listen|Fetch|Query|Scan|Select|Generate|Tail)", re.I
)

_SOURCE_TYPE_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # (regex on short_type, source_type, system)
    (re.compile(r"Kafka", re.I), "message_queue", "Apache Kafka"),
    (re.compile(r"JMS", re.I), "message_queue", "JMS"),
    (re.compile(r"AMQP", re.I), "message_queue", "AMQP / RabbitMQ"),
    (re.compile(r"MQTT", re.I), "message_queue", "MQTT"),
    (re.compile(r"SQS", re.I), "message_queue", "AWS SQS"),
    (re.compile(r"(Database|SQL|JDBC)", re.I), "database", "SQL Database"),
    (re.compile(r"Oracle", re.I), "database", "Oracle Database"),
    (re.compile(r"MySQL", re.I), "database", "MySQL"),
    (re.compile(r"Postgres", re.I), "database", "PostgreSQL"),
    (re.compile(r"Mongo", re.I), "database", "MongoDB"),
    (re.compile(r"Cassandra", re.I), "database", "Cassandra"),
    (re.compile(r"HBase", re.I), "database", "HBase"),
    (re.compile(r"Elastic", re.I), "database", "Elasticsearch"),
    (re.compile(r"Couchbase", re.I), "database", "Couchbase"),
    (re.compile(r"Solr", re.I), "database", "Apache Solr"),
    (re.compile(r"Redis", re.I), "database", "Redis"),
    (re.compile(r"Neo4j", re.I), "database", "Neo4j"),
    (re.compile(r"InfluxDB", re.I), "database", "InfluxDB"),
    (re.compile(r"Kudu", re.I), "database", "Apache Kudu"),
    (re.compile(r"Hive", re.I), "database", "Hive"),
    (re.compile(r"S3", re.I), "file", "AWS S3"),
    (re.compile(r"Azure.*Blob", re.I), "file", "Azure Blob Storage"),
    (re.compile(r"Azure.*Lake|ADLS", re.I), "file", "Azure Data Lake"),
    (re.compile(r"GCS|BigQuery", re.I), "file", "Google Cloud Storage"),
    (re.compile(r"HDFS|Hadoop", re.I), "file", "HDFS"),
    (re.compile(r"SFTP|FTP", re.I), "file", "SFTP/FTP"),
    (re.compile(r"File", re.I), "file", "Local Filesystem"),
    (re.compile(r"HTTP|REST|InvokeHTTP", re.I), "api", "HTTP/REST API"),
    (re.compile(r"Syslog", re.I), "network", "Syslog"),
    (re.compile(r"Listen(TCP|UDP)", re.I), "network", "TCP/UDP Socket"),
    (re.compile(r"GenerateFlowFile", re.I), "generated", "Test Data Generator"),
]


def _classify_source(short_type: str) -> tuple[str, str]:
    """Classify the source type and system from the processor type."""
    for pattern, src_type, system in _SOURCE_TYPE_PATTERNS:
        if pattern.search(short_type):
            return src_type, system
    return "file", "Unknown"


# ---------------------------------------------------------------------------
# Format detection from controller services and processor properties
# ---------------------------------------------------------------------------

_FORMAT_FROM_TYPE: dict[str, str] = {
    "json": "JSON",
    "csv": "CSV",
    "avro": "Avro",
    "parquet": "Parquet",
    "xml": "XML",
    "grok": "Grok (semi-structured)",
    "syslog": "Syslog",
    "cef": "CEF",
    "text": "Plain Text",
    "protobuf": "Protocol Buffers",
    "orc": "ORC",
}

_READER_TYPE_RE = re.compile(
    r"(Json|CSV|Avro|Parquet|XML|Grok|Syslog|CEF|Text|Protobuf|ORC)",
    re.I,
)


def _detect_format(
    proc: Processor,
    cs_map: dict[str, ControllerService],
) -> str:
    """Detect the data format from processor properties or controller services."""
    # Check processor type name
    short = proc.type.rsplit(".", 1)[-1] if "." in proc.type else proc.type
    for fmt_key, fmt_name in _FORMAT_FROM_TYPE.items():
        if fmt_key.lower() in short.lower():
            return fmt_name

    # Check controller service references
    for key, val in proc.properties.items():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            m = _READER_TYPE_RE.search(cs.type)
            if m:
                return _FORMAT_FROM_TYPE.get(m.group(1).lower(), m.group(1))

    # Check format-related properties
    for key, val in proc.properties.items():
        if isinstance(val, str) and "format" in key.lower():
            normalized = val.strip().lower()
            if normalized in _FORMAT_FROM_TYPE:
                return _FORMAT_FROM_TYPE[normalized]

    return "unknown"


# ---------------------------------------------------------------------------
# Location extraction
# ---------------------------------------------------------------------------

_LOCATION_PROPS = [
    "directory", "input directory", "remote path", "path",
    "bucket", "container name", "table name", "dbtable",
    "topic", "topic name", "queue name", "queue",
    "remote url", "http url", "url",
    "database connection url", "hadoop configuration resources",
    "filename", "file filter",
]


def _extract_location(proc: Processor) -> str:
    """Extract the data location/path from processor properties."""
    props_lower = {k.lower().strip(): v for k, v in proc.properties.items()}
    for candidate in _LOCATION_PROPS:
        val = props_lower.get(candidate, "")
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


# ---------------------------------------------------------------------------
# Schema inference from processor properties
# ---------------------------------------------------------------------------

_JSONPATH_RE = re.compile(r"^\$\.[\w\[\].*]+$")


def _infer_schema(proc: Processor, cs_map: dict[str, ControllerService]) -> dict:
    """Infer schema hints from processor properties.

    Returns dict with fields, source_hint, and schema_type.
    """
    schema: dict = {"fields": [], "source_hint": "", "schema_type": "inferred"}
    short = proc.type.rsplit(".", 1)[-1] if "." in proc.type else proc.type

    # EvaluateJsonPath: each non-internal property with $.path is a field
    if short == "EvaluateJsonPath":
        internal = {"Destination", "Return Type", "Path Not Found Behavior",
                     "Null Value Representation", "Max String Length"}
        for key, val in proc.properties.items():
            if key in internal or not isinstance(val, str):
                continue
            if _JSONPATH_RE.match(val.strip()) or val.strip().startswith("$."):
                schema["fields"].append({
                    "name": key,
                    "jsonpath": val.strip(),
                    "type": "string",
                })
        schema["source_hint"] = "EvaluateJsonPath field extraction"

    # EvaluateXPath: similar pattern
    elif short == "EvaluateXPath":
        internal = {"Destination", "Return Type"}
        for key, val in proc.properties.items():
            if key in internal or not isinstance(val, str):
                continue
            if val.strip().startswith("/"):
                schema["fields"].append({
                    "name": key,
                    "xpath": val.strip(),
                    "type": "string",
                })
        schema["source_hint"] = "EvaluateXPath field extraction"

    # SQL queries: extract column list
    elif "sql" in short.lower() or "database" in short.lower():
        for key, val in proc.properties.items():
            if not isinstance(val, str):
                continue
            if "select" in key.lower() or "query" in key.lower() or "sql" in key.lower():
                cols = _extract_sql_columns(val)
                for col in cols:
                    schema["fields"].append({"name": col, "type": "string"})
                schema["source_hint"] = f"SQL column extraction from '{key}'"
                break

    # Check controller services for schema text
    for key, val in proc.properties.items():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            schema_text = cs.properties.get("Schema Text", "")
            if schema_text and len(schema_text) > 10:
                schema["schema_type"] = "explicit"
                schema["source_hint"] = f"Schema from controller service '{cs.name}'"
                # Attempt to extract field names from Avro-style schema
                for m in re.finditer(r'"name"\s*:\s*"(\w+)"', schema_text):
                    schema["fields"].append({
                        "name": m.group(1),
                        "type": "from_schema",
                    })
                break

    return schema


def _extract_sql_columns(sql: str) -> list[str]:
    """Best-effort extraction of column names from a SELECT statement."""
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        return []
    # Find everything between SELECT and FROM
    m = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.I | re.S)
    if not m:
        return []
    cols_str = m.group(1)
    if cols_str.strip() == "*":
        return ["*"]
    columns: list[str] = []
    for part in cols_str.split(","):
        part = part.strip()
        # Handle aliases: col AS alias or col alias
        alias_m = re.search(r"\bAS\s+(\w+)\s*$", part, re.I)
        if alias_m:
            columns.append(alias_m.group(1))
        else:
            # Take the last word as column name
            tokens = part.split()
            if tokens:
                col = tokens[-1].strip("`\"'[]")
                if col and col.upper() not in ("DISTINCT", "ALL", "TOP"):
                    columns.append(col)
    return columns


# ---------------------------------------------------------------------------
# Credential detection and secrets mapping
# ---------------------------------------------------------------------------

_CREDENTIAL_PROPS = re.compile(
    r"(password|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|"
    r"client[_-]?secret|credentials|auth)",
    re.I,
)


def _detect_credentials(proc: Processor) -> tuple[list[str], dict]:
    """Detect credential properties and map to dbutils.secrets."""
    creds: list[str] = []
    secrets_map: dict = {}
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", proc.name).strip("_").lower()

    for key, val in proc.properties.items():
        if _CREDENTIAL_PROPS.search(key):
            cred_key = re.sub(r"[^a-zA-Z0-9_-]", "-", key).lower()
            creds.append(key)
            secrets_map[key] = (
                f'dbutils.secrets.get(scope="{{scope}}", key="{safe_name}-{cred_key}")'
            )

    return creds, secrets_map


# ---------------------------------------------------------------------------
# Databricks equivalent mapping for sources
# ---------------------------------------------------------------------------

_SOURCE_EQUIV_MAP: dict[str, str] = {
    "file": "Auto Loader (cloudFiles) from Unity Catalog Volumes",
    "message_queue": "Structured Streaming Kafka/message connector",
    "database": "JDBC connector or Lakehouse Federation",
    "api": "requests library, Model Serving endpoint, or DLT expectations",
    "generated": "spark.range() or synthetic data generator",
    "network": "Structured Streaming socket source or custom receiver",
}


# ---------------------------------------------------------------------------
# Attribute lineage from existing analysis
# ---------------------------------------------------------------------------

def _build_attribute_lineage(
    analysis_result: AnalysisResult | None,
    processors: list[Processor],
) -> dict[str, dict]:
    """Build attribute lineage from existing analysis or from scratch."""
    # Try to use existing attribute_translation or attribute flow
    if analysis_result and analysis_result.attribute_translation:
        attr_trans = analysis_result.attribute_translation
        if isinstance(attr_trans, dict) and "attribute_map" in attr_trans:
            return attr_trans["attribute_map"]

    # Build minimal lineage from processor properties
    lineage: dict[str, dict] = {}
    el_re = re.compile(r"\$\{([^}:]+)")

    attr_creators = {
        "UpdateAttribute", "PutAttribute", "EvaluateJsonPath",
        "EvaluateXPath", "ExtractText", "ExtractGrok",
    }

    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type

        # Attributes created
        if short in attr_creators:
            for key in p.properties:
                if key.startswith(("nifi-", "Record ")) or key in {
                    "Destination", "Return Type", "Path Not Found Behavior",
                    "Null Value Representation", "Character Set",
                }:
                    continue
                if key not in lineage:
                    lineage[key] = {"creators": [], "readers": []}
                lineage[key]["creators"].append(p.name)

        # Attributes read (NEL references)
        for val in p.properties.values():
            if isinstance(val, str) and "${" in val:
                for m in el_re.finditer(val):
                    attr_name = m.group(1).strip()
                    if attr_name and "(" not in attr_name and "$" not in attr_name:
                        if attr_name not in lineage:
                            lineage[attr_name] = {"creators": [], "readers": []}
                        if p.name not in lineage[attr_name]["readers"]:
                            lineage[attr_name]["readers"].append(p.name)

    return lineage


# ---------------------------------------------------------------------------
# Content transformations tracking
# ---------------------------------------------------------------------------

_TRANSFORM_TYPES = {
    "ConvertRecord", "JoltTransformJSON", "TransformXml",
    "ReplaceText", "ConvertJSONToSQL", "CompressContent",
    "EncryptContent", "ConvertCharacterSet",
}


def _track_content_transformations(
    processors: list[Processor],
    connections: list[Connection],
) -> list[dict]:
    """Track content transformation steps applied to data."""
    transforms: list[dict] = []

    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if short not in _TRANSFORM_TYPES:
            continue

        transform_desc = ""
        if short == "ConvertRecord":
            reader = p.properties.get("Record Reader", "")
            writer = p.properties.get("Record Writer", "")
            transform_desc = f"Convert format: {reader} -> {writer}"
        elif short == "JoltTransformJSON":
            spec_type = p.properties.get("Jolt Transform", "")
            transform_desc = f"Jolt {spec_type} transformation"
        elif short == "ReplaceText":
            strategy = p.properties.get("Replacement Strategy", "Regex Replace")
            transform_desc = f"Text replacement ({strategy})"
        elif short == "CompressContent":
            mode = p.properties.get("Mode", "compress")
            fmt = p.properties.get("Compression Format", "gzip")
            transform_desc = f"{mode} using {fmt}"
        elif short == "EncryptContent":
            algo = p.properties.get("Encryption Algorithm", "")
            transform_desc = f"Encrypt with {algo}"
        else:
            transform_desc = f"{short} transformation"

        transforms.append({
            "processor": p.name,
            "type": short,
            "description": transform_desc,
            "position": "inline",
        })

    return transforms


# ---------------------------------------------------------------------------
# External dependency detection
# ---------------------------------------------------------------------------

def _detect_external_deps(
    data_sources: list[DataSource],
    processors: list[Processor],
) -> list[dict]:
    """Detect all external system dependencies from sources."""
    deps: list[dict] = []
    seen: set[str] = set()

    for src in data_sources:
        key = f"{src.system}:{src.source_type}"
        if key in seen:
            continue
        seen.add(key)
        deps.append({
            "system": src.system,
            "type": src.source_type,
            "processor": src.processor,
            "requires_network": src.source_type in ("api", "message_queue", "database", "network"),
            "requires_credentials": len(src.credentials_required) > 0,
            "databricks_connectivity": _get_connectivity_advice(src.source_type, src.system),
        })

    return deps


def _get_connectivity_advice(src_type: str, system: str) -> str:
    """Return Databricks connectivity advice for a given source type."""
    system_lower = system.lower()
    if "kafka" in system_lower:
        return "Use Databricks-managed Kafka connector with secret scope for credentials"
    if any(db in system_lower for db in ("oracle", "mysql", "postgres", "sql")):
        return "Configure JDBC connection via Lakehouse Federation or spark.read.format('jdbc')"
    if any(cloud in system_lower for cloud in ("s3", "azure", "gcs", "hdfs")):
        return "Configure Unity Catalog external location with storage credentials"
    if "sftp" in system_lower or "ftp" in system_lower:
        return "Use paramiko in notebook or Databricks Volumes for file staging"
    if "http" in system_lower or "api" in system_lower:
        return "Use requests library or Model Serving endpoint"
    return "Verify network connectivity from Databricks workspace"


# ---------------------------------------------------------------------------
# Parameter injection detection
# ---------------------------------------------------------------------------

_PARAM_RE = re.compile(r"#\{([^}]+)\}")  # NiFi parameter context: #{param_name}
_NEL_RE = re.compile(r"\$\{([^}]+)\}")


def _detect_parameter_injections(
    parsed: ParseResult,
) -> list[dict]:
    """Detect NiFi Parameter Context references (#{param_name})."""
    injections: list[dict] = []
    param_contexts = parsed.parameter_contexts

    # Build parameter lookup
    param_lookup: dict[str, dict] = {}
    for ctx in param_contexts:
        for param in ctx.parameters:
            key = param.key if hasattr(param, "key") else param.get("key", "")
            param_lookup[key] = {
                "context": ctx.name,
                "sensitive": param.sensitive if hasattr(param, "sensitive") else param.get("sensitive", False),
                "value": param.value if hasattr(param, "value") else param.get("value", ""),
            }

    for p in parsed.processors:
        for key, val in p.properties.items():
            if not isinstance(val, str):
                continue
            for m in _PARAM_RE.finditer(val):
                param_name = m.group(1).strip()
                param_info = param_lookup.get(param_name, {})
                injections.append({
                    "processor": p.name,
                    "property": key,
                    "parameter": param_name,
                    "context": param_info.get("context", "unknown"),
                    "sensitive": param_info.get("sensitive", False),
                    "databricks_mapping": (
                        f'dbutils.secrets.get(scope="{{scope}}", key="{param_name}")'
                        if param_info.get("sensitive", False)
                        else f'spark.conf.get("pipeline.{param_name}")'
                    ),
                })

    return injections


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_upstream(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None = None,
    processor_report=None,
) -> UpstreamReport:
    """Analyze all upstream data sources in the flow.

    Args:
        parsed: Normalized parse result.
        analysis_result: Optional existing analysis result.
        processor_report: Optional ProcessorReport from pass 2.

    Returns:
        UpstreamReport with sources, lineage, transformations, deps, params.
    """
    processors = parsed.processors
    connections = parsed.connections
    controller_services = parsed.controller_services

    logger.info(
        "Pass 4 (Upstream): analyzing %d processors, %d controller services",
        len(processors), len(controller_services),
    )

    # Build controller service lookup
    cs_map: dict[str, ControllerService] = {cs.name: cs for cs in controller_services}

    # 1. Find all source-role processors and build DataSource entries
    data_sources: list[DataSource] = []
    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if not _SOURCE_RE.match(short):
            continue

        src_type, system = _classify_source(short)
        location = _extract_location(p)
        fmt = _detect_format(p, cs_map)
        schema = _infer_schema(p, cs_map)
        creds, secrets = _detect_credentials(p)
        equiv = _SOURCE_EQUIV_MAP.get(src_type, "Manual mapping required")

        data_sources.append(DataSource(
            processor=p.name,
            source_type=src_type,
            system=system,
            location=location,
            format=fmt,
            inferred_schema=schema,
            databricks_equivalent=equiv,
            credentials_required=creds,
            secrets_mapping=secrets,
        ))

    # 2. Build attribute lineage
    attr_lineage = _build_attribute_lineage(analysis_result, processors)

    # 3. Track content transformations
    transforms = _track_content_transformations(processors, connections)

    # 4. Detect external dependencies
    ext_deps = _detect_external_deps(data_sources, processors)

    # 5. Detect parameter injections
    param_injections = _detect_parameter_injections(parsed)

    report = UpstreamReport(
        data_sources=data_sources,
        attribute_lineage=attr_lineage,
        content_transformations=transforms,
        external_dependencies=ext_deps,
        parameter_injections=param_injections,
    )

    logger.info(
        "Pass 4 complete: %d sources, %d attributes tracked, "
        "%d transforms, %d ext deps, %d param injections",
        len(data_sources), len(attr_lineage), len(transforms),
        len(ext_deps), len(param_injections),
    )
    return report

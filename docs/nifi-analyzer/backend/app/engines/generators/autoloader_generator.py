"""Auto Loader generator — produces Databricks Auto Loader code for boundary ingestion processors.

Detects NiFi source processors (GetFile, ListHDFS, ConsumeKafka, ListS3, etc.) and generates
appropriate Spark Structured Streaming Auto Loader code with proper trigger configurations.
"""

import logging
import re

from app.models.config import DatabricksConfig
from app.models.pipeline import MappingEntry, ParseResult

logger = logging.getLogger(__name__)

# NiFi processors that represent boundary ingestion (Auto Loader candidates)
AUTOLOADER_PROCESSORS = {
    "GetFile", "GetSFTP", "GetFTP", "GetHDFS", "GetS3Object", "GetAzureBlobStorage",
    "GetGCSObject", "GetAzureEventHub",
    "ListFile", "ListHDFS", "ListS3", "ListAzureBlobStorage", "ListGCSBucket",
    "ListSFTP", "ListFTP",
    "FetchFile", "FetchHDFS", "FetchS3Object", "FetchAzureBlobStorage",
    "FetchGCSObject", "FetchSFTP", "FetchFTP",
}

KAFKA_CONSUMER_PROCESSORS = {
    "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
    "ConsumeKafka_1_0", "ConsumeKafkaRecord_1_0",
}

KAFKA_PRODUCER_PROCESSORS = {
    "PublishKafka", "PublishKafka_2_6", "PublishKafkaRecord_2_6",
}

# Union kept for backward-compatible "is this Kafka at all?" checks
KAFKA_PROCESSORS = KAFKA_CONSUMER_PROCESSORS | KAFKA_PRODUCER_PROCESSORS

JDBC_PROCESSORS = {
    "QueryDatabaseTable", "QueryDatabaseTableRecord", "GenerateTableFetch",
    "ExecuteSQL", "ExecuteSQLRecord",
}


def _extract_simple_type(proc_type: str) -> str:
    """Extract the simple class name from a possibly fully-qualified NiFi processor type.

    NiFi processor types may be fully qualified, e.g.:
      org.apache.nifi.processors.standard.GetFile -> GetFile
      GetFile -> GetFile
    """
    if "." in proc_type:
        return proc_type.rsplit(".", 1)[-1]
    return proc_type


def is_autoloader_candidate(proc_type: str) -> bool:
    """Check if a NiFi processor type should use Auto Loader."""
    return _extract_simple_type(proc_type) in AUTOLOADER_PROCESSORS


def is_kafka_source(proc_type: str) -> bool:
    """Check if a NiFi processor type is a Kafka *consumer* (source).

    PublishKafka variants are sinks, not sources, and must NOT be treated
    as readStream candidates.
    """
    return _extract_simple_type(proc_type) in KAFKA_CONSUMER_PROCESSORS


def is_jdbc_source(proc_type: str) -> bool:
    """Check if a NiFi processor type is a JDBC source."""
    return _extract_simple_type(proc_type) in JDBC_PROCESSORS


def infer_file_format(mapping: MappingEntry, parse_result: ParseResult) -> str:
    """Infer the file format from the processor's downstream readers.

    Looks at connected processors (CSVReader, JsonTreeReader, AvroReader, etc.)
    to determine the correct cloudFiles.format.
    """
    combined = (mapping.code + " " + mapping.notes + " " + str(mapping.type)).lower()
    props = {k.lower(): v.lower() for k, v in _get_processor_properties(mapping, parse_result).items()}

    # Check for explicit format in properties
    for key in ("input format", "file format", "record reader", "input.format"):
        val = props.get(key, "")
        if "csv" in val:
            return "csv"
        if "json" in val:
            return "json"
        if "avro" in val:
            return "avro"
        if "parquet" in val:
            return "parquet"
        if "orc" in val:
            return "orc"
        if "xml" in val:
            return "xml"

    # Check in combined text
    if "csv" in combined:
        return "csv"
    if "parquet" in combined:
        return "parquet"
    if "avro" in combined:
        return "avro"
    if "orc" in combined:
        return "orc"
    if "xml" in combined:
        return "xml"

    return "json"  # Default


def extract_input_path(mapping: MappingEntry, parse_result: ParseResult, config: DatabricksConfig) -> str:
    """Extract the input path from processor properties."""
    props = _get_processor_properties(mapping, parse_result)

    # Common NiFi property names for input paths
    for key in ("Input Directory", "input-directory", "Directory",
                "Bucket", "Container Name", "Remote Path",
                "HDFS Directory", "S3 Bucket", "Azure Container"):
        val = props.get(key, "")
        if val and not val.startswith("${"):
            return val

    # Default volume path
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mapping.name).strip("_").lower()
    return f"/Volumes/{config.catalog}/{config.schema_name}/landing/{safe_name}"


def extract_backpressure_config(mapping: MappingEntry, parse_result: ParseResult) -> int:
    """Extract max files per trigger from NiFi backpressure config on inbound connections."""
    for conn in parse_result.connections:
        if conn.destination_name == mapping.name:
            if conn.back_pressure_object_threshold > 0:
                return conn.back_pressure_object_threshold
    return 1000  # Default


def generate_autoloader_code(
    mapping: MappingEntry,
    parse_result: ParseResult,
    config: DatabricksConfig,
) -> str:
    """Generate Databricks Auto Loader code for a file-based ingestion processor."""
    file_format = infer_file_format(mapping, parse_result)
    input_path = extract_input_path(mapping, parse_result, config)
    max_files = extract_backpressure_config(mapping, parse_result)
    logger.info("Auto Loader: source=%s, format=%s, trigger=%s", mapping.type, file_format, "cloudFiles")
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mapping.name).strip("_").lower()
    schema_location = f"/Volumes/{config.catalog}/{config.schema_name}/checkpoints/{safe_name}_schema"

    lines = [
        f"# Auto Loader ingestion: {mapping.type} ({mapping.name})",
        f"df_{safe_name} = (spark.readStream",
        f'    .format("cloudFiles")',
        f'    .option("cloudFiles.format", "{file_format}")',
        f'    .option("cloudFiles.schemaLocation", "{schema_location}")',
        f'    .option("cloudFiles.maxFilesPerTrigger", "{max_files}")',
        f'    .option("cloudFiles.inferColumnTypes", "true")',
    ]

    # Add format-specific options
    if file_format == "csv":
        lines.append(f'    .option("header", "true")')
        lines.append(f'    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")')
    elif file_format == "json":
        lines.append(f'    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")')
        lines.append(f'    .option("multiLine", "true")')

    lines.append(f'    .load("{input_path}"))')

    return "\n".join(lines)


def generate_kafka_source_code(
    mapping: MappingEntry,
    parse_result: ParseResult,
    config: DatabricksConfig,
) -> str:
    """Generate Spark Structured Streaming code for Kafka consumers."""
    props = _get_processor_properties(mapping, parse_result)
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mapping.name).strip("_").lower()

    bootstrap_servers = props.get("Kafka Brokers", props.get("bootstrap.servers", "UNSET_kafka_bootstrap_servers"))  # USER ACTION: set actual Kafka bootstrap servers
    topic = props.get("Topic Name(s)", props.get("topic", "topic"))
    group_id = props.get("Group ID", props.get("group.id", safe_name))

    # Determine if bootstrap servers is a NEL expression
    if "${" in bootstrap_servers:
        bootstrap_servers = f'dbutils.widgets.get("kafka_bootstrap_servers")'
        servers_line = f'    .option("kafka.bootstrap.servers", {bootstrap_servers})'
    else:
        servers_line = f'    .option("kafka.bootstrap.servers", "{bootstrap_servers}")'

    lines = [
        f"# Kafka consumer: {mapping.type} ({mapping.name})",
        f"df_{safe_name} = (spark.readStream",
        f'    .format("kafka")',
        servers_line,
        f'    .option("subscribe", "{topic}")',
        f'    .option("kafka.group.id", "{group_id}")',
        f'    .option("startingOffsets", "earliest")',
        f'    .option("failOnDataLoss", "false")',
        f"    .load()",
        f'    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp"))',
    ]

    return "\n".join(lines)


def generate_jdbc_source_code(
    mapping: MappingEntry,
    parse_result: ParseResult,
    config: DatabricksConfig,
) -> str:
    """Generate JDBC source reading code."""
    props = _get_processor_properties(mapping, parse_result)
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mapping.name).strip("_").lower()

    table = props.get("Table Name", props.get("db-fetch-db-table", safe_name))

    lines = [
        f"# JDBC source: {mapping.type} ({mapping.name})",
        f"df_{safe_name} = (spark.read",
        f'    .format("jdbc")',
        f'    .option("url", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_url"))',
        f'    .option("dbtable", "{table}")',
        f'    .option("user", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_user"))',
        f'    .option("password", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_password"))',
        f'    .option("fetchsize", "10000")',
        f"    .load())",
    ]

    return "\n".join(lines)


def generate_streaming_trigger(
    mapping: MappingEntry,
    parse_result: ParseResult,
    config: DatabricksConfig,
) -> str:
    """Generate proper Spark trigger configuration based on NiFi scheduling.

    - Cron-based scheduling -> Trigger.AvailableNow() (cost-efficient batch)
    - Continuous polling -> Trigger.ProcessingTime("{interval}")
    - Includes checkpointLocation for stateful processors
    """
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mapping.name).strip("_").lower()
    checkpoint_path = f"/Volumes/{config.catalog}/{config.schema_name}/checkpoints/{safe_name}"

    # Determine trigger strategy from NiFi scheduling
    trigger_code = _derive_trigger(mapping, parse_result)

    lines = [
        f"# Write stream for {mapping.name}",
        f"query_{safe_name} = (df_{safe_name}.writeStream",
        f'    .format("delta")',
        f'    .outputMode("append")',
        f'    .option("checkpointLocation", "{checkpoint_path}")',
        f"    .trigger({trigger_code})",
        f'    .toTable("{config.catalog}.{config.schema_name}.{safe_name}"))',
    ]

    return "\n".join(lines)


def _derive_trigger(mapping: MappingEntry, parse_result: ParseResult) -> str:
    """Derive the appropriate Spark trigger from NiFi scheduling properties."""
    # Find the source processor to check scheduling
    for proc in parse_result.processors:
        if proc.name == mapping.name and proc.scheduling:
            # Support both key conventions: JSON parser uses "strategy"/"period",
            # XML parser may use "schedulingStrategy"/"schedulingPeriod"
            strategy = (proc.scheduling.get("strategy", "") or proc.scheduling.get("schedulingStrategy", "")).upper()
            period = proc.scheduling.get("period", "") or proc.scheduling.get("schedulingPeriod", "")

            # CRON_DRIVEN -> AvailableNow for cost efficiency
            if strategy == "CRON_DRIVEN":
                return "availableNow=True"

            # TIMER_DRIVEN with explicit period
            if strategy == "TIMER_DRIVEN" and period:
                # Convert NiFi period to Spark processing time
                spark_interval = _nifi_period_to_spark(period)
                if spark_interval:
                    return f'processingTime="{spark_interval}"'

    # Default: processing time every 30 seconds
    return 'processingTime="30 seconds"'


def _nifi_period_to_spark(period: str) -> str:
    """Convert NiFi scheduling period to Spark processing time interval."""
    m = re.match(r"(\d+)\s*(sec|min|hour|ms|millis)", period, re.IGNORECASE)
    if not m:
        # Check for cron expression
        if re.match(r"[\d*/?,-]+\s+[\d*/?,-]+", period):
            return ""  # Will use AvailableNow
        return "30 seconds"

    val, unit = int(m.group(1)), m.group(2).lower()
    if unit in ("ms", "millis"):
        return f"{max(val, 100)} milliseconds"
    if unit.startswith("sec"):
        return f"{val} seconds"
    if unit.startswith("min"):
        return f"{val} minutes"
    if unit.startswith("hour"):
        return f"{val} hours"
    return f"{val} seconds"


def _get_processor_properties(mapping: MappingEntry, parse_result: ParseResult) -> dict:
    """Look up properties from the original processor in the parse result."""
    for proc in parse_result.processors:
        if proc.name == mapping.name:
            return proc.properties
    return {}

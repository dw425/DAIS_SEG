"""Pass 5: Downstream Analysis -- Where is data going?

Identifies all data sinks, detects data multiplication/reduction/loss/
duplication points, maps error routing, and estimates data volume.
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
class DataSink:
    processor: str
    sink_type: str  # file, database, message_queue, api, notification, cache
    system: str
    destination: str
    format: str
    write_mode: str
    databricks_equivalent: str
    credentials_required: list[str]


@dataclass
class DownstreamReport:
    data_sinks: list[DataSink]
    data_flow_map: dict  # multiplication_points, reduction_points, loss_points, duplication_points
    error_routing: dict  # unhandled_failures, handled_failures, recommendation
    data_volume_estimates: dict


# ---------------------------------------------------------------------------
# Sink type detection
# ---------------------------------------------------------------------------

_SINK_RE = re.compile(
    r"^(Put|Publish|Send|Post|Insert|Write|Delete)", re.I
)

_SINK_TYPE_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"Kafka", re.I), "message_queue", "Apache Kafka"),
    (re.compile(r"JMS", re.I), "message_queue", "JMS"),
    (re.compile(r"AMQP", re.I), "message_queue", "AMQP / RabbitMQ"),
    (re.compile(r"MQTT", re.I), "message_queue", "MQTT"),
    (re.compile(r"SQS", re.I), "message_queue", "AWS SQS"),
    (re.compile(r"SNS", re.I), "notification", "AWS SNS"),
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
    (re.compile(r"Redis", re.I), "cache", "Redis"),
    (re.compile(r"Neo4j", re.I), "database", "Neo4j"),
    (re.compile(r"InfluxDB", re.I), "database", "InfluxDB"),
    (re.compile(r"Kudu", re.I), "database", "Apache Kudu"),
    (re.compile(r"Hive", re.I), "database", "Hive"),
    (re.compile(r"Splunk", re.I), "database", "Splunk"),
    (re.compile(r"S3", re.I), "file", "AWS S3"),
    (re.compile(r"Azure.*Blob", re.I), "file", "Azure Blob Storage"),
    (re.compile(r"Azure.*Lake|ADLS", re.I), "file", "Azure Data Lake"),
    (re.compile(r"GCS", re.I), "file", "Google Cloud Storage"),
    (re.compile(r"HDFS|Hadoop", re.I), "file", "HDFS"),
    (re.compile(r"SFTP|FTP", re.I), "file", "SFTP/FTP"),
    (re.compile(r"File", re.I), "file", "Local Filesystem"),
    (re.compile(r"HTTP|REST", re.I), "api", "HTTP/REST API"),
    (re.compile(r"Email|SMTP", re.I), "notification", "Email"),
    (re.compile(r"Slack", re.I), "notification", "Slack"),
    (re.compile(r"PagerDuty", re.I), "notification", "PagerDuty"),
    (re.compile(r"Syslog", re.I), "notification", "Syslog"),
]


def _classify_sink(short_type: str) -> tuple[str, str]:
    """Classify the sink type and target system."""
    for pattern, sink_type, system in _SINK_TYPE_PATTERNS:
        if pattern.search(short_type):
            return sink_type, system
    return "file", "Unknown"


# ---------------------------------------------------------------------------
# Destination extraction
# ---------------------------------------------------------------------------

_DEST_PROPS = [
    "directory", "output directory", "remote path", "path",
    "bucket", "container name", "table name", "dbtable",
    "topic", "topic name", "queue name", "queue",
    "remote url", "http url", "url",
    "database connection url",
    "filename", "output file",
    "recipient", "to", "email", "channel",
]


def _extract_destination(proc: Processor) -> str:
    """Extract the write destination from processor properties."""
    props_lower = {k.lower().strip(): v for k, v in proc.properties.items()}
    for candidate in _DEST_PROPS:
        val = props_lower.get(candidate, "")
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


# ---------------------------------------------------------------------------
# Write mode detection
# ---------------------------------------------------------------------------

def _detect_write_mode(proc: Processor) -> str:
    """Detect write mode (append, overwrite, merge, upsert, etc.)."""
    short = proc.type.rsplit(".", 1)[-1] if "." in proc.type else proc.type

    # Check for merge/upsert in type name
    if re.search(r"(Merge|Upsert|CDC)", short, re.I):
        return "merge"
    if short.startswith("Delete"):
        return "delete"

    # Check properties
    for key, val in proc.properties.items():
        if not isinstance(val, str):
            continue
        key_lower = key.lower()
        val_lower = val.lower()
        if "conflict" in key_lower or "mode" in key_lower:
            if "replace" in val_lower or "overwrite" in val_lower:
                return "overwrite"
            if "ignore" in val_lower or "skip" in val_lower:
                return "append_skip_duplicates"
            if "update" in val_lower or "upsert" in val_lower:
                return "upsert"

    return "append"


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

_FORMAT_MAP: dict[str, str] = {
    "json": "JSON", "csv": "CSV", "avro": "Avro", "parquet": "Parquet",
    "xml": "XML", "text": "Plain Text", "orc": "ORC",
}


def _detect_format(proc: Processor, cs_map: dict[str, ControllerService]) -> str:
    """Detect output format from processor type, properties, or controller services."""
    short = proc.type.rsplit(".", 1)[-1] if "." in proc.type else proc.type

    for fmt_key, fmt_name in _FORMAT_MAP.items():
        if fmt_key.lower() in short.lower():
            return fmt_name

    # Check controller service references
    writer_re = re.compile(r"(Writer|RecordWriter|RecordSetWriter)", re.I)
    for key, val in proc.properties.items():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            if writer_re.search(cs.type):
                for fmt_key, fmt_name in _FORMAT_MAP.items():
                    if fmt_key.lower() in cs.type.lower():
                        return fmt_name

    # Check format properties
    for key, val in proc.properties.items():
        if isinstance(val, str) and "format" in key.lower():
            normalized = val.strip().lower()
            if normalized in _FORMAT_MAP:
                return _FORMAT_MAP[normalized]

    return "unknown"


# ---------------------------------------------------------------------------
# Credential detection
# ---------------------------------------------------------------------------

_CRED_RE = re.compile(
    r"(password|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|credentials|auth)",
    re.I,
)


def _detect_credentials(proc: Processor) -> list[str]:
    """Detect credential-related properties."""
    return [k for k in proc.properties if _CRED_RE.search(k)]


# ---------------------------------------------------------------------------
# Databricks equivalent mapping
# ---------------------------------------------------------------------------

_SINK_EQUIV_MAP: dict[str, str] = {
    "file": "Delta table in Unity Catalog (df.write.format('delta'))",
    "message_queue": "Structured Streaming Kafka writer",
    "database": "JDBC write or Lakehouse Federation external table",
    "api": "requests library or webhook notification",
    "notification": "Databricks notification destinations / dbutils.notification",
    "cache": "Delta table or Feature Store",
}


# ---------------------------------------------------------------------------
# Data flow map: multiplication, reduction, loss, duplication
# ---------------------------------------------------------------------------

# Processors that multiply records (1 input -> N outputs)
_MULTIPLIER_TYPES = {
    "SplitJson", "SplitXml", "SplitText", "SplitRecord",
    "SplitContent", "ForkRecord", "UnpackContent",
}

# Processors that reduce records (N inputs -> 1 output)
_REDUCER_TYPES = {
    "MergeContent", "MergeRecord", "DefragmentContent",
}

# Processors that can lose data (auto-terminate, rate-limit, sample)
_LOSS_TYPES = {
    "ControlRate", "SampleRecord", "LimitFlowFile",
}

# Routing processors that fan out to multiple destinations
_ROUTE_TYPES = {
    "RouteOnAttribute", "RouteOnContent", "RouteText",
    "DistributeLoad", "DetectDuplicate",
}


def _analyze_data_flow_map(
    processors: list[Processor],
    connections: list[Connection],
) -> dict:
    """Analyze data multiplication, reduction, loss, and duplication points."""
    proc_names = {p.name for p in processors}
    proc_by_name = {p.name: p for p in processors}

    # Build downstream map
    downstream: dict[str, list[str]] = {p.name: [] for p in processors}
    upstream: dict[str, list[str]] = {p.name: [] for p in processors}
    conn_rels: dict[str, list[tuple[str, str]]] = {}  # proc -> [(dest, relationship)]

    for c in connections:
        if c.source_name in proc_names and c.destination_name in proc_names:
            downstream[c.source_name].append(c.destination_name)
            upstream[c.destination_name].append(c.source_name)
            conn_rels.setdefault(c.source_name, []).append(
                (c.destination_name, c.relationship)
            )

    multiplication_points: list[dict] = []
    reduction_points: list[dict] = []
    loss_points: list[dict] = []
    duplication_points: list[dict] = []

    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        ds_count = len(downstream.get(p.name, []))

        # Multiplication: split processors
        if short in _MULTIPLIER_TYPES:
            multiplication_points.append({
                "processor": p.name,
                "type": short,
                "description": f"{short} splits each input into multiple outputs",
                "databricks_pattern": "explode() or lateral view for record splitting",
            })

        # Reduction: merge processors
        if short in _REDUCER_TYPES:
            us_count = len(upstream.get(p.name, []))
            reduction_points.append({
                "processor": p.name,
                "type": short,
                "input_count": us_count,
                "description": f"{short} merges {us_count} input stream(s) into one",
                "databricks_pattern": "DataFrame union + groupBy aggregation",
            })

        # Data loss: rate limiting, sampling, auto-terminated relationships
        if short in _LOSS_TYPES:
            loss_points.append({
                "processor": p.name,
                "type": short,
                "loss_type": "rate_limiting" if short == "ControlRate" else "sampling",
                "description": f"{short} intentionally drops or limits records",
                "databricks_pattern": "df.sample() or df.limit() for sampling",
            })

        # Check for auto-terminated relationships that discard data
        auto_term = p.properties.get("Auto-Terminated Relationships", "")
        if not auto_term:
            auto_term = p.properties.get("auto-terminated-relationships-list", "")
        if isinstance(auto_term, str) and auto_term.strip():
            terminated = [r.strip() for r in auto_term.split(",") if r.strip()]
            # Exclude common non-data relationships
            data_relationships = [
                r for r in terminated
                if r.lower() not in ("original", "failure")
            ]
            if data_relationships:
                loss_points.append({
                    "processor": p.name,
                    "type": short,
                    "loss_type": "auto_terminated",
                    "terminated_relationships": data_relationships,
                    "description": (
                        f"Auto-terminated relationship(s) {data_relationships} "
                        f"discard data silently"
                    ),
                    "databricks_pattern": "Ensure all branches are captured or explicitly filtered",
                })

        # Duplication: same data broadcast to multiple sinks
        if short in _ROUTE_TYPES and ds_count > 1:
            sink_destinations = []
            for dest_name in downstream.get(p.name, []):
                dest_proc = proc_by_name.get(dest_name)
                if dest_proc:
                    dest_short = dest_proc.type.rsplit(".", 1)[-1] if "." in dest_proc.type else dest_proc.type
                    if _SINK_RE.match(dest_short):
                        sink_destinations.append(dest_name)

            if len(sink_destinations) > 1:
                duplication_points.append({
                    "processor": p.name,
                    "type": short,
                    "sink_count": len(sink_destinations),
                    "sinks": sink_destinations,
                    "description": f"Data from {p.name} is broadcast to {len(sink_destinations)} sinks",
                    "databricks_pattern": "Write same DataFrame to multiple targets in foreachBatch",
                })

        # Also detect fan-out from non-route processors to multiple sinks
        elif ds_count > 1 and short not in _ROUTE_TYPES:
            sink_dests = []
            for dest_name in downstream.get(p.name, []):
                dest_proc = proc_by_name.get(dest_name)
                if dest_proc:
                    dest_short = dest_proc.type.rsplit(".", 1)[-1] if "." in dest_proc.type else dest_proc.type
                    if _SINK_RE.match(dest_short):
                        sink_dests.append(dest_name)
            if len(sink_dests) > 1:
                duplication_points.append({
                    "processor": p.name,
                    "type": short,
                    "sink_count": len(sink_dests),
                    "sinks": sink_dests,
                    "description": f"Data duplicated from {p.name} to {len(sink_dests)} sinks",
                    "databricks_pattern": "foreachBatch with multiple write operations",
                })

    return {
        "multiplication_points": multiplication_points,
        "reduction_points": reduction_points,
        "loss_points": loss_points,
        "duplication_points": duplication_points,
    }


# ---------------------------------------------------------------------------
# Error routing analysis
# ---------------------------------------------------------------------------

def _analyze_error_routing(
    processors: list[Processor],
    connections: list[Connection],
) -> dict:
    """Analyze how errors (failure relationships) are routed."""
    proc_names = {p.name for p in processors}

    # Find all failure/retry connections
    failure_conns: list[dict] = []
    handled_failures: list[dict] = []
    unhandled_failures: list[str] = []

    failure_destinations: dict[str, list[str]] = {}  # source -> [destinations via failure]

    for c in connections:
        if c.source_name not in proc_names or c.destination_name not in proc_names:
            continue
        rel_lower = c.relationship.lower()
        if "failure" in rel_lower or "retry" in rel_lower or "error" in rel_lower:
            failure_destinations.setdefault(c.source_name, []).append(c.destination_name)
            failure_conns.append({
                "source": c.source_name,
                "destination": c.destination_name,
                "relationship": c.relationship,
            })

    # Determine which processors have no failure routing
    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        # Skip processors that typically don't have failure relationships
        if short in ("Funnel", "InputPort", "OutputPort", "LogAttribute", "LogMessage"):
            continue

        if p.name in failure_destinations:
            for dest in failure_destinations[p.name]:
                handled_failures.append({
                    "processor": p.name,
                    "error_destination": dest,
                    "routing_type": "explicit",
                })
        else:
            # Check if failure is auto-terminated
            auto_term = p.properties.get("Auto-Terminated Relationships", "")
            if not auto_term:
                auto_term = p.properties.get("auto-terminated-relationships-list", "")
            if isinstance(auto_term, str) and "failure" in auto_term.lower():
                handled_failures.append({
                    "processor": p.name,
                    "error_destination": "(auto-terminated)",
                    "routing_type": "auto_terminated",
                })
            else:
                unhandled_failures.append(p.name)

    # Generate recommendation
    if unhandled_failures:
        recommendation = (
            f"{len(unhandled_failures)} processor(s) have unhandled failure paths. "
            f"Add try/except blocks with dead-letter table writes in Databricks."
        )
    elif len(handled_failures) > 0:
        recommendation = (
            f"All {len(handled_failures)} failure paths are routed. "
            f"Translate to try/except with dead-letter Delta table pattern."
        )
    else:
        recommendation = "No explicit failure routing detected."

    return {
        "failure_connections": failure_conns,
        "handled_failures": handled_failures,
        "unhandled_failures": unhandled_failures,
        "recommendation": recommendation,
    }


# ---------------------------------------------------------------------------
# Data volume estimation
# ---------------------------------------------------------------------------

_STREAMING_TYPES = {
    "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
    "ListenHTTP", "ListenTCP", "ListenUDP", "ListenSyslog",
    "ConsumeMQTT", "ConsumeJMS", "ConsumeAMQP", "TailFile",
}

_DURATION_RE = re.compile(r"(\d+)\s*(sec|second|min|minute|hour|hr|ms|millisecond)s?", re.I)


def _estimate_data_volume(
    processors: list[Processor],
    data_sinks: list[DataSink],
) -> dict:
    """Estimate data volume characteristics based on scheduling and source types."""
    has_streaming = False
    has_batch = False
    source_count = 0
    sink_count = len(data_sinks)

    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if short in _STREAMING_TYPES:
            has_streaming = True
            source_count += 1
        elif re.match(r"^(Get|List|Fetch|Query|Scan|Select)", short, re.I):
            has_batch = True
            source_count += 1

    # Estimate throughput profile
    if has_streaming and has_batch:
        profile = "mixed (streaming + batch)"
        est_records_per_hour = "10K-1M+ (varies by source)"
    elif has_streaming:
        profile = "continuous streaming"
        est_records_per_hour = "100K-10M+ (depends on source rate)"
    else:
        profile = "batch"
        est_records_per_hour = "1K-100K (depends on schedule frequency)"

    # Detect multiplier/reducer effects
    multipliers = sum(
        1 for p in processors
        if (p.type.rsplit(".", 1)[-1] if "." in p.type else p.type) in _MULTIPLIER_TYPES
    )
    reducers = sum(
        1 for p in processors
        if (p.type.rsplit(".", 1)[-1] if "." in p.type else p.type) in _REDUCER_TYPES
    )

    volume_factor = "1:1"
    if multipliers > reducers:
        volume_factor = f"1:N (amplified by {multipliers} split processor(s))"
    elif reducers > multipliers:
        volume_factor = f"N:1 (reduced by {reducers} merge processor(s))"

    return {
        "throughput_profile": profile,
        "estimated_records_per_hour": est_records_per_hour,
        "source_count": source_count,
        "sink_count": sink_count,
        "volume_factor": volume_factor,
        "has_streaming": has_streaming,
        "has_batch": has_batch,
        "multiplier_count": multipliers,
        "reducer_count": reducers,
    }


# Reuse from flow map section
_MULTIPLIER_TYPES = {
    "SplitJson", "SplitXml", "SplitText", "SplitRecord",
    "SplitContent", "ForkRecord", "UnpackContent",
}
_REDUCER_TYPES = {
    "MergeContent", "MergeRecord", "DefragmentContent",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_downstream(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None = None,
    processor_report=None,
) -> DownstreamReport:
    """Analyze all downstream data sinks in the flow.

    Args:
        parsed: Normalized parse result.
        analysis_result: Optional existing analysis result.
        processor_report: Optional ProcessorReport from pass 2.

    Returns:
        DownstreamReport with sinks, data flow map, error routing, volume.
    """
    processors = parsed.processors
    connections = parsed.connections
    controller_services = parsed.controller_services

    logger.info(
        "Pass 5 (Downstream): analyzing %d processors, %d connections",
        len(processors), len(connections),
    )

    # Build controller service lookup
    cs_map: dict[str, ControllerService] = {cs.name: cs for cs in controller_services}

    # 1. Find all sink-role processors
    data_sinks: list[DataSink] = []
    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if not _SINK_RE.match(short):
            continue

        sink_type, system = _classify_sink(short)
        destination = _extract_destination(p)
        fmt = _detect_format(p, cs_map)
        write_mode = _detect_write_mode(p)
        creds = _detect_credentials(p)
        equiv = _SINK_EQUIV_MAP.get(sink_type, "Manual mapping required")

        data_sinks.append(DataSink(
            processor=p.name,
            sink_type=sink_type,
            system=system,
            destination=destination,
            format=fmt,
            write_mode=write_mode,
            databricks_equivalent=equiv,
            credentials_required=creds,
        ))

    # 2. Analyze data flow map
    data_flow_map = _analyze_data_flow_map(processors, connections)

    # 3. Analyze error routing
    error_routing = _analyze_error_routing(processors, connections)

    # 4. Estimate data volume
    volume_estimates = _estimate_data_volume(processors, data_sinks)

    report = DownstreamReport(
        data_sinks=data_sinks,
        data_flow_map=data_flow_map,
        error_routing=error_routing,
        data_volume_estimates=volume_estimates,
    )

    logger.info(
        "Pass 5 complete: %d sinks, %d mult points, %d loss points, "
        "%d unhandled failures",
        len(data_sinks),
        len(data_flow_map.get("multiplication_points", [])),
        len(data_flow_map.get("loss_points", [])),
        len(error_routing.get("unhandled_failures", [])),
    )
    return report

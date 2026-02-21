"""Detect external system references in processor types and properties.

Ported from external-systems.js.
"""

import re

from app.models.processor import Processor

_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"Kafka", re.I), "Apache Kafka", "kafka"),
    (re.compile(r"Oracle", re.I), "Oracle Database", "oracle"),
    (re.compile(r"MySQL", re.I), "MySQL", "mysql"),
    (re.compile(r"Postgres", re.I), "PostgreSQL", "postgresql"),
    (re.compile(r"Mongo", re.I), "MongoDB", "mongodb"),
    (re.compile(r"Elastic", re.I), "Elasticsearch", "elasticsearch"),
    (re.compile(r"Cassandra", re.I), "Cassandra", "cassandra"),
    (re.compile(r"HBase", re.I), "HBase", "hbase"),
    (re.compile(r"Kudu", re.I), "Apache Kudu", "kudu"),
    (re.compile(r"Hive", re.I), "Hive", "hive"),
    (re.compile(r"HDFS|Hadoop", re.I), "HDFS", "hdfs"),
    (re.compile(r"S3", re.I), "AWS S3", "s3"),
    (re.compile(r"Azure.*Blob", re.I), "Azure Blob", "azure_blob"),
    (re.compile(r"Azure.*Lake|ADLS", re.I), "Azure Data Lake", "azure_adls"),
    (re.compile(r"Azure.*Event", re.I), "Azure Event Hubs", "azure_eventhub"),
    (re.compile(r"GCS|BigQuery", re.I), "Google Cloud", "gcs"),
    (re.compile(r"Snowflake", re.I), "Snowflake", "snowflake"),
    (re.compile(r"Redis", re.I), "Redis", "redis"),
    (re.compile(r"Solr", re.I), "Solr", "solr"),
    (re.compile(r"SFTP|FTP", re.I), "SFTP/FTP", "sftp"),
    (re.compile(r"HTTP|REST", re.I), "HTTP/REST", "http"),
    (re.compile(r"JMS", re.I), "JMS", "jms"),
    (re.compile(r"AMQP", re.I), "AMQP", "amqp"),
    (re.compile(r"MQTT", re.I), "MQTT", "mqtt"),
    (re.compile(r"Email|SMTP", re.I), "Email", "email"),
    (re.compile(r"Syslog", re.I), "Syslog", "syslog"),
    (re.compile(r"Slack", re.I), "Slack", "slack"),
    (re.compile(r"Splunk", re.I), "Splunk", "splunk"),
    (re.compile(r"InfluxDB", re.I), "InfluxDB", "influxdb"),
    (re.compile(r"Neo4j", re.I), "Neo4j", "neo4j"),
    (re.compile(r"SQL|Database|JDBC", re.I), "SQL/JDBC", "sql_jdbc"),
]

_DIRECTION_READ_RE = re.compile(r"^(Get|List|Consume|Listen|Fetch|Query|Scan|Select)", re.I)
_DIRECTION_WRITE_RE = re.compile(r"^(Put|Publish|Send|Post|Insert|Write|Delete)", re.I)

# JDBC URL detection
_JDBC_RE = re.compile(r"jdbc:(\w+):", re.I)


def detect_external_systems(processors: list[Processor]) -> list[dict]:
    """Detect external systems referenced by processors."""
    systems: dict[str, dict] = {}

    for p in processors:
        if _DIRECTION_READ_RE.match(p.type):
            direction = "READ"
        elif _DIRECTION_WRITE_RE.match(p.type):
            direction = "WRITE"
        else:
            direction = "READ/WRITE"

        for pattern, name, key in _PATTERNS:
            if pattern.search(p.type):
                if key not in systems:
                    systems[key] = {
                        "name": name,
                        "key": key,
                        "processors": [],
                        "jdbc_urls": [],
                        "credentials": [],
                    }
                systems[key]["processors"].append(
                    {
                        "name": p.name,
                        "type": p.type,
                        "direction": direction,
                        "group": p.group,
                    }
                )

        # Scan properties for JDBC URLs
        for _k, v in p.properties.items():
            if not v:
                continue
            m = _JDBC_RE.search(v)
            if m:
                db_type = m.group(1).lower()
                db_key_map = {
                    "oracle": "oracle",
                    "mysql": "mysql",
                    "postgresql": "postgresql",
                    "sqlserver": "sqlserver",
                    "hive2": "hive",
                }
                sk = db_key_map.get(db_type, "sql_jdbc")
                if sk not in systems:
                    systems[sk] = {"name": db_type, "key": sk, "processors": [], "jdbc_urls": [], "credentials": []}
                if v not in systems[sk]["jdbc_urls"]:
                    systems[sk]["jdbc_urls"].append(v)

    return list(systems.values())

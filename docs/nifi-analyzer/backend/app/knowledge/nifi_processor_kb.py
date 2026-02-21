"""NiFi Processor Knowledge Base — 500+ processors with Databricks equivalents.

Comprehensive registry of Apache NiFi processors mapped to their Databricks
PySpark / Delta Lake / cloud-native equivalents. Each entry includes:
  - classification (category + role)
  - output relationships
  - conversion complexity rating
  - required imports / packages for the generated notebook code
  - statefulness and record-awareness flags

Usage:
    from nifi_processor_kb import lookup_processor, get_processors_by_category
    p = lookup_processor("GetFile")
    print(p.databricks_equivalent)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ProcessorDef:
    """Definition of a single NiFi processor and its Databricks mapping."""

    short_type: str
    full_type: str
    category: str  # data_ingestion | data_egress | transformation | routing |
    #   database | json_xml_csv | http_api | scripting |
    #   monitoring | content_manipulation | aws | azure |
    #   gcp | kafka | hadoop | email | ftp_sftp |
    #   encryption | record_based | state_management
    role: str  # source | sink | transform | route | utility | monitor | error_handler
    relationships: list[str]
    description: str
    databricks_equivalent: str
    conversion_complexity: str  # direct_map | template | specialized | manual_review
    requires_imports: list[str] = field(default_factory=list)
    requires_packages: list[str] = field(default_factory=list)
    is_stateful: bool = False
    is_record_aware: bool = False


# ─────────────────────────────────────────────────────────────
# Master registry  (keyed by BOTH short_type and full_type)
# ─────────────────────────────────────────────────────────────
PROCESSOR_KB: Dict[str, ProcessorDef] = {}


def _reg(p: ProcessorDef) -> None:
    """Register a ProcessorDef under both its short and fully-qualified names."""
    PROCESSOR_KB[p.short_type] = p
    PROCESSOR_KB[p.full_type] = p


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 1: DATA INGESTION  (50 processors)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Local / Generic File Ingestion ────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetFile",
    full_type="org.apache.nifi.processors.standard.GetFile",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Reads files from a local directory and creates FlowFiles from their content.",
    databricks_equivalent="spark.read.format('text').load(path)  # or .format('binaryFile')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListFile",
    full_type="org.apache.nifi.processors.standard.ListFile",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists files in a directory; emits one FlowFile per listing with metadata attributes.",
    databricks_equivalent="dbutils.fs.ls(path)  # list then iterate",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchFile",
    full_type="org.apache.nifi.processors.standard.FetchFile",
    category="data_ingestion",
    role="source",
    relationships=["success", "not.found", "permission.denied", "failure"],
    description="Fetches the content of a file given its path in a FlowFile attribute.",
    databricks_equivalent="spark.read.format('binaryFile').load(path)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── HDFS ──────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetHDFS",
    full_type="org.apache.nifi.processors.hadoop.GetHDFS",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Reads files from HDFS and creates FlowFiles from their content.",
    databricks_equivalent="spark.read.text('hdfs://...')  # native HDFS support",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListHDFS",
    full_type="org.apache.nifi.processors.hadoop.ListHDFS",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists files in an HDFS directory; emits one FlowFile per entry.",
    databricks_equivalent="dbutils.fs.ls('dbfs:/...')  # or spark listing",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchHDFS",
    full_type="org.apache.nifi.processors.hadoop.FetchHDFS",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure", "comms.failure"],
    description="Fetches the content of a specific HDFS file referenced by FlowFile attribute.",
    databricks_equivalent="spark.read.format('binaryFile').load('hdfs://...')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── AWS S3 ────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetS3Object",
    full_type="org.apache.nifi.processors.aws.s3.GetS3Object",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Fetches a single object from an S3 bucket.",
    databricks_equivalent="spark.read.format('...').load('s3://bucket/key')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListS3",
    full_type="org.apache.nifi.processors.aws.s3.ListS3",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists objects in an S3 bucket/prefix; emits one FlowFile per object.",
    databricks_equivalent="dbutils.fs.ls('s3://bucket/prefix')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchS3Object",
    full_type="org.apache.nifi.processors.aws.s3.FetchS3Object",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Fetches an S3 object whose bucket/key is specified in FlowFile attributes.",
    databricks_equivalent="spark.read.load('s3://bucket/key')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── Azure Blob / ADLS ────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetAzureBlobStorage",
    full_type="org.apache.nifi.processors.azure.storage.GetAzureBlobStorage",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Fetches a blob from Azure Blob Storage.",
    databricks_equivalent="spark.read.load('wasbs://container@account.blob.core.windows.net/path')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListAzureBlobStorage",
    full_type="org.apache.nifi.processors.azure.storage.ListAzureBlobStorage",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists blobs in an Azure Blob Storage container.",
    databricks_equivalent="dbutils.fs.ls('wasbs://container@account.blob.core.windows.net/')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchAzureBlobStorage",
    full_type="org.apache.nifi.processors.azure.storage.FetchAzureBlobStorage",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Fetches blob content from Azure Blob Storage given attributes.",
    databricks_equivalent="spark.read.load('abfss://container@account.dfs.core.windows.net/path')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── GCP Cloud Storage ────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetGCSObject",
    full_type="org.apache.nifi.processors.gcp.storage.GetGCSObject",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Fetches an object from Google Cloud Storage.",
    databricks_equivalent="spark.read.load('gs://bucket/key')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListGCSBucket",
    full_type="org.apache.nifi.processors.gcp.storage.ListGCSBucket",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists objects in a GCS bucket/prefix.",
    databricks_equivalent="dbutils.fs.ls('gs://bucket/prefix')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchGCSObject",
    full_type="org.apache.nifi.processors.gcp.storage.FetchGCSObject",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Fetches GCS object content using FlowFile attributes.",
    databricks_equivalent="spark.read.load('gs://bucket/key')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── SFTP / FTP ────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetSFTP",
    full_type="org.apache.nifi.processors.standard.GetSFTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Retrieves files from an SFTP server.",
    databricks_equivalent="# Use paramiko: sftp.get(remote, local); spark.read.load(local)",
    conversion_complexity="specialized",
    requires_imports=["import paramiko", "from pyspark.sql import SparkSession"],
    requires_packages=["paramiko"],
))

_reg(ProcessorDef(
    short_type="ListSFTP",
    full_type="org.apache.nifi.processors.standard.ListSFTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists files on an SFTP server directory.",
    databricks_equivalent="# paramiko: sftp.listdir_attr(path)",
    conversion_complexity="specialized",
    requires_imports=["import paramiko"],
    requires_packages=["paramiko"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchSFTP",
    full_type="org.apache.nifi.processors.standard.FetchSFTP",
    category="data_ingestion",
    role="source",
    relationships=["success", "not.found", "permission.denied", "failure", "comms.failure"],
    description="Fetches a specific file from an SFTP server given a path attribute.",
    databricks_equivalent="# paramiko: sftp.get(remote, local); spark.read.load(local)",
    conversion_complexity="specialized",
    requires_imports=["import paramiko", "from pyspark.sql import SparkSession"],
    requires_packages=["paramiko"],
))

_reg(ProcessorDef(
    short_type="GetFTP",
    full_type="org.apache.nifi.processors.standard.GetFTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Retrieves files from an FTP server.",
    databricks_equivalent="# ftplib: ftp.retrbinary(); spark.read.load(local)",
    conversion_complexity="specialized",
    requires_imports=["import ftplib", "from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ListFTP",
    full_type="org.apache.nifi.processors.standard.ListFTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Lists files on an FTP server directory.",
    databricks_equivalent="# ftplib: ftp.nlst(path)",
    conversion_complexity="specialized",
    requires_imports=["import ftplib"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="FetchFTP",
    full_type="org.apache.nifi.processors.standard.FetchFTP",
    category="data_ingestion",
    role="source",
    relationships=["success", "not.found", "permission.denied", "failure", "comms.failure"],
    description="Fetches a specific file from FTP given a path attribute.",
    databricks_equivalent="# ftplib: ftp.retrbinary(); spark.read.load(local)",
    conversion_complexity="specialized",
    requires_imports=["import ftplib", "from pyspark.sql import SparkSession"],
))

# ── Kafka Consumers ──────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ConsumeKafka",
    full_type="org.apache.nifi.processors.kafka.pubsub.ConsumeKafka",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes messages from Apache Kafka (legacy API).",
    databricks_equivalent="spark.readStream.format('kafka').option('subscribe', topic).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ConsumeKafka_2_6",
    full_type="org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes messages from Kafka using the 2.6+ client API.",
    databricks_equivalent="spark.readStream.format('kafka').option('subscribe', topic).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ConsumeKafkaRecord_2_6",
    full_type="org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes Kafka messages as records using a Record Reader.",
    databricks_equivalent="spark.readStream.format('kafka').load().select(from_json(col('value').cast('string'), schema))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession", "from pyspark.sql.functions import from_json, col"],
    is_stateful=True,
    is_record_aware=True,
))

# ── Messaging: AMQP / JMS ────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ConsumeAMQP",
    full_type="org.apache.nifi.amqp.processors.ConsumeAMQP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes messages from an AMQP 0.9.1 broker (e.g. RabbitMQ).",
    databricks_equivalent="# pika: channel.basic_consume(); collect into DataFrame",
    conversion_complexity="specialized",
    requires_imports=["import pika", "from pyspark.sql import SparkSession"],
    requires_packages=["pika"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ConsumeJMS",
    full_type="org.apache.nifi.jms.processors.ConsumeJMS",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes messages from a JMS queue or topic.",
    databricks_equivalent="# stomp.py or jpype + JMS API; manual bridging required",
    conversion_complexity="manual_review",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

# ── Cloud Streaming ──────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ConsumeKinesisStream",
    full_type="org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes records from an AWS Kinesis Data Stream.",
    databricks_equivalent="spark.readStream.format('kinesis').option('streamName', name).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ConsumeAzureEventHub",
    full_type="org.apache.nifi.processors.azure.eventhub.ConsumeAzureEventHub",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes events from Azure Event Hubs.",
    databricks_equivalent=(
        "spark.readStream.format('eventhubs')"
        ".options(**ehConf).load()"
    ),
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ConsumeGCPubSub",
    full_type="org.apache.nifi.processors.gcp.pubsub.ConsumeGCPubSub",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Consumes messages from a Google Cloud Pub/Sub subscription.",
    databricks_equivalent="# google-cloud-pubsub: subscriber.pull(); collect into DataFrame",
    conversion_complexity="specialized",
    requires_imports=["from google.cloud import pubsub_v1", "from pyspark.sql import SparkSession"],
    requires_packages=["google-cloud-pubsub"],
    is_stateful=True,
))

# ── Database Ingestion ───────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="QueryDatabaseTable",
    full_type="org.apache.nifi.processors.standard.QueryDatabaseTable",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Generates SQL to incrementally fetch rows from a database table using a max-value column.",
    databricks_equivalent="spark.read.format('jdbc').option('url', url).option('dbtable', tbl).option('lowerBound', lb).option('upperBound', ub).load()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="QueryDatabaseTableRecord",
    full_type="org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Like QueryDatabaseTable but outputs records using a Record Writer.",
    databricks_equivalent="spark.read.format('jdbc').option('url', url).option('dbtable', tbl).load()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="GenerateTableFetch",
    full_type="org.apache.nifi.processors.standard.GenerateTableFetch",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Generates SQL SELECT statements to fetch partitions of a table in parallel.",
    databricks_equivalent="spark.read.format('jdbc').option('numPartitions', n).option('partitionColumn', col).load()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ExecuteSQL",
    full_type="org.apache.nifi.processors.standard.ExecuteSQL",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Executes a SQL SELECT and converts the ResultSet to Avro FlowFile content.",
    databricks_equivalent="spark.read.format('jdbc').option('query', sql).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ExecuteSQLRecord",
    full_type="org.apache.nifi.processors.standard.ExecuteSQLRecord",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Executes SQL SELECT and writes results using a configured Record Writer.",
    databricks_equivalent="spark.read.format('jdbc').option('query', sql).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

# ── FlowFile Generation ─────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GenerateFlowFile",
    full_type="org.apache.nifi.processors.standard.GenerateFlowFile",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Generates FlowFiles with random or configured content; used for testing.",
    databricks_equivalent="spark.createDataFrame([...])  # synthetic test data",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── HTTP Ingestion ───────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetHTTP",
    full_type="org.apache.nifi.processors.standard.GetHTTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="(Deprecated) Fetches content from an HTTP/HTTPS URL on a schedule.",
    databricks_equivalent="import requests; data = requests.get(url).content",
    conversion_complexity="direct_map",
    requires_imports=["import requests", "from pyspark.sql import SparkSession"],
    requires_packages=["requests"],
))

_reg(ProcessorDef(
    short_type="ListenHTTP",
    full_type="org.apache.nifi.processors.standard.ListenHTTP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Starts an HTTP server that receives POST data as FlowFiles.",
    databricks_equivalent="# Flask/FastAPI endpoint; not directly translatable to batch",
    conversion_complexity="manual_review",
    requires_imports=["from flask import Flask"],
    requires_packages=["flask"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="HandleHttpRequest",
    full_type="org.apache.nifi.processors.standard.HandleHttpRequest",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Receives HTTP requests and converts them to FlowFiles for processing.",
    databricks_equivalent="# Flask/FastAPI endpoint receiving requests",
    conversion_complexity="manual_review",
    requires_imports=["from flask import Flask, request"],
    requires_packages=["flask"],
    is_stateful=True,
))

# ── TCP / UDP / Syslog ──────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ListenTCP",
    full_type="org.apache.nifi.processors.standard.ListenTCP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Listens on a TCP port and creates FlowFiles from incoming data.",
    databricks_equivalent="spark.readStream.format('socket').option('host', h).option('port', p).load()",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ListenUDP",
    full_type="org.apache.nifi.processors.standard.ListenUDP",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Listens on a UDP port and creates FlowFiles from datagrams.",
    databricks_equivalent="# socket-based UDP listener; manual implementation required",
    conversion_complexity="manual_review",
    requires_imports=["import socket"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="ListenSyslog",
    full_type="org.apache.nifi.processors.standard.ListenSyslog",
    category="data_ingestion",
    role="source",
    relationships=["success", "invalid"],
    description="Listens for Syslog messages over TCP or UDP.",
    databricks_equivalent="# Parse syslog from file/stream: spark.readStream + regexp_extract",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession", "from pyspark.sql.functions import regexp_extract"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="TailFile",
    full_type="org.apache.nifi.processors.standard.TailFile",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Tails a file (like tail -f) and creates FlowFiles from new lines/content.",
    databricks_equivalent="spark.readStream.format('text').option('path', dir).load()  # Auto Loader",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

# ── NoSQL / Search Ingestion ─────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetMongo",
    full_type="org.apache.nifi.processors.mongodb.GetMongo",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Executes a query against MongoDB and returns results as FlowFiles.",
    databricks_equivalent="spark.read.format('mongodb').option('uri', uri).option('collection', col).load()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["pymongo"],
))

_reg(ProcessorDef(
    short_type="GetElasticsearch",
    full_type="org.apache.nifi.processors.elasticsearch.GetElasticsearch",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure", "not_found", "retry"],
    description="Fetches a document from Elasticsearch by ID.",
    databricks_equivalent="spark.read.format('org.elasticsearch.spark.sql').option('es.resource', idx).load()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["elasticsearch"],
))

_reg(ProcessorDef(
    short_type="CaptureChangeMySQL",
    full_type="org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Captures Change Data Capture (CDC) events from a MySQL binlog.",
    databricks_equivalent="spark.readStream.format('delta').option('readChangeFeed', 'true').table(tbl)  # or Debezium",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

# ── AWS Queues ───────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetSQS",
    full_type="org.apache.nifi.processors.aws.sqs.GetSQS",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Receives messages from an Amazon SQS queue.",
    databricks_equivalent="import boto3; sqs = boto3.client('sqs'); msgs = sqs.receive_message(QueueUrl=url)",
    conversion_complexity="specialized",
    requires_imports=["import boto3", "from pyspark.sql import SparkSession"],
    requires_packages=["boto3"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="GetSNS",
    full_type="org.apache.nifi.processors.aws.sns.GetSNS",
    category="data_ingestion",
    role="source",
    relationships=["success"],
    description="Receives notifications from Amazon SNS (typically via SQS subscription).",
    databricks_equivalent="import boto3; # SNS -> SQS subscription pattern",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
    is_stateful=True,
))

# ── Redis / HBase ────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GetRedis",
    full_type="org.apache.nifi.processors.redis.GetRedis",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Gets a value from Redis by key.",
    databricks_equivalent="import redis; r = redis.Redis(); val = r.get(key)",
    conversion_complexity="specialized",
    requires_imports=["import redis"],
    requires_packages=["redis"],
))

_reg(ProcessorDef(
    short_type="GetHBaseRow",
    full_type="org.apache.nifi.hbase.GetHBaseRow",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure", "not_found"],
    description="Fetches a single row from HBase by row key.",
    databricks_equivalent="spark.read.format('org.apache.hadoop.hbase.spark').option('hbase.table', tbl).load()",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ScanHBase",
    full_type="org.apache.nifi.hbase.ScanHBase",
    category="data_ingestion",
    role="source",
    relationships=["success", "failure"],
    description="Scans rows from HBase within a range of row keys.",
    databricks_equivalent="spark.read.format('org.apache.hadoop.hbase.spark').option('hbase.table', tbl).load().filter(...)",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 2: DATA EGRESS  (54 processors)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Local File ───────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutFile",
    full_type="org.apache.nifi.processors.standard.PutFile",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile content to a local filesystem directory.",
    databricks_equivalent="df.write.mode('overwrite').text(path)  # or .save(path)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── HDFS ─────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutHDFS",
    full_type="org.apache.nifi.processors.hadoop.PutHDFS",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile content to HDFS.",
    databricks_equivalent="df.write.save('hdfs://...')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── AWS S3 ───────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutS3Object",
    full_type="org.apache.nifi.processors.aws.s3.PutS3Object",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Puts an object into an S3 bucket.",
    databricks_equivalent="df.write.save('s3://bucket/key')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── Azure ────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutAzureBlobStorage",
    full_type="org.apache.nifi.processors.azure.storage.PutAzureBlobStorage",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Uploads FlowFile content as a blob to Azure Blob Storage.",
    databricks_equivalent="df.write.save('wasbs://container@account.blob.core.windows.net/path')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PutAzureDataLakeStorage",
    full_type="org.apache.nifi.processors.azure.storage.PutAzureDataLakeStorage",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile content to Azure Data Lake Storage Gen2.",
    databricks_equivalent="df.write.save('abfss://container@account.dfs.core.windows.net/path')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── GCP ──────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutGCSObject",
    full_type="org.apache.nifi.processors.gcp.storage.PutGCSObject",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Uploads FlowFile content to a Google Cloud Storage bucket.",
    databricks_equivalent="df.write.save('gs://bucket/key')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── SFTP / FTP ───────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutSFTP",
    full_type="org.apache.nifi.processors.standard.PutSFTP",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "reject"],
    description="Sends FlowFile content to an SFTP server.",
    databricks_equivalent="# paramiko: sftp.put(local, remote)",
    conversion_complexity="specialized",
    requires_imports=["import paramiko"],
    requires_packages=["paramiko"],
))

_reg(ProcessorDef(
    short_type="PutFTP",
    full_type="org.apache.nifi.processors.standard.PutFTP",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "reject"],
    description="Sends FlowFile content to an FTP server.",
    databricks_equivalent="# ftplib: ftp.storbinary()",
    conversion_complexity="specialized",
    requires_imports=["import ftplib"],
))

# ── Database Egress ──────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutDatabaseRecord",
    full_type="org.apache.nifi.processors.standard.PutDatabaseRecord",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Inserts, updates, upserts, or deletes records into a database using a Record Reader.",
    databricks_equivalent="df.write.format('jdbc').option('url', url).option('dbtable', tbl).mode('append').save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutSQL",
    full_type="org.apache.nifi.processors.standard.PutSQL",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry"],
    description="Executes a SQL INSERT/UPDATE/DELETE statement against a database.",
    databricks_equivalent="spark.sql(stmt)  # or JDBC executeUpdate",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── Kafka Producers ──────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PublishKafka",
    full_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Publishes FlowFile content as a message to Apache Kafka (legacy API).",
    databricks_equivalent="df.write.format('kafka').option('topic', topic).save()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PublishKafka_2_6",
    full_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Publishes messages to Kafka using the 2.6+ client API.",
    databricks_equivalent="df.write.format('kafka').option('topic', topic).save()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── AWS Kinesis / Firehose ───────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutKinesisStream",
    full_type="org.apache.nifi.processors.aws.kinesis.stream.PutKinesisStream",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Puts records into an AWS Kinesis Data Stream.",
    databricks_equivalent="import boto3; kinesis.put_records(StreamName=name, Records=records)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

_reg(ProcessorDef(
    short_type="PutKinesisFirehose",
    full_type="org.apache.nifi.processors.aws.kinesis.firehose.PutKinesisFirehose",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Puts records into an AWS Kinesis Firehose delivery stream.",
    databricks_equivalent="import boto3; firehose.put_record_batch(DeliveryStreamName=name, Records=records)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

# ── Azure Event Hub / GCP Pub/Sub ────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutAzureEventHub",
    full_type="org.apache.nifi.processors.azure.eventhub.PutAzureEventHub",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends FlowFile content to Azure Event Hubs.",
    databricks_equivalent="df.write.format('eventhubs').options(**ehConf).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PublishGCPubSub",
    full_type="org.apache.nifi.processors.gcp.pubsub.PublishGCPubSub",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Publishes FlowFile content to a Google Cloud Pub/Sub topic.",
    databricks_equivalent="from google.cloud import pubsub_v1; publisher.publish(topic, data)",
    conversion_complexity="specialized",
    requires_imports=["from google.cloud import pubsub_v1"],
    requires_packages=["google-cloud-pubsub"],
))

# ── NoSQL / Search Sinks ────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutMongo",
    full_type="org.apache.nifi.processors.mongodb.PutMongo",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Inserts or upserts a single document into MongoDB.",
    databricks_equivalent="df.write.format('mongodb').option('uri', uri).option('collection', col).mode('append').save()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["pymongo"],
))

_reg(ProcessorDef(
    short_type="PutMongoRecord",
    full_type="org.apache.nifi.processors.mongodb.PutMongoRecord",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Inserts records into MongoDB using a Record Reader.",
    databricks_equivalent="df.write.format('mongodb').mode('append').save()",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["pymongo"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutElasticsearch",
    full_type="org.apache.nifi.processors.elasticsearch.PutElasticsearch",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry"],
    description="Indexes a FlowFile as a document in Elasticsearch.",
    databricks_equivalent="df.write.format('org.elasticsearch.spark.sql').option('es.resource', idx).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["elasticsearch"],
))

_reg(ProcessorDef(
    short_type="PutElasticsearchHttp",
    full_type="org.apache.nifi.processors.elasticsearch.PutElasticsearchHttp",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry"],
    description="Indexes documents into Elasticsearch via its HTTP REST API.",
    databricks_equivalent="import requests; requests.post(f'{es_url}/{idx}/_doc', json=doc)",
    conversion_complexity="template",
    requires_imports=["import requests"],
    requires_packages=["requests", "elasticsearch"],
))

_reg(ProcessorDef(
    short_type="PutCouchbaseDocument",
    full_type="org.apache.nifi.processors.couchbase.PutCouchbaseDocument",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry"],
    description="Stores a document in a Couchbase bucket.",
    databricks_equivalent="from couchbase.cluster import Cluster; collection.upsert(doc_id, doc)",
    conversion_complexity="specialized",
    requires_imports=["from couchbase.cluster import Cluster"],
    requires_packages=["couchbase"],
))

# ── HBase ────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutHBaseCell",
    full_type="org.apache.nifi.hbase.PutHBaseCell",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Puts a single cell value into an HBase table.",
    databricks_equivalent="# HBase connector: df.write.format('org.apache.hadoop.hbase.spark').save()",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PutHBaseJSON",
    full_type="org.apache.nifi.hbase.PutHBaseJSON",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Parses JSON content and inserts cells into an HBase table.",
    databricks_equivalent="# HBase connector: df.write.format('org.apache.hadoop.hbase.spark').save()",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── Hive ─────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutHiveQL",
    full_type="org.apache.nifi.processors.hive.PutHiveQL",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Executes HiveQL INSERT/DDL statements against a Hive Metastore.",
    databricks_equivalent="spark.sql(hiveql_stmt)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PutHiveStreaming",
    full_type="org.apache.nifi.processors.hive.PutHiveStreaming",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Streams records into a Hive transactional table via Hive Streaming API.",
    databricks_equivalent="df.write.mode('append').saveAsTable(tbl)  # Delta preferred",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

# ── Kudu / Solr / InfluxDB ──────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutKudu",
    full_type="org.apache.nifi.processors.kudu.PutKudu",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes records to an Apache Kudu table.",
    databricks_equivalent="df.write.format('org.apache.kudu.spark.kudu').option('kudu.table', tbl).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutSolr",
    full_type="org.apache.nifi.processors.solr.PutSolr",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "connection.failure"],
    description="Indexes FlowFile content or records into Apache Solr.",
    databricks_equivalent="# pysolr: solr.add(docs)",
    conversion_complexity="specialized",
    requires_imports=["import pysolr"],
    requires_packages=["pysolr"],
))

_reg(ProcessorDef(
    short_type="PutInfluxDB",
    full_type="org.apache.nifi.processors.influxdb.PutInfluxDB",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry", "max.retries.exceeded"],
    description="Writes FlowFile content as line-protocol data to InfluxDB.",
    databricks_equivalent="from influxdb_client import InfluxDBClient; write_api.write(bucket, org, data)",
    conversion_complexity="specialized",
    requires_imports=["from influxdb_client import InfluxDBClient"],
    requires_packages=["influxdb-client"],
))

# ── AWS DynamoDB / SQS / SNS ────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutDynamoDB",
    full_type="org.apache.nifi.processors.aws.dynamodb.PutDynamoDB",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "unprocessed"],
    description="Puts items into an Amazon DynamoDB table.",
    databricks_equivalent="import boto3; table.put_item(Item=item)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

_reg(ProcessorDef(
    short_type="PutSNS",
    full_type="org.apache.nifi.processors.aws.sns.PutSNS",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Publishes a message to an Amazon SNS topic.",
    databricks_equivalent="import boto3; sns.publish(TopicArn=arn, Message=msg)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

_reg(ProcessorDef(
    short_type="PutSQS",
    full_type="org.apache.nifi.processors.aws.sqs.PutSQS",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends a message to an Amazon SQS queue.",
    databricks_equivalent="import boto3; sqs.send_message(QueueUrl=url, MessageBody=body)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

# ── Email / Slack ────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutEmail",
    full_type="org.apache.nifi.processors.standard.PutEmail",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends FlowFile content as an email via SMTP.",
    databricks_equivalent="import smtplib; server.sendmail(from_addr, to_addr, msg.as_string())",
    conversion_complexity="specialized",
    requires_imports=["import smtplib", "from email.mime.text import MIMEText"],
))

_reg(ProcessorDef(
    short_type="PutSlack",
    full_type="org.apache.nifi.processors.slack.PutSlack",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Posts a message to a Slack channel via webhook.",
    databricks_equivalent="import requests; requests.post(webhook_url, json={'text': msg})",
    conversion_complexity="direct_map",
    requires_imports=["import requests"],
    requires_packages=["requests"],
))

_reg(ProcessorDef(
    short_type="PostSlack",
    full_type="org.apache.nifi.processors.slack.PostSlack",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Posts a message to Slack using the Web API (richer features than webhook).",
    databricks_equivalent="from slack_sdk import WebClient; client.chat_postMessage(channel=ch, text=msg)",
    conversion_complexity="specialized",
    requires_imports=["from slack_sdk import WebClient"],
    requires_packages=["slack-sdk"],
))

# ── TCP / UDP / Syslog ──────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutTCP",
    full_type="org.apache.nifi.processors.standard.PutTCP",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends FlowFile content over a TCP connection.",
    databricks_equivalent="import socket; s.sendall(data)",
    conversion_complexity="specialized",
    requires_imports=["import socket"],
))

_reg(ProcessorDef(
    short_type="PutUDP",
    full_type="org.apache.nifi.processors.standard.PutUDP",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends FlowFile content as a UDP datagram.",
    databricks_equivalent="import socket; s.sendto(data, (host, port))",
    conversion_complexity="specialized",
    requires_imports=["import socket"],
))

_reg(ProcessorDef(
    short_type="PutSyslog",
    full_type="org.apache.nifi.processors.standard.PutSyslog",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "invalid"],
    description="Sends FlowFile content as a syslog message over TCP or UDP.",
    databricks_equivalent="import logging.handlers; handler = SysLogHandler(address=(host, port))",
    conversion_complexity="specialized",
    requires_imports=["import logging", "from logging.handlers import SysLogHandler"],
))

# ── Delta / Iceberg / Hudi / ORC / Parquet (Lakehouse) ──────────────────────

_reg(ProcessorDef(
    short_type="PutDelta",
    full_type="org.apache.nifi.processors.delta.PutDelta",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile content to a Delta Lake table.",
    databricks_equivalent="df.write.format('delta').mode('append').save(path)  # or .saveAsTable(tbl)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PutDeltaLake",
    full_type="org.apache.nifi.processors.deltalake.PutDeltaLake",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes records to Delta Lake format (alternate processor name).",
    databricks_equivalent="df.write.format('delta').mode('append').saveAsTable(tbl)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutIceberg",
    full_type="org.apache.nifi.processors.iceberg.PutIceberg",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes records to an Apache Iceberg table.",
    databricks_equivalent="df.writeTo('catalog.db.table').using('iceberg').append()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutHudi",
    full_type="org.apache.nifi.processors.hudi.PutHudi",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes records to an Apache Hudi table.",
    databricks_equivalent="df.write.format('hudi').options(**hudiOptions).mode('append').save(path)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutORC",
    full_type="org.apache.nifi.processors.orc.PutORC",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile records in ORC format to HDFS/cloud storage.",
    databricks_equivalent="df.write.format('orc').save(path)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutParquet",
    full_type="org.apache.nifi.processors.parquet.PutParquet",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Writes FlowFile records in Parquet format to HDFS/cloud storage.",
    databricks_equivalent="df.write.format('parquet').save(path)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

# ── BigQuery ─────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutBigQueryBatch",
    full_type="org.apache.nifi.processors.gcp.bigquery.PutBigQueryBatch",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Loads data into Google BigQuery using batch load jobs.",
    databricks_equivalent="df.write.format('bigquery').option('table', tbl).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["google-cloud-bigquery"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="PutBigQueryStreaming",
    full_type="org.apache.nifi.processors.gcp.bigquery.PutBigQueryStreaming",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Streams data into Google BigQuery using the streaming insert API.",
    databricks_equivalent="df.writeStream.format('bigquery').option('table', tbl).start()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["google-cloud-bigquery"],
    is_record_aware=True,
))

# ── Redshift / Snowflake ─────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutRedshift",
    full_type="org.apache.nifi.processors.aws.redshift.PutRedshift",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Loads data into Amazon Redshift (typically via S3 COPY).",
    databricks_equivalent="df.write.format('redshift').option('url', url).option('dbtable', tbl).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="PutSnowflakeInternalStage",
    full_type="org.apache.nifi.processors.snowflake.PutSnowflakeInternalStage",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Stages files to a Snowflake internal stage for subsequent COPY INTO.",
    databricks_equivalent="df.write.format('snowflake').options(**sfOptions).option('dbtable', tbl).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["snowflake-connector-python"],
))

# ── Cassandra ────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutCassandraQL",
    full_type="org.apache.nifi.processors.cassandra.PutCassandraQL",
    category="data_egress",
    role="sink",
    relationships=["success", "failure", "retry"],
    description="Executes a CQL statement against Apache Cassandra.",
    databricks_equivalent="df.write.format('org.apache.spark.sql.cassandra').options(table=tbl, keyspace=ks).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["cassandra-driver"],
))

# ── Azure Cosmos DB / Azure Data Explorer ────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutAzureCosmosDB",
    full_type="org.apache.nifi.processors.azure.cosmos.PutAzureCosmosDB",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Inserts documents into Azure Cosmos DB.",
    databricks_equivalent="df.write.format('cosmos.oltp').options(**cosmosConf).mode('append').save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["azure-cosmos"],
))

_reg(ProcessorDef(
    short_type="PutAzureDataExplorer",
    full_type="org.apache.nifi.processors.azure.data.explorer.PutAzureDataExplorer",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Ingests data into Azure Data Explorer (Kusto).",
    databricks_equivalent="df.write.format('com.microsoft.kusto.spark.datasource').options(**kustoConf).save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    requires_packages=["azure-kusto-data"],
))

# ── Redis ────────────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutRedis",
    full_type="org.apache.nifi.processors.redis.PutRedis",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sets a key-value pair in Redis.",
    databricks_equivalent="import redis; r.set(key, value)",
    conversion_complexity="specialized",
    requires_imports=["import redis"],
    requires_packages=["redis"],
))

# ── AWS Lambda / CloudWatch ──────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutLambda",
    full_type="org.apache.nifi.processors.aws.lambda.PutLambda",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Invokes an AWS Lambda function with FlowFile content as payload.",
    databricks_equivalent="import boto3; client.invoke(FunctionName=name, Payload=payload)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

_reg(ProcessorDef(
    short_type="PutCloudWatchMetric",
    full_type="org.apache.nifi.processors.aws.cloudwatch.PutCloudWatchMetric",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Publishes a custom metric data point to Amazon CloudWatch.",
    databricks_equivalent="import boto3; cw.put_metric_data(Namespace=ns, MetricData=data)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

_reg(ProcessorDef(
    short_type="PutCloudWatchLogs",
    full_type="org.apache.nifi.processors.aws.cloudwatch.PutCloudWatchLogs",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Sends log events to Amazon CloudWatch Logs.",
    databricks_equivalent="import boto3; logs.put_log_events(logGroupName=grp, logStreamName=strm, logEvents=events)",
    conversion_complexity="specialized",
    requires_imports=["import boto3"],
    requires_packages=["boto3"],
))

# ── Salesforce ───────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="PutSalesforceRecord",
    full_type="org.apache.nifi.processors.salesforce.PutSalesforceRecord",
    category="data_egress",
    role="sink",
    relationships=["success", "failure"],
    description="Inserts/updates/upserts records into a Salesforce object via Bulk API.",
    databricks_equivalent="from simple_salesforce import Salesforce; sf.bulk.Account.insert(records)",
    conversion_complexity="specialized",
    requires_imports=["from simple_salesforce import Salesforce"],
    requires_packages=["simple-salesforce"],
    is_record_aware=True,
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 3: TRANSFORMATION  (48 processors)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Attribute Manipulation ───────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="UpdateAttribute",
    full_type="org.apache.nifi.processors.attributes.UpdateAttribute",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Adds, updates, or removes FlowFile attributes using NiFi Expression Language.",
    databricks_equivalent="df = df.withColumn(col_name, expr)  # or .withColumnRenamed()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import lit, col, expr"],
    is_stateful=True,
))

# ── Text / Regex Replacement ────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ReplaceText",
    full_type="org.apache.nifi.processors.standard.ReplaceText",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Replaces content of a FlowFile using regex, literal, or expression language substitutions.",
    databricks_equivalent="df = df.withColumn(col, regexp_replace(col, pattern, replacement))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import regexp_replace, col"],
))

# ── JSON Transforms ─────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="JoltTransformJSON",
    full_type="org.apache.nifi.processors.standard.JoltTransformJSON",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Applies a Jolt specification to transform JSON content structurally.",
    databricks_equivalent="# Manual struct transforms: df.select(col('a.b').alias('c'), ...)",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import col, struct, to_json, from_json"],
))

_reg(ProcessorDef(
    short_type="EvaluateJsonPath",
    full_type="org.apache.nifi.processors.standard.EvaluateJsonPath",
    category="transformation",
    role="transform",
    relationships=["matched", "unmatched", "failure"],
    description="Evaluates JsonPath expressions against FlowFile JSON content or extracts to attributes.",
    databricks_equivalent="df = df.withColumn('val', col('json_col.nested.field'))  # or get_json_object()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import get_json_object, col"],
))

_reg(ProcessorDef(
    short_type="FlattenJson",
    full_type="org.apache.nifi.processors.standard.FlattenJson",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Flattens nested JSON objects into a single-level structure with dotted keys.",
    databricks_equivalent="# Recursive select: df.select('a', 'b.c', 'b.d', ...) or explode()",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import col"],
))

# ── XML Transforms ──────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="EvaluateXPath",
    full_type="org.apache.nifi.processors.standard.EvaluateXPath",
    category="transformation",
    role="transform",
    relationships=["matched", "unmatched", "failure"],
    description="Evaluates XPath expressions against FlowFile XML content.",
    databricks_equivalent="from pyspark.sql.functions import xpath, xpath_string; df.withColumn('val', xpath_string(col('xml'), expr))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import xpath, xpath_string, col"],
))

_reg(ProcessorDef(
    short_type="EvaluateXQuery",
    full_type="org.apache.nifi.processors.standard.EvaluateXQuery",
    category="transformation",
    role="transform",
    relationships=["matched", "unmatched", "failure"],
    description="Evaluates XQuery expressions against FlowFile XML content.",
    databricks_equivalent="# lxml: etree.XPath(query)(doc); manual XML processing",
    conversion_complexity="manual_review",
    requires_imports=["from lxml import etree"],
    requires_packages=["lxml"],
))

_reg(ProcessorDef(
    short_type="TransformXml",
    full_type="org.apache.nifi.processors.standard.TransformXml",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Applies an XSLT stylesheet to transform XML FlowFile content.",
    databricks_equivalent="# lxml: transform = etree.XSLT(xslt_doc); result = transform(xml_doc)",
    conversion_complexity="manual_review",
    requires_imports=["from lxml import etree"],
    requires_packages=["lxml"],
))

# ── Split Processors ────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="SplitJson",
    full_type="org.apache.nifi.processors.standard.SplitJson",
    category="transformation",
    role="transform",
    relationships=["split", "original", "failure"],
    description="Splits a JSON array FlowFile into individual FlowFiles per element.",
    databricks_equivalent="df = df.withColumn('item', explode(col('array_col')))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import explode, col"],
))

_reg(ProcessorDef(
    short_type="SplitXml",
    full_type="org.apache.nifi.processors.standard.SplitXml",
    category="transformation",
    role="transform",
    relationships=["split", "original", "failure"],
    description="Splits an XML document into sub-documents at a specified depth.",
    databricks_equivalent="# Parse XML then explode: from pyspark.sql.functions import explode",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import explode, col"],
))

_reg(ProcessorDef(
    short_type="SplitRecord",
    full_type="org.apache.nifi.processors.standard.SplitRecord",
    category="transformation",
    role="transform",
    relationships=["splits", "original", "failure"],
    description="Splits a record-oriented FlowFile into smaller FlowFiles with N records each.",
    databricks_equivalent="# Partition: dfs = [df.limit(n).subtract(prev) ...] or repartition()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="ForkRecord",
    full_type="org.apache.nifi.processors.standard.ForkRecord",
    category="transformation",
    role="transform",
    relationships=["fork", "original", "failure"],
    description="Forks (explodes) a record containing an array field into multiple records.",
    databricks_equivalent="df = df.withColumn('item', explode(col('array_field'))).drop('array_field')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import explode, col"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="SplitContent",
    full_type="org.apache.nifi.processors.standard.SplitContent",
    category="transformation",
    role="transform",
    relationships=["splits", "original"],
    description="Splits FlowFile content by a byte sequence delimiter.",
    databricks_equivalent="df = df.withColumn('parts', split(col('content'), delimiter))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import split, col, explode"],
))

_reg(ProcessorDef(
    short_type="SplitText",
    full_type="org.apache.nifi.processors.standard.SplitText",
    category="transformation",
    role="transform",
    relationships=["splits", "original", "failure"],
    description="Splits text content by line count, producing multiple FlowFiles.",
    databricks_equivalent="# repartition or split by line groups",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import monotonically_increasing_id, col"],
))

# ── Merge Processors ────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="MergeContent",
    full_type="org.apache.nifi.processors.standard.MergeContent",
    category="transformation",
    role="transform",
    relationships=["merged", "original", "failure"],
    description="Merges multiple FlowFiles into one by concatenation, TAR, ZIP, or Avro.",
    databricks_equivalent="df = df1.union(df2)  # or coalesce(1) for single file output",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="MergeRecord",
    full_type="org.apache.nifi.processors.standard.MergeRecord",
    category="transformation",
    role="transform",
    relationships=["merged", "original", "failure"],
    description="Merges record-oriented FlowFiles into larger batches using Record Reader/Writer.",
    databricks_equivalent="df = df1.union(df2)  # coalesce for output batching",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
    is_record_aware=True,
))

# ── Text Extraction ─────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ExtractText",
    full_type="org.apache.nifi.processors.standard.ExtractText",
    category="transformation",
    role="transform",
    relationships=["matched", "unmatched"],
    description="Extracts text from FlowFile content using regex capture groups into attributes.",
    databricks_equivalent="df = df.withColumn('extracted', regexp_extract(col('content'), pattern, group))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import regexp_extract, col"],
))

_reg(ProcessorDef(
    short_type="ExtractGrok",
    full_type="org.apache.nifi.processors.standard.ExtractGrok",
    category="transformation",
    role="transform",
    relationships=["matched", "unmatched"],
    description="Extracts fields from unstructured text using Grok patterns (like Logstash).",
    databricks_equivalent="# UDF with pygrok: grok.match(text); or regexp_extract patterns",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import regexp_extract, col"],
))

# ── Record-Level Transforms ─────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ConvertRecord",
    full_type="org.apache.nifi.processors.standard.ConvertRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Reads records with one format (e.g. CSV) and writes them in another (e.g. JSON).",
    databricks_equivalent="df.write.format('json').save(path)  # read CSV, write JSON",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="LookupRecord",
    full_type="org.apache.nifi.processors.standard.LookupRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Enriches records by looking up values from an external source (DB, cache, etc.).",
    databricks_equivalent="df = df.join(lookup_df, on=key_col, how='left')",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="LookupAttribute",
    full_type="org.apache.nifi.processors.standard.LookupAttribute",
    category="transformation",
    role="transform",
    relationships=["success", "failure", "unmatched"],
    description="Looks up attribute values from an external lookup service.",
    databricks_equivalent="# broadcast join or map-side lookup UDF",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import broadcast"],
))

_reg(ProcessorDef(
    short_type="UpdateRecord",
    full_type="org.apache.nifi.processors.standard.UpdateRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Updates fields in records using Record Path expressions.",
    databricks_equivalent="df = df.withColumn('field', expr('...'))  # column transforms",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import col, expr, when"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="QueryRecord",
    full_type="org.apache.nifi.processors.standard.QueryRecord",
    category="transformation",
    role="transform",
    relationships=["original", "failure"],  # plus dynamic SQL-named relationships
    description="Executes SQL queries against record-oriented FlowFile content (like an in-flow SQL engine).",
    databricks_equivalent="df.createOrReplaceTempView('tbl'); spark.sql(query)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

# ── Validation Processors ───────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="ValidateRecord",
    full_type="org.apache.nifi.processors.standard.ValidateRecord",
    category="transformation",
    role="transform",
    relationships=["valid", "invalid", "failure"],
    description="Validates records against an Avro schema and routes valid/invalid records.",
    databricks_equivalent="# Schema enforcement: df.where(col('field').isNotNull() & ...)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import col"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="ValidateJSON",
    full_type="org.apache.nifi.processors.standard.ValidateJSON",
    category="transformation",
    role="transform",
    relationships=["valid", "invalid"],
    description="Validates FlowFile content against a JSON Schema.",
    databricks_equivalent="# UDF: jsonschema.validate(json_data, schema)",
    conversion_complexity="specialized",
    requires_imports=["import jsonschema"],
    requires_packages=["jsonschema"],
))

_reg(ProcessorDef(
    short_type="ValidateXML",
    full_type="org.apache.nifi.processors.standard.ValidateXML",
    category="transformation",
    role="transform",
    relationships=["valid", "invalid"],
    description="Validates FlowFile content against an XML Schema (XSD).",
    databricks_equivalent="# lxml: schema.validate(doc)",
    conversion_complexity="specialized",
    requires_imports=["from lxml import etree"],
    requires_packages=["lxml"],
))

_reg(ProcessorDef(
    short_type="ValidateCsv",
    full_type="org.apache.nifi.processors.standard.ValidateCsv",
    category="transformation",
    role="transform",
    relationships=["valid", "invalid"],
    description="Validates FlowFile content as CSV against a defined schema.",
    databricks_equivalent="# spark.read.csv with enforced schema + mode='FAILFAST'",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

# ── Record Normalization / Denormalization ───────────────────────────────────

_reg(ProcessorDef(
    short_type="NormalizeRecord",
    full_type="org.apache.nifi.processors.standard.NormalizeRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Normalizes (explodes) nested array fields in records into separate records.",
    databricks_equivalent="df = df.withColumn('item', explode(col('array_field'))).drop('array_field')",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import explode, col"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="DenormalizeRecord",
    full_type="org.apache.nifi.processors.standard.DenormalizeRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Denormalizes (collects/groups) records by a key into nested arrays.",
    databricks_equivalent="df.groupBy('key').agg(collect_list(struct('*')).alias('items'))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import collect_list, struct, col"],
    is_record_aware=True,
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="PartitionRecord",
    full_type="org.apache.nifi.processors.standard.PartitionRecord",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Partitions records into separate FlowFiles based on field values.",
    databricks_equivalent="df.write.partitionBy('col').format('delta').save(path)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

# ── Format Conversion Processors ────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="AttributesToJSON",
    full_type="org.apache.nifi.processors.standard.AttributesToJSON",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts FlowFile attributes to JSON content or to a JSON attribute.",
    databricks_equivalent="df = df.withColumn('json_str', to_json(struct([col(c) for c in cols])))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import to_json, struct, col"],
))

_reg(ProcessorDef(
    short_type="ConvertJSONToSQL",
    full_type="org.apache.nifi.processors.standard.ConvertJSONToSQL",
    category="transformation",
    role="transform",
    relationships=["sql", "failure", "original"],
    description="Converts JSON content to SQL INSERT, UPDATE, or DELETE statements.",
    databricks_equivalent="# Build INSERT from DataFrame rows; or df.write.format('jdbc').save()",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ConvertAvroToJSON",
    full_type="org.apache.nifi.processors.avro.ConvertAvroToJSON",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts Avro-encoded FlowFile content to JSON format.",
    databricks_equivalent="df = spark.read.format('avro').load(path); df.write.json(out)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ConvertJSONToAvro",
    full_type="org.apache.nifi.processors.avro.ConvertJSONToAvro",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts JSON FlowFile content to Avro format using a specified schema.",
    databricks_equivalent="df = spark.read.json(path); df.write.format('avro').save(out)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ConvertCSVToJSON",
    full_type="org.apache.nifi.processors.convertcsv.ConvertCSVToJSON",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts CSV FlowFile content to JSON format.",
    databricks_equivalent="df = spark.read.csv(path, header=True); df.write.json(out)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ConvertJSONToCSV",
    full_type="org.apache.nifi.processors.convertjson.ConvertJSONToCSV",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts JSON FlowFile content to CSV format.",
    databricks_equivalent="df = spark.read.json(path); df.write.csv(out, header=True)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ConvertExcelToCSV",
    full_type="org.apache.nifi.processors.excel.ConvertExcelToCSV",
    category="transformation",
    role="transform",
    relationships=["success", "failure", "original"],
    description="Converts Microsoft Excel spreadsheets (.xls/.xlsx) to CSV format.",
    databricks_equivalent="import pandas as pd; pdf = pd.read_excel(path); df = spark.createDataFrame(pdf)",
    conversion_complexity="template",
    requires_imports=["import pandas as pd", "from pyspark.sql import SparkSession"],
    requires_packages=["openpyxl"],
))

_reg(ProcessorDef(
    short_type="ConvertCharacterSet",
    full_type="org.apache.nifi.processors.standard.ConvertCharacterSet",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Converts FlowFile content from one character encoding to another.",
    databricks_equivalent="df = df.withColumn('content', decode(col('bytes'), 'UTF-8'))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import decode, encode, col"],
))

# ── Content Manipulation ────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="CompressContent",
    full_type="org.apache.nifi.processors.standard.CompressContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Compresses or decompresses FlowFile content (GZIP, BZIP2, XZ, Snappy, etc.).",
    databricks_equivalent="df = spark.read.option('compression', 'gzip').text(path)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="UnpackContent",
    full_type="org.apache.nifi.processors.standard.UnpackContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure", "original"],
    description="Unpacks archive content (ZIP, TAR, FlowFile-v3) into individual FlowFiles.",
    databricks_equivalent="# zipfile/tarfile: extract then spark.read; or dbutils.fs.cp + unzip",
    conversion_complexity="specialized",
    requires_imports=["import zipfile", "import tarfile"],
))

_reg(ProcessorDef(
    short_type="EncryptContent",
    full_type="org.apache.nifi.processors.standard.EncryptContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Encrypts FlowFile content using PGP, AES, or other algorithms.",
    databricks_equivalent="from cryptography.fernet import Fernet; encrypted = f.encrypt(data)",
    conversion_complexity="specialized",
    requires_imports=["from cryptography.fernet import Fernet"],
    requires_packages=["cryptography"],
))

_reg(ProcessorDef(
    short_type="DecryptContent",
    full_type="org.apache.nifi.processors.standard.DecryptContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Decrypts FlowFile content encrypted with PGP, AES, or other algorithms.",
    databricks_equivalent="from cryptography.fernet import Fernet; decrypted = f.decrypt(data)",
    conversion_complexity="specialized",
    requires_imports=["from cryptography.fernet import Fernet"],
    requires_packages=["cryptography"],
))

_reg(ProcessorDef(
    short_type="HashContent",
    full_type="org.apache.nifi.processors.standard.HashContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Computes a hash (MD5, SHA-1, SHA-256, etc.) of FlowFile content and stores it as an attribute.",
    databricks_equivalent="df = df.withColumn('hash', sha2(col('content'), 256))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import sha2, md5, col"],
))

_reg(ProcessorDef(
    short_type="HashAttribute",
    full_type="org.apache.nifi.processors.standard.HashAttribute",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Computes a hash of selected FlowFile attributes to produce a consistent hash attribute.",
    databricks_equivalent="df = df.withColumn('attr_hash', sha2(concat_ws('|', *cols), 256))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import sha2, concat_ws, col"],
))

_reg(ProcessorDef(
    short_type="Base64EncodeContent",
    full_type="org.apache.nifi.processors.standard.Base64EncodeContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Encodes FlowFile content to Base64.",
    databricks_equivalent="df = df.withColumn('encoded', base64(col('content')))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import base64, col"],
))

_reg(ProcessorDef(
    short_type="Base64DecodeContent",
    full_type="org.apache.nifi.processors.standard.Base64DecodeContent",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Decodes FlowFile content from Base64.",
    databricks_equivalent="df = df.withColumn('decoded', unbase64(col('encoded')))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import unbase64, col"],
))

# ── Geo / Enrichment ────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="GeoEnrichIP",
    full_type="org.apache.nifi.processors.standard.GeoEnrichIP",
    category="transformation",
    role="transform",
    relationships=["found", "not found"],
    description="Enriches FlowFile with geolocation data (city, country, lat/lon) from an IP address.",
    databricks_equivalent="# UDF with geoip2: reader.city(ip); add as columns",
    conversion_complexity="specialized",
    requires_imports=["import geoip2.database"],
    requires_packages=["geoip2"],
))

# ── Statistics ──────────────────────────────────────────────────────────────

_reg(ProcessorDef(
    short_type="CalculateRecordStats",
    full_type="org.apache.nifi.processors.standard.CalculateRecordStats",
    category="transformation",
    role="transform",
    relationships=["success", "failure"],
    description="Calculates statistics (count, min, max, avg, etc.) on record fields and stores as attributes.",
    databricks_equivalent="df.agg(count('*'), min('col'), max('col'), avg('col'))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import count, min, max, avg"],
    is_record_aware=True,
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 4: ROUTING  (14 processors)
# ═══════════════════════════════════════════════════════════════════════════════

_reg(ProcessorDef(
    short_type="RouteOnAttribute",
    full_type="org.apache.nifi.processors.standard.RouteOnAttribute",
    category="routing",
    role="route",
    relationships=["unmatched"],  # plus user-defined relationship names
    description="Routes FlowFiles to named relationships based on attribute value conditions (NiFi EL).",
    databricks_equivalent="df_match = df.filter(condition); df_rest = df.filter(~condition)",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import col, when"],
))

_reg(ProcessorDef(
    short_type="RouteOnContent",
    full_type="org.apache.nifi.processors.standard.RouteOnContent",
    category="routing",
    role="route",
    relationships=["unmatched"],  # plus user-defined
    description="Routes FlowFiles to named relationships based on content matching (regex or contains).",
    databricks_equivalent="df_match = df.filter(col('content').rlike(pattern))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import col"],
))

_reg(ProcessorDef(
    short_type="RouteText",
    full_type="org.apache.nifi.processors.standard.RouteText",
    category="routing",
    role="route",
    relationships=["unmatched", "original"],  # plus user-defined
    description="Routes lines of text content to relationships based on pattern matching.",
    databricks_equivalent="# Split lines, filter by pattern per route",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import col, explode, split"],
))

_reg(ProcessorDef(
    short_type="DistributeLoad",
    full_type="org.apache.nifi.processors.standard.DistributeLoad",
    category="routing",
    role="route",
    relationships=["1"],  # numbered relationships
    description="Distributes FlowFiles across numbered relationships using round-robin or weighted strategies.",
    databricks_equivalent="df.repartition(n)  # distribute across partitions",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="ControlRate",
    full_type="org.apache.nifi.processors.standard.ControlRate",
    category="routing",
    role="route",
    relationships=["success", "failure"],
    description="Throttles the flow rate of FlowFiles to a configured maximum (count, size, or data rate).",
    databricks_equivalent="# Trigger.ProcessingTime in structured streaming; or batch window logic",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="SampleRecord",
    full_type="org.apache.nifi.processors.standard.SampleRecord",
    category="routing",
    role="route",
    relationships=["success", "original", "failure"],
    description="Samples a percentage or fixed number of records from a record-oriented FlowFile.",
    databricks_equivalent="df_sample = df.sample(fraction=0.1)  # or .limit(n)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="LimitFlowFile",
    full_type="org.apache.nifi.processors.standard.LimitFlowFile",
    category="routing",
    role="route",
    relationships=["success", "failure"],
    description="Limits the number or size of FlowFiles passing through.",
    databricks_equivalent="df = df.limit(max_rows)",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="DetectDuplicate",
    full_type="org.apache.nifi.processors.standard.DetectDuplicate",
    category="routing",
    role="route",
    relationships=["duplicate", "non-duplicate", "failure"],
    description="Detects duplicate FlowFiles using a distributed cache based on a hash attribute.",
    databricks_equivalent="df = df.dropDuplicates([key_col])  # or use watermark in streaming",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="DetectDuplicateRecord",
    full_type="org.apache.nifi.processors.standard.DetectDuplicateRecord",
    category="routing",
    role="route",
    relationships=["duplicate", "non-duplicate", "original", "failure"],
    description="Detects duplicate records within record-oriented FlowFiles.",
    databricks_equivalent="df = df.dropDuplicates([key_cols])",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_stateful=True,
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="DeduplicateRecord",
    full_type="org.apache.nifi.processors.standard.DeduplicateRecord",
    category="routing",
    role="route",
    relationships=["unique", "duplicate", "failure"],
    description="Removes duplicate records from a FlowFile based on specified fields.",
    databricks_equivalent="df = df.dropDuplicates([key_cols])",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="EnforceOrder",
    full_type="org.apache.nifi.processors.standard.EnforceOrder",
    category="routing",
    role="route",
    relationships=["success", "failure", "wait", "skipped", "overtook"],
    description="Enforces ordering of FlowFiles by waiting for predecessors to arrive first.",
    databricks_equivalent="df = df.orderBy(col('sequence_id'))  # or window functions",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import col"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="Wait",
    full_type="org.apache.nifi.processors.standard.Wait",
    category="routing",
    role="route",
    relationships=["success", "wait", "expired", "failure"],
    description="Holds FlowFiles until a signal is received via a distributed cache (paired with Notify).",
    databricks_equivalent="# Orchestration: use Databricks Workflows task dependencies",
    conversion_complexity="manual_review",
    requires_imports=[],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="Notify",
    full_type="org.apache.nifi.processors.standard.Notify",
    category="routing",
    role="route",
    relationships=["success", "failure"],
    description="Signals a distributed cache to release FlowFiles held by a Wait processor.",
    databricks_equivalent="# Orchestration: Databricks Workflows signal / task completion",
    conversion_complexity="manual_review",
    requires_imports=[],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="RetryFlowFile",
    full_type="org.apache.nifi.processors.standard.RetryFlowFile",
    category="routing",
    role="route",
    relationships=["retry", "retries_exceeded", "failure"],
    description="Manages retry logic for FlowFiles, tracking attempt counts and routing accordingly.",
    databricks_equivalent="# Python retry decorator or tenacity library",
    conversion_complexity="template",
    requires_imports=["from tenacity import retry, stop_after_attempt, wait_exponential"],
    requires_packages=["tenacity"],
    is_stateful=True,
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 5: SCRIPTING  (7 processors)
# ═══════════════════════════════════════════════════════════════════════════════

_reg(ProcessorDef(
    short_type="ExecuteScript",
    full_type="org.apache.nifi.processors.standard.ExecuteScript",
    category="scripting",
    role="transform",
    relationships=["success", "failure"],
    description="Executes a script (Groovy, Python/Jython, JavaScript, Ruby, Lua, Clojure) with access to the FlowFile.",
    databricks_equivalent="# Translate script logic to PySpark UDF or native DataFrame ops",
    conversion_complexity="manual_review",
    requires_imports=["from pyspark.sql.functions import udf"],
))

_reg(ProcessorDef(
    short_type="ExecuteGroovyScript",
    full_type="org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
    category="scripting",
    role="transform",
    relationships=["success", "failure"],
    description="Executes a Groovy script with enhanced API access for FlowFile manipulation.",
    databricks_equivalent="# Translate Groovy logic to PySpark; manual conversion required",
    conversion_complexity="manual_review",
    requires_imports=["from pyspark.sql.functions import udf"],
))

_reg(ProcessorDef(
    short_type="ExecuteStreamCommand",
    full_type="org.apache.nifi.processors.standard.ExecuteStreamCommand",
    category="scripting",
    role="transform",
    relationships=["output stream", "original", "nonzero status"],
    description="Executes an external command, streaming FlowFile content as stdin and capturing stdout.",
    databricks_equivalent="import subprocess; result = subprocess.run(cmd, input=data, capture_output=True)",
    conversion_complexity="specialized",
    requires_imports=["import subprocess"],
))

_reg(ProcessorDef(
    short_type="ExecuteProcess",
    full_type="org.apache.nifi.processors.standard.ExecuteProcess",
    category="scripting",
    role="source",
    relationships=["success"],
    description="Runs an OS process and creates FlowFiles from its stdout output.",
    databricks_equivalent="import subprocess; output = subprocess.check_output(cmd)",
    conversion_complexity="specialized",
    requires_imports=["import subprocess"],
))

_reg(ProcessorDef(
    short_type="InvokeScriptedProcessor",
    full_type="org.apache.nifi.processors.script.InvokeScriptedProcessor",
    category="scripting",
    role="transform",
    relationships=["success", "failure"],
    description="Invokes a scripted processor implementation that defines its own relationships and behavior.",
    databricks_equivalent="# Translate scripted processor logic to PySpark; fully custom",
    conversion_complexity="manual_review",
    requires_imports=["from pyspark.sql.functions import udf"],
))

_reg(ProcessorDef(
    short_type="ScriptedTransformRecord",
    full_type="org.apache.nifi.processors.script.ScriptedTransformRecord",
    category="scripting",
    role="transform",
    relationships=["success", "failure"],
    description="Applies a user script to each record in a record-oriented FlowFile.",
    databricks_equivalent="# UDF over DataFrame rows: df.withColumn('out', my_udf(col('in')))",
    conversion_complexity="manual_review",
    requires_imports=["from pyspark.sql.functions import udf, col"],
    is_record_aware=True,
))

_reg(ProcessorDef(
    short_type="ExecutePythonProcessor",
    full_type="org.apache.nifi.processors.python.ExecutePythonProcessor",
    category="scripting",
    role="transform",
    relationships=["success", "failure"],
    description="Executes a Python processor (NiFi 2.x native Python support).",
    databricks_equivalent="# Direct Python logic translation to PySpark UDF or notebook code",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import udf, col"],
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 6: HTTP / API  (7 processors)
# ═══════════════════════════════════════════════════════════════════════════════

_reg(ProcessorDef(
    short_type="InvokeHTTP",
    full_type="org.apache.nifi.processors.standard.InvokeHTTP",
    category="http_api",
    role="transform",
    relationships=["Response", "Original", "Retry", "No Retry", "Failure"],
    description="Sends HTTP requests (GET/POST/PUT/DELETE) and routes responses. The Swiss Army knife of NiFi HTTP.",
    databricks_equivalent="import requests; resp = requests.request(method, url, headers=hdrs, data=body)",
    conversion_complexity="template",
    requires_imports=["import requests", "from pyspark.sql import SparkSession"],
    requires_packages=["requests"],
))

# Note: HandleHttpRequest is registered in Data Ingestion (source role).
# This registers the response handler as its companion.

_reg(ProcessorDef(
    short_type="HandleHttpResponse",
    full_type="org.apache.nifi.processors.standard.HandleHttpResponse",
    category="http_api",
    role="sink",
    relationships=["success", "failure"],
    description="Sends an HTTP response back to the client for a HandleHttpRequest-received request.",
    databricks_equivalent="# Flask: return Response(body, status=code, headers=hdrs)",
    conversion_complexity="manual_review",
    requires_imports=["from flask import Response"],
    requires_packages=["flask"],
))

_reg(ProcessorDef(
    short_type="InvokeAWSGatewayApi",
    full_type="org.apache.nifi.processors.aws.apigateway.InvokeAWSGatewayApi",
    category="http_api",
    role="transform",
    relationships=["Response", "Original", "Retry", "No Retry", "Failure"],
    description="Invokes an AWS API Gateway endpoint with SigV4 authentication.",
    databricks_equivalent="import boto3; client = boto3.client('apigateway'); # or requests with SigV4 auth",
    conversion_complexity="specialized",
    requires_imports=["import boto3", "import requests"],
    requires_packages=["boto3", "requests"],
))

_reg(ProcessorDef(
    short_type="PostHTTP",
    full_type="org.apache.nifi.processors.standard.PostHTTP",
    category="http_api",
    role="sink",
    relationships=["success", "failure"],
    description="(Deprecated) Posts FlowFile content to an HTTP endpoint.",
    databricks_equivalent="import requests; requests.post(url, data=content, headers=hdrs)",
    conversion_complexity="direct_map",
    requires_imports=["import requests"],
    requires_packages=["requests"],
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 7: MONITORING / UTILITY  (9 processors)
# ═══════════════════════════════════════════════════════════════════════════════

_reg(ProcessorDef(
    short_type="LogAttribute",
    full_type="org.apache.nifi.processors.standard.LogAttribute",
    category="monitoring",
    role="monitor",
    relationships=["success"],
    description="Logs all FlowFile attributes at a configurable log level.",
    databricks_equivalent="print(row.asDict())  # or logging.info(row)",
    conversion_complexity="direct_map",
    requires_imports=["import logging"],
))

_reg(ProcessorDef(
    short_type="LogMessage",
    full_type="org.apache.nifi.processors.standard.LogMessage",
    category="monitoring",
    role="monitor",
    relationships=["success"],
    description="Logs a user-defined message (can include NiFi EL) at a configurable log level.",
    databricks_equivalent="logging.info(message)",
    conversion_complexity="direct_map",
    requires_imports=["import logging"],
))

_reg(ProcessorDef(
    short_type="MonitorActivity",
    full_type="org.apache.nifi.processors.standard.MonitorActivity",
    category="monitoring",
    role="monitor",
    relationships=["success", "inactive", "activity.restored"],
    description="Monitors data flow activity and sends alerts when data stops or resumes flowing.",
    databricks_equivalent="# Databricks Workflows + alerting; or custom watchdog logic",
    conversion_complexity="manual_review",
    requires_imports=["import logging"],
    is_stateful=True,
))

_reg(ProcessorDef(
    short_type="UpdateCounter",
    full_type="org.apache.nifi.processors.standard.UpdateCounter",
    category="monitoring",
    role="monitor",
    relationships=["success"],
    description="Updates a named counter by a specified delta; counters are visible in NiFi UI.",
    databricks_equivalent="spark.sparkContext.accumulator(0)  # or custom metrics",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="IdentifyMimeType",
    full_type="org.apache.nifi.processors.standard.IdentifyMimeType",
    category="monitoring",
    role="utility",
    relationships=["success"],
    description="Detects the MIME type of FlowFile content using magic bytes and sets the mime.type attribute.",
    databricks_equivalent="import magic; mime = magic.from_buffer(data, mime=True)",
    conversion_complexity="specialized",
    requires_imports=["import magic"],
    requires_packages=["python-magic"],
))

_reg(ProcessorDef(
    short_type="Funnel",
    full_type="org.apache.nifi.connectable.Funnel",
    category="monitoring",
    role="utility",
    relationships=["success"],
    description="Merges multiple incoming connections into a single outgoing connection (visual organizer).",
    databricks_equivalent="df = df1.union(df2).union(df3)  # combine DataFrames",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql import SparkSession"],
))

_reg(ProcessorDef(
    short_type="InputPort",
    full_type="org.apache.nifi.connectable.InputPort",
    category="monitoring",
    role="utility",
    relationships=["success"],
    description="Receives FlowFiles from a parent process group or remote NiFi instance.",
    databricks_equivalent="# Function parameter / DataFrame input variable",
    conversion_complexity="direct_map",
    requires_imports=[],
))

_reg(ProcessorDef(
    short_type="OutputPort",
    full_type="org.apache.nifi.connectable.OutputPort",
    category="monitoring",
    role="utility",
    relationships=[],
    description="Sends FlowFiles out of a process group to a parent or remote NiFi instance.",
    databricks_equivalent="# Function return / DataFrame output variable",
    conversion_complexity="direct_map",
    requires_imports=[],
))

_reg(ProcessorDef(
    short_type="RemoteProcessGroup",
    full_type="org.apache.nifi.groups.RemoteProcessGroup",
    category="monitoring",
    role="utility",
    relationships=["success", "failure"],
    description="Transfers FlowFiles to/from a remote NiFi instance via Site-to-Site protocol.",
    databricks_equivalent="# Cross-workspace data sharing: Delta Sharing or external tables",
    conversion_complexity="manual_review",
    requires_imports=[],
    is_stateful=True,
))


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY 8: CONTENT MANIPULATION  (4 processors)
# ═══════════════════════════════════════════════════════════════════════════════

_reg(ProcessorDef(
    short_type="ModifyBytes",
    full_type="org.apache.nifi.processors.standard.ModifyBytes",
    category="content_manipulation",
    role="transform",
    relationships=["success", "failure"],
    description="Modifies FlowFile content by removing bytes from the head and/or tail.",
    databricks_equivalent="df = df.withColumn('trimmed', substring(col('content'), start, length))",
    conversion_complexity="template",
    requires_imports=["from pyspark.sql.functions import substring, col"],
))

_reg(ProcessorDef(
    short_type="SegmentContent",
    full_type="org.apache.nifi.processors.standard.SegmentContent",
    category="content_manipulation",
    role="transform",
    relationships=["segments", "original"],
    description="Segments FlowFile content into fixed-size chunks.",
    databricks_equivalent="# Chunk data: [data[i:i+size] for i in range(0, len(data), size)]",
    conversion_complexity="specialized",
    requires_imports=["from pyspark.sql.functions import col"],
))

_reg(ProcessorDef(
    short_type="CountText",
    full_type="org.apache.nifi.processors.standard.CountText",
    category="content_manipulation",
    role="transform",
    relationships=["success", "failure"],
    description="Counts lines, words, characters, and bytes in FlowFile text content.",
    databricks_equivalent="df = df.withColumn('line_count', size(split(col('content'), '\\n')))",
    conversion_complexity="direct_map",
    requires_imports=["from pyspark.sql.functions import size, split, length, col"],
))

_reg(ProcessorDef(
    short_type="SetMimeType",
    full_type="org.apache.nifi.processors.standard.SetMimeType",
    category="content_manipulation",
    role="utility",
    relationships=["success"],
    description="Sets the MIME type attribute of a FlowFile without inspecting content.",
    databricks_equivalent="# Metadata only — no Spark equivalent needed; track as variable",
    conversion_complexity="direct_map",
    requires_imports=[],
))


# ═══════════════════════════════════════════════════════════════════════════════
# LOOKUP / QUERY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def lookup_processor(proc_type: str) -> Optional[ProcessorDef]:
    """Look up a processor by short type, full type, or case-insensitive match.

    Tries exact match first, then strips the ``org.apache.nifi.`` prefix
    variations, and finally falls back to a case-insensitive scan.

    Args:
        proc_type: The processor type string (e.g. ``"GetFile"``,
            ``"org.apache.nifi.processors.standard.GetFile"``, or
            ``"getfile"``).

    Returns:
        The matching :class:`ProcessorDef`, or ``None`` if not found.
    """
    # 1. Exact match
    if proc_type in PROCESSOR_KB:
        return PROCESSOR_KB[proc_type]

    # 2. Try extracting the short name from a fully-qualified type
    if "." in proc_type:
        short = proc_type.rsplit(".", 1)[-1]
        if short in PROCESSOR_KB:
            return PROCESSOR_KB[short]

    # 3. Case-insensitive fallback
    lower = proc_type.lower()
    for key, pdef in PROCESSOR_KB.items():
        if key.lower() == lower:
            return pdef

    # 4. Partial match on short_type (e.g. "Kafka" -> first Kafka processor)
    for key, pdef in PROCESSOR_KB.items():
        if "." not in key and lower in key.lower():
            return pdef

    return None


def get_all_processors() -> Dict[str, ProcessorDef]:
    """Return a copy of the full processor registry.

    Keys include both short type names and fully-qualified class names.
    """
    return dict(PROCESSOR_KB)


def get_processors_by_category(category: str) -> List[ProcessorDef]:
    """Return all processors belonging to the given category.

    Args:
        category: One of ``data_ingestion``, ``data_egress``,
            ``transformation``, ``routing``, ``scripting``, ``http_api``,
            ``monitoring``, ``content_manipulation``, etc.

    Returns:
        De-duplicated list of :class:`ProcessorDef` objects (keyed by
        ``short_type`` to avoid duplicates from the dual-key registry).
    """
    seen: set[str] = set()
    results: List[ProcessorDef] = []
    for pdef in PROCESSOR_KB.values():
        if pdef.category == category and pdef.short_type not in seen:
            seen.add(pdef.short_type)
            results.append(pdef)
    return results


def get_processors_by_role(role: str) -> List[ProcessorDef]:
    """Return all processors with the given role.

    Args:
        role: One of ``source``, ``sink``, ``transform``, ``route``,
            ``utility``, ``monitor``, ``error_handler``.

    Returns:
        De-duplicated list of :class:`ProcessorDef` objects.
    """
    seen: set[str] = set()
    results: List[ProcessorDef] = []
    for pdef in PROCESSOR_KB.values():
        if pdef.role == role and pdef.short_type not in seen:
            seen.add(pdef.short_type)
            results.append(pdef)
    return results


def get_processors_by_complexity(complexity: str) -> List[ProcessorDef]:
    """Return all processors with the given conversion complexity.

    Args:
        complexity: One of ``direct_map``, ``template``, ``specialized``,
            ``manual_review``.

    Returns:
        De-duplicated list of :class:`ProcessorDef` objects.
    """
    seen: set[str] = set()
    results: List[ProcessorDef] = []
    for pdef in PROCESSOR_KB.values():
        if pdef.conversion_complexity == complexity and pdef.short_type not in seen:
            seen.add(pdef.short_type)
            results.append(pdef)
    return results


def get_record_aware_processors() -> List[ProcessorDef]:
    """Return all record-aware processors (those using Record Reader/Writer)."""
    seen: set[str] = set()
    results: List[ProcessorDef] = []
    for pdef in PROCESSOR_KB.values():
        if pdef.is_record_aware and pdef.short_type not in seen:
            seen.add(pdef.short_type)
            results.append(pdef)
    return results


def get_stateful_processors() -> List[ProcessorDef]:
    """Return all stateful processors (those maintaining internal state)."""
    seen: set[str] = set()
    results: List[ProcessorDef] = []
    for pdef in PROCESSOR_KB.values():
        if pdef.is_stateful and pdef.short_type not in seen:
            seen.add(pdef.short_type)
            results.append(pdef)
    return results


def get_required_packages(proc_types: List[str]) -> set[str]:
    """Collect all pip packages required by a list of processor types.

    Useful for generating a ``%pip install`` cell in a Databricks notebook.
    """
    packages: set[str] = set()
    for pt in proc_types:
        pdef = lookup_processor(pt)
        if pdef and pdef.requires_packages:
            packages.update(pdef.requires_packages)
    return packages


def get_required_imports(proc_types: List[str]) -> List[str]:
    """Collect all Python imports required by a list of processor types.

    Returns a de-duplicated, sorted list suitable for inserting at the top
    of a generated notebook cell.
    """
    imports: set[str] = set()
    for pt in proc_types:
        pdef = lookup_processor(pt)
        if pdef and pdef.requires_imports:
            imports.update(pdef.requires_imports)
    return sorted(imports)


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE SUMMARY (computed at import time)
# ═══════════════════════════════════════════════════════════════════════════════

def _summary() -> str:
    """Return a human-readable summary of the knowledge base."""
    unique = {p.short_type for p in PROCESSOR_KB.values()}
    cats = {}
    for p in PROCESSOR_KB.values():
        cats.setdefault(p.category, set()).add(p.short_type)
    lines = [f"NiFi Processor KB: {len(unique)} unique processors"]
    for cat, names in sorted(cats.items()):
        lines.append(f"  {cat}: {len(names)} processors")
    return "\n".join(lines)


if __name__ == "__main__":
    print(_summary())

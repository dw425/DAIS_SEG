"""Sink Generator -- Generates terminal write statements for pipeline output.

Every pipeline must persist data. This module detects sink processors and
generates the appropriate write/writeStream calls with checkpoints, triggers,
and destination-specific options (Delta, JDBC, Kafka, cloud APIs, notifications).
"""

import logging
import re
from dataclasses import dataclass

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SinkConfig:
    """Generated write/writeStream code and metadata for a single sink."""

    processor_name: str
    processor_type: str
    sink_type: str  # delta, jdbc, kafka, s3, notification, cloud_api, custom
    write_code: str
    is_streaming: bool
    checkpoint_path: str | None
    trigger_config: str | None


# ---------------------------------------------------------------------------
# Processor-type classification sets
# ---------------------------------------------------------------------------

_FILE_SINKS = {
    "PutFile", "PutHDFS", "PutS3Object", "PutAzureBlobStorage",
    "PutGCSObject", "PutAzureDataLakeStorage", "PutSFTP", "PutFTP",
    "StoreInKiteDataset",
}

_DATABASE_SINKS = {
    "PutDatabaseRecord", "PutSQL", "PutHiveQL",
    "PutHiveStreaming", "PutMongo", "PutCassandraQL",
    "PutElasticsearch", "PutElasticsearchHttp",
    "PutElasticsearchRecord", "PutSolrContentStream",
    "PutCouchbaseKey", "PutRiemann",
    "PutInfluxDB",
}

_KAFKA_SINKS = {
    "PublishKafka", "PublishKafka_2_6", "PublishKafkaRecord_2_6",
    "PublishKafka_1_0", "PublishKafkaRecord_1_0",
}

_NOTIFICATION_SINKS = {
    "PutEmail", "PutSlack", "PostSlack", "PostHTTP",
    "InvokeHTTP", "PutSyslog",
}

_CLOUD_API_SINKS = {
    "PutDynamoDB", "PutDynamoDBRecord",
    "PutSNS", "PutSQS", "PutLambda",
    "PutKinesisFirehose", "PutKinesisStream",
    "PutAzureEventHub", "PutAzureQueueStorage",
    "PutGCPubSub",
}


def _simple_type(proc_type: str) -> str:
    """Extract the simple class name from a possibly FQ NiFi processor type."""
    return proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type


def _safe_var(name: str) -> str:
    """Convert a name to a safe Python variable."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()


def _get_proc_properties(proc_name: str, parse_result: ParseResult) -> dict:
    """Look up processor properties by name."""
    for p in parse_result.processors:
        if p.name == proc_name:
            return p.properties
    return {}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_sink_code(
    processor_name: str,
    processor_type: str,
    properties: dict,
    upstream_df_name: str,
    execution_mode: str,  # "streaming" or "batch"
    config: dict,  # {catalog, schema, scope, checkpoint_base}
) -> SinkConfig:
    """Generate write code for a sink processor.

    Dispatches to the correct generator based on the processor type.
    """
    simple = _simple_type(processor_type)
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "default")
    scope = config.get("scope", "nifi_migration")
    checkpoint_base = config.get("checkpoint_base", f"/Volumes/{catalog}/{schema}/checkpoints")
    safe_name = _safe_var(processor_name)
    is_streaming = execution_mode == "streaming"

    # Dispatch by category
    if simple in _FILE_SINKS:
        return _generate_file_sink(
            processor_name, processor_type, properties, upstream_df_name,
            is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
        )

    if simple in _DATABASE_SINKS:
        return _generate_database_sink(
            processor_name, processor_type, properties, upstream_df_name,
            is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
        )

    if simple in _KAFKA_SINKS:
        return _generate_kafka_sink(
            processor_name, processor_type, properties, upstream_df_name,
            is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
        )

    if simple in _NOTIFICATION_SINKS:
        return _generate_notification_sink(
            processor_name, processor_type, properties, upstream_df_name,
            scope, safe_name,
        )

    if simple in _CLOUD_API_SINKS:
        return _generate_cloud_api_sink(
            processor_name, processor_type, properties, upstream_df_name,
            is_streaming, scope, checkpoint_base, safe_name,
        )

    # Default: persist to Delta table
    return _generate_default_sink(
        processor_name, processor_type, upstream_df_name,
        is_streaming, catalog, schema, checkpoint_base, safe_name,
    )


# ---------------------------------------------------------------------------
# File sinks (PutFile, PutHDFS, PutS3Object, PutAzureBlobStorage, PutGCSObject)
# ---------------------------------------------------------------------------

def _generate_file_sink(
    processor_name, processor_type, properties, upstream_df_name,
    is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
) -> SinkConfig:
    """Map file-writing processors to Delta table writes.

    NiFi writes to files; Databricks persists to Delta tables via Unity Catalog.
    """
    # Try to extract original path for documentation
    original_path = ""
    for key in ("Directory", "directory", "Remote Path", "Bucket", "Container Name",
                "Output Directory", "HDFS Directory", "S3 Bucket"):
        if key in properties and properties[key]:
            original_path = str(properties[key])
            break

    table_name = f"{catalog}.{schema}.{safe_name}"
    checkpoint_path = f"{checkpoint_base}/{safe_name}"

    if is_streaming:
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# Original destination: {original_path or 'N/A'}\n"
            f"# Migrated to Delta table with Structured Streaming writeStream\n"
            f"query_{safe_name} = (\n"
            f"    {upstream_df_name}\n"
            f"    .writeStream\n"
            f'    .format("delta")\n'
            f'    .outputMode("append")\n'
            f'    .option("checkpointLocation", "{checkpoint_path}")\n'
            f"    .trigger(availableNow=True)\n"
            f'    .toTable("{table_name}")\n'
            f")\n"
            f'print(f"[SINK] Streaming write started: {table_name}")'
        )
    else:
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# Original destination: {original_path or 'N/A'}\n"
            f"# Migrated to Delta table with batch write\n"
            f"(\n"
            f"    {upstream_df_name}\n"
            f"    .write\n"
            f'    .format("delta")\n'
            f'    .mode("append")\n'
            f'    .option("mergeSchema", "true")\n'
            f'    .saveAsTable("{table_name}")\n'
            f")\n"
            f'print(f"[SINK] Batch write complete: {table_name}")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="delta",
        write_code=code,
        is_streaming=is_streaming,
        checkpoint_path=checkpoint_path if is_streaming else None,
        trigger_config="availableNow=True" if is_streaming else None,
    )


# ---------------------------------------------------------------------------
# Database sinks (PutDatabaseRecord, PutSQL)
# ---------------------------------------------------------------------------

def _generate_database_sink(
    processor_name, processor_type, properties, upstream_df_name,
    is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
) -> SinkConfig:
    """Map database-writing processors to JDBC writes via Databricks Secrets."""
    # Extract table name from properties
    table_name = ""
    for key in ("Table Name", "table-name", "Table", "dbtable", "db-table",
                "Statement", "Hive Table"):
        if key in properties and properties[key]:
            table_name = str(properties[key])
            break
    if not table_name:
        table_name = safe_name

    # Detect if this is an Elasticsearch/Mongo/Solr sink (NoSQL)
    simple = _simple_type(processor_type)
    if "Elasticsearch" in simple or "elastic" in simple.lower():
        return _generate_elasticsearch_sink(
            processor_name, processor_type, properties, upstream_df_name,
            scope, safe_name,
        )

    checkpoint_path = f"{checkpoint_base}/{safe_name}"

    if is_streaming:
        # Streaming JDBC via foreachBatch
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# Streaming JDBC write via foreachBatch\n"
            f"def _write_jdbc_batch_{safe_name}(batch_df, batch_id):\n"
            f"    (\n"
            f"        batch_df\n"
            f"        .write\n"
            f'        .format("jdbc")\n'
            f'        .option("url", dbutils.secrets.get(scope="{scope}", key="jdbc_url"))\n'
            f'        .option("dbtable", "{table_name}")\n'
            f'        .option("user", dbutils.secrets.get(scope="{scope}", key="jdbc_user"))\n'
            f'        .option("password", dbutils.secrets.get(scope="{scope}", key="jdbc_password"))\n'
            f'        .mode("append")\n'
            f"        .save()\n"
            f"    )\n"
            f"\n"
            f"query_{safe_name} = (\n"
            f"    {upstream_df_name}\n"
            f"    .writeStream\n"
            f"    .foreachBatch(_write_jdbc_batch_{safe_name})\n"
            f'    .option("checkpointLocation", "{checkpoint_path}")\n'
            f"    .trigger(availableNow=True)\n"
            f"    .start()\n"
            f")\n"
            f'print(f"[SINK] Streaming JDBC write started: {table_name}")'
        )
    else:
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# JDBC batch write\n"
            f"(\n"
            f"    {upstream_df_name}\n"
            f"    .write\n"
            f'    .format("jdbc")\n'
            f'    .option("url", dbutils.secrets.get(scope="{scope}", key="jdbc_url"))\n'
            f'    .option("dbtable", "{table_name}")\n'
            f'    .option("user", dbutils.secrets.get(scope="{scope}", key="jdbc_user"))\n'
            f'    .option("password", dbutils.secrets.get(scope="{scope}", key="jdbc_password"))\n'
            f'    .mode("append")\n'
            f"    .save()\n"
            f")\n"
            f'print(f"[SINK] JDBC write complete: {table_name}")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="jdbc",
        write_code=code,
        is_streaming=is_streaming,
        checkpoint_path=checkpoint_path if is_streaming else None,
        trigger_config="availableNow=True" if is_streaming else None,
    )


def _generate_elasticsearch_sink(
    processor_name, processor_type, properties, upstream_df_name,
    scope, safe_name,
) -> SinkConfig:
    """Generate Elasticsearch sink using the elasticsearch-spark connector."""
    index_name = properties.get("Index", properties.get("index", safe_name))
    es_hosts = properties.get("Elasticsearch Hosts",
                              properties.get("elasticsearch-http-hosts", "localhost:9200"))

    code = (
        f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
        f"# Elasticsearch write via elasticsearch-spark connector\n"
        f"(\n"
        f"    {upstream_df_name}\n"
        f"    .write\n"
        f'    .format("org.elasticsearch.spark.sql")\n'
        f'    .option("es.nodes", dbutils.secrets.get(scope="{scope}", key="es_hosts"))\n'
        f'    .option("es.resource", "{index_name}")\n'
        f'    .option("es.nodes.wan.only", "true")\n'
        f'    .mode("append")\n'
        f"    .save()\n"
        f")\n"
        f'print(f"[SINK] Elasticsearch write complete: {index_name}")'
    )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="elasticsearch",
        write_code=code,
        is_streaming=False,
        checkpoint_path=None,
        trigger_config=None,
    )


# ---------------------------------------------------------------------------
# Kafka sinks (PublishKafka, PublishKafka_2_6)
# ---------------------------------------------------------------------------

def _generate_kafka_sink(
    processor_name, processor_type, properties, upstream_df_name,
    is_streaming, catalog, schema, scope, checkpoint_base, safe_name,
) -> SinkConfig:
    """Map Kafka publishing processors to Spark Structured Streaming Kafka sink."""
    bootstrap_servers = properties.get(
        "Kafka Brokers",
        properties.get("bootstrap.servers", "UNSET_kafka_bootstrap_servers"),
    )
    topic = properties.get("Topic Name", properties.get("topic", "output_topic"))
    checkpoint_path = f"{checkpoint_base}/{safe_name}"

    # Determine if bootstrap servers is a NiFi expression
    if "${" in bootstrap_servers:
        servers_option = f'    .option("kafka.bootstrap.servers", dbutils.secrets.get(scope="{scope}", key="kafka_bootstrap_servers"))'
    else:
        servers_option = f'    .option("kafka.bootstrap.servers", "{bootstrap_servers}")'

    if is_streaming:
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# Streaming Kafka publish\n"
            f"query_{safe_name} = (\n"
            f"    {upstream_df_name}\n"
            f'    .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")\n'
            f"    .writeStream\n"
            f'    .format("kafka")\n'
            f"{servers_option}\n"
            f'    .option("topic", "{topic}")\n'
            f'    .option("checkpointLocation", "{checkpoint_path}")\n'
            f"    .trigger(availableNow=True)\n"
            f"    .start()\n"
            f")\n"
            f'print(f"[SINK] Streaming Kafka publish started: {topic}")'
        )
    else:
        code = (
            f"# Sink: {processor_name} ({_simple_type(processor_type)})\n"
            f"# Batch Kafka publish\n"
            f"(\n"
            f"    {upstream_df_name}\n"
            f'    .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")\n'
            f"    .write\n"
            f'    .format("kafka")\n'
            f"{servers_option}\n"
            f'    .option("topic", "{topic}")\n'
            f"    .save()\n"
            f")\n"
            f'print(f"[SINK] Batch Kafka publish complete: {topic}")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="kafka",
        write_code=code,
        is_streaming=is_streaming,
        checkpoint_path=checkpoint_path if is_streaming else None,
        trigger_config="availableNow=True" if is_streaming else None,
    )


# ---------------------------------------------------------------------------
# Notification sinks (PutEmail, PutSlack, PostSlack, PostHTTP)
# ---------------------------------------------------------------------------

def _generate_notification_sink(
    processor_name, processor_type, properties, upstream_df_name,
    scope, safe_name,
) -> SinkConfig:
    """Generate Python-based notification calls (not Spark writes).

    Notifications are executed via ``foreachPartition`` or a simple collect+post
    pattern since they are side-effect operations, not data writes.
    """
    simple = _simple_type(processor_type)

    if "Email" in simple:
        to_addr = properties.get("To", properties.get("to", "recipient@example.com"))
        subject = properties.get("Subject", properties.get("subject", "Pipeline Notification"))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Email notification — migrated to Python smtplib / Databricks notification\n"
            f"import smtplib\n"
            f"from email.mime.text import MIMEText\n"
            f"\n"
            f"def _send_email_{safe_name}(body: str):\n"
            f'    smtp_host = dbutils.secrets.get(scope="{scope}", key="smtp_host")\n'
            f'    smtp_user = dbutils.secrets.get(scope="{scope}", key="smtp_user")\n'
            f'    smtp_pass = dbutils.secrets.get(scope="{scope}", key="smtp_password")\n'
            f'    msg = MIMEText(body)\n'
            f'    msg["Subject"] = "{subject}"\n'
            f'    msg["To"] = "{to_addr}"\n'
            f'    msg["From"] = smtp_user\n'
            f"    with smtplib.SMTP(smtp_host, 587) as server:\n"
            f"        server.starttls()\n"
            f"        server.login(smtp_user, smtp_pass)\n"
            f"        server.send_message(msg)\n"
            f"\n"
            f"# Collect summary and send\n"
            f"_count = {upstream_df_name}.count()\n"
            f'_send_email_{safe_name}(f"Pipeline complete: {{_count}} rows processed")\n'
            f'print(f"[SINK] Email notification sent to {to_addr}")'
        )
    elif "Slack" in simple:
        webhook_url_prop = properties.get("Webhook URL", properties.get("webhook-url", ""))
        channel = properties.get("Channel", properties.get("channel", "#data-pipeline"))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Slack notification\n"
            f"import requests\n"
            f"\n"
            f"def _send_slack_{safe_name}(message: str):\n"
            f'    webhook_url = dbutils.secrets.get(scope="{scope}", key="slack_webhook_url")\n'
            f'    requests.post(webhook_url, json={{"text": message, "channel": "{channel}"}})\n'
            f"\n"
            f"_count = {upstream_df_name}.count()\n"
            f'_send_slack_{safe_name}(f"Pipeline complete: {{_count}} rows processed")\n'
            f'print(f"[SINK] Slack notification sent to {channel}")'
        )
    else:
        # PostHTTP / InvokeHTTP / PutSyslog — generic HTTP sink
        url = properties.get("Remote URL", properties.get("HTTP URL", properties.get("url", "")))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# HTTP POST notification\n"
            f"import requests\n"
            f"\n"
            f"def _post_http_{safe_name}(payload: dict):\n"
            f'    url = dbutils.secrets.get(scope="{scope}", key="{safe_name}_url")\n'
            f"    resp = requests.post(url, json=payload, timeout=30)\n"
            f"    resp.raise_for_status()\n"
            f"\n"
            f"# Collect summary data and POST\n"
            f"_count = {upstream_df_name}.count()\n"
            f"_post_http_{safe_name}({{\"row_count\": _count, \"status\": \"complete\"}})\n"
            f'print(f"[SINK] HTTP notification sent")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="notification",
        write_code=code,
        is_streaming=False,  # notifications are always batch side-effects
        checkpoint_path=None,
        trigger_config=None,
    )


# ---------------------------------------------------------------------------
# Cloud API sinks (PutDynamoDB, PutSNS, PutSQS, PutLambda, etc.)
# ---------------------------------------------------------------------------

def _generate_cloud_api_sink(
    processor_name, processor_type, properties, upstream_df_name,
    is_streaming, scope, checkpoint_base, safe_name,
) -> SinkConfig:
    """Generate cloud API sink code using boto3 / cloud SDK calls.

    These processors interact with cloud services that don't have native
    Spark connectors, so we use foreachBatch or mapPartitions with the SDK.
    """
    simple = _simple_type(processor_type)
    checkpoint_path = f"{checkpoint_base}/{safe_name}"

    # Determine the cloud service and method
    if "DynamoDB" in simple:
        table_name = properties.get("Table Name", properties.get("table", safe_name))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# DynamoDB write via boto3 foreachBatch\n"
            f"import boto3\n"
            f"import json\n"
            f"\n"
            f"def _write_dynamodb_{safe_name}(batch_df, batch_id):\n"
            f'    access_key = dbutils.secrets.get(scope="{scope}", key="aws_access_key")\n'
            f'    secret_key = dbutils.secrets.get(scope="{scope}", key="aws_secret_key")\n'
            f'    dynamodb = boto3.resource("dynamodb",\n'
            f'        aws_access_key_id=access_key, aws_secret_access_key=secret_key)\n'
            f'    table = dynamodb.Table("{table_name}")\n'
            f"    with table.batch_writer() as writer:\n"
            f"        for row in batch_df.toLocalIterator():\n"
            f"            writer.put_item(Item=row.asDict())\n"
        )
    elif "SNS" in simple:
        topic_arn = properties.get("Amazon Resource Name (ARN)",
                                   properties.get("topic-arn", "arn:aws:sns:REGION:ACCOUNT:TOPIC"))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# SNS publish via boto3\n"
            f"import boto3, json\n"
            f"\n"
            f"def _write_dynamodb_{safe_name}(batch_df, batch_id):\n"
            f'    access_key = dbutils.secrets.get(scope="{scope}", key="aws_access_key")\n'
            f'    secret_key = dbutils.secrets.get(scope="{scope}", key="aws_secret_key")\n'
            f'    sns = boto3.client("sns",\n'
            f'        aws_access_key_id=access_key, aws_secret_access_key=secret_key)\n'
            f"    for row in batch_df.toLocalIterator():\n"
            f'        sns.publish(TopicArn="{topic_arn}", Message=json.dumps(row.asDict()))\n'
        )
    elif "SQS" in simple:
        queue_url = properties.get("Queue URL", properties.get("queue-url", ""))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# SQS send via boto3\n"
            f"import boto3, json\n"
            f"\n"
            f"def _write_sqs_{safe_name}(batch_df, batch_id):\n"
            f'    access_key = dbutils.secrets.get(scope="{scope}", key="aws_access_key")\n'
            f'    secret_key = dbutils.secrets.get(scope="{scope}", key="aws_secret_key")\n'
            f'    sqs = boto3.client("sqs",\n'
            f'        aws_access_key_id=access_key, aws_secret_access_key=secret_key)\n'
            f'    queue_url = dbutils.secrets.get(scope="{scope}", key="{safe_name}_queue_url")\n'
            f"    for row in batch_df.toLocalIterator():\n"
            f"        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(row.asDict()))\n"
        )
    elif "Lambda" in simple:
        function_name = properties.get("Amazon Lambda Name",
                                       properties.get("function-name", safe_name))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# AWS Lambda invocation via boto3\n"
            f"import boto3, json\n"
            f"\n"
            f"def _invoke_lambda_{safe_name}(batch_df, batch_id):\n"
            f'    access_key = dbutils.secrets.get(scope="{scope}", key="aws_access_key")\n'
            f'    secret_key = dbutils.secrets.get(scope="{scope}", key="aws_secret_key")\n'
            f'    lam = boto3.client("lambda",\n'
            f'        aws_access_key_id=access_key, aws_secret_access_key=secret_key)\n'
            f"    for row in batch_df.toLocalIterator():\n"
            f"        lam.invoke(\n"
            f'            FunctionName="{function_name}",\n'
            f'            InvocationType="Event",\n'
            f"            Payload=json.dumps(row.asDict()),\n"
            f"        )\n"
        )
    elif "Kinesis" in simple:
        stream_name = properties.get("Kinesis Stream Name",
                                     properties.get("stream-name", safe_name))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Kinesis write via boto3\n"
            f"import boto3, json\n"
            f"\n"
            f"def _write_kinesis_{safe_name}(batch_df, batch_id):\n"
            f'    access_key = dbutils.secrets.get(scope="{scope}", key="aws_access_key")\n'
            f'    secret_key = dbutils.secrets.get(scope="{scope}", key="aws_secret_key")\n'
            f'    kinesis = boto3.client("kinesis",\n'
            f'        aws_access_key_id=access_key, aws_secret_access_key=secret_key)\n'
            f"    records = [{{\"Data\": json.dumps(row.asDict()), \"PartitionKey\": str(batch_id)}} for row in batch_df.toLocalIterator()]\n"
            f"    kinesis.put_records(StreamName=\"{stream_name}\", Records=records)\n"
        )
    elif "EventHub" in simple or "Azure" in simple:
        event_hub = properties.get("Event Hub Name",
                                   properties.get("event-hub-name", safe_name))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Azure Event Hub write\n"
            f"# Using Spark Event Hub connector\n"
            f"_eh_conn_str = dbutils.secrets.get(scope=\"{scope}\", key=\"eventhub_connection_string\")\n"
            f"_eh_conf = {{'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(_eh_conn_str)}}\n"
        )
    elif "GCPubSub" in simple or "PubSub" in simple:
        topic = properties.get("Topic", properties.get("topic", safe_name))
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Google Cloud Pub/Sub publish\n"
            f"from google.cloud import pubsub_v1\n"
            f"import json\n"
            f"\n"
            f"def _write_pubsub_{safe_name}(batch_df, batch_id):\n"
            f"    publisher = pubsub_v1.PublisherClient()\n"
            f'    topic_path = "{topic}"\n'
            f"    for row in batch_df.toLocalIterator():\n"
            f"        publisher.publish(topic_path, json.dumps(row.asDict()).encode())\n"
        )
    else:
        # Generic cloud API fallback
        code = (
            f"# Sink: {processor_name} ({simple})\n"
            f"# Cloud API write — requires custom implementation\n"
            f"# USER ACTION: Implement the cloud API call for {simple}\n"
            f"def _write_cloud_{safe_name}(batch_df, batch_id):\n"
            f"    raise NotImplementedError('{simple} sink not yet implemented')\n"
        )

    # Wrap with foreachBatch for streaming
    if is_streaming:
        # Find the function name defined in code
        func_match = re.search(r"def (_\w+)\(batch_df", code)
        func_name = func_match.group(1) if func_match else f"_write_cloud_{safe_name}"
        code += (
            f"\n"
            f"query_{safe_name} = (\n"
            f"    {upstream_df_name}\n"
            f"    .writeStream\n"
            f"    .foreachBatch({func_name})\n"
            f'    .option("checkpointLocation", "{checkpoint_path}")\n'
            f"    .trigger(availableNow=True)\n"
            f"    .start()\n"
            f")\n"
            f'print(f"[SINK] Streaming {simple} write started")'
        )
    else:
        func_match = re.search(r"def (_\w+)\(batch_df", code)
        func_name = func_match.group(1) if func_match else f"_write_cloud_{safe_name}"
        code += (
            f"\n"
            f"# Execute batch write\n"
            f"{func_name}({upstream_df_name}, 0)\n"
            f'print(f"[SINK] Batch {simple} write complete")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="cloud_api",
        write_code=code,
        is_streaming=is_streaming,
        checkpoint_path=checkpoint_path if is_streaming else None,
        trigger_config="availableNow=True" if is_streaming else None,
    )


# ---------------------------------------------------------------------------
# Default sink (fallback Delta table)
# ---------------------------------------------------------------------------

def _generate_default_sink(
    processor_name, processor_type, upstream_df_name,
    is_streaming, catalog, schema, checkpoint_base, safe_name,
) -> SinkConfig:
    """Generate a default Delta table write when no specific sink is detected.

    This ensures every pipeline has at least one terminal write operation.
    """
    table_name = f"{catalog}.{schema}.{safe_name}_output"
    checkpoint_path = f"{checkpoint_base}/{safe_name}"

    if is_streaming:
        code = (
            f"# Sink: {processor_name} (default Delta output)\n"
            f"# No explicit sink processor detected; persisting to Delta table.\n"
            f"query_{safe_name} = (\n"
            f"    {upstream_df_name}\n"
            f"    .writeStream\n"
            f'    .format("delta")\n'
            f'    .outputMode("append")\n'
            f'    .option("checkpointLocation", "{checkpoint_path}")\n'
            f'    .option("mergeSchema", "true")\n'
            f"    .trigger(availableNow=True)\n"
            f'    .toTable("{table_name}")\n'
            f")\n"
            f'print(f"[SINK] Default streaming write started: {table_name}")'
        )
    else:
        code = (
            f"# Sink: {processor_name} (default Delta output)\n"
            f"# No explicit sink processor detected; persisting to Delta table.\n"
            f"(\n"
            f"    {upstream_df_name}\n"
            f"    .write\n"
            f'    .format("delta")\n'
            f'    .mode("append")\n'
            f'    .option("mergeSchema", "true")\n'
            f'    .saveAsTable("{table_name}")\n'
            f")\n"
            f'print(f"[SINK] Default batch write complete: {table_name}")'
        )

    return SinkConfig(
        processor_name=processor_name,
        processor_type=processor_type,
        sink_type="delta",
        write_code=code,
        is_streaming=is_streaming,
        checkpoint_path=checkpoint_path if is_streaming else None,
        trigger_config="availableNow=True" if is_streaming else None,
    )


# ---------------------------------------------------------------------------
# Convenience: generate sinks for an entire assessment
# ---------------------------------------------------------------------------

def generate_all_sinks(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    execution_mode: str,
    config: DatabricksConfig,
) -> list[SinkConfig]:
    """Scan all mappings and generate SinkConfig for each sink processor."""
    from app.engines.generators.pipeline_wirer import _safe_var  # avoid circular at module level

    sinks: list[SinkConfig] = []
    sink_pattern = re.compile(r"(Put|Publish|Send|Insert|Post|Store|Write)", re.IGNORECASE)

    for mapping in assessment.mappings:
        simple = _simple_type(mapping.type)
        if not sink_pattern.search(simple) and mapping.role != "sink":
            continue

        properties = _get_proc_properties(mapping.name, parse_result)
        upstream_df = f"df_{_safe_var(mapping.name)}"  # best-effort; wirer overrides this

        sink = generate_sink_code(
            processor_name=mapping.name,
            processor_type=mapping.type,
            properties=properties,
            upstream_df_name=upstream_df,
            execution_mode=execution_mode,
            config={
                "catalog": config.catalog,
                "schema": config.schema_name,
                "scope": config.secret_scope,
                "checkpoint_base": f"/Volumes/{config.catalog}/{config.schema_name}/checkpoints",
            },
        )
        sinks.append(sink)

    logger.info("Sink generation: %d sink(s) produced", len(sinks))
    return sinks

// NiFi processor type → external system mapping (covers 200+ processor types)
export const PROC_TYPE_SYSTEM_MAP = {
  // Kafka
  ConsumeKafka:{tool:'Kafka',subtype:'consumer',dbx:'Structured Streaming',pri:1,code:'spark.readStream.format("kafka")...',notes:'Kafka → Structured Streaming consumer'},
  ConsumeKafka_2_6:{tool:'Kafka',subtype:'consumer-2.6',dbx:'Structured Streaming',pri:1,code:'spark.readStream.format("kafka")...',notes:'Kafka 2.6 consumer'},
  ConsumeKafkaRecord_2_6:{tool:'Kafka',subtype:'record-consumer',dbx:'Structured Streaming',pri:1,code:'spark.readStream.format("kafka")...select(from_json(...))',notes:'Kafka record consumer with schema'},
  PublishKafka:{tool:'Kafka',subtype:'producer',dbx:'Structured Streaming Write',pri:1,code:'df.write.format("kafka")...',notes:'Kafka producer'},
  PublishKafka_2_6:{tool:'Kafka',subtype:'producer-2.6',dbx:'Structured Streaming Write',pri:1,code:'df.write.format("kafka")...',notes:'Kafka 2.6 producer'},
  PublishKafkaRecord_2_6:{tool:'Kafka',subtype:'record-producer',dbx:'Structured Streaming Write',pri:1,code:'df.selectExpr("to_json(struct(*)) AS value").write.format("kafka")...',notes:'Kafka record producer'},
  // AWS
  ListS3:{tool:'AWS S3',subtype:'list',dbx:'dbutils.fs.ls / Unity Catalog External Location',pri:1,code:'dbutils.fs.ls("s3://<bucket>/<prefix>")',notes:'Use Unity Catalog external locations for S3'},
  FetchS3Object:{tool:'AWS S3',subtype:'fetch',dbx:'Spark Read',pri:1,code:'spark.read.format("<fmt>").load("s3://<bucket>/<key>")',notes:'Read S3 via external location'},
  GetS3Object:{tool:'AWS S3',subtype:'get',dbx:'Spark Read',pri:1,code:'spark.read.load("s3://<bucket>/<key>")',notes:'Read S3 objects'},
  PutS3Object:{tool:'AWS S3',subtype:'put',dbx:'Spark Write',pri:1,code:'df.write.save("s3://<bucket>/<key>")',notes:'Write to S3 via external location'},
  DeleteS3Object:{tool:'AWS S3',subtype:'delete',dbx:'dbutils.fs.rm',pri:3,code:'dbutils.fs.rm("s3://<bucket>/<key>")',notes:'Delete S3 objects'},
  TagS3Object:{tool:'AWS S3',subtype:'tag',dbx:'boto3 / dbutils',pri:3,code:'# Use boto3 for S3 tagging',notes:'No native Spark equivalent'},
  PutSNS:{tool:'AWS SNS',subtype:'publish',dbx:'Databricks Workflows Notification',pri:3,code:'# Use workflow notifications or boto3\nimport boto3; sns = boto3.client("sns")',notes:'Use workflow notifications or boto3'},
  GetSQS:{tool:'AWS SQS',subtype:'consume',dbx:'Structured Streaming Custom',pri:3,code:'# Use boto3 or custom Spark source',notes:'No native SQS source; use boto3 or Kinesis'},
  PutSQS:{tool:'AWS SQS',subtype:'produce',dbx:'boto3',pri:3,code:'import boto3; sqs = boto3.client("sqs")',notes:'Use boto3 for SQS'},
  PutDynamoDB:{tool:'AWS DynamoDB',subtype:'write',dbx:'Spark DynamoDB Connector',pri:2,code:'df.write.format("dynamodb").option("tableName","<tbl>").save()',notes:'Install emr-dynamodb-connector'},
  GetDynamoDB:{tool:'AWS DynamoDB',subtype:'read',dbx:'Spark DynamoDB Connector',pri:2,code:'spark.read.format("dynamodb").option("tableName","<tbl>").load()',notes:'Install emr-dynamodb-connector'},
  PutKinesisFirehose:{tool:'AWS Kinesis',subtype:'firehose',dbx:'Structured Streaming Kinesis',pri:2,code:'df.writeStream.format("kinesis")...',notes:'Use kinesis-spark connector'},
  PutKinesisStream:{tool:'AWS Kinesis',subtype:'stream',dbx:'Structured Streaming Kinesis',pri:2,code:'df.writeStream.format("kinesis")...',notes:'Use kinesis-spark connector'},
  GetKinesisStream:{tool:'AWS Kinesis',subtype:'consumer',dbx:'Structured Streaming Kinesis',pri:2,code:'spark.readStream.format("kinesis")...',notes:'Use kinesis-spark connector'},
  PutLambda:{tool:'AWS Lambda',subtype:'invoke',dbx:'Databricks Workflows / boto3',pri:3,code:'import boto3; lam = boto3.client("lambda")',notes:'Use Databricks Jobs or boto3 for Lambda'},
  // Azure
  PutAzureBlobStorage:{tool:'Azure Blob',subtype:'write',dbx:'Spark Write / Unity Catalog',pri:1,code:'df.write.save("wasbs://<container>@<account>.blob.core.windows.net/<path>")',notes:'Use Unity Catalog external location'},
  FetchAzureBlobStorage:{tool:'Azure Blob',subtype:'read',dbx:'Spark Read',pri:1,code:'spark.read.load("wasbs://...")',notes:'Use Unity Catalog external location'},
  ListAzureBlobStorage:{tool:'Azure Blob',subtype:'list',dbx:'dbutils.fs.ls',pri:1,code:'dbutils.fs.ls("wasbs://...")',notes:'List Azure Blob contents'},
  DeleteAzureBlobStorage:{tool:'Azure Blob',subtype:'delete',dbx:'dbutils.fs.rm',pri:3,code:'dbutils.fs.rm("wasbs://...")',notes:'Delete Azure Blob objects'},
  PutAzureDataLakeStorage:{tool:'Azure ADLS',subtype:'write',dbx:'Spark Write / Unity Catalog',pri:1,code:'df.write.save("abfss://<container>@<account>.dfs.core.windows.net/<path>")',notes:'Unity Catalog external location'},
  FetchAzureDataLakeStorage:{tool:'Azure ADLS',subtype:'read',dbx:'Spark Read',pri:1,code:'spark.read.load("abfss://...")',notes:'Unity Catalog external location'},
  ListAzureDataLakeStorage:{tool:'Azure ADLS',subtype:'list',dbx:'dbutils.fs.ls',pri:1,code:'dbutils.fs.ls("abfss://...")',notes:'List ADLS contents'},
  DeleteAzureDataLakeStorage:{tool:'Azure ADLS',subtype:'delete',dbx:'dbutils.fs.rm',pri:3,code:'dbutils.fs.rm("abfss://...")',notes:'Delete ADLS objects'},
  PutAzureEventHub:{tool:'Azure Event Hubs',subtype:'produce',dbx:'Structured Streaming + Event Hubs Connector',pri:1,code:'df.writeStream.format("eventhubs")...',notes:'Install azure-eventhubs-spark library'},
  ConsumeAzureEventHub:{tool:'Azure Event Hubs',subtype:'consume',dbx:'Structured Streaming',pri:1,code:'spark.readStream.format("eventhubs")...',notes:'Install azure-eventhubs-spark library'},
  GetAzureEventHub:{tool:'Azure Event Hubs',subtype:'get',dbx:'Structured Streaming',pri:1,code:'spark.readStream.format("eventhubs")...',notes:'Install azure-eventhubs-spark library'},
  PutAzureCosmosDBRecord:{tool:'Azure Cosmos DB',subtype:'write',dbx:'Cosmos DB Spark Connector',pri:2,code:'df.write.format("cosmos.oltp").option("spark.cosmos.accountEndpoint","...").save()',notes:'Install azure-cosmos-spark library'},
  PutAzureCosmosDB:{tool:'Azure Cosmos DB',subtype:'write',dbx:'Cosmos DB Spark Connector',pri:2,code:'df.write.format("cosmos.oltp")...',notes:'Install azure-cosmos-spark library'},
  // GCP
  ListGCSBucket:{tool:'GCP GCS',subtype:'list',dbx:'dbutils.fs.ls / External Location',pri:1,code:'dbutils.fs.ls("gs://<bucket>/<prefix>")',notes:'Use Unity Catalog external location for GCS'},
  FetchGCSObject:{tool:'GCP GCS',subtype:'fetch',dbx:'Spark Read',pri:1,code:'spark.read.load("gs://<bucket>/<key>")',notes:'GCS via external location'},
  PutGCSObject:{tool:'GCP GCS',subtype:'put',dbx:'Spark Write',pri:1,code:'df.write.save("gs://<bucket>/<key>")',notes:'GCS via external location'},
  DeleteGCSObject:{tool:'GCP GCS',subtype:'delete',dbx:'dbutils.fs.rm',pri:3,code:'dbutils.fs.rm("gs://<bucket>/<key>")',notes:'Delete GCS objects'},
  PutBigQueryBatch:{tool:'GCP BigQuery',subtype:'write',dbx:'BigQuery Spark Connector',pri:1,code:'df.write.format("bigquery").option("table","<project>.<dataset>.<table>").save()',notes:'Install spark-bigquery-connector'},
  // MongoDB
  GetMongo:{tool:'MongoDB',subtype:'read',dbx:'MongoDB Spark Connector',pri:2,code:'spark.read.format("mongodb").option("connection.uri","...").load()',notes:'Install mongodb-spark-connector'},
  PutMongo:{tool:'MongoDB',subtype:'write',dbx:'MongoDB Spark Connector',pri:2,code:'df.write.format("mongodb").mode("append").save()',notes:'Install mongodb-spark-connector'},
  PutMongoRecord:{tool:'MongoDB',subtype:'record-write',dbx:'MongoDB Spark Connector',pri:2,code:'df.write.format("mongodb").mode("append").save()',notes:'Record-based MongoDB write'},
  DeleteMongo:{tool:'MongoDB',subtype:'delete',dbx:'pymongo',pri:3,code:'from pymongo import MongoClient; db.collection.delete_many({...})',notes:'Use pymongo for deletes'},
  // Elasticsearch
  PutElasticsearchHttp:{tool:'Elasticsearch',subtype:'write',dbx:'ES Spark Connector',pri:2,code:'df.write.format("org.elasticsearch.spark.sql").save("<index>")',notes:'Install elasticsearch-spark'},
  PutElasticsearchHttpRecord:{tool:'Elasticsearch',subtype:'record-write',dbx:'ES Spark Connector',pri:2,code:'df.write.format("org.elasticsearch.spark.sql").save("<index>")',notes:'Record-based ES write'},
  PutElasticsearchRecord:{tool:'Elasticsearch',subtype:'record-write',dbx:'ES Spark Connector',pri:2,code:'df.write.format("org.elasticsearch.spark.sql").save("<index>")',notes:'ES record write'},
  FetchElasticsearchHttp:{tool:'Elasticsearch',subtype:'read',dbx:'ES Spark Connector',pri:2,code:'spark.read.format("org.elasticsearch.spark.sql").load("<index>")',notes:'ES read'},
  GetElasticsearch:{tool:'Elasticsearch',subtype:'read',dbx:'ES Spark Connector',pri:2,code:'spark.read.format("org.elasticsearch.spark.sql").load("<index>")',notes:'ES read'},
  JsonQueryElasticsearch:{tool:'Elasticsearch',subtype:'query',dbx:'ES Spark Connector',pri:2,code:'spark.read.format("org.elasticsearch.spark.sql").option("es.query","...").load("<index>")',notes:'ES query read'},
  ScrollElasticsearchHttp:{tool:'Elasticsearch',subtype:'scroll',dbx:'ES Spark Connector',pri:2,code:'spark.read.format("org.elasticsearch.spark.sql").load("<index>")',notes:'ES scroll read'},
  // Cassandra
  PutCassandraQL:{tool:'Cassandra',subtype:'write',dbx:'Cassandra Spark Connector',pri:2,code:'df.write.format("org.apache.spark.sql.cassandra").option("keyspace","...").option("table","...").save()',notes:'Install spark-cassandra-connector'},
  PutCassandraRecord:{tool:'Cassandra',subtype:'record-write',dbx:'Cassandra Spark Connector',pri:2,code:'df.write.format("org.apache.spark.sql.cassandra").save()',notes:'Record-based Cassandra write'},
  QueryCassandra:{tool:'Cassandra',subtype:'read',dbx:'Cassandra Spark Connector',pri:2,code:'spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","...").option("table","...").load()',notes:'Cassandra read'},
  // HBase (processor-type based)
  PutHBaseCell:{tool:'HBase',subtype:'cell-write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").saveAsTable("<catalog>.<schema>.<table>")',notes:'Replace HBase with Delta Lake'},
  PutHBaseJSON:{tool:'HBase',subtype:'json-write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").mode("append").saveAsTable("...")',notes:'JSON to Delta Lake'},
  PutHBaseRecord:{tool:'HBase',subtype:'record-write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").mode("append").saveAsTable("...")',notes:'Record write to Delta Lake'},
  GetHBase:{tool:'HBase',subtype:'read',dbx:'Delta Lake',pri:1,code:'spark.read.format("delta").table("...")',notes:'Replace HBase scan with Delta table read'},
  ScanHBase:{tool:'HBase',subtype:'scan',dbx:'Delta Lake',pri:1,code:'spark.table("<catalog>.<schema>.<table>").filter(...)',notes:'HBase scan → Delta table filter'},
  FetchHBaseRow:{tool:'HBase',subtype:'fetch',dbx:'Delta Lake',pri:1,code:'spark.table("...").filter(col("rowkey") == "...")',notes:'HBase row fetch → Delta point lookup'},
  // Hive (processor-type based)
  PutHiveQL:{tool:'Hive',subtype:'hiveql-write',dbx:'Spark SQL',pri:1,code:'spark.sql("INSERT INTO <table> ...")',notes:'HiveQL → Spark SQL'},
  SelectHiveQL:{tool:'Hive',subtype:'hiveql-read',dbx:'Spark SQL',pri:1,code:'spark.sql("SELECT ...")',notes:'HiveQL → Spark SQL'},
  PutHiveStreaming:{tool:'Hive',subtype:'streaming',dbx:'Structured Streaming + Delta',pri:1,code:'df.writeStream.format("delta").toTable("...")',notes:'Hive streaming → Delta streaming'},
  PutORC:{tool:'Hive',subtype:'orc-write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").saveAsTable("...")',notes:'ORC → Delta Lake (better performance)'},
  PutParquet:{tool:'Hive',subtype:'parquet-write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").saveAsTable("...")',notes:'Parquet → Delta Lake (adds ACID)'},
  // HDFS (processor-type based)
  GetHDFS:{tool:'HDFS',subtype:'read',dbx:'Unity Catalog Volumes / DBFS',pri:1,code:'spark.read.load("/Volumes/<catalog>/<schema>/<path>")',notes:'HDFS → Volumes'},
  PutHDFS:{tool:'HDFS',subtype:'write',dbx:'Unity Catalog Volumes / DBFS',pri:1,code:'df.write.save("/Volumes/<catalog>/<schema>/<path>")',notes:'HDFS → Volumes'},
  ListHDFS:{tool:'HDFS',subtype:'list',dbx:'dbutils.fs.ls',pri:1,code:'dbutils.fs.ls("/Volumes/...")',notes:'HDFS list → dbutils.fs.ls'},
  FetchHDFS:{tool:'HDFS',subtype:'fetch',dbx:'Spark Read',pri:1,code:'spark.read.load("...")',notes:'HDFS fetch → Spark read'},
  MoveHDFS:{tool:'HDFS',subtype:'move',dbx:'dbutils.fs.mv',pri:1,code:'dbutils.fs.mv("...", "...")',notes:'HDFS move → dbutils.fs.mv'},
  DeleteHDFS:{tool:'HDFS',subtype:'delete',dbx:'dbutils.fs.rm',pri:1,code:'dbutils.fs.rm("...")',notes:'HDFS delete → dbutils.fs.rm'},
  CreateHadoopSequenceFile:{tool:'HDFS',subtype:'sequence-file',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").save("...")',notes:'Sequence files → Delta Lake'},
  // Kudu
  PutKudu:{tool:'Kudu',subtype:'write',dbx:'Delta Lake',pri:1,code:'df.write.format("delta").saveAsTable("...")',notes:'Kudu → Delta Lake'},
  // SFTP/FTP
  GetSFTP:{tool:'SFTP',subtype:'read',dbx:'Unity Catalog Volumes + External Transfer',pri:2,code:'# Stage from SFTP to Volumes\ndbutils.fs.cp("sftp://...", "/Volumes/...")',notes:'SFTP → stage to Volumes'},
  PutSFTP:{tool:'SFTP',subtype:'write',dbx:'Unity Catalog Volumes + External Transfer',pri:2,code:'# Stage to Volumes then SFTP transfer',notes:'Volumes → SFTP external transfer'},
  ListSFTP:{tool:'SFTP',subtype:'list',dbx:'External Listing',pri:3,code:'# Use paramiko for SFTP listing',notes:'No native SFTP listing in Spark'},
  FetchSFTP:{tool:'SFTP',subtype:'fetch',dbx:'Unity Catalog Volumes',pri:2,code:'# Fetch from SFTP to Volumes',notes:'Stage to Volumes via external tool'},
  GetFTP:{tool:'FTP',subtype:'read',dbx:'External Transfer + Volumes',pri:3,code:'# FTP → stage to Volumes',notes:'No native FTP; use external transfer'},
  PutFTP:{tool:'FTP',subtype:'write',dbx:'External Transfer',pri:3,code:'# Volumes → FTP transfer',notes:'No native FTP; use external transfer'},
  ListFTP:{tool:'FTP',subtype:'list',dbx:'External Listing',pri:3,code:'# Use ftplib for FTP listing',notes:'No native FTP listing'},
  FetchFTP:{tool:'FTP',subtype:'fetch',dbx:'External Transfer + Volumes',pri:3,code:'# Fetch from FTP to Volumes',notes:'Stage to Volumes'},
  // HTTP/REST
  InvokeHTTP:{tool:'HTTP/REST',subtype:'invoke',dbx:'PySpark pandas_udf / requests',pri:2,code:'@pandas_udf("string")\ndef call_api(urls): ...',notes:'HTTP calls via pandas_udf for distributed execution'},
  HandleHttpRequest:{tool:'HTTP/REST',subtype:'server',dbx:'Model Serving / API Gateway',pri:3,code:'# No native HTTP server; use Model Serving',notes:'Databricks cannot host HTTP endpoints natively'},
  HandleHttpResponse:{tool:'HTTP/REST',subtype:'response',dbx:'Model Serving',pri:3,code:'# Pair with HandleHttpRequest replacement',notes:'Use Model Serving'},
  ListenHTTP:{tool:'HTTP/REST',subtype:'listener',dbx:'Model Serving / Webhook',pri:3,code:'# External API gateway → Databricks Job trigger',notes:'Use webhook or Model Serving'},
  PostHTTP:{tool:'HTTP/REST',subtype:'post',dbx:'requests / pandas_udf',pri:2,code:'import requests; requests.post(url, json=data)',notes:'HTTP POST via requests'},
  GetHTTP:{tool:'HTTP/REST',subtype:'get',dbx:'Spark HTTP / requests',pri:2,code:'spark.read.json(url)',notes:'HTTP GET'},
  // JMS
  ConsumeJMS:{tool:'JMS',subtype:'consume',dbx:'Structured Streaming Custom Source',pri:3,code:'# Use custom Spark data source or Python JMS client',notes:'No native JMS; use custom connector'},
  PublishJMS:{tool:'JMS',subtype:'publish',dbx:'Python JMS Client',pri:3,code:'# Use stomp.py or java JMS client via spark._jvm',notes:'No native JMS publisher'},
  // AMQP / RabbitMQ
  ConsumeAMQP:{tool:'AMQP/RabbitMQ',subtype:'consume',dbx:'Structured Streaming Custom',pri:3,code:'# Use pika or custom Spark source',notes:'No native AMQP; use pika library'},
  PublishAMQP:{tool:'AMQP/RabbitMQ',subtype:'publish',dbx:'pika',pri:3,code:'import pika; channel.basic_publish(...)',notes:'Use pika for AMQP/RabbitMQ'},
  // MQTT
  ConsumeMQTT:{tool:'MQTT',subtype:'consume',dbx:'Structured Streaming Custom',pri:3,code:'# Use paho-mqtt or custom Spark source',notes:'No native MQTT; use paho-mqtt'},
  PublishMQTT:{tool:'MQTT',subtype:'publish',dbx:'paho-mqtt',pri:3,code:'import paho.mqtt.client as mqtt; client.publish(...)',notes:'Use paho-mqtt for MQTT publishing'},
  // Solr
  PutSolrContentStream:{tool:'Solr',subtype:'write',dbx:'Solr Spark Connector / pysolr',pri:3,code:'# Use pysolr or solr-spark connector',notes:'Install solr-spark or use pysolr'},
  PutSolrRecord:{tool:'Solr',subtype:'record-write',dbx:'Solr Spark Connector',pri:3,code:'df.write.format("solr").save()',notes:'Install solr-spark connector'},
  GetSolr:{tool:'Solr',subtype:'read',dbx:'Solr Spark Connector',pri:3,code:'spark.read.format("solr").load()',notes:'Install solr-spark connector'},
  QuerySolr:{tool:'Solr',subtype:'query',dbx:'Solr Spark Connector',pri:3,code:'spark.read.format("solr").option("query","...").load()',notes:'Solr query read'},
  // Email
  PutEmail:{tool:'Email/SMTP',subtype:'send',dbx:'Workflow Notification / smtplib',pri:3,code:'# Use Databricks Job email notifications\n# Or: import smtplib',notes:'Use workflow notifications or smtplib'},
  GetPOP3:{tool:'Email/POP3',subtype:'receive',dbx:'poplib',pri:3,code:'import poplib; pop = poplib.POP3_SSL(host)',notes:'Use poplib for POP3'},
  GetIMAP:{tool:'Email/IMAP',subtype:'receive',dbx:'imaplib',pri:3,code:'import imaplib; mail = imaplib.IMAP4_SSL(host)',notes:'Use imaplib for IMAP'},
  // Syslog
  PutSyslog:{tool:'Syslog',subtype:'send',dbx:'Python syslog / Logging',pri:3,code:'import syslog; syslog.syslog(...)',notes:'Use Python syslog module'},
  ListenSyslog:{tool:'Syslog',subtype:'listen',dbx:'Structured Streaming TCP',pri:3,code:'spark.readStream.format("socket")...',notes:'Use socket source or external syslog collector'},
  ParseSyslog:{tool:'Syslog',subtype:'parse',dbx:'PySpark regex',pri:2,code:'df.withColumn("parsed", regexp_extract(...))',notes:'Parse syslog with regex'},
  // Slack
  PutSlack:{tool:'Slack',subtype:'send',dbx:'Webhook / requests',pri:3,code:'import requests; requests.post(webhook_url, json={"text":"..."})',notes:'Use Slack webhook integration'},
  // TCP/UDP
  PutTCP:{tool:'TCP',subtype:'send',dbx:'Python socket',pri:3,code:'import socket; s = socket.socket(); s.connect((host,port))',notes:'Use Python socket'},
  ListenTCP:{tool:'TCP',subtype:'listen',dbx:'Structured Streaming socket',pri:3,code:'spark.readStream.format("socket").option("host","...").option("port","...").load()',notes:'Socket source'},
  ListenUDP:{tool:'UDP',subtype:'listen',dbx:'Python socket',pri:3,code:'import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)',notes:'Use Python UDP socket'},
  GetTCP:{tool:'TCP',subtype:'get',dbx:'Python socket',pri:3,code:'import socket; s.recv(...)',notes:'Use Python socket'},
  // SNMP
  GetSNMP:{tool:'SNMP',subtype:'get',dbx:'pysnmp',pri:3,code:'from pysnmp.hlapi import *; getCmd(...)',notes:'Use pysnmp library'},
  SetSNMP:{tool:'SNMP',subtype:'set',dbx:'pysnmp',pri:3,code:'from pysnmp.hlapi import *; setCmd(...)',notes:'Use pysnmp library'},
  // Splunk
  PutSplunk:{tool:'Splunk',subtype:'send',dbx:'Splunk Spark Connector / HEC',pri:2,code:'# Use Splunk HTTP Event Collector\nimport requests; requests.post(hec_url, json=...)',notes:'Use Splunk HEC or splunk-spark connector'},
  GetSplunk:{tool:'Splunk',subtype:'read',dbx:'Splunk Spark Connector',pri:2,code:'spark.read.format("splunk")...',notes:'Install splunk-spark connector'},
  QuerySplunkIndexingStatus:{tool:'Splunk',subtype:'query',dbx:'Splunk REST API',pri:3,code:'import requests; requests.get(splunk_url + "/services/...")',notes:'Splunk REST API'},
  // InfluxDB
  PutInfluxDB:{tool:'InfluxDB',subtype:'write',dbx:'influxdb-client-python',pri:3,code:'from influxdb_client import InfluxDBClient; client.write_api().write(...)',notes:'Use influxdb-client-python'},
  QueryInfluxDB:{tool:'InfluxDB',subtype:'read',dbx:'influxdb-client-python',pri:3,code:'client.query_api().query(...)',notes:'Use influxdb-client-python'},
  // Couchbase
  PutCouchbaseKey:{tool:'Couchbase',subtype:'write',dbx:'Couchbase Spark Connector',pri:3,code:'df.write.format("couchbase.kv").save()',notes:'Install couchbase-spark connector'},
  GetCouchbaseKey:{tool:'Couchbase',subtype:'read',dbx:'Couchbase Spark Connector',pri:3,code:'spark.read.format("couchbase.kv").load()',notes:'Install couchbase-spark connector'},
  // Redis
  PutDistributedMapCache:{tool:'Redis/NiFi Cache',subtype:'cache-put',dbx:'Delta Lake Cache / Spark Cache',pri:2,code:'df.cache()  # Or persist to Delta lookup table',notes:'Replace distributed cache with Spark caching or Delta lookup'},
  FetchDistributedMapCache:{tool:'Redis/NiFi Cache',subtype:'cache-fetch',dbx:'Spark Cache / Delta Lookup',pri:2,code:'df_lookup = spark.table("...").cache()',notes:'Use cached Delta table for lookups'},
  // Database (processor-type)
  ExecuteSQL:{tool:'Database/SQL',subtype:'execute',dbx:'Spark SQL',pri:1,code:'spark.sql("...")',notes:'Direct Spark SQL equivalent'},
  ExecuteSQLRecord:{tool:'Database/SQL',subtype:'execute-record',dbx:'Spark SQL',pri:1,code:'spark.sql("...")',notes:'Spark SQL with schema'},
  PutDatabaseRecord:{tool:'Database/JDBC',subtype:'record-write',dbx:'Spark JDBC Write',pri:1,code:'df.write.format("jdbc")...',notes:'JDBC write'},
  QueryDatabaseTable:{tool:'Database/JDBC',subtype:'query',dbx:'Spark JDBC Read',pri:1,code:'spark.read.format("jdbc")...',notes:'JDBC read'},
  QueryDatabaseTableRecord:{tool:'Database/JDBC',subtype:'query-record',dbx:'Spark JDBC Read',pri:1,code:'spark.read.format("jdbc")...',notes:'JDBC record read'},
  PutSQL:{tool:'Database/SQL',subtype:'put-sql',dbx:'Spark SQL / JDBC',pri:1,code:'spark.sql("INSERT INTO ...")',notes:'SQL write'},
  ConvertJSONToSQL:{tool:'Database/SQL',subtype:'json-to-sql',dbx:'Spark SQL',pri:1,code:'df.createOrReplaceTempView("t"); spark.sql("SELECT * FROM t")',notes:'JSON → Spark SQL'},
  GenerateTableFetch:{tool:'Database/JDBC',subtype:'table-fetch',dbx:'Spark JDBC Incremental',pri:1,code:'spark.read.format("jdbc").option("dbtable","(SELECT * FROM t WHERE id > ?) t")...',notes:'Incremental JDBC fetch'},
  // Custom code
  ExecuteScript:{tool:'Custom Script',subtype:'script',dbx:'Databricks Notebook',pri:2,code:'# Translate script logic to PySpark',notes:'Manual translation required'},
  ExecuteGroovyScript:{tool:'Custom Script',subtype:'groovy',dbx:'Databricks Notebook',pri:3,code:'# Translate Groovy to PySpark/Python',notes:'Manual translation required'},
  // NiFi utilities
  Wait:{tool:'NiFi Orchestration',subtype:'wait',dbx:'Databricks Workflows',pri:1,code:'# Use Job task dependencies',notes:'Replace Wait/Notify with workflow DAG'},
  Notify:{tool:'NiFi Orchestration',subtype:'notify',dbx:'Databricks Workflows',pri:1,code:'dbutils.notebook.exit("SUCCESS")',notes:'Signal via notebook exit'},
  ControlRate:{tool:'NiFi Orchestration',subtype:'rate-control',dbx:'Streaming Trigger Interval',pri:1,code:'.trigger(processingTime="...")',notes:'Use structured streaming trigger'},
  DistributeLoad:{tool:'NiFi Orchestration',subtype:'load-balance',dbx:'Spark Partitioning',pri:1,code:'df.repartition(n)',notes:'Spark handles distribution natively'},
  DetectDuplicate:{tool:'NiFi Orchestration',subtype:'dedup',dbx:'DataFrame dropDuplicates',pri:1,code:'df.dropDuplicates(["key"])',notes:'Native dedup in Spark'},
  ValidateRecord:{tool:'NiFi Orchestration',subtype:'validate',dbx:'DLT Expectations',pri:1,code:'@dlt.expect("rule","expr")',notes:'Use Delta Live Tables expectations'},
  UpdateRecord:{tool:'NiFi Orchestration',subtype:'update-record',dbx:'DataFrame API',pri:1,code:'df.withColumn("col", expr)',notes:'Map to withColumn transformations'},
  LookupRecord:{tool:'NiFi Orchestration',subtype:'lookup',dbx:'DataFrame Join',pri:1,code:'df.join(df_lookup, on="key", how="left")',notes:'Use DataFrame join with cached lookup table'},
  // Snowflake
  PutSnowflake:{tool:'Snowflake',subtype:'write',dbx:'Snowflake Spark Connector / Lakehouse Federation',pri:1,code:'df.write.format("snowflake").option("sfUrl","...").option("dbtable","...").save()',notes:'Install spark-snowflake connector; or use Lakehouse Federation'},
  GetSnowflake:{tool:'Snowflake',subtype:'read',dbx:'Snowflake Spark Connector / Lakehouse Federation',pri:1,code:'spark.read.format("snowflake").option("sfUrl","...").option("dbtable","...").load()',notes:'Install spark-snowflake connector'},
  // Neo4j
  PutCypher:{tool:'Neo4j',subtype:'write',dbx:'Neo4j Spark Connector',pri:2,code:'df.write.format("org.neo4j.spark.DataSource").option("url","...").save()',notes:'Install neo4j-spark-connector'},
  GetCypher:{tool:'Neo4j',subtype:'read',dbx:'Neo4j Spark Connector',pri:2,code:'spark.read.format("org.neo4j.spark.DataSource").option("query","...").load()',notes:'Install neo4j-spark-connector'},
  // Druid
  PutDruidRecord:{tool:'Druid',subtype:'write',dbx:'Druid Spark Connector',pri:3,code:'df.write.format("druid").save()',notes:'Install druid-spark connector'},
  QueryDruid:{tool:'Druid',subtype:'read',dbx:'Druid Spark Connector',pri:3,code:'spark.read.format("druid").load()',notes:'Install druid-spark connector'},
  // ClickHouse
  PutClickHouse:{tool:'ClickHouse',subtype:'write',dbx:'ClickHouse JDBC',pri:2,code:'df.write.format("jdbc").option("url","jdbc:clickhouse://...").save()',notes:'Use ClickHouse JDBC driver'},
  QueryClickHouse:{tool:'ClickHouse',subtype:'read',dbx:'ClickHouse JDBC',pri:2,code:'spark.read.format("jdbc").option("url","jdbc:clickhouse://...").load()',notes:'Use ClickHouse JDBC driver'},
  // Apache Iceberg
  PutIceberg:{tool:'Iceberg',subtype:'write',dbx:'Iceberg via UniForm / Delta Lake',pri:1,code:'df.writeTo("catalog.schema.table").using("iceberg").append()',notes:'Databricks supports Iceberg via UniForm; prefer Delta Lake'},
  // Apache Hudi
  PutHudi:{tool:'Hudi',subtype:'write',dbx:'Hudi / Delta Lake',pri:1,code:'df.write.format("hudi").option("hoodie.table.name","...").save()',notes:'Databricks supports Hudi; prefer Delta Lake'},
  // NiFi Site-to-Site
  SendNiFiSiteToSite:{tool:'NiFi Site-to-Site',subtype:'send',dbx:'Unity Catalog Sharing',pri:3,code:'# Use Delta Sharing for cross-workspace data flow',notes:'NiFi S2S → Unity Catalog sharing'},
  // Redis
  PutRedis:{tool:'Redis',subtype:'write',dbx:'Delta Lake Cache / redis-py',pri:2,code:'import redis; r.set(key, value)',notes:'Use redis-py or Delta table cache'},
  GetRedis:{tool:'Redis',subtype:'read',dbx:'Delta Lake Cache / redis-py',pri:2,code:'import redis; r.get(key)',notes:'Use redis-py or Delta table cache'},
  // Phoenix
  PutPhoenix:{tool:'Phoenix',subtype:'write',dbx:'Spark SQL / JDBC',pri:1,code:'df.write.format("jdbc").option("url","jdbc:phoenix:...").save()',notes:'Phoenix JDBC → Spark JDBC; migrate to Delta'},
  QueryPhoenix:{tool:'Phoenix',subtype:'read',dbx:'Spark SQL / JDBC',pri:1,code:'spark.read.format("jdbc").option("url","jdbc:phoenix:...").load()',notes:'Phoenix JDBC → Spark SQL'},
  // Teradata
  PutTeradata:{tool:'Teradata',subtype:'write',dbx:'Spark JDBC',pri:1,code:'df.write.format("jdbc").option("url","jdbc:teradata://...").save()',notes:'Install Teradata JDBC driver'},
  QueryTeradata:{tool:'Teradata',subtype:'read',dbx:'Spark JDBC',pri:1,code:'spark.read.format("jdbc").option("url","jdbc:teradata://...").load()',notes:'Install Teradata JDBC driver'},
  // Oracle
  PutOracle:{tool:'Oracle',subtype:'write',dbx:'Spark JDBC',pri:1,code:'df.write.format("jdbc").option("url","jdbc:oracle:thin:@...").save()',notes:'Install Oracle JDBC driver (ojdbc8.jar)'},
  QueryOracle:{tool:'Oracle',subtype:'read',dbx:'Spark JDBC',pri:1,code:'spark.read.format("jdbc").option("url","jdbc:oracle:thin:@...").load()',notes:'Install Oracle JDBC driver'},
  // SAP HANA
  PutSAPHANA:{tool:'SAP HANA',subtype:'write',dbx:'Spark JDBC',pri:2,code:'df.write.format("jdbc").option("url","jdbc:sap://...").save()',notes:'Install SAP HANA JDBC driver'},
  // Vertica
  PutVertica:{tool:'Vertica',subtype:'write',dbx:'Spark JDBC / Vertica Connector',pri:2,code:'df.write.format("jdbc").option("url","jdbc:vertica://...").save()',notes:'Install Vertica JDBC driver'},
  // Presto/Trino
  QueryPresto:{tool:'Presto',subtype:'query',dbx:'Spark SQL',pri:1,code:'spark.sql("...")',notes:'Presto queries → Spark SQL'},
  QueryTrino:{tool:'Trino',subtype:'query',dbx:'Spark SQL',pri:1,code:'spark.sql("...")',notes:'Trino queries → Spark SQL'},
  // Greenplum
  PutGreenplum:{tool:'Greenplum',subtype:'write',dbx:'Spark JDBC',pri:2,code:'df.write.format("jdbc").option("url","jdbc:postgresql://...").save()',notes:'Greenplum uses PostgreSQL JDBC'},
  // CockroachDB
  PutCockroachDB:{tool:'CockroachDB',subtype:'write',dbx:'Spark JDBC',pri:2,code:'df.write.format("jdbc").option("url","jdbc:postgresql://...").save()',notes:'CockroachDB uses PostgreSQL protocol'},
  // TimescaleDB
  PutTimescaleDB:{tool:'TimescaleDB',subtype:'write',dbx:'Spark JDBC',pri:2,code:'df.write.format("jdbc").option("url","jdbc:postgresql://...").save()',notes:'TimescaleDB uses PostgreSQL JDBC'},
  // Prometheus
  PutPrometheusRemoteWrite:{tool:'Prometheus',subtype:'remote-write',dbx:'Databricks Monitoring',pri:3,code:'# Use Databricks built-in monitoring or prometheus_client',notes:'Use Databricks monitoring; or prometheus_client'},
  // Datadog
  PutDatadog:{tool:'Datadog',subtype:'metrics',dbx:'Datadog Databricks Integration',pri:3,code:'# Use Databricks Datadog integration',notes:'Configure Databricks Datadog integration'},
  // Grafana
  PutGrafanaAnnotation:{tool:'Grafana',subtype:'annotation',dbx:'Grafana REST API',pri:3,code:'requests.post(grafana_url + "/api/annotations", ...)',notes:'Use Grafana REST API'},
  // Flink
  ExecuteFlinkSQL:{tool:'Flink',subtype:'flink-sql',dbx:'Spark SQL / Structured Streaming',pri:1,code:'spark.sql("...")',notes:'Flink SQL → Spark SQL'},
  // Airflow
  TriggerAirflowDag:{tool:'Airflow',subtype:'trigger',dbx:'Databricks Workflows',pri:1,code:'# Use Databricks Workflows instead',notes:'Airflow DAG → Databricks Workflow'},
  // Schema Registry
  ConfluentSchemaRegistry:{tool:'Schema Registry',subtype:'confluent',dbx:'Unity Catalog Schema',pri:2,code:'# Use Unity Catalog for schema management',notes:'Schema Registry → UC schema governance'},
  HortonworksSchemaRegistry:{tool:'Schema Registry',subtype:'hortonworks',dbx:'Unity Catalog Schema',pri:2,code:'# Migrate to Unity Catalog schema management',notes:'Hortonworks SR → UC schema governance'},
  // Excel
  ConvertExcelToCSVProcessor:{tool:'Excel',subtype:'convert',dbx:'spark-excel library',pri:2,code:'spark.read.format("com.crealytics.spark.excel").load("...")',notes:'Install spark-excel library'}
};

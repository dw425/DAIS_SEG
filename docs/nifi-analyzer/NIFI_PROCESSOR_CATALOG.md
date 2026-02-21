# Apache NiFi Comprehensive Component Catalog

> Exhaustive catalog of all Apache NiFi processors, expression language functions, controller services,
> relationship types, and property types — with Databricks/PySpark equivalents.
>
> Sources: [Apache NiFi Components](https://nifi.apache.org/components/),
> [NiFi.rocks Processor List](https://www.nifi.rocks/apache-nifi-processors/),
> [NiFi Expression Language Guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html),
> [NiFi Developer's Guide](https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html),
> [DeepWiki NiFi Standard Processors](https://deepwiki.com/apache/nifi/5-standard-processors),
> [NiFi GitHub - AWS Processors](https://github.com/apache/nifi/tree/master/nifi-nar-bundles/nifi-aws-bundle/nifi-aws-processors)

---

## Table of Contents

1. [Data Ingestion Processors](#1-data-ingestion-processors)
2. [Data Egress / Sink Processors](#2-data-egress--sink-processors)
3. [Transformation Processors](#3-transformation-processors)
4. [Routing & Mediation Processors](#4-routing--mediation-processors)
5. [Database Processors](#5-database-processors)
6. [JSON / XML / CSV Processing](#6-json--xml--csv-processing)
7. [HTTP / API Processors](#7-http--api-processors)
8. [Scripting Processors](#8-scripting-processors)
9. [System / Monitoring Processors](#9-system--monitoring-processors)
10. [Content Manipulation Processors](#10-content-manipulation-processors)
11. [AWS Processors](#11-aws-processors)
12. [Azure Processors](#12-azure-processors)
13. [GCP Processors](#13-gcp-processors)
14. [Kafka Processors](#14-kafka-processors)
15. [Hadoop / HDFS / Hive / HBase Processors](#15-hadoop--hdfs--hive--hbase-processors)
16. [Email Processors](#16-email-processors)
17. [FTP / SFTP Processors](#17-ftp--sftp-processors)
18. [Encryption / Security Processors](#18-encryption--security-processors)
19. [Record-Based Processors](#19-record-based-processors)
20. [State Management / Stateful Processors](#20-state-management--stateful-processors)
21. [NiFi Expression Language Functions](#21-nifi-expression-language-functions)
22. [Controller Service Types](#22-controller-service-types)
23. [Connection Relationship Types](#23-connection-relationship-types)
24. [NiFi Property Types & Databricks Equivalents](#24-nifi-property-types--databricks-equivalents)
25. [NiFi Flow Constructs (Non-Processor)](#25-nifi-flow-constructs-non-processor)
26. [Parameter Providers](#26-parameter-providers)
27. [Reporting Tasks](#27-reporting-tasks)
28. [Flow Registry Clients](#28-flow-registry-clients)
29. [Flow Analysis Rules](#29-flow-analysis-rules)

---

## 1. Data Ingestion Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.GetFile` | **GetFile** | Creates FlowFiles from files in a local directory | Auto Loader `spark.readStream.format("cloudFiles")` |
| `o.a.n.processors.standard.FetchFile` | **FetchFile** | Reads file contents from disk by path attribute | `spark.read.format(fmt).load(path)` from Volumes |
| `o.a.n.processors.standard.ListFile` | **ListFile** | Lists files in a local directory | `dbutils.fs.ls("/Volumes/...")` |
| `o.a.n.processors.standard.TailFile` | **TailFile** | Tails a file, reading new lines as they are added | Auto Loader streaming from Volumes |
| `o.a.n.processors.standard.GetHTTP` | **GetHTTP** | *(Deprecated)* Fetches a resource from an HTTP/HTTPS URL | `requests.get(url)` |
| `o.a.n.processors.standard.ListenHTTP` | **ListenHTTP** | Starts an HTTP server and listens for incoming requests | Databricks Model Serving endpoint |
| `o.a.n.processors.standard.ListenTCP` | **ListenTCP** | Listens for incoming TCP connections and reads data | `spark.readStream.format("socket")` |
| `o.a.n.processors.standard.ListenUDP` | **ListenUDP** | Listens for incoming UDP datagrams on a port | Python `socket.socket(AF_INET, SOCK_DGRAM)` |
| `o.a.n.processors.standard.ListenUDPRecord` | **ListenUDPRecord** | Listens for UDP and reads data using a Record Reader | Route through Kafka, read with Structured Streaming |
| `o.a.n.processors.standard.ListenTCPRecord` | **ListenTCPRecord** | Listens for TCP and reads data using a Record Reader | `spark.readStream.format("socket")` |
| `o.a.n.processors.standard.ListenSyslog` | **ListenSyslog** | Listens for Syslog messages on TCP or UDP | Socket source or external syslog collector |
| `o.a.n.processors.standard.ListenRELP` | **ListenRELP** | Listens for RELP (Reliable Event Logging) messages | HTTP endpoint or cloud log collection |
| `o.a.n.processors.standard.ListenSMTP` | **ListenSMTP** | Lightweight SMTP server on arbitrary port | Databricks App with `aiosmtpd` |
| `o.a.n.processors.standard.ListenFTP` | **ListenFTP** | Starts an FTP server to receive files | Stage to Volumes + Auto Loader |
| `o.a.n.processors.standard.ListenWebSocket` | **ListenWebSocket** | WebSocket server endpoint | Databricks App with `websockets` |
| `o.a.n.processors.standard.ListenOTLP` | **ListenOTLP** | OpenTelemetry Protocol listener | OTLP collector + Delta ingestion |
| `o.a.n.processors.standard.ListenGRPC` | **ListenGRPC** | gRPC server endpoint | Databricks App with `grpcio` |
| `o.a.n.processors.standard.GenerateFlowFile` | **GenerateFlowFile** | Creates FlowFiles with random/custom data (for testing) | `spark.range(n).toDF("id")` |
| `o.a.n.processors.standard.GenerateRecord` | **GenerateRecord** | Generates synthetic records for testing | `spark.range(n)` + schema definition |
| `o.a.n.processors.standard.GenerateTableFetch` | **GenerateTableFetch** | Generates paged SQL SELECT queries for incremental fetch | JDBC read with `partitionColumn` / pushdown |
| `o.a.n.processors.standard.GetTCP` | **GetTCP** | Connects over TCP and reads data | `spark.readStream.format("socket")` |
| `o.a.n.processors.standard.ConnectWebSocket` | **ConnectWebSocket** | WebSocket client connection | `websocket.create_connection(url)` |
| `o.a.n.processors.cdc.mysql.CaptureChangeMySQL` | **CaptureChangeMySQL** | Captures CDC events from MySQL binlog | Delta CDF or Debezium + Kafka |
| `o.a.n.processors.standard.GetSNMP` | **GetSNMP** | Retrieves SNMP data via GET request | `pysnmp.hlapi.getCmd()` |
| `o.a.n.processors.standard.ListenTrapSNMP` | **ListenTrapSNMP** | Listens for SNMP trap messages | Scheduled SNMP polling job |
| `o.a.n.processors.standard.ConsumeWindowsEventLog` | **ConsumeWindowsEventLog** | Receives Windows Event Log entries | Forward to cloud storage + Auto Loader |
| `o.a.n.processors.standard.ConsumeTwitter` | **ConsumeTwitter** | Consumes tweets from Twitter streaming API | `tweepy` library |
| `o.a.n.processors.standard.ConsumeSlack` | **ConsumeSlack** | Consumes messages from Slack | Slack SDK / webhook |
| `o.a.n.processors.standard.GetWorkdayReport` | **GetWorkdayReport** | Fetches reports from Workday | `requests` + Workday API |
| `o.a.n.processors.standard.GetHubSpot` | **GetHubSpot** | Fetches data from HubSpot | `requests` + HubSpot API |
| `o.a.n.processors.standard.GetShopify` | **GetShopify** | Fetches data from Shopify | `requests` + Shopify API |
| `o.a.n.processors.standard.GetZendesk` | **GetZendesk** | Fetches data from Zendesk | `requests` + Zendesk API |
| `o.a.n.processors.standard.QueryAirtableTable` | **QueryAirtableTable** | Queries an Airtable table | `requests` + Airtable API |

---

## 2. Data Egress / Sink Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.PutFile` | **PutFile** | Writes FlowFile contents to local file system | `df.write.format("delta").saveAsTable(...)` |
| `o.a.n.processors.standard.PutSQL` | **PutSQL** | Executes a SQL UPDATE or INSERT command | `df.write.format("jdbc").save()` |
| `o.a.n.processors.standard.PutDatabaseRecord` | **PutDatabaseRecord** | Writes records to a database using JDBC | `df.write.format("jdbc").save()` |
| `o.a.n.processors.standard.PutRecord` | **PutRecord** | Writes records using a configured Record Writer | `df.write.format("delta").saveAsTable(...)` |
| `o.a.n.processors.standard.PutEmail` | **PutEmail** | Sends email for each incoming FlowFile | Databricks workflow email notifications |
| `o.a.n.processors.standard.PutSyslog` | **PutSyslog** | Sends Syslog messages over TCP or UDP | Python `socket` UDP send |
| `o.a.n.processors.standard.PutTCP` | **PutTCP** | Sends data over a TCP connection | Python `socket.send()` |
| `o.a.n.processors.standard.PutUDP` | **PutUDP** | Sends data over UDP | Python `socket.sendto()` |
| `o.a.n.processors.standard.PutWebSocket` | **PutWebSocket** | Sends data over WebSocket | `websocket.send()` |
| `o.a.n.processors.standard.PutDistributedMapCache` | **PutDistributedMapCache** | Puts key/value into distributed cache | Delta lookup table write |
| `o.a.n.processors.standard.PostHTTP` | **PostHTTP** | *(Deprecated)* Posts data via HTTP | `requests.post()` with foreachBatch |
| `o.a.n.processors.standard.PublishSlack` | **PublishSlack** | Publishes a message to Slack | Slack webhook `requests.post()` |
| `o.a.n.processors.standard.PostSlack` | **PostSlack** | Posts a message to Slack | Slack webhook `requests.post()` |
| `o.a.n.processors.standard.ListenSlack` | **ListenSlack** | Listens for Slack events | Slack SDK / event subscription |
| `o.a.n.processors.standard.ConsumeSlack` | **ConsumeSlack** | Consumes Slack messages | Slack SDK / webhook |
| `o.a.n.processors.standard.PutZendeskTicket` | **PutZendeskTicket** | Creates a Zendesk ticket | `requests` + Zendesk API |

---

## 3. Transformation Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.UpdateAttribute` | **UpdateAttribute** | Sets or updates FlowFile attributes using EL | `df.withColumn(name, lit(value))` |
| `o.a.n.processors.standard.ReplaceText` | **ReplaceText** | Replaces text in FlowFile content using regex | `regexp_replace(col, pattern, repl)` |
| `o.a.n.processors.standard.ReplaceTextWithMapping` | **ReplaceTextWithMapping** | Replaces text using a mapping file | Iterative `regexp_replace()` |
| `o.a.n.processors.standard.JoltTransformJSON` | **JoltTransformJSON** | Applies Jolt transform specs to JSON | `from_json()` + schema mapping + `select()` |
| `o.a.n.processors.standard.JoltTransformRecord` | **JoltTransformRecord** | Applies Jolt specs to Record-oriented data | `withColumnRenamed()` / `select()` |
| `o.a.n.processors.standard.JSLTTransformJSON` | **JSLTTransformJSON** | Applies JSLT transformations to JSON | `from_json()` + PySpark transforms |
| `o.a.n.processors.standard.ConvertRecord` | **ConvertRecord** | Converts records between formats (CSV, JSON, Avro, etc.) | Spark handles format conversion natively via `df.write.format()` |
| `o.a.n.processors.standard.ConvertCharacterSet` | **ConvertCharacterSet** | Converts content character encoding | `spark.read.option("encoding", ...)` |
| `o.a.n.processors.standard.ConvertJSONToSQL` | **ConvertJSONToSQL** | Converts JSON to SQL INSERT/UPDATE statements | `createOrReplaceTempView()` + `spark.sql()` |
| `o.a.n.processors.standard.ConvertAvroToJSON` | **ConvertAvroToJSON** | Converts Avro binary to JSON | `from_avro()` function |
| `o.a.n.processors.standard.ConvertAvroToORC` | **ConvertAvroToORC** | Converts Avro records to ORC format | `spark.read.format("avro").write.format("orc")` |
| `o.a.n.processors.standard.ConvertAvroToParquet` | **ConvertAvroToParquet** | Converts Avro records to Parquet format | `spark.read.format("avro").write.format("parquet")` |
| `o.a.n.processors.standard.ConvertExcelToCSVProcessor` | **ConvertExcelToCSVProcessor** | Converts Excel worksheets to CSV | spark-excel library |
| `o.a.n.processors.standard.TransformXml` | **TransformXml** | Applies XSLT to XML content | spark-xml library + lxml |
| `o.a.n.processors.standard.FlattenJson` | **FlattenJson** | Flattens nested JSON to flat structure | PySpark struct navigation + `select("field.*")` |
| `o.a.n.processors.standard.EncodeContent` | **EncodeContent** | Encodes/decodes content (Base64, hex) | `base64()` / `unbase64()` |
| `o.a.n.processors.standard.Base64EncodeContent` | **Base64EncodeContent** | Encodes/decodes content to/from Base64 | `base64()` / `unbase64()` |
| `o.a.n.processors.standard.AttributesToJSON` | **AttributesToJSON** | Converts FlowFile attributes to JSON string | `to_json(struct("*"))` |
| `o.a.n.processors.standard.AttributesToCSV` | **AttributesToCSV** | Converts FlowFile attributes to CSV string | `concat_ws(",", *cols)` |
| `o.a.n.processors.standard.AttributeRollingWindow` | **AttributeRollingWindow** | Tracks rolling window metrics via EL | `groupBy(window(...)).agg(...)` |
| `o.a.n.processors.standard.UpdateRecord` | **UpdateRecord** | Updates specific record fields | `df.withColumn(field, expr(...))` |
| `o.a.n.processors.standard.LookupRecord` | **LookupRecord** | Enriches records via lookup service join | `df.join(lookup_df, key, "left")` |
| `o.a.n.processors.standard.LookupAttribute` | **LookupAttribute** | Enriches attributes via lookup service | `df.join(lookup_df, key, "left")` |
| `o.a.n.processors.standard.FilterAttribute` | **FilterAttribute** | Filters FlowFile attributes by name pattern | `df.select(matching_cols)` |
| `o.a.n.processors.standard.RemoveRecordField` | **RemoveRecordField** | Removes a field from records | `df.drop("field")` |
| `o.a.n.processors.standard.RenameRecordField` | **RenameRecordField** | Renames a record field | `df.withColumnRenamed(old, new)` |
| `o.a.n.processors.standard.ScriptedTransformRecord` | **ScriptedTransformRecord** | Transforms records using a user script | PySpark UDF |
| `o.a.n.processors.standard.ScriptedFilterRecord` | **ScriptedFilterRecord** | Filters records using a user script | PySpark UDF + `.filter()` |
| `o.a.n.processors.standard.ScriptedPartitionRecord` | **ScriptedPartitionRecord** | Partitions records using a user script | PySpark UDF + `.repartition()` |
| `o.a.n.processors.standard.ScriptedValidateRecord` | **ScriptedValidateRecord** | Validates records using a user script | PySpark UDF + DLT expectations |
| `o.a.n.processors.standard.GeoEnrichIP` | **GeoEnrichIP** | Enriches IP address with geolocation data | geoip2 UDF |
| `o.a.n.processors.standard.GeoEnrichIPRecord` | **GeoEnrichIPRecord** | Record-aware geolocation enrichment | geoip2 UDF on record fields |
| `o.a.n.processors.standard.GeohashRecord` | **GeohashRecord** | Adds geohash to records with lat/lon | Python geohash UDF |
| `o.a.n.processors.standard.ISPEnrichIP` | **ISPEnrichIP** | Enriches IP with ISP/ASN info | geoip2 ASN database UDF |
| `o.a.n.processors.standard.CalculateRecordStats` | **CalculateRecordStats** | Calculates statistics on record sets | `df.describe()` / `df.summary()` |
| `o.a.n.processors.standard.DeduplicateRecord` | **DeduplicateRecord** | Removes duplicate records | `df.dropDuplicates([keys])` |

---

## 4. Routing & Mediation Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.RouteOnAttribute` | **RouteOnAttribute** | Routes FlowFiles based on attribute conditions | `df.filter(condition)` |
| `o.a.n.processors.standard.RouteOnContent` | **RouteOnContent** | Routes based on content matching | `df.filter(col("value").rlike(pattern))` |
| `o.a.n.processors.standard.RouteText` | **RouteText** | Routes text-based FlowFiles by line matching | `df.filter(col("value").rlike(pattern))` |
| `o.a.n.processors.standard.RouteHL7` | **RouteHL7** | Routes HL7 messages by message type | `df.filter(col("value").contains("ADT"))` |
| `o.a.n.processors.standard.DistributeLoad` | **DistributeLoad** | Distributes FlowFiles across downstream paths | `df.repartition(n)` |
| `o.a.n.processors.standard.ControlRate` | **ControlRate** | Throttles FlowFile throughput | `.trigger(processingTime="10 seconds")` |
| `o.a.n.processors.standard.DetectDuplicate` | **DetectDuplicate** | Detects and routes duplicate FlowFiles | `df.dropDuplicates([key])` |
| `o.a.n.processors.standard.EnforceOrder` | **EnforceOrder** | Enforces ordering of FlowFiles in a group | `df.orderBy(col)` |
| `o.a.n.processors.standard.RetryFlowFile` | **RetryFlowFile** | Retries FlowFiles with configurable retry count | Python `try/except` with backoff |
| `o.a.n.processors.standard.Wait` | **Wait** | Pauses FlowFile until a signal is received | Databricks Workflow task dependencies |
| `o.a.n.processors.standard.Notify` | **Notify** | Sends a signal to release waiting FlowFiles | Delta table signal write |
| `o.a.n.processors.standard.SampleRecord` | **SampleRecord** | Samples a subset of records | `df.sample(fraction)` |
| `o.a.n.processors.standard.PartitionRecord` | **PartitionRecord** | Partitions records by field value | `df.repartition("field")` |

---

## 5. Database Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.ExecuteSQL` | **ExecuteSQL** | Executes SQL SELECT, returns Avro result | `spark.sql(query)` or JDBC read |
| `o.a.n.processors.standard.ExecuteSQLRecord` | **ExecuteSQLRecord** | Executes SQL with Record Writer output | `spark.sql(query)` |
| `o.a.n.processors.standard.QueryDatabaseTable` | **QueryDatabaseTable** | Generates SQL query for incremental table fetch | `spark.read.format("jdbc").load()` |
| `o.a.n.processors.standard.QueryDatabaseTableRecord` | **QueryDatabaseTableRecord** | Same as above with Record API | `spark.read.format("jdbc").load()` |
| `o.a.n.processors.standard.GenerateTableFetch` | **GenerateTableFetch** | Generates paged SQL for fetching table rows | JDBC with partition pushdown |
| `o.a.n.processors.standard.PutDatabaseRecord` | **PutDatabaseRecord** | Writes records to DB via JDBC | `df.write.format("jdbc").save()` |
| `o.a.n.processors.standard.PutSQL` | **PutSQL** | Executes SQL INSERT/UPDATE/DELETE | `df.write.format("jdbc").save()` |
| `o.a.n.processors.standard.ListDatabaseTables` | **ListDatabaseTables** | Lists tables in a database | `spark.sql("SHOW TABLES IN db")` |
| `o.a.n.processors.standard.UpdateDatabaseTable` | **UpdateDatabaseTable** | Updates database table schema | `spark.sql("ALTER TABLE ...")` |
| `o.a.n.processors.standard.QueryRecord` | **QueryRecord** | Runs SQL on FlowFile record content | `createTempView()` + `spark.sql()` |
| `o.a.n.processors.cassandra.PutCassandraQL` | **PutCassandraQL** | Executes CQL statements on Cassandra | spark-cassandra-connector write |
| `o.a.n.processors.cassandra.PutCassandraRecord` | **PutCassandraRecord** | Record-aware Cassandra write | spark-cassandra-connector write |
| `o.a.n.processors.cassandra.QueryCassandra` | **QueryCassandra** | Queries Cassandra tables | spark-cassandra-connector read |
| `o.a.n.processors.couchbase.GetCouchbaseKey` | **GetCouchbaseKey** | Gets document from Couchbase by key | couchbase-spark-connector read |
| `o.a.n.processors.couchbase.PutCouchbaseKey` | **PutCouchbaseKey** | Puts document to Couchbase by key | couchbase-spark-connector write |
| `o.a.n.processors.mongodb.GetMongo` | **GetMongo** | Queries MongoDB collection | mongodb-spark-connector read |
| `o.a.n.processors.mongodb.GetMongoRecord` | **GetMongoRecord** | Record-based MongoDB query | pymongo + createDataFrame |
| `o.a.n.processors.mongodb.PutMongo` | **PutMongo** | Writes to MongoDB collection | mongodb-spark-connector write |
| `o.a.n.processors.mongodb.PutMongoRecord` | **PutMongoRecord** | Record-based MongoDB write | mongodb-spark-connector write |
| `o.a.n.processors.mongodb.PutMongoBulkOperations` | **PutMongoBulkOperations** | Bulk write operations to MongoDB | pymongo bulk_write |
| `o.a.n.processors.mongodb.DeleteMongo` | **DeleteMongo** | Deletes documents from MongoDB | pymongo delete_many |
| `o.a.n.processors.mongodb.RunMongoAggregation` | **RunMongoAggregation** | Runs MongoDB aggregation pipeline | pymongo aggregate |
| `o.a.n.processors.rethinkdb.GetRethinkDB` | **GetRethinkDB** | Reads from RethinkDB | rethinkdb driver |
| `o.a.n.processors.rethinkdb.PutRethinkDB` | **PutRethinkDB** | Writes to RethinkDB | rethinkdb driver |
| `o.a.n.processors.rethinkdb.DeleteRethinkDB` | **DeleteRethinkDB** | Deletes from RethinkDB | rethinkdb driver |
| `o.a.n.processors.elasticsearch.*` | **PutElasticsearch** | Writes to Elasticsearch | elasticsearch-spark write |
| `o.a.n.processors.elasticsearch.*` | **PutElasticsearchJson** | Writes JSON to Elasticsearch | elasticsearch-spark write |
| `o.a.n.processors.elasticsearch.*` | **PutElasticsearchRecord** | Record-aware Elasticsearch write | elasticsearch-spark write |
| `o.a.n.processors.elasticsearch.*` | **PutElasticsearchHttp** | HTTP-based Elasticsearch write | elasticsearch-spark write |
| `o.a.n.processors.elasticsearch.*` | **PutElasticsearchHttpRecord** | HTTP record-based ES write | elasticsearch-spark write |
| `o.a.n.processors.elasticsearch.*` | **GetElasticsearch** | Reads from Elasticsearch | elasticsearch-spark read |
| `o.a.n.processors.elasticsearch.*` | **FetchElasticsearch** | Fetches single ES document by ID | elasticsearch-py `get()` |
| `o.a.n.processors.elasticsearch.*` | **FetchElasticsearchHttp** | HTTP-based ES document fetch | elasticsearch-py |
| `o.a.n.processors.elasticsearch.*` | **JsonQueryElasticsearch** | JSON DSL query against ES | elasticsearch-spark read with query |
| `o.a.n.processors.elasticsearch.*` | **PaginatedJsonQueryElasticsearch** | Paginated JSON DSL ES query | elasticsearch-spark with scroll |
| `o.a.n.processors.elasticsearch.*` | **SearchElasticsearch** | Searches Elasticsearch | elasticsearch-spark read |
| `o.a.n.processors.elasticsearch.*` | **ConsumeElasticsearch** | Consumes from Elasticsearch | elasticsearch-spark streaming |
| `o.a.n.processors.elasticsearch.*` | **QueryElasticsearchHttp** | HTTP query against Elasticsearch | elasticsearch-py search |
| `o.a.n.processors.elasticsearch.*` | **ScrollElasticsearchHttp** | Scrolls through ES results | elasticsearch-py scroll |
| `o.a.n.processors.elasticsearch.*` | **DeleteByQueryElasticsearch** | Deletes ES documents by query | elasticsearch-py delete_by_query |
| `o.a.n.processors.elasticsearch.*` | **UpdateByQueryElasticsearch** | Updates ES documents by query | elasticsearch-py update_by_query |
| `o.a.n.processors.solr.PutSolrContentStream` | **PutSolrContentStream** | Writes to Apache Solr | pysolr / solr-spark |
| `o.a.n.processors.solr.PutSolrRecord` | **PutSolrRecord** | Record-based Solr write | pysolr |
| `o.a.n.processors.solr.QuerySolr` | **QuerySolr** | Queries Solr | pysolr search |
| `o.a.n.processors.solr.GetSolr` | **GetSolr** | Reads from Solr | pysolr |
| `o.a.n.processors.redis.*` | **PutRedisHashRecord** | Writes records to Redis hash | redis-py |
| `o.a.n.processors.kudu.PutKudu` | **PutKudu** | Writes to Apache Kudu | Delta Lake (replacement) |
| `o.a.n.processors.influxdb.PutInfluxDB` | **PutInfluxDB** | Writes to InfluxDB in line protocol | influxdb-client write |
| `o.a.n.processors.influxdb.ExecuteInfluxDBQuery` | **ExecuteInfluxDBQuery** | Queries InfluxDB with Flux | influxdb-client query |
| `o.a.n.processors.splunk.PutSplunk` | **PutSplunk** | Sends data to Splunk via TCP/UDP | Splunk HEC `requests.post()` |
| `o.a.n.processors.splunk.PutSplunkHTTP` | **PutSplunkHTTP** | Sends data to Splunk HEC over HTTP | Splunk HEC `requests.post()` |
| `o.a.n.processors.splunk.GetSplunk` | **GetSplunk** | Reads search results from Splunk | splunklib SDK |
| `o.a.n.processors.splunk.QuerySplunkIndexingStatus` | **QuerySplunkIndexingStatus** | Checks Splunk indexing acknowledgment | splunklib SDK |
| `o.a.n.processors.gridfs.FetchGridFS` | **FetchGridFS** | Reads files from MongoDB GridFS | pymongo gridfs |
| `o.a.n.processors.gridfs.PutGridFS` | **PutGridFS** | Writes files to GridFS | pymongo gridfs |
| `o.a.n.processors.gridfs.DeleteGridFS` | **DeleteGridFS** | Deletes files from GridFS | pymongo gridfs |

---

## 6. JSON / XML / CSV Processing

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.EvaluateJsonPath` | **EvaluateJsonPath** | Extracts values using JsonPath expressions | `get_json_object(col, "$.path")` |
| `o.a.n.processors.standard.SplitJson` | **SplitJson** | Splits a JSON array into individual FlowFiles | `explode(from_json(col, ArrayType))` |
| `o.a.n.processors.standard.FlattenJson` | **FlattenJson** | Flattens nested JSON to flat key/value | PySpark struct navigation |
| `o.a.n.processors.standard.ValidateJson` | **ValidateJson** | Validates JSON structure | `from_json()` + null check |
| `o.a.n.processors.standard.ConvertJSONToSQL` | **ConvertJSONToSQL** | Converts JSON to SQL statements | `createTempView()` + `spark.sql()` |
| `o.a.n.processors.standard.EvaluateXPath` | **EvaluateXPath** | Evaluates XPath expressions against XML | PySpark `xpath()` function |
| `o.a.n.processors.standard.EvaluateXQuery` | **EvaluateXQuery** | Evaluates XQuery expressions | lxml UDF |
| `o.a.n.processors.standard.SplitXml` | **SplitXml** | Splits XML document into sub-documents | spark-xml `rowTag` option |
| `o.a.n.processors.standard.TransformXml` | **TransformXml** | Applies XSLT transform to XML | spark-xml + lxml |
| `o.a.n.processors.standard.ValidateXml` | **ValidateXml** | Validates XML against XSD schema | lxml validation UDF |
| `o.a.n.processors.standard.ValidateCsv` | **ValidateCsv** | Validates CSV format | `spark.read.option("mode","PERMISSIVE").csv()` |
| `o.a.n.processors.standard.SplitText` | **SplitText** | Splits text by line count | `explode(split(col, "\n"))` |
| `o.a.n.processors.standard.SplitExcel` | **SplitExcel** | Splits Excel workbook by worksheet | spark-excel per-sheet read |
| `o.a.n.processors.standard.SplitAvro` | **SplitAvro** | Splits Avro file into individual records | Not needed -- Spark reads Avro as DataFrame |
| `o.a.n.processors.standard.SplitContent` | **SplitContent** | Splits content by byte boundary/delimiter | `split()` + `explode()` |
| `o.a.n.processors.standard.ExtractText` | **ExtractText** | Extracts text matching regex groups | `regexp_extract(col, pattern, group)` |
| `o.a.n.processors.standard.ExtractGrok` | **ExtractGrok** | Extracts using Grok patterns | `regexp_extract()` (translate Grok to regex) |
| `o.a.n.processors.standard.ExtractRecordSchema` | **ExtractRecordSchema** | Extracts schema from record content | `df.schema` |
| `o.a.n.processors.standard.CountText` | **CountText** | Counts lines, words, chars in text | `df.count()` / string length UDFs |
| `o.a.n.processors.standard.GetHTMLElement` | **GetHTMLElement** | Extracts HTML element values by CSS selector | BeautifulSoup UDF |
| `o.a.n.processors.standard.ModifyHTMLElement` | **ModifyHTMLElement** | Modifies HTML element values | BeautifulSoup UDF |
| `o.a.n.processors.standard.PutHTMLElement` | **PutHTMLElement** | Inserts new HTML element into DOM | BeautifulSoup UDF |
| `o.a.n.processors.standard.ParseSyslog` | **ParseSyslog** | Parses Syslog messages (RFC 3164/5424) | `regexp_extract()` for syslog |
| `o.a.n.processors.standard.ParseSyslog5424` | **ParseSyslog5424** | Parses RFC 5424 Syslog messages | `regexp_extract()` |
| `o.a.n.processors.standard.ParseCEF` | **ParseCEF** | Parses Common Event Format security logs | Regex-based UDF |
| `o.a.n.processors.standard.ParseEvtx` | **ParseEvtx** | Parses Windows Event Log XML | lxml UDF |
| `o.a.n.processors.standard.ParseNetflowv5` | **ParseNetflowv5** | Parses NetFlow v5 binary data | Binary parsing UDF |
| `o.a.n.processors.standard.ExtractHL7Attributes` | **ExtractHL7Attributes** | Extracts HL7 message fields | HL7 parsing UDF (hl7apy) |
| `o.a.n.processors.standard.ExtractCCDAAttributes` | **ExtractCCDAAttributes** | Extracts C-CDA clinical document fields | lxml XPath UDF |
| `o.a.n.processors.standard.ExtractTNEFAttachments` | **ExtractTNEFAttachments** | Extracts TNEF email attachments | tnefparse UDF |
| `o.a.n.processors.standard.ExtractEmailHeaders` | **ExtractEmailHeaders** | Extracts email RFC headers | Python `email` module UDF |
| `o.a.n.processors.standard.ExtractEmailAttachments` | **ExtractEmailAttachments** | Extracts email MIME attachments | Python `email` module UDF |

---

## 7. HTTP / API Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.InvokeHTTP` | **InvokeHTTP** | HTTP client for GET/POST/PUT/DELETE/PATCH | `requests` in pandas_udf |
| `o.a.n.processors.standard.HandleHttpRequest` | **HandleHttpRequest** | HTTP server that accepts incoming requests | Databricks Model Serving endpoint |
| `o.a.n.processors.standard.HandleHttpResponse` | **HandleHttpResponse** | Sends HTTP response to requestor | Databricks Model Serving endpoint |
| `o.a.n.processors.standard.ListenHTTP` | **ListenHTTP** | HTTP listener server | Databricks Model Serving / App |
| `o.a.n.processors.standard.PostHTTP` | **PostHTTP** | *(Deprecated)* HTTP POST sender | `requests.post()` |
| `o.a.n.processors.standard.GetHTTP` | **GetHTTP** | *(Deprecated)* HTTP GET fetcher | `requests.get()` |
| `o.a.n.processors.aws.wag.InvokeAWSGatewayApi` | **InvokeAWSGatewayApi** | AWS API Gateway client | `requests.post()` with AWS auth |
| `o.a.n.processors.grpc.InvokeGRPC` | **InvokeGRPC** | gRPC client for remote service calls | grpcio client |
| `o.a.n.processors.grpc.ListenGRPC` | **ListenGRPC** | gRPC server endpoint | Databricks App with grpcio |

---

## 8. Scripting Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.script.ExecuteScript` | **ExecuteScript** | Executes script in Groovy, Python, Jython, JavaScript, Ruby, Lua, Clojure | PySpark `pandas_udf` |
| `o.a.n.processors.groovyx.ExecuteGroovyScript` | **ExecuteGroovyScript** | Extended Groovy script execution | Manual port to PySpark (no Groovy) |
| `o.a.n.processors.standard.ExecuteStreamCommand` | **ExecuteStreamCommand** | Pipes FlowFile through external command | Python `subprocess.run()` |
| `o.a.n.processors.standard.ExecuteProcess` | **ExecuteProcess** | Runs an OS command | Python `subprocess.run()` |
| `o.a.n.processors.standard.InvokeScriptedProcessor` | **InvokeScriptedProcessor** | Invokes a script-defined Processor | PySpark UDF |
| `o.a.n.processors.standard.ScriptedTransformRecord` | **ScriptedTransformRecord** | Transforms records using script | PySpark UDF |
| `o.a.n.processors.standard.ScriptedFilterRecord` | **ScriptedFilterRecord** | Filters records using script | PySpark UDF + `.filter()` |
| `o.a.n.processors.standard.ScriptedPartitionRecord` | **ScriptedPartitionRecord** | Partitions records using script | PySpark UDF + `.repartition()` |
| `o.a.n.processors.standard.ScriptedValidateRecord` | **ScriptedValidateRecord** | Validates records using script | PySpark UDF + DLT expectations |
| `o.a.n.processors.spring.SpringContextProcessor` | **SpringContextProcessor** | Processes via Spring Framework context | Manual migration to Python |

---

## 9. System / Monitoring Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.LogAttribute` | **LogAttribute** | Logs FlowFile attributes at configured level | `display(df)` + `df.printSchema()` |
| `o.a.n.processors.standard.LogMessage` | **LogMessage** | Logs a custom message | `print(message)` |
| `o.a.n.processors.standard.MonitorActivity` | **MonitorActivity** | Monitors flow activity, alerts on inactivity | Databricks Workflows monitoring |
| `o.a.n.processors.standard.DebugFlow` | **DebugFlow** | Debug helper for testing flow behavior | `display(df)` + `printSchema()` |
| `o.a.n.processors.standard.UpdateCounter` | **UpdateCounter** | Increments a named counter | Spark accumulator |
| `o.a.n.processors.standard.UpdateGauge` | **UpdateGauge** | Updates a named gauge metric | Prometheus gauge / Spark metric |
| `o.a.n.processors.standard.DuplicateFlowFile` | **DuplicateFlowFile** | Creates N copies of a FlowFile (load testing) | `reduce(DataFrame.union, [df]*n)` |
| `o.a.n.processors.standard.IdentifyMimeType` | **IdentifyMimeType** | Detects MIME type of content | `mimetypes.guess_type()` UDF |

---

## 10. Content Manipulation Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.MergeContent` | **MergeContent** | Merges multiple FlowFiles into one | `df1.unionByName(df2)` |
| `o.a.n.processors.standard.MergeRecord` | **MergeRecord** | Merges multiple record-oriented FlowFiles | `df1.unionByName(df2)` |
| `o.a.n.processors.standard.SplitContent` | **SplitContent** | Splits content by byte boundary | `split()` + `explode()` |
| `o.a.n.processors.standard.CompressContent` | **CompressContent** | Compresses/decompresses (GZIP, BZIP2, XZ, Snappy, LZ4, ZSTD) | Delta Lake handles compression automatically |
| `o.a.n.processors.standard.ModifyCompression` | **ModifyCompression** | Modifies compression format | Delta Lake auto-compression |
| `o.a.n.processors.standard.ModifyBytes` | **ModifyBytes** | Discards byte ranges from content | `substring(col, start, len)` |
| `o.a.n.processors.standard.SegmentContent` | **SegmentContent** | Segments content into fixed-size pieces | `split()` + `explode()` |
| `o.a.n.processors.standard.UnpackContent` | **UnpackContent** | Unpacks archived content (tar, zip, FlowFileV3) | Spark auto-decompresses + Python `zipfile`/`tarfile` |
| `o.a.n.processors.standard.PackageFlowFile` | **PackageFlowFile** | Packages FlowFile into FlowFile-v3 format | Not needed in Databricks |
| `o.a.n.processors.standard.SplitPCAP` | **SplitPCAP** | Splits PCAP network capture files | scapy UDF |
| `o.a.n.processors.standard.ForkEnrichment` | **ForkEnrichment** | Forks FlowFile for enrichment pattern | DataFrame clone + join |
| `o.a.n.processors.standard.JoinEnrichment` | **JoinEnrichment** | Joins enriched FlowFile back | `df.join(enriched_df)` |
| `o.a.n.processors.standard.ForkRecord` | **ForkRecord** | Forks records into multiple FlowFiles | `explode(col(array_field))` |
| `o.a.n.processors.standard.SplitRecord` | **SplitRecord** | Splits records into separate FlowFiles | `explode(col(array_field))` |

---

## 11. AWS Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| **S3** | | | |
| `o.a.n.processors.aws.s3.ListS3` | **ListS3** | Lists objects in S3 bucket | `dbutils.fs.ls("s3://...")` |
| `o.a.n.processors.aws.s3.FetchS3Object` | **FetchS3Object** | Reads S3 object content | `spark.read.format(fmt).load("s3://...")` |
| `o.a.n.processors.aws.s3.PutS3Object` | **PutS3Object** | Writes to S3 bucket | `df.write.save("s3a://...")` |
| `o.a.n.processors.aws.s3.DeleteS3Object` | **DeleteS3Object** | Deletes S3 object | `boto3.client("s3").delete_object()` |
| `o.a.n.processors.aws.s3.TagS3Object` | **TagS3Object** | Tags S3 object | `boto3.client("s3").put_object_tagging()` |
| `o.a.n.processors.aws.s3.CopyS3Object` | **CopyS3Object** | Copies S3 object between buckets | `boto3.client("s3").copy_object()` |
| `o.a.n.processors.aws.s3.GetS3ObjectMetadata` | **GetS3ObjectMetadata** | Gets S3 object metadata without downloading | `boto3.client("s3").head_object()` |
| `o.a.n.processors.aws.s3.GetS3ObjectTags` | **GetS3ObjectTags** | Gets tags on S3 object | `boto3.client("s3").get_object_tagging()` |
| **SQS** | | | |
| `o.a.n.processors.aws.sqs.GetSQS` | **GetSQS** | Receives messages from SQS queue | `boto3.client("sqs").receive_message()` |
| `o.a.n.processors.aws.sqs.PutSQS` | **PutSQS** | Sends messages to SQS queue | `boto3.client("sqs").send_message()` |
| `o.a.n.processors.aws.sqs.DeleteSQS` | **DeleteSQS** | Deletes SQS messages | `boto3.client("sqs").delete_message()` |
| **SNS** | | | |
| `o.a.n.processors.aws.sns.PutSNS` | **PutSNS** | Publishes to SNS topic | `boto3.client("sns").publish()` |
| **Lambda** | | | |
| `o.a.n.processors.aws.lambda.PutLambda` | **PutLambda** | Invokes AWS Lambda function | `boto3.client("lambda").invoke()` |
| **DynamoDB** | | | |
| `o.a.n.processors.aws.dynamodb.GetDynamoDB` | **GetDynamoDB** | Gets item from DynamoDB | `spark.read.format("dynamodb")` |
| `o.a.n.processors.aws.dynamodb.PutDynamoDB` | **PutDynamoDB** | Puts item to DynamoDB | `df.write.format("dynamodb")` |
| `o.a.n.processors.aws.dynamodb.PutDynamoDBRecord` | **PutDynamoDBRecord** | Record-based DynamoDB write | `df.write.format("dynamodb")` |
| `o.a.n.processors.aws.dynamodb.DeleteDynamoDB` | **DeleteDynamoDB** | Deletes item from DynamoDB | `boto3 batch_writer().delete_item()` |
| **Kinesis** | | | |
| `o.a.n.processors.aws.kinesis.stream.PutKinesisStream` | **PutKinesisStream** | Writes to Kinesis Data Stream | `boto3.client("kinesis").put_records()` |
| `o.a.n.processors.aws.kinesis.stream.ConsumeKinesisStream` | **ConsumeKinesisStream** | Reads from Kinesis Data Stream | `spark.readStream.format("kinesis")` |
| `o.a.n.processors.aws.kinesis.firehose.PutKinesisFirehose` | **PutKinesisFirehose** | Writes to Kinesis Firehose | `df.writeStream.format("kinesis")` |
| **CloudWatch** | | | |
| `o.a.n.processors.aws.cloudwatch.PutCloudWatchMetric` | **PutCloudWatchMetric** | Publishes metrics to CloudWatch | `boto3.client("cloudwatch").put_metric_data()` |
| **API Gateway** | | | |
| `o.a.n.processors.aws.wag.InvokeAWSGatewayApi` | **InvokeAWSGatewayApi** | Invokes AWS API Gateway endpoint | `requests.post()` |
| **AI/ML** | | | |
| `o.a.n.processors.aws.*` | **StartAwsPollyJob** | Starts AWS Polly text-to-speech job | `boto3.client("polly")` |
| `o.a.n.processors.aws.*` | **GetAwsPollyJobStatus** | Gets Polly job status | `boto3.client("polly")` |
| `o.a.n.processors.aws.*` | **StartAwsTextractJob** | Starts AWS Textract OCR job | `boto3.client("textract")` |
| `o.a.n.processors.aws.*` | **GetAwsTextractJobStatus** | Gets Textract job status | `boto3.client("textract")` |
| `o.a.n.processors.aws.*` | **StartAwsTranscribeJob** | Starts AWS Transcribe speech-to-text | `boto3.client("transcribe")` |
| `o.a.n.processors.aws.*` | **GetAwsTranscribeJobStatus** | Gets Transcribe job status | `boto3.client("transcribe")` |
| `o.a.n.processors.aws.*` | **StartAwsTranslateJob** | Starts AWS Translate batch job | `boto3.client("translate")` |
| `o.a.n.processors.aws.*` | **GetAwsTranslateJobStatus** | Gets Translate job status | `boto3.client("translate")` |

---

## 12. Azure Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| **Blob Storage** | | | |
| `o.a.n.processors.azure.storage.ListAzureBlobStorage_v12` | **ListAzureBlobStorage_v12** | Lists blobs in Azure container | `dbutils.fs.ls("wasbs://...")` |
| `o.a.n.processors.azure.storage.FetchAzureBlobStorage_v12` | **FetchAzureBlobStorage_v12** | Reads Azure blob content | `spark.read.load("wasbs://...")` |
| `o.a.n.processors.azure.storage.PutAzureBlobStorage_v12` | **PutAzureBlobStorage_v12** | Writes to Azure blob | `df.write.save("wasbs://...")` |
| `o.a.n.processors.azure.storage.DeleteAzureBlobStorage_v12` | **DeleteAzureBlobStorage_v12** | Deletes Azure blob | `dbutils.fs.rm("wasbs://...")` |
| `o.a.n.processors.azure.storage.CopyAzureBlobStorage_v12` | **CopyAzureBlobStorage_v12** | Copies Azure blob | azure-storage-blob SDK |
| **ADLS Gen2** | | | |
| `o.a.n.processors.azure.storage.ListAzureDataLakeStorage` | **ListAzureDataLakeStorage** | Lists files in ADLS Gen2 | `dbutils.fs.ls("abfss://...")` |
| `o.a.n.processors.azure.storage.FetchAzureDataLakeStorage` | **FetchAzureDataLakeStorage** | Reads from ADLS Gen2 | `spark.read.load("abfss://...")` |
| `o.a.n.processors.azure.storage.PutAzureDataLakeStorage` | **PutAzureDataLakeStorage** | Writes to ADLS Gen2 | `df.write.save("abfss://...")` |
| `o.a.n.processors.azure.storage.DeleteAzureDataLakeStorage` | **DeleteAzureDataLakeStorage** | Deletes from ADLS Gen2 | `dbutils.fs.rm("abfss://...")` |
| `o.a.n.processors.azure.storage.MoveAzureDataLakeStorage` | **MoveAzureDataLakeStorage** | Moves/renames in ADLS Gen2 | `dbutils.fs.mv()` |
| **Event Hubs** | | | |
| `o.a.n.processors.azure.eventhub.ConsumeAzureEventHub` | **ConsumeAzureEventHub** | Receives from Azure Event Hubs | `spark.readStream.format("eventhubs")` |
| `o.a.n.processors.azure.eventhub.PutAzureEventHub` | **PutAzureEventHub** | Sends to Azure Event Hubs | `df.writeStream.format("eventhubs")` |
| `o.a.n.processors.azure.eventhub.GetAzureEventHub` | **GetAzureEventHub** | *(Legacy)* Receives from Event Hubs | `spark.readStream.format("eventhubs")` |
| **Queue Storage** | | | |
| `o.a.n.processors.azure.storage.queue.GetAzureQueueStorage_v12` | **GetAzureQueueStorage_v12** | Reads from Azure Queue Storage | azure-storage-queue SDK |
| `o.a.n.processors.azure.storage.queue.PutAzureQueueStorage_v12` | **PutAzureQueueStorage_v12** | Writes to Azure Queue Storage | azure-storage-queue SDK |
| **Cosmos DB** | | | |
| `o.a.n.processors.azure.cosmos.PutAzureCosmosDBRecord` | **PutAzureCosmosDBRecord** | Writes records to Cosmos DB | `df.write.format("cosmos.oltp")` |
| **Data Explorer (Kusto)** | | | |
| `o.a.n.processors.azure.data.explorer.PutAzureDataExplorer` | **PutAzureDataExplorer** | Ingests into Azure Data Explorer | Kusto Spark connector |
| `o.a.n.processors.azure.data.explorer.QueryAzureDataExplorer` | **QueryAzureDataExplorer** | Queries Azure Data Explorer | Kusto Spark connector |

---

## 13. GCP Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| **Cloud Storage** | | | |
| `o.a.n.processors.gcp.storage.ListGCSBucket` | **ListGCSBucket** | Lists objects in GCS bucket | `dbutils.fs.ls("gs://...")` |
| `o.a.n.processors.gcp.storage.FetchGCSObject` | **FetchGCSObject** | Reads GCS object | `spark.read.load("gs://...")` |
| `o.a.n.processors.gcp.storage.PutGCSObject` | **PutGCSObject** | Writes to GCS | `df.write.save("gs://...")` |
| `o.a.n.processors.gcp.storage.DeleteGCSObject` | **DeleteGCSObject** | Deletes GCS object | `dbutils.fs.rm("gs://...")` |
| **BigQuery** | | | |
| `o.a.n.processors.gcp.bigquery.PutBigQuery` | **PutBigQuery** | Writes to BigQuery (record-based) | `df.write.format("bigquery")` |
| `o.a.n.processors.gcp.bigquery.PutBigQueryBatch` | **PutBigQueryBatch** | Batch loads to BigQuery | `df.write.format("bigquery")` |
| `o.a.n.processors.gcp.bigquery.PutBigQueryStreaming` | **PutBigQueryStreaming** | Streaming writes to BigQuery | `df.write.format("bigquery").option("writeMethod","direct")` |
| **Pub/Sub** | | | |
| `o.a.n.processors.gcp.pubsub.ConsumeGCPubSub` | **ConsumeGCPubSub** | Subscribes to GCP Pub/Sub topic | google-cloud-pubsub SDK |
| `o.a.n.processors.gcp.pubsub.PublishGCPubSub` | **PublishGCPubSub** | Publishes to GCP Pub/Sub topic | google-cloud-pubsub SDK |
| **Vision AI** | | | |
| `o.a.n.processors.gcp.vision.*` | **StartGcpVisionAnnotateFilesOperation** | Starts GCP Vision AI batch annotation | google-cloud-vision SDK |
| `o.a.n.processors.gcp.vision.*` | **GetGcpVisionAnnotateFilesOperationStatus** | Gets Vision AI operation status | google-cloud-vision SDK |
| `o.a.n.processors.gcp.vision.*` | **StartGcpVisionAnnotateImagesOperation** | Starts image annotation | google-cloud-vision SDK |
| `o.a.n.processors.gcp.vision.*` | **GetGcpVisionAnnotateImagesOperationStatus** | Gets image annotation status | google-cloud-vision SDK |

---

## 14. Kafka Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| **Consumers** | | | |
| `o.a.n.processors.kafka.pubsub.ConsumeKafka` | **ConsumeKafka** | Consumes from Kafka (latest API) | `spark.readStream.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.ConsumeKafka_1_0` | **ConsumeKafka_1_0** | Consumes from Kafka 1.0 API | `spark.readStream.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.ConsumeKafka_2_0` | **ConsumeKafka_2_0** | Consumes from Kafka 2.0 API | `spark.readStream.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.ConsumeKafka_2_6` | **ConsumeKafka_2_6** | Consumes from Kafka 2.6 API | `spark.readStream.format("kafka")` |
| **Record Consumers** | | | |
| `o.a.n.processors.kafka.pubsub.ConsumeKafkaRecord_1_0` | **ConsumeKafkaRecord_1_0** | Kafka 1.0 consumer with Record Reader | `readStream.format("kafka")` + `from_json()` |
| `o.a.n.processors.kafka.pubsub.ConsumeKafkaRecord_2_0` | **ConsumeKafkaRecord_2_0** | Kafka 2.0 consumer with Record Reader | `readStream.format("kafka")` + `from_json()` |
| `o.a.n.processors.kafka.pubsub.ConsumeKafkaRecord_2_6` | **ConsumeKafkaRecord_2_6** | Kafka 2.6 consumer with Record Reader | `readStream.format("kafka")` + `from_json()` |
| **Producers** | | | |
| `o.a.n.processors.kafka.pubsub.PublishKafka` | **PublishKafka** | Publishes to Kafka (latest API) | `df.write.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.PublishKafka_1_0` | **PublishKafka_1_0** | Publishes to Kafka 1.0 API | `df.write.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.PublishKafka_2_0` | **PublishKafka_2_0** | Publishes to Kafka 2.0 API | `df.write.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.PublishKafka_2_6` | **PublishKafka_2_6** | Publishes to Kafka 2.6 API | `df.write.format("kafka")` |
| **Record Producers** | | | |
| `o.a.n.processors.kafka.pubsub.PublishKafkaRecord_1_0` | **PublishKafkaRecord_1_0** | Kafka 1.0 record producer | `to_json(struct(*))` + `write.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.PublishKafkaRecord_2_0` | **PublishKafkaRecord_2_0** | Kafka 2.0 record producer | `to_json(struct(*))` + `write.format("kafka")` |
| `o.a.n.processors.kafka.pubsub.PublishKafkaRecord_2_6` | **PublishKafkaRecord_2_6** | Kafka 2.6 record producer | `to_json(struct(*))` + `write.format("kafka")` |

> **Note:** NiFi 2.0+ consolidates to version-agnostic `ConsumeKafka` and `PublishKafka` using Kafka 3+ connector service.

---

## 15. Hadoop / HDFS / Hive / HBase Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| **HDFS** | | | |
| `o.a.n.processors.hadoop.GetHDFS` | **GetHDFS** | Reads file from HDFS | `spark.read.load("/Volumes/...")` |
| `o.a.n.processors.hadoop.FetchHDFS` | **FetchHDFS** | Fetches specific HDFS file by path | `spark.read.load(path)` |
| `o.a.n.processors.hadoop.ListHDFS` | **ListHDFS** | Lists files in HDFS directory | `dbutils.fs.ls(path)` |
| `o.a.n.processors.hadoop.PutHDFS` | **PutHDFS** | Writes FlowFile to HDFS | `df.write.save(path)` |
| `o.a.n.processors.hadoop.DeleteHDFS` | **DeleteHDFS** | Deletes file/directory from HDFS | `dbutils.fs.rm(path, recurse=True)` |
| `o.a.n.processors.hadoop.MoveHDFS` | **MoveHDFS** | Renames/moves files in HDFS | `dbutils.fs.mv(src, dst)` |
| `o.a.n.processors.hadoop.GetHDFSEvents` | **GetHDFSEvents** | Polls HDFS notification events | Auto Loader streaming |
| `o.a.n.processors.hadoop.GetHDFSFileInfo` | **GetHDFSFileInfo** | Gets HDFS file metadata | `dbutils.fs.ls()` as DataFrame |
| `o.a.n.processors.hadoop.GetHDFSSequenceFile` | **GetHDFSSequenceFile** | Reads Hadoop SequenceFile | `sparkContext.sequenceFile()` |
| `o.a.n.processors.hadoop.CreateHadoopSequenceFile` | **CreateHadoopSequenceFile** | Creates SequenceFile from FlowFile | Replace with Delta Lake |
| `o.a.n.processors.hadoop.FetchParquet` | **FetchParquet** | Reads Parquet file | `spark.read.format("parquet")` |
| `o.a.n.processors.hadoop.PutParquet` | **PutParquet** | Writes Parquet file | `df.write.format("delta")` (preferred) |
| `o.a.n.processors.hadoop.PutORC` | **PutORC** | Writes ORC file | `df.write.format("delta")` (preferred) |
| **Hive** | | | |
| `o.a.n.processors.hive.PutHiveQL` | **PutHiveQL** | Executes HiveQL DDL/DML | `spark.sql(hiveql)` |
| `o.a.n.processors.hive.SelectHiveQL` | **SelectHiveQL** | Executes HiveQL SELECT | `spark.sql(hiveql)` |
| `o.a.n.processors.hive.PutHiveStreaming` | **PutHiveStreaming** | Streams Avro data to Hive table | `df.writeStream.format("delta").toTable()` |
| `o.a.n.processors.hive.UpdateHiveTable` | **UpdateHiveTable** | Updates Hive table metadata | `spark.sql("ALTER TABLE ...")` |
| **HBase** | | | |
| `o.a.n.processors.hbase.GetHBase` | **GetHBase** | Scans HBase table for records | `spark.table(delta_table)` (replace HBase) |
| `o.a.n.processors.hbase.ScanHBase` | **ScanHBase** | Scans HBase with filter | `spark.table().filter()` |
| `o.a.n.processors.hbase.FetchHBaseRow` | **FetchHBaseRow** | Fetches single HBase row by key | `spark.table().filter(col("key")==val)` |
| `o.a.n.processors.hbase.PutHBaseCell` | **PutHBaseCell** | Writes single cell to HBase | Delta Lake append |
| `o.a.n.processors.hbase.PutHBaseJSON` | **PutHBaseJSON** | Writes JSON to HBase | Delta Lake append |
| `o.a.n.processors.hbase.PutHBaseRecord` | **PutHBaseRecord** | Writes records to HBase | Delta Lake append |
| `o.a.n.processors.hbase.DeleteHBaseCells` | **DeleteHBaseCells** | Deletes specific HBase cells | happybase / Delta DELETE |
| `o.a.n.processors.hbase.DeleteHBaseRow` | **DeleteHBaseRow** | Deletes HBase rows | happybase / Delta DELETE |

---

## 16. Email Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.ConsumeIMAP` | **ConsumeIMAP** | Consumes emails via IMAP protocol | Python `imaplib` |
| `o.a.n.processors.standard.ConsumePOP3` | **ConsumePOP3** | Consumes emails via POP3 protocol | Python `poplib` |
| `o.a.n.processors.standard.PutEmail` | **PutEmail** | Sends email via SMTP | Databricks workflow notifications |
| `o.a.n.processors.ews.ConsumeEWS` | **ConsumeEWS** | Consumes via Exchange Web Services | Microsoft Graph API |
| `o.a.n.processors.standard.ListenSMTP` | **ListenSMTP** | SMTP server listener | Databricks App with aiosmtpd |
| `o.a.n.processors.standard.ExtractEmailHeaders` | **ExtractEmailHeaders** | Extracts email header fields | Python `email` module UDF |
| `o.a.n.processors.standard.ExtractEmailAttachments` | **ExtractEmailAttachments** | Extracts MIME attachments from email | Python `email` module UDF |

---

## 17. FTP / SFTP Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.GetFTP` | **GetFTP** | Fetches files from FTP server | `ftplib` + Volumes staging |
| `o.a.n.processors.standard.GetSFTP` | **GetSFTP** | Fetches files from SFTP server | `paramiko` + Volumes staging |
| `o.a.n.processors.standard.PutFTP` | **PutFTP** | Sends files to FTP server | `ftplib.storbinary()` |
| `o.a.n.processors.standard.PutSFTP` | **PutSFTP** | Sends files to SFTP server | `paramiko.SFTPClient.put()` |
| `o.a.n.processors.standard.ListFTP` | **ListFTP** | Lists files on FTP server | `ftplib.nlst()` |
| `o.a.n.processors.standard.ListSFTP` | **ListSFTP** | Lists files on SFTP server | `paramiko.SFTPClient.listdir()` |
| `o.a.n.processors.standard.FetchFTP` | **FetchFTP** | Fetches specific FTP file by path | `ftplib.retrbinary()` |
| `o.a.n.processors.standard.FetchSFTP` | **FetchSFTP** | Fetches specific SFTP file by path | `paramiko.SFTPClient.get()` |
| `o.a.n.processors.standard.DeleteSFTP` | **DeleteSFTP** | Deletes file from SFTP server | `paramiko.SFTPClient.remove()` |
| `o.a.n.processors.standard.ListenFTP` | **ListenFTP** | Starts FTP server listener | Stage to Volumes + Auto Loader |
| **SMB** | | | |
| `o.a.n.processors.smb.GetSmbFile` | **GetSmbFile** | Reads from SMB/CIFS share | smbprotocol |
| `o.a.n.processors.smb.PutSmbFile` | **PutSmbFile** | Writes to SMB/CIFS share | smbprotocol |
| `o.a.n.processors.smb.ListSmb` | **ListSmb** | Lists files on SMB share | smbprotocol |
| `o.a.n.processors.smb.FetchSmb` | **FetchSmb** | Fetches file from SMB share | smbprotocol |

---

## 18. Encryption / Security Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.EncryptContent` | **EncryptContent** | Encrypts/decrypts content (AES, 3DES, etc.) | `aes_encrypt()` / `aes_decrypt()` |
| `o.a.n.processors.standard.EncryptContentPGP` | **EncryptContentPGP** | Encrypts with OpenPGP | python-gnupg UDF |
| `o.a.n.processors.standard.DecryptContentPGP` | **DecryptContentPGP** | Decrypts OpenPGP content | python-gnupg UDF |
| `o.a.n.processors.standard.EncryptContentAge` | **EncryptContentAge** | Encrypts with Age encryption | age library UDF |
| `o.a.n.processors.standard.DecryptContentAge` | **DecryptContentAge** | Decrypts Age-encrypted content | age library UDF |
| `o.a.n.processors.standard.SignContentPGP` | **SignContentPGP** | Signs content with PGP | python-gnupg UDF |
| `o.a.n.processors.standard.VerifyContentPGP` | **VerifyContentPGP** | Verifies PGP signature | python-gnupg UDF |
| `o.a.n.processors.standard.VerifyContentMAC` | **VerifyContentMAC** | Verifies MAC (HMAC) | Python `hmac` module |
| `o.a.n.processors.standard.CryptographicHashContent` | **CryptographicHashContent** | Computes SHA-256/512/MD5 hash of content | `sha2(col, 256)` / `md5(col)` |
| `o.a.n.processors.standard.CryptographicHashAttribute` | **CryptographicHashAttribute** | Computes hash of attribute value | `sha2(col, 256)` |
| `o.a.n.processors.standard.HashAttribute` | **HashAttribute** | Hashes multiple attribute key/value pairs | `sha2(concat_ws(...), 256)` |
| `o.a.n.processors.standard.HashContent` | **HashContent** | *(Deprecated)* Hashes content | `sha2()` |
| `o.a.n.processors.standard.FuzzyHashContent` | **FuzzyHashContent** | Computes fuzzy/locality-sensitive hash | ssdeep UDF |
| `o.a.n.processors.standard.CompareFuzzyHash` | **CompareFuzzyHash** | Compares fuzzy hashes for similarity | Jaccard/ssdeep similarity UDF |
| `o.a.n.processors.standard.ValidateXml` | **ValidateXml** | Validates XML against schema | lxml validation UDF |
| `o.a.n.processors.standard.ValidateJson` | **ValidateJson** | Validates JSON structure | `from_json()` + null check |
| `o.a.n.processors.standard.ValidateCsv` | **ValidateCsv** | Validates CSV format | Permissive mode + corrupt record check |
| `o.a.n.processors.standard.ValidateRecord` | **ValidateRecord** | Validates records against schema | DLT Expectations |

---

## 19. Record-Based Processors

| Full Class Name | Short Name | Description | Databricks/PySpark Equivalent |
|---|---|---|---|
| `o.a.n.processors.standard.ConvertRecord` | **ConvertRecord** | Converts between record formats | `df.write.format(target)` |
| `o.a.n.processors.standard.QueryRecord` | **QueryRecord** | SQL query against record content | `createTempView()` + `spark.sql()` |
| `o.a.n.processors.standard.UpdateRecord` | **UpdateRecord** | Updates record fields | `df.withColumn(field, expr)` |
| `o.a.n.processors.standard.LookupRecord` | **LookupRecord** | Enriches records via lookup | `df.join(lookup_df)` |
| `o.a.n.processors.standard.ValidateRecord` | **ValidateRecord** | Validates records | DLT Expectations |
| `o.a.n.processors.standard.PartitionRecord` | **PartitionRecord** | Partitions records by field | `df.repartition("field")` |
| `o.a.n.processors.standard.SplitRecord` | **SplitRecord** | Splits records into groups | `explode()` |
| `o.a.n.processors.standard.MergeRecord` | **MergeRecord** | Merges record FlowFiles | `df.unionByName()` |
| `o.a.n.processors.standard.ForkRecord` | **ForkRecord** | Forks nested array records | `explode(col(array))` |
| `o.a.n.processors.standard.SampleRecord` | **SampleRecord** | Random sample of records | `df.sample(fraction)` |
| `o.a.n.processors.standard.DeduplicateRecord` | **DeduplicateRecord** | Removes duplicate records | `df.dropDuplicates()` |
| `o.a.n.processors.standard.PutRecord` | **PutRecord** | Writes records via Record Writer | `df.write.format("delta")` |
| `o.a.n.processors.standard.GenerateRecord` | **GenerateRecord** | Generates test records | `spark.range(n)` |
| `o.a.n.processors.standard.RemoveRecordField` | **RemoveRecordField** | Removes field from records | `df.drop("field")` |
| `o.a.n.processors.standard.RenameRecordField` | **RenameRecordField** | Renames record field | `df.withColumnRenamed()` |
| `o.a.n.processors.standard.ExtractRecordSchema` | **ExtractRecordSchema** | Extracts schema from records | `df.schema.json()` |
| `o.a.n.processors.standard.CalculateRecordStats` | **CalculateRecordStats** | Calculates record statistics | `df.describe()` |
| `o.a.n.processors.standard.ScriptedTransformRecord` | **ScriptedTransformRecord** | Script-based record transform | PySpark UDF |
| `o.a.n.processors.standard.ScriptedFilterRecord` | **ScriptedFilterRecord** | Script-based record filter | PySpark UDF + filter |
| `o.a.n.processors.standard.ScriptedValidateRecord` | **ScriptedValidateRecord** | Script-based record validation | PySpark UDF + DLT expect |

---

## 20. State Management / Stateful Processors

These processors maintain state across invocations (e.g., tracking last-processed offset).

| Short Name | State Behavior | Databricks Equivalent |
|---|---|---|
| **QueryDatabaseTable** | Tracks max value of incremental column | JDBC with checkpoint / watermark |
| **GenerateTableFetch** | Tracks paging offsets | Incremental JDBC with partition bounds |
| **ListS3** | Tracks listed object timestamps | Auto Loader with checkpoint |
| **ListHDFS** | Tracks listed file timestamps | Auto Loader with checkpoint |
| **ListFile** | Tracks listed file timestamps | Auto Loader with checkpoint |
| **ListFTP** / **ListSFTP** | Tracks listed file timestamps | Scheduled job with state table |
| **ListAzureBlobStorage** | Tracks listed blob timestamps | Auto Loader with checkpoint |
| **ListAzureDataLakeStorage** | Tracks listed file timestamps | Auto Loader with checkpoint |
| **ListGCSBucket** | Tracks listed object timestamps | Auto Loader with checkpoint |
| **TailFile** | Tracks file offset (byte position) | Auto Loader streaming |
| **ConsumeKafka** (all versions) | Tracks Kafka offsets | Structured Streaming checkpoints |
| **ConsumeKinesisStream** | Tracks Kinesis sequence numbers | Structured Streaming checkpoints |
| **ConsumeAzureEventHub** | Tracks Event Hub offsets | Structured Streaming checkpoints |
| **ConsumeGCPubSub** | Tracks Pub/Sub acknowledgment | Structured Streaming checkpoints |
| **GetSQS** | Tracks message receipt handles | boto3 + Delta state table |
| **CaptureChangeMySQL** | Tracks binlog position | Debezium + Kafka checkpoints |
| **DetectDuplicate** | Tracks seen keys in distributed cache | Delta lookup table |
| **MonitorActivity** | Tracks last activity timestamp | Workflows monitoring |
| **Wait** / **Notify** | Signal state in distributed cache | Delta signal table |
| **AttributeRollingWindow** | Tracks window values | Spark window functions |

---

## 21. NiFi Expression Language Functions

### String Manipulation

| Function | Description | PySpark Equivalent |
|---|---|---|
| `toUpper()` | Converts to uppercase | `upper(col)` |
| `toLower()` | Converts to lowercase | `lower(col)` |
| `trim()` | Removes leading/trailing whitespace | `trim(col)` |
| `length()` | Returns string length | `length(col)` |
| `append(str)` | Appends text to end | `concat(col, lit(str))` |
| `prepend(str)` | Prepends text to beginning | `concat(lit(str), col)` |
| `repeat(n)` | Repeats string N times | `repeat(col, n)` (Spark 3.x) |
| `substring(start, end)` | Extracts substring | `substring(col, start+1, length)` |
| `substringBefore(str)` | Text before first occurrence | `substring_index(col, str, 1)` |
| `substringBeforeLast(str)` | Text before last occurrence | `substring_index(col, str, -1)` variant |
| `substringAfter(str)` | Text after first occurrence | `regexp_extract` or custom |
| `substringAfterLast(str)` | Text after last occurrence | `regexp_extract` or custom |
| `getDelimitedField(n, delim)` | Gets Nth delimited field | `split(col, delim)[n]` |
| `replace(search, repl)` | Replaces all literal occurrences | `regexp_replace(col, lit_escape(search), repl)` |
| `replaceFirst(search, repl)` | Replaces first occurrence | `regexp_replace` with lazy pattern |
| `replaceAll(regex, repl)` | Regex-based replacement | `regexp_replace(col, regex, repl)` |
| `replaceNull(val)` | Returns val if null | `coalesce(col, lit(val))` |
| `replaceEmpty(val)` | Returns val if empty/whitespace | `when(trim(col)=="", lit(val)).otherwise(col)` |
| `padLeft(len, char)` | Left-pads to length | `lpad(col, len, char)` |
| `padRight(len, char)` | Right-pads to length | `rpad(col, len, char)` |

### Searching

| Function | Description | PySpark Equivalent |
|---|---|---|
| `startsWith(str)` | Tests if begins with str | `col.startswith(str)` |
| `endsWith(str)` | Tests if ends with str | `col.endswith(str)` |
| `contains(str)` | Tests if contains substring | `col.contains(str)` |
| `in(val1, val2, ...)` | Tests membership | `col.isin([val1, val2])` |
| `find(regex)` | Finds regex match anywhere | `col.rlike(regex)` |
| `matches(regex)` | Full string regex match | `col.rlike("^" + regex + "$")` |
| `indexOf(str)` | First position of substring | `instr(col, str)` (1-based) |
| `lastIndexOf(str)` | Last position of substring | Custom UDF |
| `jsonPath(expr)` | Evaluates JsonPath expression | `get_json_object(col, expr)` |
| `jsonPathDelete(expr)` | Removes JSON element at path | Custom JSON UDF |
| `jsonPathAdd(expr)` | Adds JSON element at path | Custom JSON UDF |
| `jsonPathSet(expr)` | Sets JSON value at path | Custom JSON UDF |
| `jsonPathPut(expr)` | Creates/updates JSON property | Custom JSON UDF |

### Type Conversion

| Function | Description | PySpark Equivalent |
|---|---|---|
| `toString()` | Converts to string | `col.cast("string")` |
| `toNumber()` | Converts to integer | `col.cast("long")` |
| `toDecimal()` | Converts to decimal | `col.cast("double")` |
| `toDate(fmt, tz)` | Converts to date | `to_date(col, fmt)` |
| `toInstant()` | Converts to instant/timestamp | `to_timestamp(col)` |
| `toMicros()` | Converts to microseconds | `unix_micros(col)` |
| `toNanos()` | Converts to nanoseconds | Custom: `unix_timestamp * 1e9` |

### Mathematical Operations

| Function | Description | PySpark Equivalent |
|---|---|---|
| `plus(n)` | Addition | `col + n` |
| `minus(n)` | Subtraction | `col - n` |
| `multiply(n)` | Multiplication | `col * n` |
| `divide(n)` | Division | `col / n` |
| `mod(n)` | Modulo | `col % n` |
| `toRadix(base)` | Number to specified base string | `conv(col, 10, base)` |
| `fromRadix(base)` | String in base to decimal | `conv(col, base, 10)` |
| `random()` | Random number | `rand()` |
| `math(expr)` | Evaluate math expression | `expr(math_expression)` |

### Date/Time Functions

| Function | Description | PySpark Equivalent |
|---|---|---|
| `now()` | Current timestamp | `current_timestamp()` |
| `format(fmt, tz)` | Formats date to string | `date_format(col, fmt)` |
| `formatInstant(fmt, tz)` | Formats instant to string | `date_format(col, fmt)` |

### Encoding/Decoding

| Function | Description | PySpark Equivalent |
|---|---|---|
| `escapeJson()` | Escape for JSON | Custom UDF / `to_json()` |
| `unescapeJson()` | Unescape JSON | Custom UDF |
| `escapeXml()` | Escape for XML | Custom UDF |
| `unescapeXml()` | Unescape XML | Custom UDF |
| `escapeCsv()` | Escape for CSV (RFC 4180) | Built into CSV writer |
| `unescapeCsv()` | Unescape CSV | Built into CSV reader |
| `escapeHtml3()` | Escape for HTML 3 | Custom UDF |
| `unescapeHtml3()` | Unescape HTML 3 | Custom UDF |
| `escapeHtml4()` | Escape for HTML 4 | Custom UDF |
| `unescapeHtml4()` | Unescape HTML 4 | Custom UDF |
| `urlEncode()` | URL-encode string | Custom UDF: `urllib.parse.quote()` |
| `urlDecode()` | URL-decode string | Custom UDF: `urllib.parse.unquote()` |
| `base64Encode()` | Base64 encode | `base64(col)` |
| `base64Decode()` | Base64 decode | `unbase64(col)` |
| `hash(algo)` | Hex-encoded hash | `sha2(col, 256)` / `md5(col)` |
| `UUID3(namespace, name)` | MD5-based UUID | `uuid.uuid3()` UDF |
| `UUID5(namespace, name)` | SHA-1-based UUID | `uuid.uuid5()` UDF |

### Boolean / Comparison

| Function | Description | PySpark Equivalent |
|---|---|---|
| `isNull()` | Tests for null | `col.isNull()` |
| `notNull()` | Tests for non-null | `col.isNotNull()` |
| `isEmpty()` | Tests for null/empty/whitespace | `col.isNull() | (trim(col) == "")` |
| `equals(str)` | Case-sensitive equality | `col == str` |
| `equalsIgnoreCase(str)` | Case-insensitive equality | `lower(col) == lower(lit(str))` |
| `gt(n)` | Greater than | `col > n` |
| `ge(n)` | Greater than or equal | `col >= n` |
| `lt(n)` | Less than | `col < n` |
| `le(n)` | Less than or equal | `col <= n` |
| `and(bool)` | Logical AND | `cond1 & cond2` |
| `or(bool)` | Logical OR | `cond1 | cond2` |
| `not()` | Logical NOT | `~condition` |
| `ifElse(trueVal, falseVal)` | Conditional | `when(cond, val1).otherwise(val2)` |
| `isJson()` | Tests if valid JSON | `from_json()` null check UDF |

### Subjectless / System Functions

| Function | Description | PySpark Equivalent |
|---|---|---|
| `hostname()` | Machine hostname | `socket.gethostname()` |
| `ip()` | System IP address | `socket.gethostbyname(hostname)` |
| `UUID()` | Random UUID | `uuid()` (Spark 3.x) or `uuid.uuid4()` UDF |
| `nextInt()` | Incrementing counter | `monotonically_increasing_id()` |
| `literal(val)` | Literal value | `lit(val)` |
| `getStateValue(key)` | Reads processor state | Spark checkpoint / Delta state table |
| `thread()` | Current thread name | Not applicable in Spark |
| `getUri()` | System URI | Not applicable |
| `evaluateELString()` | Evaluates nested EL | Not applicable (inline resolution) |

### Multi-Attribute Functions

| Function | Description | PySpark Equivalent |
|---|---|---|
| `anyAttribute(attr1, attr2, ...)` | Tests any attribute matches | `col1 | col2 | ...` |
| `allAttributes(attr1, attr2, ...)` | Tests all attributes match | `col1 & col2 & ...` |
| `anyMatchingAttribute(regex)` | Tests any regex-matched attr | Filter by column name regex |
| `allMatchingAttributes(regex)` | Tests all regex-matched attrs | Filter by column name regex |
| `anyDelineatedValue(val, delim)` | Tests any delimited value | `array_contains(split(col, delim), val)` |
| `allDelineatedValues(val, delim)` | Tests all delimited values | Custom UDF |
| `join(separator)` | Joins multiple values | `concat_ws(separator, *cols)` |
| `count()` | Counts matching elements | `size(array)` or `count()` |

---

## 22. Controller Service Types

### Record Readers

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **AvroReader** | Reads Avro format data | `spark.read.format("avro")` |
| **JsonTreeReader** | Reads JSON (tree model) data | `spark.read.format("json")` |
| **JsonPathReader** | Reads JSON using JsonPath expressions | `get_json_object()` |
| **CSVReader** | Reads CSV format data | `spark.read.format("csv")` |
| **XMLReader** | Reads XML format data | `spark.read.format("xml")` (spark-xml) |
| **ExcelReader** | Reads Excel workbook data | `spark.read.format("com.crealytics.spark.excel")` |
| **GrokReader** | Reads log data using Grok patterns | `regexp_extract()` |
| **SyslogReader** | Reads Syslog format data | `regexp_extract()` for syslog |
| **Syslog5424Reader** | Reads RFC 5424 Syslog data | `regexp_extract()` |
| **CEFReader** | Reads Common Event Format data | Regex UDF |
| **WindowsEventLogReader** | Reads Windows Event Log XML | lxml UDF |
| **ProtobufReader** | Reads Protocol Buffers data | protobuf library |
| **StandardProtobufReader** | Standard Protobuf reader | protobuf library |
| **YamlTreeReader** | Reads YAML data | PyYAML + createDataFrame |
| **ScriptedReader** | Custom script-based reader | PySpark UDF |

### Record Writers

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **AvroRecordSetWriter** | Writes Avro format | `df.write.format("avro")` |
| **JsonRecordSetWriter** | Writes JSON format | `df.write.format("json")` |
| **CSVRecordSetWriter** | Writes CSV format | `df.write.format("csv")` |
| **XMLRecordSetWriter** | Writes XML format | spark-xml writer |
| **FreeFormTextRecordSetWriter** | Writes free-form text | `df.write.format("text")` |
| **ScriptedRecordSetWriter** | Custom script-based writer | PySpark UDF |
| **ParquetIcebergWriter** | Writes Parquet via Iceberg | `df.write.format("iceberg")` |

### Record Sinks

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **DatabaseRecordSink** | Writes records to database | `df.write.format("jdbc")` |
| **AzureEventHubRecordSink** | Writes records to Event Hubs | azure-eventhubs-spark |
| **HttpRecordSink** | Writes records via HTTP | `requests.post()` foreachBatch |
| **EmailRecordSink** | Writes records as email | SMTP library |
| **LoggingRecordSink** | Logs records | `print()` / `display()` |
| **SlackRecordSink** | Writes records to Slack | Slack webhook |
| **ZendeskRecordSink** | Writes records to Zendesk | Zendesk API |
| **UDPEventRecordSink** | Writes records via UDP | Python socket |
| **SiteToSiteReportingRecordSink** | Writes to NiFi Site-to-Site | Not applicable |
| **ScriptedRecordSink** | Custom script-based sink | PySpark UDF |
| **RecordSinkServiceLookup** | Lookup-based record sink selection | Not applicable |

### Schema Registries

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **AvroSchemaRegistry** | Manages Avro schemas locally | Unity Catalog schema |
| **ConfluentSchemaRegistry** | Confluent Schema Registry client | confluent-kafka Schema Registry client |
| **AmazonGlueSchemaRegistry** | AWS Glue Schema Registry | Glue catalog integration |
| **StandardJsonSchemaRegistry** | JSON Schema registry | Unity Catalog |
| **DatabaseTableSchemaRegistry** | Schema from database tables | JDBC metadata |
| **VolatileSchemaCache** | In-memory schema cache | Spark schema caching |
| **ApicurioSchemaRegistry** | Apicurio Registry client | Apicurio REST API |

### Schema Reference Handlers

| Controller Service | Description |
|---|---|
| **ConfluentEncodedSchemaReferenceReader** | Reads Confluent-encoded schema references |
| **ConfluentEncodedSchemaReferenceWriter** | Writes Confluent-encoded schema references |
| **AmazonGlueEncodedSchemaReferenceReader** | Reads Glue-encoded schema references |
| **ConfluentProtobufMessageNameResolver** | Resolves Protobuf message names |

### Database Connection Pools

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **DBCPConnectionPool** | Standard JDBC connection pool | `spark.read.format("jdbc")` |
| **DBCPConnectionPoolLookup** | Lookup-based connection pool | Dynamic JDBC URL |
| **HikariCPConnectionPool** | HikariCP-based connection pool | `spark.read.format("jdbc")` |

### Lookup Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **DatabaseRecordLookupService** | Lookup via database query | `df.join(spark.read.format("jdbc"))` |
| **CSVRecordLookupService** | Lookup via CSV file | `df.join(spark.read.csv())` |
| **SimpleCsvFileLookupService** | Simple CSV key/value lookup | `df.join()` |
| **SimpleDatabaseLookupService** | Simple database lookup | `df.join()` |
| **SimpleKeyValueLookupService** | Key/value pair lookup | `df.join()` on broadcast |
| **XMLFileLookupService** | Lookup via XML file | spark-xml + join |
| **RestLookupService** | Lookup via REST API | `requests` UDF |
| **ElasticSearchLookupService** | Lookup via Elasticsearch | elasticsearch-py UDF |
| **ElasticSearchStringLookupService** | String lookup via ES | elasticsearch-py UDF |
| **MongoDBLookupService** | Lookup via MongoDB | pymongo UDF |
| **IPLookupService** | IP geolocation lookup | geoip2 UDF |
| **DistributedMapCacheLookupService** | Lookup via distributed cache | Delta lookup table |
| **PropertiesFileLookupService** | Lookup via properties file | Python config parser |
| **ScriptedLookupService** | Custom script lookup | PySpark UDF |
| **SimpleScriptedLookupService** | Simple script lookup | PySpark UDF |

### SSL / Security Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **StandardSSLContextService** | Provides SSL/TLS credentials | Unity Catalog secrets |
| **StandardRestrictedSSLContextService** | Restricted SSL context | Unity Catalog secrets |
| **PEMEncodedSSLContextProvider** | PEM-encoded certificates | Databricks Secret Scopes |
| **StandardPGPPrivateKeyService** | PGP private key management | Databricks Secret Scopes |
| **StandardPGPPublicKeyService** | PGP public key management | Databricks Secret Scopes |
| **StandardPrivateKeyService** | SSH private key management | Databricks Secret Scopes |
| **StandardHashiCorpVaultClientService** | HashiCorp Vault integration | Databricks Secret Scopes / Vault integration |

### Cloud Credential Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **AWSCredentialsProviderControllerService** | AWS credential management | Unity Catalog external locations |
| **AwsRdsIamDatabasePasswordProvider** | AWS RDS IAM auth | Unity Catalog + IAM |
| **GCPCredentialsControllerService** | GCP credential management | Unity Catalog external locations |
| **ADLSCredentialsControllerService** | ADLS credential management | Unity Catalog external locations |
| **ADLSCredentialsControllerServiceLookup** | ADLS credential lookup | Dynamic credential selection |
| **StandardAzureCredentialsControllerService** | Azure credential management | Unity Catalog external locations |
| **AzureStorageCredentialsControllerService_v12** | Azure Storage credentials | Unity Catalog |
| **AzureStorageCredentialsControllerServiceLookup_v12** | Azure credential lookup | Dynamic credential selection |
| **AzureCosmosDBClientService** | Cosmos DB client management | Cosmos Spark connector config |
| **StandardDropboxCredentialService** | Dropbox credentials | Databricks Secret Scopes |

### Distributed Cache Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **MapCacheClientService** | Distributed map cache client | Delta lookup table |
| **MapCacheServer** | Distributed map cache server | Delta Lake table |
| **SetCacheClientService** | Distributed set cache client | Delta table + DISTINCT |
| **SetCacheServer** | Distributed set cache server | Delta Lake table |
| **RedisConnectionPoolService** | Redis connection pool | redis-py connection pool |
| **RedisDistributedMapCacheClientService** | Redis-backed distributed cache | redis-py |
| **SimpleRedisDistributedMapCacheClientService** | Simple Redis cache | redis-py |
| **EmbeddedHazelcastCacheManager** | Hazelcast cache (embedded) | Delta lookup table |
| **ExternalHazelcastCacheManager** | Hazelcast cache (external) | External cache + Delta |
| **HazelcastMapCacheClient** | Hazelcast map cache client | Delta lookup table |

### Messaging Connection Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **Kafka3ConnectionService** | Kafka 3.x connection management | `spark.readStream.format("kafka")` options |
| **AmazonMSKConnectionService** | Amazon MSK connection | Kafka connector with MSK config |
| **JMSConnectionFactoryProvider** | JMS connection factory | stomp.py / pika connection |
| **JndiJmsConnectionFactoryProvider** | JNDI-based JMS connection | stomp.py / pika connection |
| **MongoDBControllerService** | MongoDB connection management | pymongo / mongodb-spark-connector |

### Iceberg Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **RESTIcebergCatalog** | Iceberg REST catalog | Unity Catalog + Iceberg |
| **ADLSIcebergFileIOProvider** | ADLS-based Iceberg file I/O | Unity Catalog + Iceberg |
| **S3IcebergFileIOProvider** | S3-based Iceberg file I/O | Unity Catalog + Iceberg |

### Kerberos Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **KerberosKeytabUserService** | Kerberos keytab authentication | Unity Catalog identity federation |
| **KerberosPasswordUserService** | Kerberos password authentication | Unity Catalog identity federation |
| **KerberosTicketCacheUserService** | Kerberos ticket cache | Unity Catalog identity federation |

### OAuth2 Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **StandardOauth2AccessTokenProvider** | Standard OAuth2 token provider | `requests` + OAuth2 flow |
| **JWTBearerOAuth2AccessTokenProvider** | JWT Bearer OAuth2 tokens | `requests` + JWT library |

### Other Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **StandardHttpContextMap** | HTTP request/response context | Model Serving state |
| **StandardWebClientServiceProvider** | HTTP client service | `requests` library |
| **StandardProxyConfigurationService** | HTTP proxy configuration | `requests` proxy config |
| **ElasticSearchClientServiceImpl** | Elasticsearch client | elasticsearch-py |
| **SmbjClientProviderService** | SMB client | smbprotocol |
| **StandardDatabaseDialectService** | Database SQL dialect | Spark SQL auto-detection |
| **StandardKustoIngestService** | Azure Data Explorer ingestion | Kusto Spark connector |
| **StandardKustoQueryService** | Azure Data Explorer query | Kusto Spark connector |
| **S3FileResourceService** | S3 file resource provider | `dbutils.fs` / boto3 |
| **GCSFileResourceService** | GCS file resource provider | `dbutils.fs` / google-cloud-storage |
| **AzureBlobStorageFileResourceService** | Azure Blob file resource | `dbutils.fs` / azure-storage-blob |
| **AzureDataLakeStorageFileResourceService** | ADLS file resource | `dbutils.fs` / azure ADLS SDK |
| **StandardFileResourceService** | Local file resource | Volumes / DBFS |
| **ReaderLookup** | Dynamic Record Reader selection | Not applicable |
| **RecordSetWriterLookup** | Dynamic Record Writer selection | Not applicable |

### Box.com Services

| Controller Service | Description |
|---|---|
| **DeveloperBoxClientService** | Box.com developer client |
| **JsonConfigBasedBoxClientService** | Box.com JSON config client |

### WebSocket Services

| Controller Service | Description | Databricks Equivalent |
|---|---|---|
| **JettyWebSocketClient** | WebSocket client | websocket-client library |
| **JettyWebSocketServer** | WebSocket server | Databricks App + websockets |

---

## 23. Connection Relationship Types

### Universal Relationships (present on most processors)

| Relationship | Description | PySpark Pattern |
|---|---|---|
| **success** | Processing completed successfully | Main DataFrame output |
| **failure** | Processing encountered an error | `try/except` error handling |

### Routing Relationships

| Relationship | Processors | Description | PySpark Pattern |
|---|---|---|---|
| **matched** | EvaluateJsonPath, EvaluateXPath, RouteOnAttribute, RouteOnContent, RouteText | Content/attribute matched condition | `df.filter(condition)` |
| **unmatched** | EvaluateJsonPath, EvaluateXPath, RouteOnAttribute, RouteOnContent, RouteText | Did not match any condition | `df.filter(~condition)` |
| **original** | SplitJson, SplitXml, SplitText, SplitContent, SplitRecord, SplitAvro, ForkRecord, UnpackContent, ExtractText, ExtractGrok | Original unsplit FlowFile | Retained as `df_original` |
| **split** | SplitJson, SplitXml, SplitText, SplitContent | Individual split segments | `explode()` output |

### Content Relationships

| Relationship | Processors | Description |
|---|---|---|
| **merged** | MergeContent, MergeRecord | Successfully merged FlowFile |
| **response** | InvokeHTTP, HandleHttpResponse | HTTP response FlowFile |
| **request** | HandleHttpRequest | HTTP request FlowFile |
| **retry** | InvokeHTTP, RetryFlowFile | Should be retried |
| **no retry** | InvokeHTTP | Should NOT be retried (permanent failure) |
| **retries_exceeded** | RetryFlowFile | Max retries exhausted |

### Database Relationships

| Relationship | Processors | Description |
|---|---|---|
| **results** | ExecuteSQL, ExecuteSQLRecord, QueryDatabaseTable | Query result set |
| **original** | ExecuteSQL, PutSQL | Original FlowFile after processing |

### Validation Relationships

| Relationship | Processors | Description |
|---|---|---|
| **valid** | ValidateRecord, ValidateCsv, ValidateXml, ValidateJson | Passed validation |
| **invalid** | ValidateRecord, ValidateCsv, ValidateXml, ValidateJson | Failed validation |
| **compatible** | ValidateRecord | Schema-compatible records |
| **incompatible** | ValidateRecord | Schema-incompatible records |

### Monitor Relationships

| Relationship | Processors | Description |
|---|---|---|
| **success** | MonitorActivity | Normal flow through |
| **inactive** | MonitorActivity | No activity detected for threshold period |
| **activity.restored** | MonitorActivity | Activity resumed after inactivity |

### Wait/Notify Relationships

| Relationship | Processors | Description |
|---|---|---|
| **wait** | Wait | Still waiting for signal |
| **expired** | Wait | Wait timeout exceeded |
| **signal.released** | Wait | Signal received, processing released |

### Custom/Dynamic Relationships

| Relationship | Processors | Description |
|---|---|---|
| **User-defined names** | RouteOnAttribute, RouteText | Each routing rule creates a named relationship |
| **1, 2, 3, ...** | DistributeLoad | Numbered relationships for round-robin |
| **found, not found** | FetchDistributedMapCache, LookupAttribute | Cache/lookup hit or miss |
| **duplicate** | DetectDuplicate | FlowFile was a duplicate |
| **non-duplicate** | DetectDuplicate | FlowFile was unique |

---

## 24. NiFi Property Types & Databricks Equivalents

### Property Value Types

| NiFi Property Type | Description | Databricks Equivalent |
|---|---|---|
| **String** | Free-form text | Python string / Spark `StringType` |
| **Integer** | Whole number | Python int / Spark `IntegerType` |
| **Long** | Large whole number | Python int / Spark `LongType` |
| **Boolean** | true/false | Python bool / Spark `BooleanType` |
| **Data Size** | e.g., "1 MB", "10 GB" | Python integer (bytes) |
| **Time Period** | e.g., "30 sec", "5 min" | Python timedelta / string |
| **Character Set** | e.g., "UTF-8", "ISO-8859-1" | Python encoding string |
| **URL** | HTTP/HTTPS URL | Python string |
| **File Path** | Local file system path | Volumes path: `/Volumes/catalog/schema/...` |
| **Directory Path** | Local directory path | Volumes path |
| **Remote URL** | Remote resource URL | Cloud storage path: `s3://`, `abfss://`, `gs://` |
| **Password** | Sensitive string value | `dbutils.secrets.get(scope, key)` |
| **Controller Service** | Reference to a controller service | Spark DataFrame config options |
| **Expression Language** | Supports NiFi EL evaluation | PySpark `col()`, `lit()`, `expr()` |
| **Regex** | Regular expression pattern | Python regex / PySpark `.rlike()` |
| **JSON Path** | JsonPath expression | PySpark `get_json_object()` |
| **XPath** | XPath expression | PySpark `xpath()` |
| **SQL** | SQL statement | `spark.sql()` |
| **Avro Schema** | Avro schema definition | `StructType` schema |

### Expression Language Scope

| NiFi EL Scope | Description | Databricks Equivalent |
|---|---|---|
| **NONE** | No Expression Language allowed | Literal values only |
| **FLOWFILE_ATTRIBUTES** | Can reference FlowFile attributes | `col("attribute_name")` |
| **ENVIRONMENT** | Can reference environment variables | `dbutils.widgets.get()` / env vars |

### Scheduling Strategy

| NiFi Strategy | Description | Databricks Equivalent |
|---|---|---|
| **Timer Driven** | Runs at fixed interval | Databricks Workflow schedule (cron) |
| **CRON Driven** | Runs on CRON schedule | Databricks Workflow cron trigger |
| **Event Driven** | Runs when FlowFile arrives | Structured Streaming (continuous) |
| **Primary Node Only** | Runs on primary cluster node only | Single-node job or driver-only logic |

### Bulletin Level

| NiFi Level | Description | Databricks Equivalent |
|---|---|---|
| **DEBUG** | Debug messages | `print()` / Python `logging.DEBUG` |
| **INFO** | Informational messages | `print()` / Python `logging.INFO` |
| **WARN** | Warning messages | Python `logging.WARNING` |
| **ERROR** | Error messages | Python `logging.ERROR` / Job failure |

### Processor Execution

| NiFi Setting | Description | Databricks Equivalent |
|---|---|---|
| **Concurrent Tasks** | Max simultaneous threads | Spark parallelism / executor count |
| **Run Schedule** | Time between executions | Workflow trigger interval / `.trigger()` |
| **Penalty Duration** | Time to penalize failed FlowFiles | Retry backoff in error handling |
| **Yield Duration** | Time processor yields after no work | Not applicable (streaming auto-manages) |
| **Run Duration** | Max batch processing time | `.trigger(processingTime="...")` |
| **Backpressure Threshold** | Object count / data size limit before backpressure | Spark back-pressure (auto-managed) |

---

## 25. NiFi Flow Constructs (Non-Processor)

| NiFi Construct | Description | Databricks Equivalent |
|---|---|---|
| **Funnel** | Merges multiple connections into one | `df1.union(df2)` |
| **Input Port** | Receives data into a process group | Notebook parameter / Delta table input |
| **Output Port** | Sends data out of a process group | `dbutils.notebook.exit()` / Delta table output |
| **Process Group** | Encapsulates a sub-flow | Separate notebook / module |
| **Remote Process Group** | NiFi Site-to-Site remote connection | Unity Catalog Delta Sharing |
| **Label** | Visual annotation in the canvas | Code comments / notebook markdown cells |
| **Connection** | Links processors with queue and backpressure | DataFrame variable passing |
| **Parameter Context** | Named set of parameters | Databricks Workflow parameters / widgets |
| **Flow File** | Unit of data with content + attributes | DataFrame row |
| **Provenance** | Data lineage tracking | Unity Catalog Lineage |
| **Registry Client** | Version control for flows | Git integration / Repos |
| **Variable Registry** | *(Deprecated)* Flow-level variables | Notebook widgets / env vars |

---

## 26. Parameter Providers

| Provider | Description | Databricks Equivalent |
|---|---|---|
| **AwsSecretsManagerParameterProvider** | Reads from AWS Secrets Manager | `dbutils.secrets.get()` with AWS scope |
| **AzureKeyVaultSecretsParameterProvider** | Reads from Azure Key Vault | `dbutils.secrets.get()` with Azure scope |
| **GcpSecretManagerParameterProvider** | Reads from GCP Secret Manager | `dbutils.secrets.get()` with GCP scope |
| **HashiCorpVaultParameterProvider** | Reads from HashiCorp Vault | `dbutils.secrets.get()` with Vault scope |
| **DatabaseParameterProvider** | Reads from database table | JDBC config table read |
| **EnvironmentVariableParameterProvider** | Reads OS environment variables | `os.environ` / `spark.conf.get()` |
| **KubernetesSecretParameterProvider** | Reads Kubernetes secrets | K8s secrets via pod config |
| **OnePasswordParameterProvider** | Reads from 1Password | 1Password CLI / SDK |

---

## 27. Reporting Tasks

| Reporting Task | Description | Databricks Equivalent |
|---|---|---|
| **SiteToSiteProvenanceReportingTask** | Reports provenance via Site-to-Site | Unity Catalog Lineage |
| **SiteToSiteStatusReportingTask** | Reports flow status via S2S | Databricks monitoring API |
| **SiteToSiteBulletinReportingTask** | Reports bulletins via S2S | Databricks alerting |
| **SiteToSiteMetricsReportingTask** | Reports metrics via S2S | Databricks metrics / Ganglia |
| **ControllerStatusReportingTask** | Reports controller status | Spark UI / Workflows monitoring |
| **MonitorDiskUsage** | Monitors disk space | Databricks cluster monitoring |
| **MonitorMemory** | Monitors JVM memory usage | Spark UI memory tab |
| **AzureLogAnalyticsReportingTask** | Reports to Azure Log Analytics | Azure Log Analytics integration |
| **AzureLogAnalyticsProvenanceReportingTask** | Reports provenance to Azure | Azure Log Analytics |
| **ScriptedReportingTask** | Custom script-based reporting | Databricks notebook job |

---

## 28. Flow Registry Clients

| Client | Description | Databricks Equivalent |
|---|---|---|
| **NifiRegistryFlowRegistryClient** | Apache NiFi Registry | Databricks Repos / Git |
| **GitHubFlowRegistryClient** | GitHub-based flow versioning | Databricks Repos (GitHub) |
| **GitLabFlowRegistryClient** | GitLab-based flow versioning | Databricks Repos (GitLab) |
| **BitbucketFlowRegistryClient** | Bitbucket-based flow versioning | Databricks Repos (Bitbucket) |
| **AzureDevOpsFlowRegistryClient** | Azure DevOps flow versioning | Databricks Repos (Azure DevOps) |

---

## 29. Flow Analysis Rules

| Rule | Description | Databricks Equivalent |
|---|---|---|
| **DisallowComponentType** | Prevents use of specific component types | Databricks policy / Unity Catalog governance |
| **RequireServerSSLContextService** | Requires SSL for server components | Cluster SSL configuration |
| **RestrictBackpressureSettings** | Enforces backpressure limits | Spark back-pressure auto-tuning |
| **RestrictFlowFileExpiration** | Enforces FlowFile expiration policies | Delta table retention policies |

---

## Appendix A: Additional / Third-Party Processors

These processors are from the NiFi ecosystem but may require separate NARs or be community-contributed:

| Short Name | Description | Databricks Equivalent |
|---|---|---|
| **ConsumeBoxEvents** | Consumes events from Box.com | Box API via requests |
| **ConsumeBoxEnterpriseEvents** | Consumes Box enterprise events | Box API via requests |
| **FetchBoxFile** | Fetches file from Box.com | Box API via requests |
| **FetchBoxFileInfo** | Gets Box file metadata | Box API via requests |
| **FetchBoxFileRepresentation** | Gets Box file representation | Box API via requests |
| **FetchBoxFileMetadataInstance** | Gets Box file metadata instance | Box API via requests |
| **ListBoxFile** | Lists files in Box folder | Box API via requests |
| **ListBoxFileInfo** | Lists Box file info | Box API via requests |
| **ListBoxFileMetadataInstances** | Lists Box metadata instances | Box API via requests |
| **ListBoxFileMetadataTemplates** | Lists Box metadata templates | Box API via requests |
| **CreateBoxFileMetadataInstance** | Creates Box metadata | Box API via requests |
| **CreateBoxMetadataTemplate** | Creates Box metadata template | Box API via requests |
| **UpdateBoxFileMetadataInstance** | Updates Box metadata | Box API via requests |
| **DeleteBoxFileMetadataInstance** | Deletes Box metadata | Box API via requests |
| **ExtractStructuredBoxFileMetadata** | Extracts structured metadata | Box API via requests |
| **GetBoxFileCollaborators** | Gets Box collaborators | Box API via requests |
| **GetBoxGroupMembers** | Gets Box group members | Box API via requests |
| **PutBoxFile** | Uploads file to Box.com | Box API via requests |
| **FetchDropbox** | Fetches file from Dropbox | Dropbox API |
| **ListDropbox** | Lists Dropbox files | Dropbox API |
| **PutDropbox** | Uploads to Dropbox | Dropbox API |
| **FetchGoogleDrive** | Fetches from Google Drive | Google Drive API |
| **ListGoogleDrive** | Lists Google Drive files | Google Drive API |
| **PutGoogleDrive** | Uploads to Google Drive | Google Drive API |
| **PutSalesforceObject** | Writes to Salesforce | Salesforce API / Spark connector |
| **QuerySalesforceObject** | Queries Salesforce | Salesforce API / Spark connector |
| **PutIcebergRecord** | Writes records to Iceberg table | `df.write.format("iceberg")` |
| **YandexTranslate** | Translates text via Yandex API | Yandex API via requests |

---

## Appendix B: Processor Count Summary

| Category | Count |
|---|---|
| Data Ingestion | ~35 |
| Data Egress / Sinks | ~16 |
| Transformation | ~36 |
| Routing & Mediation | ~13 |
| Database (all DB types) | ~65 |
| JSON/XML/CSV Processing | ~30 |
| HTTP/API | ~9 |
| Scripting | ~10 |
| System/Monitoring | ~8 |
| Content Manipulation | ~14 |
| AWS | ~22 |
| Azure | ~18 |
| GCP | ~13 |
| Kafka | ~15 |
| Hadoop/HDFS/Hive/HBase | ~22 |
| Email | ~7 |
| FTP/SFTP/SMB | ~14 |
| Encryption/Security | ~18 |
| Record-Based | ~20 |
| Box/Dropbox/Drive | ~20 |
| **Total Unique Processors** | **~500+** |
| **Controller Services** | **~150+** |
| **Expression Language Functions** | **~90+** |
| **Relationship Types** | **~25+ standard** |

---

## Appendix C: NiFi-to-Databricks Migration Quick Reference

| NiFi Concept | Databricks Equivalent |
|---|---|
| FlowFile | DataFrame row |
| FlowFile Content | Column value (`value` column) |
| FlowFile Attribute | DataFrame column |
| Processor | DataFrame transformation / action |
| Connection/Queue | DataFrame variable, checkpoint |
| Back Pressure | Structured Streaming auto-backpressure |
| Process Group | Notebook / Python module |
| Controller Service | Spark config / Secret Scope / library |
| Parameter Context | Workflow parameters / widgets |
| NiFi Expression Language | PySpark functions (`col()`, `lit()`, `expr()`) |
| Provenance | Unity Catalog Lineage |
| NiFi Registry | Databricks Repos (Git) |
| NiFi Clustering | Spark distributed execution |
| Site-to-Site | Unity Catalog Delta Sharing |
| Bulletin Board | Spark UI / Workflows monitoring |
| FlowFile Repository | Checkpoint directory |
| Content Repository | Cloud storage (S3/ADLS/GCS) |
| Provenance Repository | Unity Catalog system tables |

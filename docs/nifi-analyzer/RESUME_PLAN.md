# NiFi Flow Analyzer — Resume Plan

## File Location
`/Users/darkstar33/Documents/GitHub/TMP_remod_v1/docs/nifi-analyzer/index.html`
- Single monolithic HTML file with embedded CSS + JS
- Currently ~5,580 lines, ~437K chars
- Backup at `index.html.bak`

## What's Been Built (Complete)
1. **Step 1 - Load**: Multi-format input (XML, JSON), sample flows, file upload
2. **Step 2 - Analyze**: Deep flow analysis with external systems, per-processor detail, dependency mapping, EL analysis, scheduling summary
3. **Step 3 - Assess**: New scoring formula (autoConvertPct*50 + avgConf*20 + coverage*20 + simplicity*10), effort-days estimation, risk matrix
4. **Step 4 - Convert**: NiFi→Databricks notebook generation with 242 NIFI_DATABRICKS_MAP entries, role-based fallback templates, package requirements cell
5. **Step 5 - Report**: Migration report with markdown download
6. **Step 6 - Final Report**: JSON report with all data, downloadable
7. **Step 7 - Validate**: 4-angle comparison tab (Intent Analysis, Line Validation, Reverse Engineering Readiness, Function Mapping) + Accelerator Feedback
8. **Auto-run**: All steps chain automatically when Parse is clicked

## What Needs To Be Done

### Task A: Verify Step 7 Syntax
Run this to check JS is valid:
```bash
python3 -c "
import re
with open('docs/nifi-analyzer/index.html') as f:
    html = f.read()
m = re.search(r'<script>(.*?)</script>', html, re.DOTALL)
if m:
    js = m.group(1)
    with open('/tmp/_check.js', 'w') as f2:
        f2.write(js)
    print(f'Extracted {len(js)} chars')
" && node --check /tmp/_check.js
```
If syntax errors exist, fix them. The runValidation() function was added via Python script at `/tmp/add_step7.py`.

### Task B: Massively Expand the Converter (NIFI_DATABRICKS_MAP)
**Current state**: 242 processor types mapped. Need to cover ALL known NiFi processors (300+).

**What to add**:
- Every Apache NiFi processor from all bundles (standard, nifi-aws, nifi-azure, nifi-gcp, nifi-elasticsearch, nifi-mongodb, nifi-kudu, nifi-hbase, nifi-hive, nifi-kafka, nifi-redis, nifi-slack, nifi-scripting, etc.)
- Each entry needs: `{ desc: "...", conf: 0.0-1.0, tpl: "PySpark code template" }`
- Include ALL required pip packages in PACKAGE_MAP for each system
- Include ALL NiFi Expression Language functions mapped to Python/PySpark equivalents
- Cover every controller service type (DBCPConnectionPool, SSLContextService, etc.)

**Categories to ensure 100% coverage**:
| Category | Examples | Databricks Equivalent |
|----------|----------|----------------------|
| AWS | GetS3Object, PutS3Object, ListS3, FetchS3Object, PutSNS, PutSQS, PutLambda, PutDynamoDB, PutKinesisStream | spark.read/write with S3, boto3 |
| Azure | PutAzureBlobStorage, FetchAzureBlobStorage, PutAzureEventHub, ConsumeAzureEventHub, PutAzureDataLakeStorage | ABFSS paths, Azure SDK |
| GCP | PutGCSObject, FetchGCSObject, PublishGCPubSub, ConsumeGCPubSub, PutBigQueryBatch | GCS paths, google-cloud SDK |
| Kafka | PublishKafka, ConsumeKafka, PublishKafkaRecord, ConsumeKafkaRecord | spark.readStream.format("kafka") |
| Database | ExecuteSQL, PutDatabaseRecord, QueryDatabaseTable, GenerateTableFetch, PutSQL, ExecuteSQLRecord | spark.read.format("jdbc"), databricks-sql |
| Kudu | PutKudu, FetchKudu | spark.read/write.format("kudu") or Delta migration |
| HBase | PutHBaseCell, PutHBaseJSON, FetchHBaseRow, ScanHBase | happybase or Delta migration |
| HDFS | PutHDFS, GetHDFS, FetchHDFS, ListHDFS, MoveHDFS | dbutils.fs, spark.read |
| Elasticsearch | PutElasticsearchHttp, FetchElasticsearch, PutElasticsearchRecord, ScrollElasticsearchHttp | elasticsearch-py |
| MongoDB | PutMongo, GetMongo, DeleteMongo, PutMongoRecord | pymongo |
| Redis | PutDistributedMapCache, FetchDistributedMapCache, PutRedis | redis-py |
| Solr | PutSolrContentStream, GetSolr, PutSolrRecord | pysolr |
| HTTP | InvokeHTTP, HandleHttpRequest, HandleHttpResponse, ListenHTTP, GetHTTP, PostHTTP | requests, Flask/FastAPI |
| FTP/SFTP | GetSFTP, PutSFTP, ListSFTP, FetchSFTP, GetFTP, PutFTP, ListFTP | paramiko, pysftp |
| Email | PutEmail, ExtractEmailHeaders, ExtractEmailAttachments, ConsumeIMAP, ConsumePOP3 | smtplib, imaplib |
| JSON | EvaluateJsonPath, SplitJson, ConvertJSONToSQL, JoltTransformJSON, TransformJSON | PySpark JSON functions |
| XML | EvaluateXPath, EvaluateXQuery, TransformXml, SplitXml, ConvertXMLToJSON | lxml, spark.read.format("xml") |
| Avro | ConvertAvroToJSON, ConvertJSONToAvro, SplitAvro | spark.read.format("avro") |
| CSV | ConvertRecord (CSV), CSVReader, CSVRecordSetWriter | spark.read.format("csv") |
| Parquet | ConvertAvroToParquet, PutParquet, FetchParquet | spark.read.format("parquet") |
| ORC | PutORC, FetchORC | spark.read.format("orc") |
| Scripting | ExecuteScript, ExecuteStreamCommand, ExecuteProcess, ExecuteGroovyScript | Python UDF, subprocess |
| Flow Control | Wait, Notify, ControlRate, RouteOnAttribute, RouteOnContent, UpdateAttribute, LogAttribute, LogMessage | PySpark operations |
| Merge/Split | MergeContent, SplitText, SplitRecord, SplitJson, SplitXml, SplitAvro, MergeRecord | PySpark union/split |
| Encryption | EncryptContent, HashContent, CryptographicHashContent, SignContent, VerifyContentMAC | hashlib, cryptography |
| Compression | CompressContent, UnpackContent | gzip, zipfile |
| Lookup | LookupAttribute, LookupRecord | PySpark join/broadcast |
| Hive | PutHiveQL, SelectHiveQL, PutHiveStreaming, PutORC (Hive) | spark.sql() with Hive metastore |
| Slack | PutSlack, ConsumeSlack | slack_sdk |
| SNMP | GetSNMP, SetSNMP, ListenSNMP | pysnmp |
| Syslog | ListenSyslog, PutSyslog, ParseSyslog | syslog parsing |
| TCP/UDP | ListenTCP, ListenUDP, PutTCP, PutUDP | socket |
| JMS | PublishJMS, ConsumeJMS | stomp.py |
| AMQP | PublishAMQP, ConsumeAMQP | pika |
| Cassandra | PutCassandraQL, PutCassandraRecord, QueryCassandra | cassandra-driver |
| CouchDB | PutCouchbaseKey, GetCouchbaseKey | couchbase SDK |
| InfluxDB | PutInfluxDB | influxdb-client |
| Prometheus | PrometheusReportingTask | prometheus_client |

**Also expand**:
- `PROC_TYPE_SYSTEM_MAP` — currently 171 entries, needs all of the above
- `PACKAGE_MAP` — currently ~30 systems, needs all pip packages
- `ROLE_FALLBACK_TEMPLATES` — ensure all roles have rich templates
- `NIFI_ROLE_MAP` — ensure all processors classified correctly

### Task C: Build Step 8 — Workflow Value Analysis
New tab "8. Value Analysis" that outputs:

1. **What This Workflow Does** — Plain-language summary of the flow's purpose:
   - Data sources and destinations
   - Business logic (routing, transforms)
   - Integration points
   - Data formats and schemas involved

2. **How to Build It Better in Databricks** — For each processor/stage:
   - Current NiFi approach vs optimal Databricks approach
   - Use Delta Live Tables (DLT) where applicable
   - Use Auto Loader instead of file polling
   - Use Structured Streaming instead of batch loops
   - Use Unity Catalog for governance
   - Use Workflows for orchestration instead of NiFi scheduling
   - Performance optimizations (partition pruning, Z-ordering, liquid clustering)

3. **Steps That Aren't Needed** — Identify NiFi processors that are unnecessary in Databricks:
   - MergeContent → not needed (Spark handles partitioned reads natively)
   - CompressContent → not needed (Delta handles compression)
   - SplitText/SplitJson → not needed (Spark reads entire datasets)
   - UpdateAttribute → not needed (use .withColumn)
   - RouteOnAttribute for simple filters → just use .filter()
   - Wait/Notify coordination → use Workflows task dependencies
   - LogAttribute/LogMessage → use Spark logging
   - DetectDuplicate → use dropDuplicates()
   - ControlRate → not needed (Spark handles backpressure)

4. **Actions That Can Be Dropped** — Quantified list:
   - Each droppable processor with reason
   - Estimated complexity reduction (% fewer cells in notebook)
   - Estimated performance improvement
   - Risk assessment of dropping each one

5. **Migration ROI Summary**:
   - Current NiFi complexity score
   - Projected Databricks complexity score
   - Processors eliminated
   - New capabilities gained (ACID, time travel, ML integration, governance)

**Implementation**: Add tab 8, panel, `runValueAnalysis()` function, wire into auto-chain.

### Task D: Update Auto-Chain
After adding Step 8, update the auto-chain in `parseInput()` to include:
```javascript
await new Promise(r => setTimeout(r, 150));
switchTab('value'); runValueAnalysis();
```

## Key Code Locations (line numbers approximate after edits)
- **CSS**: lines 1-350
- **HTML tabs**: ~line 345-370
- **HTML panels**: ~lines 370-520
- **JS STATE + tab nav**: ~lines 530-560
- **File upload + samples**: ~lines 560-830
- **parseNiFiXML()**: ~lines 830-1860
- **buildResourceManifest()**: ~lines 1861-2180
- **NIFI_ROLE_MAP + classifyNiFiProcessor()**: ~lines 2181-2270
- **NIFI_DATABRICKS_MAP** (242 entries): ~lines 2271-2570
- **PACKAGE_MAP, ROLE_FALLBACK_TEMPLATES, buildDependencyGraph, detectExternalSystems**: ~lines 2571-2780
- **mapNiFiToDatabricks()**: ~lines 2780-2920
- **generateDatabricksNotebook()**: ~lines 2920-3100
- **buildNiFiTierData() + renderTierDiagram()**: ~lines 3100-4300
- **parseInput() + auto-chain**: ~lines 4320-4470
- **runAnalysis()**: ~lines 4480-4550
- **runAssessment()**: ~lines 4550-4660
- **generateNotebook()**: ~lines 4660-4730
- **generateReport()**: ~lines 4760-4860
- **generateFinalReport()**: ~lines 5050-5100
- **runValidation()**: ~lines 5130-5500
- **</script>**: ~line 5575

## Approach for Large Edits
DO NOT try to edit this file with inline bash/heredoc — template literals and backticks will break shell quoting. Instead:
1. Write a Python script to `/tmp/` that reads the HTML, makes targeted string replacements, and writes it back
2. Run the Python script with `python3 /tmp/script.py`
3. Verify with `node --check` on extracted JS

## Summary of Work Order
1. Verify Step 7 JS syntax (Task A)
2. Expand converter to 400+ processor types (Task B) — use Python script
3. Build Step 8 Value Analysis tab (Task C) — use Python script
4. Wire Step 8 into auto-chain (Task D)
5. Final JS syntax check
6. Test in browser with all 3 sample flows

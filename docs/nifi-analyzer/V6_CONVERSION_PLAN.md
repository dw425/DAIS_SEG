# V6.0 — Universal ETL Conversion Engine

**Goal**: Ingest ANY ETL flow (NiFi first, then SSIS, Informatica, Talend, Airflow, etc.) and output a **fully functional, executable** Python notebook for Databricks. Zero manual edits. Press "Run All" and it works.

**Date**: 2026-02-21
**Baseline**: v5.0.0 (full-stack rewrite, 302 NiFi processor mappings, 90 NEL functions)
**References**:
- "Architectural Blueprint for Migrating Apache NiFi to Databricks Workflows" (PDF)
- `docs/nifi-analyzer/FUNCTIONAL_ANALYSIS.md` (v5.0 platform analysis — 20 parsers, 19 sub-analyzers, 73 API endpoints, 25 YAML maps)
- `docs/nifi-analyzer/NIFI_PROCESSOR_CATALOG.md` (500+ processor catalog)

**Build Status**: Branch `v6.0` — 18 new files, 15,143 lines, 32/32 tests passing

### V5.0 Foundation (from FUNCTIONAL_ANALYSIS.md)
The V6 engine builds ON TOP of the existing v5.0 infrastructure, not replacing it:
- **20 parsers** (NiFi XML/JSON, SSIS, Informatica, Talend, Airflow, dbt, ADF, Glue, Snowflake, Dagster, Prefect, Spark, SQL, DataStage, Fivetran, Matillion, Oracle ODI, Airbyte, Stitch, Pentaho)
- **19 sub-analyzers** (dependency graph, cycle detection, external systems, security scanner, stage classifier, flow metrics, attribute flow, backpressure, task clusterer, transaction, state, schema, execution mode, site-to-site, process group, complexity, confidence, lineage, impact)
- **25 YAML processor maps** (14,724 lines of mapping templates)
- **8 generators** (notebook, DLT, DAB, workflow, test, CI/CD, connection, DQ rules)
- **2 code scrubbers** (credential sanitization, path normalization)
- **8-dimension validator** (intent, code quality, completeness, delta format, checkpoints, credentials, error handling, schema evolution)
- **SSE streaming pipeline** + watchdog heartbeat
- **73 HTTP endpoints + 1 WebSocket**

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [The 6-Pass Deep Analysis Engine](#the-6-pass-deep-analysis-engine)
3. [Complete NiFi Knowledge Base](#complete-nifi-knowledge-base)
4. [Gap Analysis: What's Broken Today](#gap-analysis-whats-broken-today)
5. [Implementation Plan: 12 Phases](#implementation-plan-12-phases)
6. [Validation & Quality Gates](#validation--quality-gates)
7. [Architecture Diagram](#architecture-diagram)
8. [File Impact Summary](#file-impact-summary)
9. [Success Metrics](#success-metrics)

---

## Executive Summary

V5.0 has real infrastructure — parsers, NEL transpiler, DAG analysis, 302 processor templates. But it produces **demo notebooks**, not **runnable pipelines**. The cells aren't wired together. There are no writes. No secrets management. No streaming triggers. No relationship routing.

V6.0 does three things:

1. **Builds a complete NiFi knowledge base** — every processor, every EL function, every controller service, every relationship type, every property, hardcoded and categorized. The system should never encounter a NiFi component it doesn't recognize.

2. **Implements a 6-pass deep analysis engine** — before any code is generated, the flow is analyzed from 6 different angles, producing a comprehensive understanding that drives intelligent conversion.

3. **Generates truly executable notebooks** — data flows end-to-end, variables chain correctly, writes persist to Delta, secrets are secured, streaming is triggered, errors are handled.

---

## The 6-Pass Deep Analysis Engine

Before converting a single line of code, V6 runs **6 independent analysis passes** over the parsed flow. Each pass produces a structured report. Together, they form a complete understanding of what the flow does, how it works, and what needs to be converted.

---

### Pass 1: Functional Analysis

**Purpose**: Understand WHAT the flow does at a business level. Break it down into logical functional areas.

**What it examines:**
- The overall purpose of the pipeline (ETL, CDC, streaming, API orchestration, file processing, etc.)
- Logical functional zones within the flow (ingestion zone, transformation zone, enrichment zone, routing zone, output zone)
- Data domains being processed (customer data, transactions, IoT telemetry, logs, etc.)
- Business rules embedded in routing logic, filters, and transformations
- SLA implications (real-time vs batch, latency requirements inferred from scheduling)

**Output — Functional Report:**
```json
{
  "flow_purpose": "Real-time IoT sensor data ingestion, enrichment with geo-lookup, anomaly detection via threshold routing, persistence to Delta Lake and alerting via Kafka",
  "functional_zones": [
    {
      "zone": "Ingestion",
      "processors": ["ConsumeKafka_sensors", "EvaluateJsonPath_parse"],
      "description": "Consumes raw sensor JSON from Kafka topic 'iot-sensors', extracts device_id, temperature, humidity, timestamp fields"
    },
    {
      "zone": "Enrichment",
      "processors": ["LookupRecord_geo", "UpdateAttribute_region"],
      "description": "Enriches sensor data with geographic region via lookup service, adds region classification attribute"
    },
    {
      "zone": "Routing & Detection",
      "processors": ["RouteOnAttribute_anomaly"],
      "description": "Routes records based on temperature threshold (>100°F = anomaly, else normal). Business rule: anomalies trigger immediate alert."
    },
    {
      "zone": "Output — Normal",
      "processors": ["PutDatabaseRecord_delta"],
      "description": "Persists normal readings to Delta table for batch analytics"
    },
    {
      "zone": "Output — Alerts",
      "processors": ["PublishKafka_alerts", "PutEmail_oncall"],
      "description": "Publishes anomaly events to alert Kafka topic AND sends email to on-call team"
    }
  ],
  "data_domains": ["IoT/Telemetry", "Geospatial"],
  "pipeline_pattern": "streaming_fan_out",
  "estimated_complexity": "medium",
  "sla_profile": "near_real_time"
}
```

**How it works internally:**
1. Classify each processor's role (source/transform/route/sink/utility) using the hardcoded processor knowledge base
2. Run connected-component analysis to identify functional zones (clusters of processors with dense internal connections)
3. Analyze processor properties for domain keywords (financial, healthcare, IoT, log, etc.)
4. Infer pipeline pattern from graph shape (linear, fan-out, fan-in, diamond, complex DAG)
5. Determine SLA profile from scheduling configs and source types

---

### Pass 2: Processor Analysis

**Purpose**: Identify and deeply categorize EVERY processor in the flow. No unknowns.

**What it examines:**
- Every processor's type, version, configuration properties
- Category classification (20 categories — see Knowledge Base section)
- Role classification (source / transform / route / sink / utility / monitor / error-handler)
- Conversion complexity (direct-map / template / specialized-translator / manual-review)
- All configured properties and their values
- All dynamic properties (NiFi EL expressions in property values)
- Scheduling strategy and period for each processor
- Enabled/disabled/running state
- Penalty and yield duration settings
- Bulletin level and auto-terminate relationships
- Concurrency settings (max concurrent tasks)

**Output — Processor Report:**
```json
{
  "total_processors": 23,
  "by_category": {
    "data_ingestion": 3,
    "transformation": 7,
    "routing": 4,
    "data_egress": 5,
    "scripting": 2,
    "monitoring": 2
  },
  "by_role": {
    "source": 3,
    "transform": 7,
    "route": 4,
    "sink": 5,
    "utility": 4
  },
  "by_conversion_complexity": {
    "direct_map": 15,
    "specialized_translator": 5,
    "manual_review": 3
  },
  "unknown_processors": [],
  "processors": [
    {
      "id": "proc-001",
      "name": "ConsumeKafka_sensors",
      "type": "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
      "short_type": "ConsumeKafka_2_6",
      "category": "data_ingestion",
      "role": "source",
      "conversion_complexity": "direct_map",
      "databricks_equivalent": "Spark Structured Streaming Kafka Source",
      "scheduling": { "strategy": "TIMER_DRIVEN", "period": "0 sec", "concurrent_tasks": 2 },
      "properties": {
        "bootstrap.servers": "kafka-prod:9092",
        "topic": "iot-sensors",
        "group.id": "nifi-sensor-consumer",
        "auto.offset.reset": "latest",
        "key.deserializer": "StringDeserializer",
        "value.deserializer": "StringDeserializer"
      },
      "dynamic_properties": {},
      "nel_expressions_found": 0,
      "controller_services_referenced": ["ssl-context-service-001"],
      "auto_terminate_relationships": ["parse.failure"],
      "warnings": []
    }
  ]
}
```

**Critical requirement**: The processor analysis must NEVER return `unknown_processors: [anything]`. Every NiFi processor type must be in the knowledge base. If an unrecognized processor appears, it should be flagged with maximum severity and classified as `manual_review` with the best-guess category based on name heuristics.

---

### Pass 3: Workflow Analysis

**Purpose**: Break down the exact order of operations — what executes when, where, and why.

**What it examines:**
- Full topological sort of the processor DAG
- Execution phases (which processors can run in parallel vs must run sequentially)
- Process Group hierarchy and nesting levels
- Connection queue configurations (backpressure thresholds, prioritizers, expiration)
- Scheduling relationships (which processors are timer-driven vs event-driven vs CRON)
- Load balancing strategies (round-robin, single-node, partition-by-attribute)
- Funnel aggregation points
- Input/Output port boundaries
- Remote Process Group connections (Site-to-Site)
- Wait/Notify synchronization points

**Output — Workflow Report:**
```json
{
  "execution_phases": [
    {
      "phase": 1,
      "name": "Ingestion",
      "parallel_processors": ["ConsumeKafka_sensors"],
      "execution_mode": "streaming",
      "trigger": "continuous"
    },
    {
      "phase": 2,
      "name": "Parse & Enrich",
      "parallel_processors": ["EvaluateJsonPath_parse"],
      "sequential_after": ["ConsumeKafka_sensors"],
      "execution_mode": "streaming",
      "reason": "single downstream connection, no fan-out"
    },
    {
      "phase": 3,
      "name": "Route",
      "parallel_processors": ["RouteOnAttribute_anomaly"],
      "sequential_after": ["EvaluateJsonPath_parse"],
      "fan_out": {
        "anomaly": ["PublishKafka_alerts", "PutEmail_oncall"],
        "normal": ["PutDatabaseRecord_delta"]
      }
    }
  ],
  "process_groups": [
    {
      "id": "pg-root",
      "name": "Root",
      "depth": 0,
      "children": ["pg-ingestion", "pg-processing", "pg-output"],
      "databricks_mapping": "Workflow Job (parent)"
    }
  ],
  "cycles_detected": [
    {
      "cycle_id": "cyc-001",
      "type": "error_retry",
      "processors": ["InvokeHTTP_api", "LogAttribute_error"],
      "edge": "failure",
      "remediation": "max_retries=3, wait_exponential"
    }
  ],
  "synchronization_points": [],
  "total_execution_phases": 5,
  "critical_path_length": 4,
  "parallelism_factor": 1.8
}
```

**Key analysis algorithms:**
- Topological sort with Kahn's algorithm (handles disconnected subgraphs)
- Phase grouping via longest-path computation (processors at same depth can parallelize)
- Cycle detection via Tarjan's SCC, classification into error_retry / pagination / data_reevaluation
- Critical path analysis for latency estimation
- Process Group → DAB Job/Task mapping

---

### Pass 4: Upstream Analysis

**Purpose**: Trace where EVERY piece of data comes from and what happens to it before it reaches each processor.

**What it examines:**
- All data sources (files, databases, message queues, APIs, generated data)
- Source format detection (JSON, CSV, Avro, Parquet, XML, binary, etc.)
- Source schema inference (from Record Readers, EvaluateJsonPath selectors, SQL column lists)
- Data lineage from source to each processor (full attribute chain)
- FlowFile attribute creation history (which processor created/modified each attribute)
- Content transformation history (what content modifications occurred at each step)
- Parameter Context variable injection points
- Controller Service dependencies (which services feed which processors)
- External system connections (databases, APIs, cloud storage, message brokers)

**Output — Upstream Report:**
```json
{
  "data_sources": [
    {
      "processor": "ConsumeKafka_sensors",
      "source_type": "message_queue",
      "system": "Apache Kafka",
      "location": "kafka-prod:9092 / topic: iot-sensors",
      "format": "JSON",
      "inferred_schema": {
        "device_id": "string",
        "temperature": "double",
        "humidity": "double",
        "timestamp": "long (epoch_ms)"
      },
      "databricks_equivalent": "spark.readStream.format('kafka')",
      "credentials_required": ["kafka.ssl.truststore", "kafka.ssl.keystore"],
      "secrets_mapping": {
        "kafka.ssl.truststore.location": "dbutils.secrets.get('nifi_migration', 'kafka-truststore')",
        "kafka.ssl.truststore.password": "dbutils.secrets.get('nifi_migration', 'kafka-truststore-pw')"
      }
    }
  ],
  "attribute_lineage": {
    "device_id": {
      "created_by": "EvaluateJsonPath_parse",
      "creation_method": "JsonPath extraction: $.device_id",
      "modified_by": [],
      "read_by": ["RouteOnAttribute_anomaly", "PutDatabaseRecord_delta"],
      "databricks_column": "device_id (StringType)"
    },
    "region": {
      "created_by": "LookupRecord_geo",
      "creation_method": "Lookup service: GeoIP → region field",
      "modified_by": ["UpdateAttribute_region"],
      "read_by": ["RouteOnAttribute_anomaly"],
      "databricks_column": "region (StringType)"
    }
  },
  "content_transformations": [
    {
      "processor": "EvaluateJsonPath_parse",
      "input_content": "Raw JSON bytes from Kafka",
      "output_content": "Same JSON bytes (attributes extracted, content unchanged)",
      "output_attributes_added": ["device_id", "temperature", "humidity", "timestamp"]
    }
  ],
  "external_dependencies": [
    {
      "type": "controller_service",
      "service": "GeoIPLookupService",
      "service_type": "SimpleDatabaseLookupService",
      "connection": "JDBC to geo_db",
      "databricks_equivalent": "Delta table join or broadcast lookup"
    }
  ],
  "parameter_injections": [
    {
      "context": "Production",
      "parameter": "kafka.brokers",
      "value": "kafka-prod:9092",
      "used_by": ["ConsumeKafka_sensors"],
      "databricks_mapping": "dbutils.widgets.get('kafka_brokers')"
    }
  ]
}
```

---

### Pass 5: Downstream Analysis

**Purpose**: Trace where EVERY piece of data goes and what happens to it after each processor.

**What it examines:**
- All data sinks (files, databases, message queues, APIs, emails, alerts)
- Sink format and schema (what exactly gets written and in what format)
- Data routing decisions (which rows go where based on what conditions)
- Data multiplication points (where one record becomes many — SplitJson, ForkRecord, fan-out)
- Data reduction points (where many records become one — MergeContent, aggregation)
- Data loss points (where records are intentionally dropped — auto-terminate, filter-out, ControlRate)
- Data duplication points (where same data goes to multiple destinations)
- Truncation or sampling (ControlRate, SampleRecord, LimitFlowFile)
- Error routing (where do failed records end up)
- Terminal vs intermediate writes (staging tables vs final output)

**Output — Downstream Report:**
```json
{
  "data_sinks": [
    {
      "processor": "PutDatabaseRecord_delta",
      "sink_type": "database",
      "system": "PostgreSQL (via JDBC)",
      "destination": "analytics.sensor_readings",
      "format": "Avro (via RecordWriter)",
      "write_mode": "INSERT",
      "databricks_equivalent": ".write.format('delta').mode('append').saveAsTable('catalog.schema.sensor_readings')",
      "credentials_required": ["jdbc-url", "jdbc-user", "jdbc-password"]
    },
    {
      "processor": "PublishKafka_alerts",
      "sink_type": "message_queue",
      "system": "Apache Kafka",
      "destination": "kafka-prod:9092 / topic: sensor-alerts",
      "format": "JSON",
      "databricks_equivalent": ".writeStream.format('kafka').option('topic', 'sensor-alerts')"
    }
  ],
  "data_flow_map": {
    "multiplication_points": [
      {
        "processor": "RouteOnAttribute_anomaly",
        "input_records": "N",
        "output_branches": {
          "anomaly": "~5% of N (temperature > 100)",
          "normal": "~95% of N"
        },
        "type": "conditional_split"
      }
    ],
    "reduction_points": [],
    "loss_points": [
      {
        "processor": "ConsumeKafka_sensors",
        "relationship": "parse.failure",
        "action": "auto-terminated",
        "impact": "Malformed JSON messages are silently dropped",
        "recommendation": "Route to error Delta table instead of dropping"
      }
    ],
    "duplication_points": [
      {
        "processor": "RouteOnAttribute_anomaly",
        "relationship": "anomaly",
        "destinations": ["PublishKafka_alerts", "PutEmail_oncall"],
        "type": "broadcast (same data to multiple sinks)"
      }
    ]
  },
  "error_routing": {
    "unhandled_failures": ["EvaluateJsonPath_parse.failure → auto-terminated"],
    "handled_failures": ["InvokeHTTP_api.failure → retry loop (3 attempts)"],
    "recommendation": "Add error Delta table for all unhandled failure routes"
  },
  "data_volume_estimates": {
    "ingestion_rate": "inferred continuous (scheduling period = 0 sec)",
    "fan_out_ratio": 1.05,
    "total_output_destinations": 3
  }
}
```

---

### Pass 6: Line-by-Line Code Analysis

**Purpose**: Examine EVERY line of configuration, every property value, every expression, every script body — and determine exactly what it is, what it does, why it's there, and whether/how it needs to be converted.

**What it examines:**
- Every processor property key and value
- Every NiFi Expression Language expression — parse it, understand it, map it
- Every embedded script body (ExecuteScript, ExecuteGroovyScript) — line by line
- Every SQL statement (ExecuteSQL, QueryDatabaseTable) — parse and translate dialect
- Every Jolt specification (JoltTransformJSON) — operation by operation
- Every regex pattern (ExtractText, RouteOnContent, ReplaceText) — validate and map
- Every JSON path expression (EvaluateJsonPath) — convert to PySpark selectors
- Every XPath/XQuery expression (EvaluateXPath, EvaluateXQuery) — convert to XML parsing
- Every Avro schema definition (in Record Readers/Writers)
- Every connection configuration (relationships, backpressure, prioritizer, expiration, load balancing)
- Every Controller Service configuration (all properties)
- Every Parameter Context entry

**Output — Line-by-Line Report:**

For EACH processor, produce a detailed breakdown:

```json
{
  "processor": "RouteOnAttribute_anomaly",
  "type": "RouteOnAttribute",
  "lines": [
    {
      "line_id": 1,
      "property": "Routing Strategy",
      "value": "Route to Property name",
      "line_type": "configuration",
      "what_it_does": "Configures processor to route FlowFiles to the relationship matching the property name whose condition evaluates to true",
      "why_its_there": "Standard routing strategy — each dynamic property defines a named route",
      "needs_conversion": false,
      "conversion_note": "This is a config directive, not code. The routing logic itself needs conversion."
    },
    {
      "line_id": 2,
      "property": "anomaly",
      "value": "${temperature:gt(100)}",
      "line_type": "nel_expression",
      "what_it_does": "Evaluates NiFi Expression Language: checks if the 'temperature' FlowFile attribute is greater than 100",
      "why_its_there": "Business rule: temperature readings above 100°F are classified as anomalies",
      "needs_conversion": true,
      "original_nel": "${temperature:gt(100)}",
      "parsed_ast": {
        "subject": "temperature",
        "functions": [{ "name": "gt", "args": ["100"] }]
      },
      "pyspark_equivalent": "F.col('temperature').cast('double') > 100",
      "conversion_confidence": 0.95,
      "conversion_notes": "Direct NEL→PySpark mapping. Assumes 'temperature' column exists as string, needs cast."
    },
    {
      "line_id": 3,
      "property": "high_humidity",
      "value": "${humidity:ge(80):and(${temperature:lt(50)})}",
      "line_type": "nel_expression_compound",
      "what_it_does": "Compound boolean: humidity >= 80 AND temperature < 50",
      "why_its_there": "Secondary routing condition for cold+humid conditions",
      "needs_conversion": true,
      "original_nel": "${humidity:ge(80):and(${temperature:lt(50)})}",
      "parsed_ast": {
        "subject": "humidity",
        "functions": [
          { "name": "ge", "args": ["80"] },
          { "name": "and", "args": ["${temperature:lt(50)}"] }
        ]
      },
      "pyspark_equivalent": "(F.col('humidity').cast('double') >= 80) & (F.col('temperature').cast('double') < 50)",
      "conversion_confidence": 0.90,
      "conversion_notes": "Nested NEL with compound boolean. Requires bitwise & operator for PySpark column conditions."
    }
  ]
}
```

**For embedded scripts (ExecuteScript, ExecuteGroovyScript):**

```json
{
  "processor": "ExecuteScript_transform",
  "type": "ExecuteScript",
  "script_engine": "python",
  "script_lines": [
    {
      "line_num": 1,
      "code": "import json",
      "line_type": "import",
      "what_it_does": "Imports Python JSON library",
      "needs_conversion": false,
      "note": "json is available in Databricks Python"
    },
    {
      "line_num": 2,
      "code": "flowFile = session.get()",
      "line_type": "nifi_session_api",
      "what_it_does": "Retrieves the next FlowFile from the incoming queue",
      "needs_conversion": true,
      "conversion": "Remove — Databricks uses DataFrame rows, not FlowFile objects",
      "replacement": "# DataFrame row processing (handled by Spark)"
    },
    {
      "line_num": 3,
      "code": "if flowFile is not None:",
      "line_type": "nifi_session_guard",
      "what_it_does": "Null check on FlowFile (NiFi pattern for empty queue)",
      "needs_conversion": true,
      "conversion": "Remove — Spark handles empty DataFrames natively"
    },
    {
      "line_num": 4,
      "code": "    content = session.read(flowFile).read()",
      "line_type": "nifi_content_read",
      "what_it_does": "Reads the FlowFile content bytes into memory",
      "needs_conversion": true,
      "conversion": "Replace with DataFrame column access",
      "replacement": "content = row['value']  # or appropriate column"
    },
    {
      "line_num": 5,
      "code": "    data = json.loads(content)",
      "line_type": "business_logic",
      "what_it_does": "Parses JSON content into Python dict",
      "needs_conversion": "partial",
      "conversion": "Use F.from_json() for columnar parsing, or keep as UDF logic",
      "replacement": "data = F.from_json(F.col('value'), schema)"
    },
    {
      "line_num": 6,
      "code": "    data['processed_at'] = str(datetime.now())",
      "line_type": "business_logic",
      "what_it_does": "Adds processing timestamp to the data dict",
      "needs_conversion": true,
      "conversion": "Use F.current_timestamp() column addition",
      "replacement": ".withColumn('processed_at', F.current_timestamp())"
    },
    {
      "line_num": 7,
      "code": "    session.transfer(flowFile, REL_SUCCESS)",
      "line_type": "nifi_session_api",
      "what_it_does": "Routes the FlowFile to the 'success' relationship",
      "needs_conversion": true,
      "conversion": "Remove — Spark DataFrame flows implicitly to next operation"
    }
  ],
  "script_summary": {
    "nifi_boilerplate_lines": 4,
    "business_logic_lines": 2,
    "total_lines": 7,
    "conversion_strategy": "Extract business logic (lines 5-6), discard NiFi session boilerplate, wrap as DataFrame transformation",
    "generated_pyspark": "df = df.withColumn('data', F.from_json(F.col('value'), schema)).withColumn('processed_at', F.current_timestamp())"
  }
}
```

**For SQL statements (ExecuteSQL, QueryDatabaseTable):**

```json
{
  "processor": "QueryDatabaseTable_orders",
  "sql_analysis": {
    "original_sql": "SELECT o.order_id, o.customer_id, c.name, o.amount, o.created_at FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.created_at > '${last_run_timestamp}'",
    "sql_lines": [
      {
        "clause": "SELECT",
        "columns": ["o.order_id", "o.customer_id", "c.name", "o.amount", "o.created_at"],
        "what_it_does": "Selects 5 columns from joined tables",
        "needs_conversion": "dialect",
        "note": "Column selection translates directly to .select() in Spark SQL"
      },
      {
        "clause": "FROM/JOIN",
        "tables": ["orders o", "customers c"],
        "join_type": "INNER JOIN",
        "join_condition": "o.customer_id = c.id",
        "what_it_does": "Joins orders with customers on customer_id",
        "needs_conversion": true,
        "conversion": "If tables exist in Unity Catalog, use spark.sql(). If external JDBC, use spark.read.format('jdbc') with pushdown."
      },
      {
        "clause": "WHERE",
        "condition": "o.created_at > '${last_run_timestamp}'",
        "nel_reference": "${last_run_timestamp}",
        "what_it_does": "Incremental fetch — only rows newer than last run",
        "needs_conversion": true,
        "conversion": "Replace NEL variable with Databricks widget or checkpoint-based incremental logic"
      }
    ],
    "dialect_source": "PostgreSQL (inferred from JDBC URL)",
    "dialect_target": "Spark SQL / ANSI SQL",
    "dialect_changes_needed": ["DATE functions may differ", "String concatenation syntax"]
  }
}
```

---

## Complete NiFi Knowledge Base

The V6 system must have **hardcoded knowledge** of every NiFi component. Nothing should be unknown.

### Processor Knowledge Base (500+ Processors, 20 Categories)

Every processor entry must include:
- Full class name and short name
- Category and role
- All standard properties with types and defaults
- All standard relationships (output ports)
- Databricks equivalent approach
- PySpark code template
- Required imports and packages
- Conversion complexity rating
- Known edge cases and limitations

#### Category 1: Data Ingestion (35+ processors)
| Processor | Databricks Equivalent | Template Strategy |
|---|---|---|
| GetFile | Auto Loader (cloudFiles) | Streaming read with schema evolution |
| ListFile / FetchFile | Auto Loader (cloudFiles) | Two-phase list+fetch → single Auto Loader |
| GetHDFS / ListHDFS / FetchHDFS | Auto Loader or spark.read | Path-based file ingestion |
| GetS3Object / ListS3 / FetchS3Object | Auto Loader with S3 path | Cloud-native ingestion |
| GetAzureBlobStorage / ListAzureBlobStorage | Auto Loader with abfss:// | Azure-native ingestion |
| GetGCSObject / ListGCSBucket / FetchGCSObject | Auto Loader with gs:// | GCP-native ingestion |
| GetSFTP / ListSFTP / FetchSFTP | Volume mount + Auto Loader | Stage to Volume, then process |
| GetFTP / ListFTP / FetchFTP | Volume mount + Auto Loader | Stage to Volume, then process |
| ConsumeKafka / ConsumeKafka_2_6 | Spark Kafka source | readStream.format("kafka") |
| ConsumeKafkaRecord_2_6 | Spark Kafka + schema | readStream with from_json() |
| ConsumeAMQP / ConsumeJMS | Custom source or Kafka bridge | May need connector library |
| ConsumeKinesisStream | Spark Kinesis source | readStream.format("kinesis") |
| ConsumeAzureEventHub | Spark Event Hub source | readStream with eventhubs connector |
| ConsumeGCPubSub | Spark Pub/Sub source | readStream with pubsub connector |
| QueryDatabaseTable / QueryDatabaseTableRecord | spark.read.format("jdbc") | JDBC batch read |
| GenerateTableFetch | spark.read.format("jdbc") with partitioning | Parallel JDBC read |
| ExecuteSQL / ExecuteSQLRecord | spark.read.format("jdbc") | Direct SQL execution |
| GenerateFlowFile | spark.createDataFrame() | Test data generation |
| GetHTTP / InvokeHTTP (as source) | requests + spark.createDataFrame | API ingestion |
| ListenHTTP / HandleHttpRequest | Not applicable (webhook) | Requires API gateway |
| ListenTCP / ListenUDP / ListenSyslog | Spark socket source or custom | Network listeners |
| TailFile | Auto Loader (streaming append) | Tail-like semantics via streaming |
| GetMongo / GetMongoRecord | spark.read.format("mongo") | MongoDB connector |
| GetElasticsearch | spark.read.format("es") | Elasticsearch connector |
| GetCouchbaseKey / GetCouchbaseDocument | Couchbase Spark connector | Custom connector |
| GetSolr / QuerySolr | Solr JDBC or API | Custom ingestion |
| GetHBaseRow / ScanHBase | HBase Spark connector | Custom connector |
| CaptureChangeMySQL | Delta Live Tables CDC | spark.readStream.format("delta").option("readChangeFeed") |
| GetSNS / GetSQS | Boto3 + createDataFrame | AWS SDK ingestion |
| ConsumeSlack | Slack API + createDataFrame | API-based ingestion |
| ConsumeWindowsEventLog | Log ingestion | Custom parser |

#### Category 2: Data Egress / Sinks (50+ processors)
| Processor | Databricks Equivalent | Template Strategy |
|---|---|---|
| PutFile | .write.format("delta") or dbutils.fs | File output |
| PutHDFS | .write.format("delta") | HDFS-compatible write |
| PutS3Object | .write.format("delta").save("s3://...") | S3 write |
| PutAzureBlobStorage / PutAzureDataLakeStorage | .write.format("delta").save("abfss://...") | Azure write |
| PutGCSObject | .write.format("delta").save("gs://...") | GCS write |
| PutDatabaseRecord / PutSQL | .write.format("jdbc") | JDBC write |
| PublishKafka / PublishKafka_2_6 | .writeStream.format("kafka") | Kafka publish |
| PutKinesisStream / PutKinesisFirehose | Kinesis connector write | AWS streaming write |
| PutAzureEventHub | Event Hub connector write | Azure streaming write |
| PublishGCPubSub | Pub/Sub connector write | GCP streaming write |
| PutMongo / PutMongoRecord | MongoDB connector write | Document store write |
| PutElasticsearch / PutElasticsearchHttp | Elasticsearch connector write | Search index write |
| PutCouchbaseDocument / PutCouchbaseKey | Couchbase connector write | Document store write |
| PutHBaseCell / PutHBaseJSON | HBase connector write | Column store write |
| PutHiveQL / PutHiveStreaming | spark.sql("INSERT INTO ...") | Hive write |
| PutKudu | Kudu connector write | Kudu write |
| PutSolr | Solr API write | Search index write |
| PutInfluxDB | InfluxDB connector write | Time-series write |
| PutDynamoDB | Boto3 DynamoDB write | DynamoDB write |
| PutSNS / PutSQS | Boto3 SNS/SQS send | AWS messaging |
| PutEmail | smtplib or Databricks notification | Email send |
| PutSlack / PostSlack | Slack webhook API | Slack notification |
| PutSFTP / PutFTP | paramiko / ftplib | File transfer |
| PutTCP / PutUDP / PutSyslog | socket library | Network output |
| PutDelta / PutDeltaLake | .write.format("delta") | Native Delta write |
| PutIceberg | .write.format("iceberg") | Iceberg write |
| PutHudi | .write.format("hudi") | Hudi write |
| PutORC / PutParquet | .write.format("orc/parquet") | Columnar write |
| PutBigQueryBatch / PutBigQueryStreaming | BigQuery connector | GCP analytics write |
| PutRedshift | Redshift connector | AWS analytics write |
| PutSnowflakeInternalStage | Snowflake connector | Snowflake write |
| PutCassandraQL / QueryCassandra | Cassandra connector | Cassandra write |
| PutAzureCosmosDB | Cosmos DB connector | Azure NoSQL write |
| PutAzureDataExplorer | Kusto connector | Azure analytics write |
| PutRedis | Redis connector | Cache write |
| PutLambda | Boto3 Lambda invoke | Serverless trigger |
| PutCloudWatchMetric / PutCloudWatchLogs | CloudWatch API | AWS monitoring |
| PutSalesforceRecord | Salesforce API | CRM write |

#### Category 3: Transformation (40+ processors)
| Processor | Databricks Equivalent |
|---|---|
| UpdateAttribute | .withColumn() / .withColumnRenamed() |
| ReplaceText | .withColumn(regexp_replace()) |
| JoltTransformJSON | .select() with struct manipulation |
| ConvertRecord | .select() with schema cast |
| ConvertJSONToAvro / ConvertAvroToJSON / etc. | .select(from_json/to_json/from_avro/to_avro) |
| EvaluateJsonPath | .select(get_json_object()) / .withColumn() |
| EvaluateXPath / EvaluateXQuery | xpath() UDF or xml parsing |
| SplitJson | .select(explode(from_json())) |
| SplitXml | xml parsing + explode |
| SplitRecord / ForkRecord | .filter() or explode() |
| SplitContent / SplitText | .withColumn(split()) |
| MergeContent / MergeRecord | .unionByName() or .groupBy().agg() |
| ExtractText | .withColumn(regexp_extract()) |
| ExtractGrok | .withColumn(regexp_extract()) with grok patterns |
| FlattenJson | .select(col("nested.*")) |
| TransformXml | XSLT via UDF |
| ConvertCharacterSet | .withColumn(encode/decode) |
| CompressContent / UnpackContent | Python gzip/zip UDF |
| EncryptContent / DecryptContent | Cryptography UDF |
| HashContent / HashAttribute | .withColumn(sha2/md5) |
| Base64EncodeContent / Base64DecodeContent | .withColumn(base64/unbase64) |
| LookupRecord / LookupAttribute | .join() with broadcast lookup table |
| UpdateRecord | .withColumn() with struct modification |
| QueryRecord | spark.sql() on temp view |
| ValidateRecord / ValidateJSON / ValidateXML / ValidateCsv | Data quality expectations |
| NormalizeRecord / DenormalizeRecord | Pivot/unpivot operations |
| PartitionRecord | .repartition() |
| CalculateRecordStats | .agg() with statistical functions |
| GeoEnrichIP | IP lookup UDF or broadcast join |
| AttributesToJSON | .withColumn(to_json(struct())) |
| ConvertJSONToSQL | SQL generation from JSON schema |

#### Category 4: Routing & Mediation (15+ processors)
| Processor | Databricks Equivalent |
|---|---|
| RouteOnAttribute | .filter() with branching |
| RouteOnContent | .filter(col.contains/rlike) |
| RouteText | .filter() with text matching |
| RouteBasedOnContent | .filter() with content inspection |
| DistributeLoad | .repartition() or random routing |
| ControlRate | Rate limiting — Trigger.ProcessingTime() |
| SampleRecord | .sample(fraction) |
| LimitFlowFile | .limit(n) |
| DetectDuplicate / DetectDuplicateRecord | .dropDuplicates() |
| DeduplicateRecord | .dropDuplicates(subset=[]) |
| EnforceOrder | .orderBy() |
| Wait / Notify | dbutils.jobs.taskValues or Delta table signal |
| RetryFlowFile | try/except with retry decorator |

#### Category 5: Scripting (10+ processors)
| Processor | Conversion Strategy |
|---|---|
| ExecuteScript (Python) | Extract business logic → PySpark UDF or DataFrame ops |
| ExecuteScript (Groovy) | Manual port to Python UDF |
| ExecuteScript (Jython) | Port to native Python |
| ExecuteScript (Clojure/Ruby) | Manual port to Python |
| ExecuteGroovyScript | Manual port to Python UDF |
| ExecuteStreamCommand | subprocess.run() or dbutils.notebook.run() |
| ExecuteProcess | subprocess.run() |
| InvokeScriptedProcessor | Extract logic → UDF |
| ScriptedTransformRecord | Extract logic → mapInPandas UDF |
| ExecutePythonProcessor | Direct port (NiFi 2.0 native Python) |

#### Category 6: HTTP/API (10+ processors)
| Processor | Databricks Equivalent |
|---|---|
| InvokeHTTP | requests library or spark UDF |
| HandleHttpRequest / HandleHttpResponse | Not directly translatable (webhook) |
| InvokeAWSGatewayApi | Boto3 API Gateway call |
| InvokeGRPC | grpcio library |
| GetHTTP / PostHTTP | requests.get/post |
| ListenHTTP | Requires external trigger mechanism |

#### Category 7: Database (65+ processors)
Full coverage of MySQL, PostgreSQL, Oracle, SQL Server, MongoDB, Elasticsearch, Cassandra, Couchbase, Redis, Kudu, InfluxDB, HBase, Solr, DynamoDB, Cosmos DB, BigQuery, Redshift, Snowflake, Teradata, DB2, Sybase, Trino — all mapped to spark.read/write.format("jdbc") or native connectors.

#### Category 8-20: Cloud, Hadoop, Email, FTP, Encryption, Record, State, Monitoring, etc.
(All covered in the processor catalog — see `NIFI_PROCESSOR_CATALOG.md`)

### NiFi Expression Language — Complete Function Map (100+ functions)

Every NEL function must have a hardcoded PySpark equivalent:

#### String Functions (30+)
| NEL Function | PySpark Equivalent |
|---|---|
| `toUpper()` | `F.upper(col)` |
| `toLower()` | `F.lower(col)` |
| `trim()` | `F.trim(col)` |
| `length()` | `F.length(col)` |
| `substring(start, length)` | `F.substring(col, start+1, length)` |
| `substringBefore(value)` | `F.substring_index(col, value, 1)` |
| `substringAfter(value)` | `F.expr(f"substring_index({col}, '{value}', -1)")` |
| `replace(search, replace)` | `F.regexp_replace(col, search, replace)` |
| `replaceFirst(regex, replace)` | `F.regexp_replace(col, regex, replace)` (first only via regex) |
| `replaceAll(regex, replace)` | `F.regexp_replace(col, regex, replace)` |
| `append(value)` | `F.concat(col, F.lit(value))` |
| `prepend(value)` | `F.concat(F.lit(value), col)` |
| `contains(value)` | `col.contains(value)` |
| `startsWith(value)` | `col.startswith(value)` |
| `endsWith(value)` | `col.endswith(value)` |
| `equals(value)` | `col == value` |
| `equalsIgnoreCase(value)` | `F.lower(col) == value.lower()` |
| `matches(regex)` | `col.rlike(regex)` |
| `find(regex)` | `F.regexp_extract(col, regex, 0)` |
| `indexOf(value)` | `F.locate(value, col) - 1` |
| `lastIndexOf(value)` | Custom UDF |
| `split(delimiter)` | `F.split(col, delimiter)` |
| `padLeft(length, char)` | `F.lpad(col, length, char)` |
| `padRight(length, char)` | `F.rpad(col, length, char)` |
| `urlEncode()` | UDF with urllib.parse.quote |
| `urlDecode()` | UDF with urllib.parse.unquote |
| `escapeJson()` | UDF with json.dumps |
| `escapeHtml3()` / `escapeHtml4()` | UDF with html.escape |
| `escapeXml()` | UDF with xml.sax.saxutils.escape |
| `jsonPath(path)` | `F.get_json_object(col, path)` |

#### Math Functions (15+)
| NEL Function | PySpark Equivalent |
|---|---|
| `plus(value)` | `col + value` |
| `minus(value)` | `col - value` |
| `multiply(value)` | `col * value` |
| `divide(value)` | `col / value` |
| `mod(value)` | `col % value` |
| `toNumber()` | `col.cast(DoubleType())` |
| `gt(value)` | `col > value` |
| `ge(value)` | `col >= value` |
| `lt(value)` | `col < value` |
| `le(value)` | `col <= value` |
| `math:abs()` | `F.abs(col)` |
| `math:ceil()` | `F.ceil(col)` |
| `math:floor()` | `F.floor(col)` |
| `math:round()` | `F.round(col)` |
| `math:sqrt()` | `F.sqrt(col)` |
| `math:log()` | `F.log(col)` |
| `math:pow(exp)` | `F.pow(col, exp)` |
| `math:random()` | `F.rand()` |

#### Date/Time Functions (10+)
| NEL Function | PySpark Equivalent |
|---|---|
| `now()` | `F.current_timestamp()` |
| `format(pattern)` | `F.date_format(col, pattern)` |
| `toDate(pattern)` | `F.to_timestamp(col, pattern)` |
| `toDate(pattern, timezone)` | `F.from_utc_timestamp(F.to_timestamp(col, pattern), timezone)` |
| `getYear()` | `F.year(col)` |
| `getMonth()` | `F.month(col)` |
| `getDayOfMonth()` | `F.dayofmonth(col)` |
| `getHour()` | `F.hour(col)` |
| `getMinute()` | `F.minute(col)` |
| `getSecond()` | `F.second(col)` |

#### Boolean/Logic Functions (15+)
| NEL Function | PySpark Equivalent |
|---|---|
| `isNull()` | `col.isNull()` |
| `notNull()` | `col.isNotNull()` |
| `isEmpty()` | `(col.isNull()) \| (F.trim(col) == "")` |
| `isBlank()` | `(col.isNull()) \| (F.trim(col) == "")` |
| `not()` | `~condition` |
| `and(expr)` | `condition & other` |
| `or(expr)` | `condition \| other` |
| `ifElse(true_val, false_val)` | `F.when(condition, true_val).otherwise(false_val)` |
| `in(val1, val2, ...)` | `col.isin([val1, val2, ...])` |
| `literal(value)` | `F.lit(value)` |

#### Encoding Functions (10+)
| NEL Function | PySpark Equivalent |
|---|---|
| `base64Encode()` | `F.base64(col)` |
| `base64Decode()` | `F.unbase64(col)` |
| `hash(algorithm)` | `F.sha2(col, 256)` / `F.md5(col)` |
| `UUID()` | `F.expr("uuid()")` |

#### System/Subjectless Functions (10+)
| NEL Function | PySpark Equivalent |
|---|---|
| `now()` | `F.current_timestamp()` |
| `UUID()` | `F.expr("uuid()")` |
| `hostname()` | `F.lit(socket.gethostname())` |
| `ip()` | `F.lit(socket.gethostbyname(socket.gethostname()))` |
| `nextInt()` | `F.monotonically_increasing_id()` |
| `thread()` | `F.spark_partition_id()` |

#### Multi-Value Functions (10+)
| NEL Function | PySpark Equivalent |
|---|---|
| `anyAttribute(attr1, attr2, ...)` | Multiple column OR check |
| `allAttributes(attr1, attr2, ...)` | Multiple column AND check |
| `anyMatchingAttribute(regex)` | Regex column name filter |
| `allMatchingAttributes(regex)` | Regex column name filter |
| `anyDelineatedValue(delimiter)` | `F.array_contains(F.split(col, delim), value)` |
| `allDelineatedValues(delimiter)` | `F.forall(F.split(col, delim), lambda)` |
| `count()` | `F.size(F.split(col, delim))` |
| `join(delimiter)` | `F.concat_ws(delim, col)` |

### Controller Service Knowledge Base (150+ services)

Every controller service type mapped:

| Service Type | Databricks Equivalent |
|---|---|
| DBCPConnectionPool | Unity Catalog Connection + dbutils.secrets |
| DBCPConnectionPoolLookup | Dynamic connection routing |
| HikariCPConnectionPool | Unity Catalog Connection + dbutils.secrets |
| StandardSSLContextService | Cluster SSL configuration |
| StandardRestrictedSSLContextService | Cluster SSL configuration |
| JsonTreeReader / JsonPathReader | from_json() schema |
| CSVReader | csv format options |
| AvroReader | from_avro() |
| ParquetReader | parquet format |
| XMLReader | xml format or UDF |
| GrokReader | regex extraction |
| JsonRecordSetWriter / CSVRecordSetWriter | to_json() / csv write options |
| AvroRecordSetWriter | to_avro() |
| ConfluentSchemaRegistry | Schema Registry connector |
| AvroSchemaRegistry | Embedded schema |
| HortonworksSchemaRegistry | Schema Registry connector |
| DistributedMapCacheClientService | Delta table or Redis state |
| DistributedMapCacheServer | Delta table state |
| AWSCredentialsProviderControllerService | Instance Profile or dbutils.secrets |
| AzureStorageCredentialsControllerService_v12 | Service Principal or Managed Identity |
| GCPCredentialsControllerService | Service Account credentials |
| StandardOAuth2AccessTokenProvider | OAuth2 token via dbutils.secrets |
| KeytabCredentialsService | Kerberos not needed (Unity Catalog) |
| PrometheusReportingTask | Databricks system tables |
| SiteToSiteProvenanceReportingTask | Unity Catalog Data Lineage |

### Connection Relationship Knowledge Base (25+ types)

| Relationship | When Used | Conversion Strategy |
|---|---|---|
| `success` | Universal — processor completed normally | Default DataFrame pass-through |
| `failure` | Universal — processor encountered error | try/except → error Delta table |
| `matched` | RouteOnAttribute — condition is true | .filter(condition) |
| `unmatched` | RouteOnAttribute — no condition matched | .filter(~any_condition) |
| `original` | EvaluateJsonPath, ExtractText — unmodified input | Preserve original alongside extracted |
| `split` | SplitJson, SplitContent — individual pieces | explode() output |
| `merged` | MergeContent — combined result | unionByName result |
| `response` | InvokeHTTP — API response body | Response DataFrame |
| `request` | HandleHttpRequest — incoming request | Request DataFrame |
| `retry` | InvokeHTTP — retriable error | Retry logic wrapper |
| `no retry` | InvokeHTTP — non-retriable error | Error table write |
| `valid` | ValidateRecord — passes validation | .filter(valid_condition) |
| `invalid` | ValidateRecord — fails validation | .filter(~valid_condition) |
| `compatible` | Schema compatibility check | Pass-through |
| `incompatible` | Schema incompatibility | Error handling |
| `results` | ExecuteSQL — query results | Query result DataFrame |
| `inactive` | MonitorActivity — no data received | Alerting logic |
| `activity.restored` | MonitorActivity — data resumed | Alert resolution |
| `wait` | Wait processor — waiting for signal | Delta table polling |
| `expired` | Wait processor — timeout reached | Timeout handling |
| `duplicate` | DetectDuplicate — already seen | Separate DataFrame |
| `non-duplicate` | DetectDuplicate — new record | Pass-through |

---

## Gap Analysis: What's Broken Today

### CRITICAL GAPS (Notebook won't execute)

#### GAP 1: No Inter-Processor Data Flow Wiring
Each cell generates independent `df_X = ...` variables. No cell references the previous cell's output. Data doesn't flow.

#### GAP 2: No Terminal Write Statements
No `.write()` or `.writeStream()` calls. The pipeline produces DataFrames that vanish into thin air.

#### GAP 3: Streaming vs Batch Decision Not Applied
The execution mode analyzer detects batch/streaming but the generator ignores it. No triggers on any streaming operations.

### HIGH GAPS (Notebook runs but produces wrong results)

#### GAP 4: Connection Relationship Routing Ignored
All 25+ relationship types are parsed but never acted upon during generation.

#### GAP 5: FlowFile Attributes Not Materialized as Columns
Attributes must become DataFrame columns. The analyzer tracks them but the generator doesn't use the data.

#### GAP 6: Credentials Embedded as Plaintext
JDBC URLs, passwords, API keys appear as string literals instead of `dbutils.secrets.get()`.

#### GAP 7: No Checkpoint Injection on Streaming Writes
Structured Streaming fails immediately without `checkpointLocation`.

#### GAP 8: ~200 NiFi Processors Missing from Knowledge Base
302 mapped out of 500+. Every unknown processor is a conversion failure.

#### GAP 9: ~10 NEL Functions Missing
90 of ~100+ functions covered. Missing functions produce broken expressions.

### MEDIUM GAPS (Notebook works but suboptimal)

#### GAP 10: Process Groups Flattened
Nested groups should produce separate notebooks/workflow tasks, not a flat list.

#### GAP 11: Cycle Remediation Not in Generated Code
Cycles detected but retry/pagination logic never generated.

#### GAP 12: Task Clustering Not Used
Contiguous transforms should merge into single DataFrame chains for Catalyst optimization.

#### GAP 13: Schema Evolution Options Incomplete
Auto Loader sources missing `schemaLocation` and `inferColumnTypes` consistently.

#### GAP 14: No Parameterized Configuration Cell
Parameter Contexts extracted but not generating runtime widget/variable cells.

#### GAP 15: Only 3 Specialized Translators
RouteOnAttribute, JoltTransformJSON, ExecuteScript have custom translation. The other 497+ processors rely entirely on YAML templates which may be insufficient for complex configurations.

#### GAP 16: No 6-Pass Analysis Engine
The current analysis is a single pass. The 6-pass deep analysis described above doesn't exist yet.

---

## Implementation Plan: 12 Phases

### Phase 1: Complete NiFi Knowledge Base
**New files**: `nifi_processor_kb.py`, `nifi_nel_kb.py`, `nifi_services_kb.py`, `nifi_relationships_kb.py`

Hardcode EVERY NiFi component into structured Python dictionaries:

1. **500+ processor definitions** with: type, category, role, relationships, properties, defaults, Databricks equivalent, code template, imports, conversion complexity, edge cases
2. **100+ NEL function mappings** with: function name, argument spec, PySpark equivalent (col mode), Python equivalent (string mode), edge cases
3. **150+ controller service definitions** with: service type, configuration properties, Databricks equivalent, secrets mapping
4. **25+ relationship type definitions** with: name, when used, conversion strategy, code pattern
5. **All property types** with: NiFi type, value format, Databricks equivalent, validation rules

The knowledge base is the **single source of truth**. Parsers, analyzers, mappers, and generators all reference it. No more scattered hardcoded strings.

---

### Phase 2: 6-Pass Analysis Engine
**New files**: `functional_analyzer.py`, `processor_analyzer.py`, `workflow_analyzer.py`, `upstream_analyzer.py`, `downstream_analyzer.py`, `line_by_line_analyzer.py`, `analysis_orchestrator.py`

Build each of the 6 analysis passes as independent modules:

#### 2.1 Functional Analyzer
- Input: parsed flow + processor KB
- Classify flow purpose (ETL, CDC, streaming, API orchestration, file processing)
- Identify functional zones via connected-component analysis
- Detect data domains from processor names and properties
- Infer pipeline pattern from graph shape
- Determine SLA profile from scheduling configs
- Output: FunctionalReport

#### 2.2 Processor Analyzer
- Input: parsed flow + processor KB
- Match every processor against the KB (zero unknowns)
- Categorize into 20 categories
- Classify roles (source/transform/route/sink/utility)
- Rate conversion complexity (direct-map / template / specialized / manual)
- Detect all dynamic properties containing NEL expressions
- Identify all controller service references
- Flag all auto-terminated relationships
- Output: ProcessorReport

#### 2.3 Workflow Analyzer
- Input: parsed flow + DAG + cycle data
- Compute topological sort
- Group into execution phases (parallel vs sequential)
- Map process group hierarchy
- Analyze all connection queue configs
- Detect scheduling relationships
- Identify synchronization points (Wait/Notify)
- Compute critical path and parallelism factor
- Output: WorkflowReport

#### 2.4 Upstream Analyzer
- Input: parsed flow + processor KB + controller services
- Identify all data sources and their types
- Detect source format from Record Readers and downstream processors
- Infer source schema from JsonPath/XPath selectors, SQL columns, Record schemas
- Build complete attribute lineage from source to each processor
- Map all controller service dependencies
- Extract all parameter context injection points
- Map credentials to Databricks secrets
- Output: UpstreamReport

#### 2.5 Downstream Analyzer
- Input: parsed flow + processor KB + connections
- Identify all data sinks and their types
- Map data routing decisions (which rows go where)
- Detect data multiplication points (split, fork, fan-out)
- Detect data reduction points (merge, aggregate, fan-in)
- Detect data loss points (auto-terminate, filter-out, rate limiting)
- Detect data duplication points (same data to multiple sinks)
- Map error routing (where do failed records end up)
- Classify terminal vs intermediate writes
- Output: DownstreamReport

#### 2.6 Line-by-Line Analyzer
- Input: parsed flow + processor KB + NEL KB
- For EVERY processor, examine EVERY property:
  - Classify the line type (configuration, nel_expression, sql_statement, script_code, json_path, xpath, regex, jolt_spec, avro_schema, static_value)
  - Describe what it does in plain English
  - Determine if it needs conversion and why
  - If NEL: parse the AST, map to PySpark, compute confidence
  - If SQL: parse dialect, identify translation needs
  - If script: classify each line (nifi_boilerplate vs business_logic)
  - If Jolt: parse operations, map to PySpark struct ops
  - If regex: validate pattern, check Spark compatibility
  - If JsonPath: convert to get_json_object() selectors
- Output: LineByLineReport

#### 2.7 Analysis Orchestrator
- Runs all 6 passes in dependency order (some can parallelize)
- Merges results into a unified `DeepAnalysisResult`
- Generates a human-readable analysis summary
- This becomes the input to the conversion engine

---

### Phase 3: Pipeline Wiring Engine
**New file**: `pipeline_wirer.py`

The core fix that makes notebooks runnable:

1. **Topological sort** → determines cell order
2. **DataFrame variable chaining** → each cell references upstream output
3. **Connection relationship routing** → success/failure/matched/unmatched all handled
4. **Fan-out handling** → one processor feeding multiple downstream via different relationships
5. **Fan-in / merge handling** → multiple processors feeding one (unionByName)
6. **Broadcast handling** → same data to multiple sinks

---

### Phase 4: Sink Generation & Write Semantics
**New file**: `sink_generator.py`

1. Detect all terminal processors (no downstream connections or sink role)
2. Generate correct write call per sink type (50+ sink processors mapped)
3. Inject checkpoint on every writeStream
4. Select correct trigger (AvailableNow vs ProcessingTime) from workflow analysis
5. Generate error table writes for failure routes

---

### Phase 5: Credential Security & Configuration
**New file**: `config_generator.py`

1. Replace ALL plaintext credentials with `dbutils.secrets.get()`
2. Generate config cell with widgets for all parameters
3. Generate secrets setup documentation cell
4. Map Parameter Contexts to runtime widgets
5. Deduplicate and organize all imports

---

### Phase 6: FlowFile Attribute Materialization

1. Inject core attributes (filename, uuid, timestamp) at ingestion
2. Convert UpdateAttribute → withColumn for every attribute
3. Convert EvaluateJsonPath extractions → select/withColumn
4. Validate attribute availability at each routing point
5. Track attribute lineage through the notebook

---

### Phase 7: Complete NEL Transpiler

Expand from 90 to 100+ functions:
1. Add missing string functions (lastIndexOf, etc.)
2. Add missing date arithmetic (toNumber on dates, epoch conversions)
3. Add missing multi-value functions (allDelineatedValues, etc.)
4. Handle deeply nested compound expressions: `${literal(${a:equals('x')}):or(${b:equals('y')})}`
5. Handle subject-less function chaining: `${now():format('yyyy-MM-dd')}`
6. Handle literal values in NEL: `${literal('fixed'):append(${var})}`

---

### Phase 8: Specialized Processor Translators

Expand from 3 to 15+ specialized translators:

1. **RouteOnAttribute** (exists — enhance for compound expressions)
2. **JoltTransformJSON** (exists — add wildcard and chainSpec support)
3. **ExecuteScript** (exists — enhance line-by-line extraction)
4. **EvaluateJsonPath** → generate precise get_json_object/from_json select chains
5. **QueryDatabaseTable** → generate incremental JDBC reads with watermark tracking
6. **ConvertRecord** → generate schema-aware format conversions
7. **LookupRecord** → generate broadcast join with lookup table
8. **MergeContent** → generate unionByName or collect_list aggregation
9. **SplitJson / SplitRecord** → generate explode() operations
10. **InvokeHTTP** → generate requests library calls with retry
11. **Wait / Notify** → generate Delta table signal mechanism
12. **DetectDuplicate** → generate dropDuplicates with state tracking
13. **ValidateRecord** → generate data quality expectations (DLT expects)
14. **ControlRate** → generate Trigger.ProcessingTime rate limiting
15. **GenerateTableFetch** → generate partitioned JDBC parallel reads

---

### Phase 9: Task Clustering in Generation

1. Merge contiguous transform chains into single DataFrame chain operations
2. Eliminate intermediate DataFrame variables
3. Add markdown cells documenting which processors are merged
4. Preserve readability with inline comments

---

### Phase 10: Process Group → Multi-Notebook Workflows

1. Top-level Process Groups → separate notebook files
2. Inter-group communication via Delta staging tables
3. DAB workflow with `notebook_task` per group
4. `dbutils.jobs.taskValues` for metadata passing
5. Input/Output Ports → read/write from shared Delta tables

---

### Phase 11: Cycle Remediation in Generated Code

1. Error retry cycles → `tenacity` retry decorator or try/except with loop
2. Pagination cycles → while loop with break condition
3. Data reevaluation cycles → for_each_task in DAB or collect/re-process pattern
4. DAB retry configuration (max_retries, min_retry_interval_millis)

---

### Phase 12: Enhanced Validation (6-Point Quality Gate)

Upgrade the validator to check:

1. **Syntax validation** — py_compile on all generated code
2. **Import validation** — all imports available on DBR 15+
3. **Data flow validation** — every referenced DataFrame defined in prior cell
4. **Write validation** — at least one terminal write exists
5. **Streaming validation** — all writeStream calls have checkpoint + trigger
6. **Security validation** — zero plaintext credentials
7. **Completeness validation** — every NiFi processor has a corresponding notebook cell
8. **NEL validation** — all expressions transpiled with confidence > 0.8
9. **Relationship validation** — all non-success relationships properly handled
10. **Schema validation** — inferred schemas consistent across pipeline
11. **DAG validation** — cell order matches topological sort
12. **Cluster validation** — required packages available on target DBR version

---

## Validation & Quality Gates

### Pre-Conversion Gate (After 6-Pass Analysis)
| Check | Pass Criteria |
|---|---|
| All processors recognized | `unknown_processors == []` |
| All NEL expressions parsed | Zero parse failures |
| All controller services resolved | All service UUIDs mapped |
| All connections have valid relationships | No orphan connections |
| Workflow DAG is valid | Topological sort succeeds |
| All data sources identified | `upstream.data_sources.length > 0` |
| All data sinks identified | `downstream.data_sinks.length > 0` |

### Post-Conversion Gate (After Notebook Generation)
| Check | Pass Criteria |
|---|---|
| Python syntax valid | `py_compile.compile()` passes |
| All imports available on DBR 15+ | Package resolution succeeds |
| Data flows end-to-end | Every `df_X` reference is defined upstream |
| No dangling variables | No unbound `df` references |
| All streams have checkpoints | Every `writeStream` has checkpointLocation |
| All streams have triggers | Every `writeStream` has trigger() |
| No plaintext secrets | Zero hardcoded passwords/keys |
| Terminal writes exist | At least one `.write` or `.writeStream` |
| Config cell present | CATALOG, SCHEMA, SECRETS_SCOPE defined |
| DAG order correct | Cell order = topological sort |
| Relationships routed | failure → error table, matched/unmatched → filter |
| Processor coverage 100% | Every parsed processor has a notebook cell |

---

## Architecture Diagram

```
                           NiFi Flow XML/JSON (or any ETL format)
                                        │
                           ┌────────────▼────────────┐
                           │    PARSER (enhanced)     │
                           │  Processors, Connections │
                           │  Services, Parameters    │
                           │  Scripts, SQL, Jolt      │
                           └────────────┬────────────┘
                                        │
              ┌─────────────────────────▼─────────────────────────┐
              │          ★ 6-PASS DEEP ANALYSIS ENGINE            │
              │                                                    │
              │  ┌──────────┐ ┌──────────┐ ┌──────────┐          │
              │  │ Pass 1:  │ │ Pass 2:  │ │ Pass 3:  │          │
              │  │Functional│ │Processor │ │Workflow  │          │
              │  │ Analysis │ │ Analysis │ │ Analysis │          │
              │  └──────────┘ └──────────┘ └──────────┘          │
              │  ┌──────────┐ ┌──────────┐ ┌──────────┐          │
              │  │ Pass 4:  │ │ Pass 5:  │ │ Pass 6:  │          │
              │  │Upstream  │ │Downstream│ │Line-by-  │          │
              │  │ Analysis │ │ Analysis │ │Line      │          │
              │  └──────────┘ └──────────┘ └──────────┘          │
              │                                                    │
              │  Output: DeepAnalysisResult (unified)              │
              └─────────────────────────┬─────────────────────────┘
                                        │
              ┌─────────────────────────▼─────────────────────────┐
              │              KNOWLEDGE BASE LOOKUP                 │
              │  500+ Processors │ 100+ NEL Functions              │
              │  150+ Services   │ 25+ Relationships               │
              └─────────────────────────┬─────────────────────────┘
                                        │
              ┌─────────────────────────▼─────────────────────────┐
              │           ★ CONVERSION ENGINE                     │
              │                                                    │
              │  ┌─────────────┐  ┌──────────────┐               │
              │  │Pipeline     │  │Specialized   │               │
              │  │Wiring       │  │Translators   │               │
              │  │(DAG→cells)  │  │(15+ types)   │               │
              │  └─────────────┘  └──────────────┘               │
              │  ┌─────────────┐  ┌──────────────┐               │
              │  │Sink         │  │Config &      │               │
              │  │Generator    │  │Security      │               │
              │  │(writes)     │  │(secrets)     │               │
              │  └─────────────┘  └──────────────┘               │
              │  ┌─────────────┐  ┌──────────────┐               │
              │  │Task         │  │Process Group │               │
              │  │Clustering   │  │Splitter      │               │
              │  └─────────────┘  └──────────────┘               │
              └─────────────────────────┬─────────────────────────┘
                                        │
              ┌─────────────────────────▼─────────────────────────┐
              │           ★ 12-POINT QUALITY GATE                 │
              │  Syntax │ Imports │ DataFlow │ Writes │ Streaming │
              │  Security │ Coverage │ NEL │ Routing │ Schema     │
              │  DAG │ Packages                                    │
              └─────────────────────────┬─────────────────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │        OUTPUT            │
                           │                          │
                           │  .ipynb  (Jupyter)       │
                           │  .py    (Databricks)     │
                           │  DAB.zip (Asset Bundle)  │
                           │  DLT.py  (Delta Live)    │
                           │                          │
                           │  + 6 Analysis Reports    │
                           │  + Migration Playbook    │
                           └──────────────────────────┘
```

---

## File Impact Summary

| File | Action | Phase |
|---|---|---|
| **Knowledge Base** | | |
| `backend/app/knowledge/nifi_processor_kb.py` | **NEW** — 500+ processor definitions | 1 |
| `backend/app/knowledge/nifi_nel_kb.py` | **NEW** — 100+ function mappings | 1 |
| `backend/app/knowledge/nifi_services_kb.py` | **NEW** — 150+ service definitions | 1 |
| `backend/app/knowledge/nifi_relationships_kb.py` | **NEW** — 25+ relationship types | 1 |
| `backend/app/knowledge/__init__.py` | **NEW** — KB access layer | 1 |
| **6-Pass Analysis Engine** | | |
| `backend/app/engines/analyzers/functional_analyzer.py` | **NEW** — Pass 1 | 2 |
| `backend/app/engines/analyzers/processor_analyzer.py` | **NEW** — Pass 2 | 2 |
| `backend/app/engines/analyzers/workflow_analyzer.py` | **NEW** — Pass 3 | 2 |
| `backend/app/engines/analyzers/upstream_analyzer.py` | **NEW** — Pass 4 | 2 |
| `backend/app/engines/analyzers/downstream_analyzer.py` | **NEW** — Pass 5 | 2 |
| `backend/app/engines/analyzers/line_by_line_analyzer.py` | **NEW** — Pass 6 | 2 |
| `backend/app/engines/analyzers/analysis_orchestrator.py` | **NEW** — Orchestrator | 2 |
| **Conversion Engine** | | |
| `backend/app/engines/generators/pipeline_wirer.py` | **NEW** — DataFrame chaining | 3 |
| `backend/app/engines/generators/sink_generator.py` | **NEW** — Write generation | 4 |
| `backend/app/engines/generators/config_generator.py` | **NEW** — Config + secrets | 5 |
| `backend/app/engines/generators/notebook_generator.py` | **REWRITE** — Orchestrate all engines | 3-9 |
| `backend/app/engines/generators/processor_translators.py` | **EXPAND** — 3→15+ translators | 8 |
| `backend/app/engines/generators/autoloader_generator.py` | **MODIFY** — Checkpoints + triggers | 4 |
| `backend/app/engines/generators/dab_generator.py` | **MODIFY** — Multi-notebook | 10 |
| `backend/app/engines/parsers/nel/functions.py` | **EXPAND** — Complete function coverage | 7 |
| `backend/app/engines/parsers/nel/functions_extended.py` | **EXPAND** — Missing functions | 7 |
| **Validation** | | |
| `backend/app/engines/validators/__init__.py` | **REWRITE** — 12-point checklist | 12 |
| `backend/app/engines/validators/runnable_checker.py` | **NEW** — Execution readiness | 12 |
| **Tests** | | |
| `backend/tests/test_knowledge_base.py` | **NEW** — KB completeness | 1 |
| `backend/tests/test_6pass_analysis.py` | **NEW** — Analysis passes | 2 |
| `backend/tests/test_pipeline_wiring.py` | **NEW** — Wiring correctness | 3 |
| `backend/tests/test_sink_generation.py` | **NEW** — Write generation | 4 |
| `backend/tests/test_runnable_notebook.py` | **NEW** — Full integration | 3-5 |
| `backend/tests/test_line_by_line.py` | **NEW** — Line analysis | 2 |
| **API Routes** | | |
| `backend/app/routers/analyze.py` | **MODIFY** — Return 6-pass results | 2 |
| `backend/app/routers/generate.py` | **MODIFY** — Use wired pipeline | 3-5 |
| **Frontend** | | |
| `frontend/src/components/steps/Step2Analyze.tsx` | **MODIFY** — Display 6-pass reports | 2 |
| `frontend/src/types/analysis.ts` | **MODIFY** — New report types | 2 |

---

## Priority Order

```
Phase  1 ████████████████████ Complete NiFi Knowledge Base     — FOUNDATION (everything builds on this)
Phase  2 ████████████████████ 6-Pass Analysis Engine           — INTELLIGENCE (deep understanding)
Phase  3 ████████████████████ Pipeline Wiring Engine            — CRITICAL (nothing runs without this)
Phase  4 ██████████████████   Sink Generation & Writes          — CRITICAL (no output = no pipeline)
Phase  5 ████████████████     Credential Security & Config      — CRITICAL (won't run without secrets)
Phase  6 ██████████████       FlowFile Attribute Materialization — HIGH (correctness)
Phase  7 ████████████         Complete NEL Transpiler            — HIGH (expression accuracy)
Phase  8 ██████████           Specialized Translators (15+)     — HIGH (complex processor handling)
Phase  9 ████████             Task Clustering                    — MEDIUM (optimization)
Phase 10 ██████               Process Group Workflows            — MEDIUM (enterprise flows)
Phase 11 ████                 Cycle Remediation                  — MEDIUM (edge cases)
Phase 12 ████████████████     Enhanced Validation (12-point)     — HIGH (quality assurance)
```

**Phases 1-5**: Mandatory for "load and run" quality
**Phases 6-8**: Required for production correctness
**Phases 9-12**: Enterprise polish and optimization

---

## Success Metrics

| Metric | Target |
|---|---|
| NiFi processor recognition | 100% (zero unknowns) |
| NEL expression transpilation | 100% of standard functions |
| Generated notebook syntax validity | 100% pass py_compile |
| DataFrame flow continuity | 100% (no dangling variables) |
| Terminal write coverage | 100% of sink processors generate writes |
| Credential security | 0 plaintext secrets |
| Streaming checkpoint coverage | 100% of writeStream calls |
| Analysis report completeness | All 6 passes produce valid output |
| Test coverage | 90%+ on all new modules |

**The ultimate test**: Take any real-world NiFi flow XML, run it through V6, open the output in Databricks, click "Run All", and watch it execute the exact same pipeline — reading the same sources, applying the same transformations, routing the same conditions, and writing to the same destinations. No manual edits required.

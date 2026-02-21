# Universal ETL Migration Platform — Complete Functional Analysis

> **Version:** 5.0.0
> **Generated:** 2026-02-21
> **Codebase:** 318 files | 50,609 lines (22,863 Python + 13,022 TypeScript + 14,724 YAML)
> **Architecture:** FastAPI backend (193 .py) + React/Vite frontend (100 .ts/.tsx) + 25 YAML processor maps

---

## Table of Contents

1. [Platform Overview](#1-platform-overview)
2. [Technology Stack](#2-technology-stack)
3. [Phase 1 — Parse (Source Ingestion)](#3-phase-1--parse-source-ingestion)
4. [Phase 2 — Analyze (Flow Intelligence)](#4-phase-2--analyze-flow-intelligence)
5. [Phase 3 — Assess & Map (Databricks Translation)](#5-phase-3--assess--map-databricks-translation)
6. [Phase 4 — Generate (Code Production)](#6-phase-4--generate-code-production)
7. [Phase 5 — Report (Migration Planning)](#7-phase-5--report-migration-planning)
8. [Phase 6 — Final Report (Executive Summary)](#8-phase-6--final-report-executive-summary)
9. [Phase 7 — Validate (Quality Assurance)](#9-phase-7--validate-quality-assurance)
10. [Phase 8 — Value Analysis (ROI & Business Case)](#10-phase-8--value-analysis-roi--business-case)
11. [Functional Workflow (End-to-End Pipeline)](#11-functional-workflow-end-to-end-pipeline)
12. [Frontend Architecture](#12-frontend-architecture)
13. [Infrastructure Layer](#13-infrastructure-layer)
14. [API Endpoint Inventory](#14-api-endpoint-inventory)
15. [Data Model Reference](#15-data-model-reference)

---

## 1. Platform Overview

The **Universal ETL Migration Platform** converts ETL pipeline definitions from **20+ source platforms** into **Databricks notebooks, workflows, DLT pipelines, and Asset Bundles**. It operates as an 8-phase pipeline:

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌──────────┐
│  PARSE  │───▸│ ANALYZE │───▸│ ASSESS  │───▸│ GENERATE │
│  Step 1 │    │  Step 2 │    │  Step 3 │    │  Step 4  │
└─────────┘    └─────────┘    └─────────┘    └──────────┘
                                                   │
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌────▼─────┐
│  VALUE  │◂───│VALIDATE │◂───│  FINAL  │◂───│  REPORT  │
│  Step 8 │    │  Step 7 │    │  Step 6 │    │  Step 5  │
└─────────┘    └─────────┘    └─────────┘    └──────────┘
```

### Supported Source Platforms (20)

| Category | Platforms |
|----------|-----------|
| **Data Flow** | Apache NiFi (JSON + XML), Apache Beam |
| **ETL Tools** | Informatica PowerCenter, Talend, Pentaho, IBM DataStage, Matillion, SSIS |
| **Orchestrators** | Apache Airflow, Prefect, Dagster, Luigi |
| **Cloud Native** | AWS Glue, Azure ADF, Fivetran, Airbyte, Stitch |
| **Data Platforms** | dbt, Snowflake, Oracle ODI |
| **Code-Based** | Apache Spark (PySpark), Generic SQL |

### Target Platform

**Databricks** — Notebooks, Workflows, Delta Live Tables (DLT), Asset Bundles (DAB), Unity Catalog

---

## 2. Technology Stack

### Backend

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Framework | FastAPI | ≥0.115 | REST API + WebSocket |
| Server | Uvicorn | ≥0.30 | ASGI server |
| Database | SQLAlchemy + SQLite | ≥2.0 | Persistence |
| XML Parser | lxml | ≥5.0 | NiFi/SSIS/Informatica/Pentaho XML |
| Graph Engine | NetworkX | ≥3.0 | DAG analysis, cycle detection |
| SQL Parser | sqlparse | ≥0.5 | Snowflake/SQL parsing |
| Config | PyYAML | ≥6.0 | Processor mapping tables |
| Validation | Pydantic | ≥2.0 | Request/response models |
| Auth | Custom JWT (HMAC-SHA256) | — | Token-based auth |
| Password | PBKDF2-SHA256 | — | 100K iterations |
| Python | CPython | ≥3.11 | Required runtime |

### Frontend

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| UI Framework | React | 18.3.1 | Component rendering |
| Language | TypeScript | 5.5.0 | Type safety |
| State | Zustand | 4.5.0 | Lightweight stores |
| Routing | React Router | 6.30.3 | SPA navigation |
| Visualization | D3.js | 7.9.0 | Flow diagrams, charts |
| Syntax | React Syntax Highlighter | 15.5.0 | Code preview |
| Styling | Tailwind CSS | 3.4.0 | Utility-first CSS |
| Build | Vite | 6.0.0 | Dev server + bundler |
| Testing | Vitest + Testing Library | 2.0/16.0 | Unit/component tests |

---

## 3. Phase 1 — Parse (Source Ingestion)

### Purpose
Ingest any supported ETL format and normalize into a unified `ParseResult` data model.

### Endpoint
```
POST /api/parse  ← UploadFile (multipart)
```

### Engine: `app.engines.parsers`

#### Auto-Detection Pipeline

```
parse_flow(content, filename)
├── Extension-based lookup (_EXT_MAP)
│   .xml → _detect_xml()   → NiFi XML / SSIS / Informatica / Pentaho / Oracle ODI
│   .json → _detect_json() → NiFi JSON / ADF / Glue / dbt / Matillion / Fivetran / Airbyte / Stitch
│   .py → _detect_python()  → Airflow / Prefect / Dagster / Spark
│   .sql → _detect_sql()    → Snowflake / Generic SQL
│   .zip → _detect_zip()    → Talend archives
│   .dtsx → ssis             → SSIS packages
│   .ktr/.kjb → pentaho      → Pentaho transformations/jobs
│   .dsx → datastage         → IBM DataStage exports
│   .yml/.yaml → dbt         → dbt manifests
│   .item → talend           → Talend items
├── Content sniffing (first 2000 bytes) if no extension match
└── Dispatch to specialized parser
```

#### Parser Inventory (20 parsers)

| Parser | File | Library | Input Format | Detection Strategy |
|--------|------|---------|-------------|-------------------|
| `parse_nifi_json` | `nifi_json.py` | `json` | NiFi flow JSON / Registry export | `flowContents`, `versionedFlowSnapshot` keys |
| `parse_nifi_xml` | `nifi_xml.py` | `lxml.etree` | NiFi template XML | `<template>`, `<flowController>` tags |
| `parse_ssis` | `ssis_parser.py` | `lxml.etree` | DTSX XML | DTS namespace, `.dtsx` extension |
| `parse_informatica` | `informatica_parser.py` | `lxml.etree` | PowerCenter XML | `<POWERMART>`, `<REPOSITORY>` tags |
| `parse_talend` | `talend_parser.py` | `lxml.etree`, `zipfile` | `.item` XML or ZIP | ZIP with `.item` files |
| `parse_pentaho` | `pentaho_parser.py` | `lxml.etree` | KTR/KJB XML | `<transformation>`, `<job>` tags |
| `parse_airflow` | `airflow_parser.py` | `ast` (Python AST) | Python DAG files | `from airflow`, `DAG()` patterns |
| `parse_dbt` | `dbt_parser.py` | `json`, `re` | manifest.json or SQL | `nodes`, `sources` keys; `{{ ref() }}` |
| `parse_adf` | `azure_adf_parser.py` | `json` | ARM template JSON | `resources[].type` = `Microsoft.DataFactory` |
| `parse_glue` | `aws_glue_parser.py` | `json` | Glue job config JSON | `JobName`, `Command` keys |
| `parse_snowflake` | `snowflake_parser.py` | `sqlparse`, `re` | Snowflake SQL | CREATE TASK/PIPE/STREAM syntax |
| `parse_dagster` | `dagster_parser.py` | `ast` | Python code | `@op`, `@asset`, `@job` decorators |
| `parse_prefect` | `prefect_parser.py` | `ast`, `re` | Python code | `@flow`, `@task` decorators |
| `parse_spark` | `spark_parser.py` | `ast`, `re` | PySpark code | `spark.read`, `spark.write` patterns |
| `parse_sql` | `sql_parser.py` | `sqlparse`, `re` | Generic SQL | CREATE/INSERT/SELECT/MERGE statements |
| `parse_datastage` | `datastage_parser.py` | `lxml.etree` | DataStage DSX XML | `DSJOB`, `DSRECORD` elements |
| `parse_fivetran` | `fivetran_parser.py` | `json` | Fivetran config JSON | `group`, `service`, `schema` keys |
| `parse_matillion` | `matillion_parser.py` | `json` | Matillion export JSON | `objects[].components` structure |
| `parse_oracle_odi` | `oracle_parser.py` | `lxml.etree` | Oracle ODI XML | `Folder`, `Interface`, `Package` elements |
| `parse_airbyte` | `airbyte_parser.py` | `json` | Airbyte catalog JSON | `catalog`, `syncCatalog`, `streams` keys |
| `parse_stitch` | `stitch_parser.py` | `json` | Stitch Data JSON | `integration`, `streams` keys |

#### NiFi Expression Language (NEL) Sub-Parser

The NEL parser (`app.engines.parsers.nel`) translates NiFi attribute expressions to PySpark/Python:

| Module | Functions | Purpose |
|--------|-----------|---------|
| `tokenizer.py` | `tokenize_nel_chain()` | Splits `${attr:func1():func2()}` on top-level colons |
| `parser.py` | `parse_nel_expression()`, `translate_nel_string()` | Parses `${...}` expressions, handles system functions (`now()`, `UUID()`, `hostname()`) |
| `functions.py` | `apply_nel_function()`, `resolve_variable_context()`, `java_date_to_python()` | 40+ function translations (string, math, date, logic, encoding) |
| `functions_extended.py` | `apply_nel_function_extended()` | 66 additional function handlers |

**Supported NEL Functions (100+):** `toUpper`, `toLower`, `trim`, `substring`, `replace`, `replaceAll`, `contains`, `startsWith`, `endsWith`, `matches`, `split`, `substringBefore`, `substringAfter`, `append`, `prepend`, `equals`, `isEmpty`, `isNull`, `notNull`, `ifElse`, `plus`, `minus`, `multiply`, `divide`, `mod`, `gt`, `ge`, `lt`, `le`, `format`, `toDate`, `toNumber`, `toString`, `grok`, `in`, `base64Encode`, `base64Decode`, `urlEncode`, `urlDecode`, `escapeJson`, `math:abs`, `math:ceil`, `math:floor`, `math:round`, `math:sqrt`, `math:log`, and more.

#### Parse Cache

`parse_cache.py` implements an **LRU cache** (max 50 entries) with thread-safe access:
- Key: SHA-256 content hash
- Stats tracking: hits, misses, hit_rate

#### Output: `ParseResult`

```python
ParseResult(
    platform: str,                          # "nifi", "airflow", etc.
    version: str,                           # Platform version
    processors: list[Processor],            # Normalized processor list
    connections: list[Connection],           # DAG edges
    process_groups: list[ProcessGroup],      # Logical groupings
    controller_services: list[ControllerService],  # Shared services
    parameter_contexts: list[ParameterContext],    # NiFi parameters → DAB variables
    metadata: dict,                         # Parser-specific metadata
    warnings: list[Warning],                # Diagnostic warnings
)
```

---

## 4. Phase 2 — Analyze (Flow Intelligence)

### Purpose
Build a comprehensive understanding of the flow: dependencies, cycles, external systems, security risks, pipeline stages, and 12 advanced analyses.

### Endpoint
```
POST /api/analyze  ← { parsed: ParseResult }
```

### Engine: `app.engines.analyzers`

The orchestrator `run_analysis()` calls **19 sub-analyzers** sequentially:

#### Core Analyzers (Phase 1-2)

| Analyzer | File | Algorithm | Library | Output |
|----------|------|-----------|---------|--------|
| **Dependency Graph** | `dependency_graph.py` | BFS transitive closure on NetworkX DiGraph | `networkx`, `collections.deque` | upstream/downstream maps, fan_in/fan_out |
| **Cycle Detection** | `cycle_detection.py` | Tarjan's SCC via `nx.strongly_connected_components()` | `networkx` | Cycles classified as error_retry / pagination / data_reevaluation |
| **External Systems** | `external_systems.py` | 28 regex patterns on processor types & properties | `re` | Detected systems (Kafka, S3, JDBC, etc.) with direction |
| **Security Scanner v2** | `security_scanner_v2.py` | 19 regex patterns with CWE IDs & CVSS scores | `re` | SQL injection, XSS, SSRF, hardcoded secrets, XXE, etc. |
| **Stage Classifier** | `stage_classifier.py` | Regex type matching → 8 pipeline stages | `re` | ingestion, extraction, routing, enrichment, loading, monitoring, transformation, processing |
| **Flow Metrics** | `flow_metrics.py` | Aggregate statistics computation | — | processor_count, connection_density, max_depth, avg_fan_out |
| **Attribute Flow** | `attribute_flow.py` | Depth-aware `${...}` extraction + lineage mapping | `re` | attribute_map, processor_attributes, attribute_lineage |
| **Backpressure** | `backpressure.py` | NiFi queue thresholds → Auto Loader config | `re` | maxFilesPerTrigger, maxBytesPerTrigger |
| **Task Clusterer** | `task_clusterer.py` | Chain walking on adjacency maps | — | Merged processor chains (2+ sequential non-boundary) |

#### Advanced Analyzers (Phase 3)

| Analyzer | File | Algorithm | Output |
|----------|------|-----------|--------|
| **Transaction Analyzer** | `transaction_analyzer.py` | Write detection + Delta format enforcement | ACID annotations, non-Delta warnings, merge code |
| **State Analyzer** | `state_analyzer.py` | Stateful processor classification + checkpoint gen | Checkpoint configs, RocksDB state stores, cache mappings |
| **Schema Analyzer** | `schema_analyzer.py` | Avro→PySpark conversion + schema evolution | StructType definitions, Auto Loader evolution configs |
| **Execution Mode** | `execution_mode_analyzer.py` | Streaming vs. batch classification + cost estimation | Pipeline mode recommendation, trigger configs, DBU costs |
| **Site-to-Site** | `site_to_site_analyzer.py` | Remote Process Group detection | Delta Sharing configs, Lakehouse Federation SQL |
| **Process Group** | `process_group_analyzer.py` | Group→DAB job mapping | Job definitions, inter-group dependencies, DAB YAML |
| **Complexity Scorer** | `complexity_scorer.py` | Multi-factor scoring (0-100) | Per-processor scores: connections×5 + properties×2 + NEL×10 + external×15 + cycle×20 + custom_code×25 |
| **Confidence Calibrator** | `confidence_calibrator.py` | Context-aware confidence adjustment | Adjusted confidence with reasons |
| **Lineage Tracker** | `lineage_tracker.py` | BFS + topological sort + DP critical path | Sources, sinks, critical path, max depth, orphans |
| **Impact Analyzer** | `impact_analyzer.py` | Transitive closure blast radius | Affected processors, risk level (critical/high/medium/low) |

#### Code Generation Analyzers (integrated into Phase 2)

| Generator | File | Output |
|-----------|------|--------|
| **Attribute Translator** | `attribute_translator.py` | FlowFile attributes → `withColumn()` calls |
| **Connection Generator** | `connection_generator.py` | Unity Catalog Connections + Secrets + Service Principals |
| **CI/CD Generator** | `cicd_generator.py` | GitHub Actions workflow + DAB deploy commands |

#### Output: `AnalysisResult`

```python
AnalysisResult(
    dependency_graph: dict,               # upstream/downstream/fan_in/fan_out
    external_systems: list[dict],         # Detected databases, clouds, messaging
    cycles: list[list[str]],             # Detected cycles
    cycle_classifications: list[CycleClassification],  # Classified with Databricks translations
    task_clusters: list[TaskCluster],     # Merged processor chains
    backpressure_configs: list[BackpressureConfig],    # Queue → Auto Loader
    flow_metrics: dict,                   # Aggregate statistics
    security_findings: list[dict],        # CWE-mapped vulnerabilities
    stages: list[dict],                   # Pipeline stage classification
    transaction_analysis: dict,           # ACID / Delta enforcement
    state_analysis: dict,                 # Checkpointing / state stores
    schema_analysis: dict,                # Schema evolution configs
    execution_mode_analysis: dict,        # Streaming vs. batch + costs
    site_to_site_analysis: dict,          # Delta Sharing / Federation
    process_group_analysis: dict,         # DAB job mapping
    attribute_translation: dict,          # Attribute → column translation
    connection_generation: dict,          # UC Connection generation
    cicd_generation: dict,                # CI/CD artifacts
)
```

---

## 5. Phase 3 — Assess & Map (Databricks Translation)

### Purpose
Map each source processor to its Databricks equivalent using YAML lookup tables, assign confidence scores, and generate PySpark code templates.

### Endpoint
```
POST /api/assess  ← { parsed: ParseResult, analysis: AnalysisResult }
```

### Engine: `app.engines.mappers`

#### Dispatch Architecture

```
map_to_databricks(parse_result, analysis_result)
│
├── platform == "nifi"?
│   └── YES → nifi_mapper.map_nifi()      # Dedicated NiFi mapper
│             (3-pass property resolution, 104-entry alias table,
│              environment vs. processor placeholder distinction)
│
├── platform in _MAPPER_MAP?
│   └── YES → importlib.import_module(platform_mapper)
│             └── map_platform() → base_mapper.map_platform_generic()
│
└── FALLBACK → base_mapper.map_platform_generic()
                (prepends dispatcher warning entry)
```

#### YAML Processor Maps (25 files, 14,724 lines)

Each YAML file defines processor-to-Databricks mappings:

```yaml
mappings:
  - type: GetFile                           # Source processor type
    category: Auto Loader                   # Databricks pattern
    template: |                             # PySpark code template
      df_{name} = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", "/Volumes/{catalog}/{schema}/checkpoints/{name}")
          .load("/Volumes/{catalog}/{schema}/{path}"))
    description: Read files using Auto Loader
    imports:
      - from pyspark.sql.functions import *
    confidence: 0.9                         # Base confidence [0.0–1.0]
    role: source                            # Functional role
```

| YAML File | Platform | Description |
|-----------|----------|-------------|
| `nifi_databricks.yaml` | NiFi | 100+ NiFi processor mappings |
| `adf_databricks.yaml` | Azure ADF | ADF activity mappings |
| `glue_databricks.yaml` | AWS Glue | Glue job/crawler mappings |
| `airflow_databricks.yaml` | Airflow | Operator → notebook mappings |
| `dbt_databricks.yaml` | dbt | Model/source → Delta table |
| `ssis_databricks.yaml` | SSIS | SSIS task mappings |
| `informatica_databricks.yaml` | Informatica | Transformation mappings |
| `talend_databricks.yaml` | Talend | Component mappings |
| `pentaho_databricks.yaml` | Pentaho | Step/entry mappings |
| `snowflake_databricks.yaml` | Snowflake | Object → Databricks mappings |
| `spark_databricks.yaml` | Spark | PySpark pattern upgrades |
| `sql_databricks.yaml` | SQL | SQL statement mappings |
| ... | (13 more) | Additional platform maps |
| `droppable.yaml` | All | Utility processors to drop |

#### Confidence Scoring

**NiFi Mapper (dedicated):**
- Base: `entry.confidence` (default 0.9)
- Penalty: -5% per **processor-level** unresolved `{placeholder}`
- Environment placeholders (`{catalog}`, `{schema}`, `{scope}`) excluded from penalty
- Floor: max(0.5, adjusted)

**Generic Mapper (all others):**
- Base: `entry.confidence` (default 0.7)
- Penalty: -10% per unresolved `{placeholder}`
- Floor: max(0.3, adjusted)

**Unmapped:** confidence=0.0, code=`"# UNMAPPED: {platform} {type} -- manual migration required"`

#### Property Resolution (3-pass for NiFi)

1. **Direct match:** `{property_name_snake_case}` → processor property value
2. **Alias lookup:** 104-entry `_NIFI_PROPERTY_ALIASES` table maps NiFi property names to template placeholders
3. **Generic aliases:** `{input}`, `{name}`, `{in}`, `{v}` → processor name

#### Support Modules

| Module | File | Purpose |
|--------|------|---------|
| **Mapping Cache** | `cache.py` | LRU+TTL (5min) cache for YAML files, thread-safe |
| **Property Resolver** | `property_resolver.py` | Cloud provider detection (S3/Azure/GCS), format-aware template adjustment |
| **Recommender** | `recommender.py` | Suggests top-3 alternatives for unmapped processors (Jaccard similarity + keyword matching) |
| **Template Registry** | `template_registry.py` | Unified search/coverage API across all platforms |
| **YAML Validator** | `yaml_validator.py` | Validates YAML files: required fields, confidence ranges, Python syntax, duplicates |

#### Output: `AssessmentResult`

```python
AssessmentResult(
    mappings: list[MappingEntry],   # Per-processor mapping with code + confidence
    packages: list[str],            # Required Python packages
    unmapped_count: int,            # Number of unmapped processors
)
```

---

## 6. Phase 4 — Generate (Code Production)

### Purpose
Produce executable Databricks artifacts: notebooks, workflows, DLT pipelines, Asset Bundles, tests, and CI/CD configs.

### Endpoint
```
POST /api/generate  ← { parsed, assessment, config: DatabricksConfig }
```

### Engine: `app.engines.generators`

#### Notebook Generator (`notebook_generator.py`)

Produces a multi-cell Databricks notebook:

| Cell | Builder | Content |
|------|---------|---------|
| 1. Title | markdown | Migration notebook header with metadata |
| 2. Imports | `build_imports_cell()` | `from pyspark.sql.functions import *`, collected packages |
| 3. Config | `build_config_cell()` | Catalog/schema/scope setup, `USE CATALOG`, `USE SCHEMA` |
| 4. Setup | `build_setup_cell()` | `CREATE SCHEMA IF NOT EXISTS`, volume creation |
| 5-N. Steps | Per-mapping code | Specialized or template-based PySpark code |
| N+1. Teardown | `build_teardown_cell()` | Drop temp views, print completion |

**Specialized Translators** (route complex processors to dedicated code generators):

| Processor Type | Translator | Generated Code |
|----------------|-----------|----------------|
| GetFile, ListFile, FetchFile | `autoloader_generator.py` | `spark.readStream.format("cloudFiles")` with schema evolution |
| ConsumeKafka, ConsumeKafkaRecord | `autoloader_generator.py` | `spark.readStream.format("kafka")` with bootstrap servers, topic, offsets |
| QueryDatabaseTable, ExecuteSQL | `autoloader_generator.py` | `spark.read.format("jdbc")` with secrets references |
| RouteOnAttribute | `processor_translators.py` | `df.filter()` chains or Workflow `condition_task` entries |
| JoltTransformJSON | `processor_translators.py` | Jolt spec → PySpark select/alias/drop/coalesce/array |
| ExecuteScript/ExecuteGroovyScript | `processor_translators.py` | Script → `@F.udf` wrapper |

#### Workflow Generator (`workflow_generator.py` + `workflow_orchestrator.py`)

Generates Databricks Workflows job definitions:

```python
{
    "name": "migration_{flow_name}",
    "tasks": [
        {
            "task_key": "step_get_file",
            "notebook_task": {"notebook_path": "..."},
            "depends_on": [],                     # Respects NiFi DAG topology
            "job_cluster_key": "migration_cluster"
        },
        ...
    ],
    "job_clusters": [{
        "job_cluster_key": "migration_cluster",
        "new_cluster": {
            "autoscale": {"min_workers": 2, "max_workers": 8},
            "spark_version": "15.4.x-scala2.12"
        }
    }],
    "schedule": null,                             # Derived from NiFi CRON_DRIVEN
    "email_notifications": {},                    # From PutEmail processors
    "tags": {"platform": "nifi", "migration_date": "..."}
}
```

#### DLT Pipeline Generator (`dlt_generator.py`)

Generates Delta Live Tables pipeline code:

| Mapping Role | DLT Decorator | Pattern |
|-------------|---------------|---------|
| Source | `@dlt.table` | Streaming read (Kafka/cloudFiles/JDBC) |
| Transform | `@dlt.view` | Transformation logic |
| Sink | `@dlt.table` (materialized) | Final Delta table |
| Route | `@dlt.view` | Filter conditions |
| DQ Rules | `@dlt.expect` / `@dlt.expect_or_drop` | From ValidateRecord/RouteOnAttribute |

#### DAB Generator (`dab_generator.py`)

Generates complete Databricks Asset Bundle:

```
databricks_asset_bundle.zip
├── databricks.yml           # Bundle config with variables, resources, targets
├── notebooks/
│   ├── ingest_{flow}.py     # Ingestion notebook
│   ├── transform_{flow}.py  # Transform notebook
│   └── load_{flow}.py       # Load notebook
└── resources/
    ├── README.md
    └── bundle_structure.json
```

Bundle config includes:
- **Variables:** Extracted from controller services + processor properties (secrets vs. config)
- **Resources:** Jobs with tasks per stage (ingest → transform → load)
- **Targets:** dev / staging / prod with workspace URLs

#### Additional Generators

| Generator | File | Output |
|-----------|------|--------|
| **Test Generator** | `test_generator.py` | pytest file with MockDBUtils, SparkSession fixture, per-processor tests |
| **CI/CD Generator** | `cicd_generator.py` | GitHub Actions YAML (validate → deploy-dev → staging → prod) |
| **Connection Generator** | `connection_generator.py` | CREATE CONNECTION + CREATE FOREIGN CATALOG SQL |
| **DQ Rules Generator** | `dq_rules_generator.py` | DLT expectations from ValidateRecord, RouteOnAttribute, Filter |
| **UC Helper** | `uc_helper.py` | CREATE CATALOG/SCHEMA/VOLUME/EXTERNAL LOCATION + GRANT |
| **Attribute Translator** | `attribute_translator.py` | FlowFile attributes → `df.withColumn()` calls |

#### Code Security

**Code Scrubber v1** (`code_scrubber.py`):
- Replaces hardcoded passwords/tokens/API keys with `dbutils.secrets.get()`
- Sanitizes JDBC URLs with embedded credentials
- Cleans Spark `.option("password", "...")` calls

**Code Scrubber v2** (`code_scrubber_v2.py`):
- AWS keys (`AKIA...`) → `dbutils.secrets.get(scope, "aws_access_key")`
- Azure connection strings → `dbutils.secrets.get(scope, "azure_connection_string")`
- GCP service account paths → `dbutils.secrets.get(scope, "gcp_sa_key")`
- Hardcoded IPs/ports → `# REVIEW: hardcoded address`
- NiFi filesystem paths → `/Volumes/{catalog}/{schema}/...`
- Adds `# REVIEW` markers for unmapped/JDBC/Kafka/external URL patterns

#### Output: `NotebookResult`

```python
NotebookResult(
    cells: list[NotebookCell],   # [{type: "code"|"markdown", source: str, label: str}]
    workflow: dict,               # Databricks Workflow job definition
)
```

---

## 7. Phase 5 — Report (Migration Planning)

### Purpose
Generate migration readiness report with gap playbook, risk matrix, and estimated timeline.

### Endpoint
```
POST /api/report  ← { type: "migration", parsed, analysis, assessment }
```

### Engine: `app.engines.reporters`

#### Migration Report (`_build_migration` in `report.py`)

| Section | Logic | Output |
|---------|-------|--------|
| **Gap Playbook** | Unmapped → critical; confidence < 0.7 → high; 0.7–0.9 → medium | List of {severity, processor, type, remediation} |
| **Risk Matrix** | Counts: critical/high/medium/low risks + security findings + cycles | Dict of risk counts |
| **Timeline** | Setup (1 wk) + Auto (procs ÷ 50) + Manual (unmapped × 2) + Testing (2 wk) + UAT (1 wk) | Phased week estimates |
| **Summary** | Text: "X processors, Y% mapped, Z external systems" | String |

#### Output: `MigrationReport`

```python
{
    "gapPlaybook": [{severity, processor, type, remediation}],
    "riskMatrix": {critical, high, medium, low, securityFindings, cycles},
    "estimatedTimeline": [{phase, weeks, description}],
    "summary": str
}
```

---

## 8. Phase 6 — Final Report (Executive Summary)

### Purpose
Produce an executive-level summary combining readiness assessment, risk factors, and recommendations.

### Endpoint
```
POST /api/report  ← { type: "final", parsed, analysis, assessment, notebook, validation }
```

### Engine: `app.engines.reporters.final_report`

#### Readiness Classification

| Condition | Readiness | Color |
|-----------|-----------|-------|
| avg_confidence ≥ 0.9 AND unmapped_count = 0 | GREEN | Green |
| avg_confidence ≥ 0.7 | AMBER | Amber |
| Otherwise | RED | Red |

#### Report Sections

| Section | Content |
|---------|---------|
| **Executive Summary** | Platform, processor count, coverage %, avg confidence |
| **Confidence Breakdown** | High (≥90%), Medium (70–89%), Low (<70%), Unmapped |
| **Risk Factors** | Cycles, security findings, unmapped processors |
| **Recommendations** | Review unmapped, fix security, configure external systems, test |
| **External Systems** | Detected systems with protocol details |

#### Export Formats (`export_formats.py`)

| Format | Function | Output |
|--------|----------|--------|
| JSON | `to_json()` | Formatted JSON string |
| Markdown | `to_markdown()` | Markdown with tables and sections |
| CSV | `to_csv_rows()` | List of processor mapping dicts |

---

## 9. Phase 7 — Validate (Quality Assurance)

### Purpose
Score the generated notebook across 8 quality dimensions and identify gaps.

### Endpoint
```
POST /api/validate  ← { parsed: ParseResult, notebook: NotebookResult }
```

### Engine: `app.engines.validators`

#### Validation Dimensions

| Dimension | Weight | Validator | What It Checks |
|-----------|--------|-----------|----------------|
| **Intent Coverage** | 3.0 | `intent_analyzer.py` | % of source processors represented in notebook |
| **Code Quality** | 2.5 | `line_validator.py` | AST syntax + anti-patterns (polling loops, `.collect()`, `.toPandas()`, Flask, hardcoded passwords) |
| **Completeness** | 2.0 | `feedback.py` | Structure (imports/config/setup/teardown) + coverage + controller services |
| **Delta Format** | 1.5 | `_validate_delta_format()` | All writes use Delta format |
| **Checkpoint Coverage** | 1.5 | `_validate_checkpoints()` | Streaming writes have `checkpointLocation` |
| **Credential Security** | 2.0 | `_validate_credentials()` | No hardcoded passwords/tokens/AWS keys |
| **Error Handling** | 1.0 | `_validate_error_handling()` | try/except around JDBC operations |
| **Schema Evolution** | 1.0 | `_validate_schema_evolution()` | cloudFiles.schemaLocation for Auto Loader |

#### Scoring Engine (`score_engine.py`)

```python
overall_score = Σ(weight_i × score_i) / Σ(weight_i)   # excluding vacuous dimensions
```

**Vacuous dimension:** Scored 1.0 only because nothing to validate (e.g., "no writes" → Delta Format = 1.0). Excluded from weighted average to prevent score inflation.

#### Readiness Classification

| Score | Status | Label |
|-------|--------|-------|
| ≥ 0.85 | READY | Production-ready |
| ≥ 0.60 | NEEDS WORK | Requires refinement |
| < 0.60 | NOT READY | Significant gaps |

#### Output: `ValidationResult`

```python
ValidationResult(
    overall_score: float,              # Weighted score [0.0–1.0]
    scores: list[ValidationScore],     # Per-dimension scores
    gaps: list[dict],                  # Missing processors/connections
    errors: list[str],                 # Validation errors
)
```

---

## 10. Phase 8 — Value Analysis (ROI & Business Case)

### Purpose
Build a comprehensive business case with effort estimation, ROI modeling, cost comparison, and Monte Carlo simulation.

### Endpoint
```
POST /api/report  ← { type: "value", parsed, analysis, assessment }
POST /api/report  ← { type: "roi", parsed, analysis, assessment }
```

### Engine: `app.engines.reporters`

#### Value Analysis (`value_analysis.py`)

| Metric | Formula |
|--------|---------|
| **Auto-migratable** | Processors with confidence ≥ 0.9 |
| **Semi-automated** | Processors with 0.7 ≤ confidence < 0.9 |
| **Manual** | Processors with confidence < 0.7 or unmapped |
| **Effort (hours)** | auto × 0.5h + semi × 2.0h + manual × 8.0h |
| **Baseline (no tool)** | total_processors × 6.0h |
| **Hours saved** | baseline - effort |
| **ROI multiplier** | baseline / effort |
| **Automation rate** | (auto + semi) / total × 100% |

#### Droppable Processor Detection

Identifies processors that can be eliminated in migration:

| Category | Examples | Reason |
|----------|----------|--------|
| NiFi utility types | LogMessage, LogAttribute, ControlRate, MonitorActivity, DebugFlow | Replaced by Databricks built-in monitoring |
| Utility roles | logging, monitoring, funnel, noop | No equivalent needed |
| High-confidence mapped | Processors with confidence ≥ 95% | Fully automated migration |

#### ROI Comparison (`roi_comparison.py`)

Compares two migration strategies:

| Strategy | Cost Formula | Risk |
|----------|-------------|------|
| **Lift & Shift** | auto × 0.25h + semi × 0.75h + manual × 2.0h (× $150/hr) | unmapped_fraction |
| **Refactor** | all processors at full base effort (× $150/hr) | 0.05 + unmapped × 0.15 + scale_factor |

Recommends the lower-cost, lower-risk approach with break-even analysis.

#### TCO Calculator (`tco_calculator.py`)

Multi-year Total Cost of Ownership projection:

| Component | Current Platform | Databricks |
|-----------|-----------------|------------|
| Infrastructure | nodes × monthly × 12 × scale | batch_DBU + streaming_DBU |
| Operations | FTE_fraction × salary × scale | Reduced ops overhead |
| Storage | Base storage cost | Delta Lake storage |
| **Projection** | Year 1 / Year 3 / Year 5 | With 5% annual growth |

#### License Savings (`license_savings.py`)

Annual cost comparison across platforms:

| Platform | License Cost | Infra Cost |
|----------|-------------|-----------|
| NiFi | $0 (OSS) | $50K |
| SSIS | $15K | $20K |
| Informatica | $200K | $30K |
| Talend | $50K | $15K |
| DataStage | $150K | $25K |
| Airflow | $0 (OSS) | $40K |
| Matillion | $60K | $10K |
| Fivetran | $24K | $0 |
| Oracle ODI | $120K | $25K |

#### FTE Impact (`fte_impact.py`)

Role-specific resource allocation:

| Role | Tasks | Hours Per |
|------|-------|-----------|
| Data Engineer | Review auto-mapped processors | 1h each |
| Senior Data Engineer | Refine semi-mapped processors | 4h each |
| Solutions Architect | Redesign process groups + unmapped | 8h / 2h each |
| QA Engineer | Integration tests | 2h per processor |

Phased timeline: Discovery → Automated → Refinement → Manual → Testing → Deployment

#### Monte Carlo Simulation (`monte_carlo_roi.py`)

Probabilistic cost modeling (10,000 simulations):

| Parameter | Distribution |
|-----------|-------------|
| Effort per processor | Normal(base, σ=30%) |
| Rework probability | Per-processor random trigger |
| Rework multiplier | 1.5–2.0× when triggered |
| Hourly rate | Uniform($100–$200) |

Returns percentiles: P10, P25, P50, P75, P90 with confidence band.

#### Industry Benchmarks (`benchmarks.py`)

| Tier | Processors | Avg Migration Weeks | Avg Team Size |
|------|-----------|-------------------|---------------|
| Small | 0–50 | Benchmark baseline | Small team |
| Medium | 51–200 | Scaled estimate | Medium team |
| Large | 201–500 | Scaled estimate | Large team |
| Enterprise | 501+ | Scaled estimate | Enterprise team |

Adjustment: Good automation + confidence → 0.7× (30% faster); Poor → 1.3× (30% slower)

---

## 11. Functional Workflow (End-to-End Pipeline)

### Single-Request Full Pipeline

```
POST /api/pipeline/run  ← UploadFile
```

Executes all 8 steps sequentially with per-step timing:

```
Step 1: parse_flow(content, filename)           → ParseResult
Step 2: run_analysis(parsed)                    → AnalysisResult
Step 3: map_to_databricks(parsed, analysis)     → AssessmentResult
Step 4: generate_notebook(parsed, assessment)   → NotebookResult
Step 5: validate_notebook(parsed, notebook)     → ValidationResult
Step 6: _build_migration(req)                   → MigrationReport
Step 7: _build_final(req)                       → FinalReport
Step 8: _build_value(req)                       → ValueAnalysis
```

Returns `PipelineResponse` with all results + step durations + total duration.

### Streaming Pipeline

```
POST /api/pipeline/run-streaming  ← UploadFile
```

Returns SSE (Server-Sent Events) stream:
```
event: step_start
data: {"step": "parse", "index": 0}

event: step_complete
data: {"step": "parse", "index": 0, "duration_ms": 245}

event: step_start
data: {"step": "analyze", "index": 1}
...
event: pipeline_complete
data: {"steps_completed": 8, "total_duration_ms": 3400}
```

### Step-by-Step Pipeline

Individual endpoints for manual execution:

```
POST /api/parse     → ParseResult
POST /api/analyze   → AnalysisResult  (requires parsed)
POST /api/assess    → AssessmentResult (requires parsed + analysis)
POST /api/generate  → NotebookResult   (requires parsed + assessment)
POST /api/validate  → ValidationResult (requires parsed + notebook)
POST /api/report    → Report           (requires parsed + assessment + analysis)
```

### Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ETL SOURCE FILE                              │
│   .xml .json .dtsx .ktr .kjb .py .sql .yaml .zip .item .dsx       │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   PHASE 1: PARSE      │
                    │   dispatcher.py       │
                    │   20 specialized      │
                    │   parsers + NEL       │
                    │   sub-parser          │
                    └───────────┬───────────┘
                                │ ParseResult
                    ┌───────────▼───────────┐
                    │   PHASE 2: ANALYZE    │
                    │   19 sub-analyzers    │
                    │   NetworkX DAG        │
                    │   Tarjan SCC          │
                    │   Security scanning   │
                    └───────────┬───────────┘
                                │ AnalysisResult
                    ┌───────────▼───────────┐
                    │   PHASE 3: ASSESS     │
                    │   YAML lookup (25     │
                    │   mapping files)      │
                    │   Confidence scoring  │
                    │   Property resolution │
                    └───────────┬───────────┘
                                │ AssessmentResult
                ┌───────────────┼───────────────┐
                │               │               │
    ┌───────────▼──┐  ┌────────▼────────┐  ┌───▼──────────┐
    │ PHASE 4:     │  │ PHASE 5-6:      │  │ PHASE 7:     │
    │ GENERATE     │  │ REPORT          │  │ VALIDATE     │
    │ Notebooks    │  │ Migration plan  │  │ 8 dimensions │
    │ Workflows    │  │ Executive report│  │ Weighted     │
    │ DLT          │  │ Gap playbook    │  │ scoring      │
    │ DAB          │  │ Risk matrix     │  │              │
    │ Tests        │  │ Timeline        │  │              │
    │ CI/CD        │  │                 │  │              │
    └──────────────┘  └─────────────────┘  └──────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   PHASE 8: VALUE      │
                    │   ROI estimation      │
                    │   TCO calculation     │
                    │   FTE impact          │
                    │   License savings     │
                    │   Monte Carlo sim     │
                    │   Industry benchmarks │
                    └───────────────────────┘
```

---

## 12. Frontend Architecture

### Application Structure

```
React App (Vite + TypeScript + Tailwind CSS)
├── Auth Layer (JWT + localStorage)
│   ├── LoginPage / RegisterPage
│   └── AuthGuard (protected routes)
├── State Management (Zustand)
│   ├── pipelineStore — 8 step outputs + ROI + file metadata
│   ├── uiStore — activeStep, stepStatuses, progress, toasts, errors
│   ├── authStore — user, token, refreshToken
│   └── projectsStore — project CRUD
├── Routing (React Router v6)
│   ├── /dashboard → PortfolioDashboard
│   ├── /pipeline → PipelinePage (8-step wizard)
│   ├── /admin → AdminPage
│   ├── /login, /register → Auth pages
│   └── /* → NotFoundPage
├── Pipeline Steps (8 components)
│   ├── Step1Parse — File upload + auto-run trigger
│   ├── Step2Analyze — D3 visualizations (TierDiagram, SecurityTreemap)
│   ├── Step3Assess — Searchable mapping table with confidence colors
│   ├── Step4Convert — Cell navigator + code preview + downloads
│   ├── Step5Report — Gap playbook + risk matrix + timeline
│   ├── Step6FinalReport — Executive report with export (JSON/MD/TXT)
│   ├── Step7Validate — Score breakdown + readiness badge
│   └── Step8ValueAnalysis — ROI dashboard + droppable processors
├── Visualization Layer (D3.js)
│   ├── FlowGraph — Interactive DAG visualization
│   ├── TierDiagram — Processor stage diagram
│   ├── ConfidenceChart — Confidence distribution
│   ├── SecurityTreemap — Security findings treemap
│   ├── RiskHeatmap — Risk level heatmap
│   ├── MigrationTimeline — Phased timeline
│   └── CoverageDonut — Coverage ring chart
├── Hooks
│   ├── usePipeline — Step execution + watchdog (300s heartbeat)
│   ├── useSessionPersistence — localStorage sync (500ms debounce)
│   ├── useTheme — Dark/Light/System mode
│   ├── useKeyboardShortcuts — Ctrl+K, Ctrl+1-9 navigation
│   ├── usePresence — WebSocket collaboration
│   └── useTranslation — i18n with fallback
└── Shared Components
    ├── FileUpload (drag-drop)
    ├── CodePreview (syntax highlighting)
    ├── JsonExplorer (expandable tree)
    ├── ErrorModal / ErrorPanel / Toast
    ├── SearchOverlay (Ctrl+K command palette)
    ├── SettingsPanel / HelpPanel
    ├── PresenceAvatars (real-time collaboration)
    └── ExportPDFButton / ExportManager
```

### Pipeline Execution (usePipeline hook)

```
runAll(file)
├── pipelineRunning = true (blocks manual navigation)
├── Step 1: POST /api/parse → pipeline.parsed
├── Step 2: POST /api/analyze → pipeline.analysis
├── Step 3: POST /api/assess → pipeline.assessment
├── Step 4: POST /api/generate → pipeline.notebook
├── Step 5: POST /api/report (type=migration) → pipeline.report
├── Step 6: POST /api/report (type=final) → pipeline.finalReport
├── Step 7: POST /api/validate → pipeline.validation
├── Step 8: POST /api/report (type=value) → pipeline.valueAnalysis
├── pipelineRunning = false
└── Navigate to Summary page
```

**Watchdog:** Every 300s during step execution, polls `GET /api/health/heartbeat`.

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| Ctrl/Cmd+K | Toggle search overlay |
| Ctrl/Cmd+1..8 | Navigate to steps 1-8 |
| Ctrl/Cmd+9 | Summary page |
| Ctrl/Cmd+0 | Admin console |
| Escape | Close any overlay |

---

## 13. Infrastructure Layer

### Authentication & Authorization

| Component | Implementation | Details |
|-----------|---------------|---------|
| **JWT** | Custom HMAC-SHA256 | 1h access token, 7d refresh token |
| **Password** | PBKDF2-SHA256 | 100K iterations, 16-byte salt |
| **API Keys** | SHA-256 hash | `etl_{base64}` format, prefix display, TTL support |
| **RBAC** | 3-tier hierarchy | admin (3) > analyst (2) > viewer (1) |
| **Audit** | Middleware | Logs all POST/PUT/DELETE with user, IP, duration |

### Middleware Stack

| Middleware | Purpose | Config |
|-----------|---------|--------|
| **CORSMiddleware** | Cross-origin requests | Origins from `ETL_CORS_ORIGINS` env var |
| **RateLimiterMiddleware** | Per-IP token bucket | 60 requests/minute default |
| **AuditMiddleware** | Request logging | Skips GET and health endpoints |

### Database (SQLite via SQLAlchemy)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `users` | User accounts | email, hashed_password, role |
| `projects` | Project management | name, platform, owner_id |
| `pipeline_runs` | Run tracking | project_id, status, step_results (JSON) |
| `api_keys` | API key storage | key_hash, prefix, scopes, expires_at |
| `audit_logs` | Request audit trail | action, resource_type, ip_address, details (JSON) |
| `run_history` | Pipeline run history | filename, platform, processor_count, duration_ms |
| `comments` | Threaded comments | target_type, target_id, parent_id, text |
| `favorites` | User bookmarks | target_type, target_id |
| `schedules` | Scheduled runs | cron, project_id, enabled, next_run |
| `share_links` | Shareable URLs | token, target_type, expires_hours |
| `tags` | Color-coded tags | name, color, target_type, target_id |
| `webhooks` | Event notifications | url, events (JSON), secret, active |
| `flow_versions` | Flow snapshots | project_id, label, data (JSON) |

### Configuration

| Env Variable | Default | Purpose |
|-------------|---------|---------|
| `ETL_CORS_ORIGINS` | `["http://localhost:5174"]` | Allowed CORS origins |
| `ETL_LOG_LEVEL` | `INFO` | Logging level |
| `ETL_MAX_FILE_SIZE` | 50 MB | Upload size limit |
| `ETL_JWT_SECRET` | `dev-secret-...` | JWT signing key (MUST set in prod) |
| `ETL_ENV` | `dev` | Environment (dev/staging/prod) |

### Utilities

| Module | Purpose |
|--------|---------|
| `errors.py` | Exception hierarchy (ParseError, MappingError, etc.) |
| `error_classifier.py` | Classifies exceptions → user-friendly messages + remediation |
| `input_validator.py` | File upload validation (extension, size, content sniffing) |
| `sanitizer.py` | HTML stripping, sensitive value masking, filename sanitization |
| `logging.py` | Error ring buffer (200 entries) for admin endpoint |
| `structured_logging.py` | JSON log formatter + `timed_step()` context manager |
| `metrics.py` | Thread-safe per-endpoint request/error/latency tracking |
| `streaming.py` | SSE formatting + `StreamingPipelineRunner` |
| `processing_status.py` | Thread-safe step progress tracker for health endpoint |
| `checkpoint_manager.py` | Session checkpoint save/load to `/tmp/etl-checkpoints/` |

---

## 14. API Endpoint Inventory

### Core Pipeline (7 endpoints)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/parse` | Parse uploaded ETL file |
| POST | `/api/analyze` | Run flow analysis |
| POST | `/api/assess` | Map to Databricks |
| POST | `/api/generate` | Generate notebook |
| POST | `/api/validate` | Validate notebook |
| POST | `/api/report` | Generate report (migration/final/value/roi) |
| POST | `/api/pipeline/run` | Full 8-step pipeline |
| POST | `/api/pipeline/run-streaming` | SSE streaming pipeline |

### Export (5 endpoints)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/export/notebook` | Export as .py notebook |
| POST | `/api/export/dlt` | Export DLT pipeline |
| POST | `/api/export/tests` | Export pytest file |
| POST | `/api/export/workflow` | Export workflow JSON |
| POST | `/api/export/dab` | Export Asset Bundle ZIP |
| POST | `/api/export/pdf` | Export HTML for PDF |

### Analysis (3 endpoints)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/lineage` | Data lineage graph |
| POST | `/api/lineage/impact` | Change impact analysis |
| POST | `/api/compare` | Multi-flow comparison |

### Enterprise (30+ endpoints)

| Category | Endpoints | Methods |
|----------|-----------|---------|
| Auth | `/api/auth/register`, `/login`, `/me`, `/refresh` | POST, GET |
| Projects | `/api/projects`, `/{id}`, `/{id}/runs` | GET, POST, PUT, DELETE |
| Versions | `/api/projects/{id}/versions`, `/diff` | GET, POST |
| History | `/api/history`, `/{run_id}` | GET, POST |
| API Keys | `/api/api-keys`, `/{id}` | GET, POST, DELETE |
| Audit | `/api/audit` | GET (admin only) |
| Comments | `/api/comments`, `/{id}` | GET, POST, DELETE |
| Tags | `/api/tags`, `/{id}` | GET, POST, DELETE |
| Favorites | `/api/favorites`, `/{id}` | GET, POST, DELETE |
| Shares | `/api/shares`, `/{token}` | GET, POST |
| Schedules | `/api/schedules`, `/{id}` | GET, POST, DELETE |
| Webhooks | `/api/webhooks`, `/{id}` | GET, POST, DELETE |
| Dashboard | `/api/dashboard` | GET |
| Health | `/api/health/detailed`, `/heartbeat`, `/mappings` | GET |
| Admin | `/api/admin/health`, `/logs`, `/platforms` | GET |

### WebSocket (1 endpoint)

| Protocol | Path | Purpose |
|----------|------|---------|
| WS | `/ws/presence` | Real-time collaboration presence |

**Total: 73 HTTP endpoints + 1 WebSocket endpoint**

---

## 15. Data Model Reference

### Core Pipeline Models

```
ParseResult
├── platform: str                    # Source platform identifier
├── version: str                     # Platform version
├── processors: list[Processor]      # Normalized processor list
│   ├── name: str
│   ├── type: str
│   ├── platform: str
│   ├── properties: dict
│   ├── group: str
│   ├── state: str
│   ├── scheduling: dict | None
│   └── resolved_services: dict | None
├── connections: list[Connection]    # DAG edges
│   ├── source_name: str
│   ├── destination_name: str
│   ├── relationship: str
│   ├── back_pressure_object_threshold: int
│   └── back_pressure_data_size_threshold: str
├── process_groups: list[ProcessGroup]
├── controller_services: list[ControllerService]
├── parameter_contexts: list[ParameterContext]
├── metadata: dict
└── warnings: list[Warning]

AnalysisResult
├── dependency_graph: dict           # upstream/downstream/fan_in/fan_out
├── external_systems: list[dict]     # Kafka, S3, JDBC, etc.
├── cycles: list[list[str]]         # Detected cycles
├── cycle_classifications: list      # error_retry/pagination/data_reevaluation
├── task_clusters: list              # Merged sequential chains
├── backpressure_configs: list       # NiFi queue → Auto Loader
├── flow_metrics: dict               # Aggregate statistics
├── security_findings: list[dict]    # CWE-mapped vulnerabilities
├── stages: list[dict]               # Pipeline stage classification
├── transaction_analysis: dict       # ACID/Delta enforcement
├── state_analysis: dict             # Checkpointing/state stores
├── schema_analysis: dict            # Schema evolution
├── execution_mode_analysis: dict    # Streaming vs. batch
├── site_to_site_analysis: dict      # Delta Sharing/Federation
└── process_group_analysis: dict     # DAB job mapping

AssessmentResult
├── mappings: list[MappingEntry]     # Per-processor mapping
│   ├── name, type, role, category
│   ├── mapped: bool
│   ├── confidence: float [0.0–1.0]
│   ├── code: str (PySpark template)
│   └── notes: str
├── packages: list[str]              # Required packages
└── unmapped_count: int

NotebookResult
├── cells: list[NotebookCell]        # Code + markdown cells
└── workflow: dict                    # Databricks job definition

ValidationResult
├── overall_score: float [0.0–1.0]
├── scores: list[ValidationScore]    # Per-dimension scores
├── gaps: list[dict]                  # Missing coverage
└── errors: list[str]                 # Validation errors
```

### Configuration Model

```
DatabricksConfig
├── catalog: str = "main"
├── schema_name: str = "default"
├── cloud_provider: "aws" | "azure" | "gcp"
├── compute_type: "jobs_compute" | "all_purpose" | "serverless"
├── runtime_version: str = "15.4"
├── secret_scope: str = "etl-migration"
├── volume_path: str = "/Volumes/main/default/landing"
├── use_unity_catalog: bool = True
├── use_dlt: bool = False
└── streaming_enabled: bool = True
```

---

*Generated by Claude Code — Universal ETL Migration Platform v5.0.0 Functional Analysis*

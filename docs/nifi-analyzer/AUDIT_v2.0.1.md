# NiFi Flow Analyzer v2.0.1 — Comprehensive Audit Report

**Date:** 2026-02-20
**Branch:** v2.0.1 (commit 562d542)
**Tests:** 93/93 passing | **Modules:** 150 | **Build:** 675 KB single-file

---

## 1. CODE HYGIENE (TODOs, Orphans, Dead Code)

### 3 HIGH-severity findings

| # | Issue | File | Lines |
|---|-------|------|-------|
| 1 | **`core/constants.js` does not exist** — `graph-utils.js` imports from it → latent crash | `utils/graph-utils.js` | 7 |
| 2 | **`window.analyzeFlowGraph` never set** — cycle detection in notebook generator permanently disabled despite module existing at `analyzers/flow-graph-analyzer.js` | `generators/notebook-generator.js` | 158-162 |
| 3 | **`wrapBatchSinkForStreaming` duplicated byte-for-byte** in two live files — will diverge silently | `mappers/streaming-propagator.js:65` + `generators/streaming-wrapper.js:35` |

### 5 orphaned modules (never imported)

- `utils/score-helpers.js`
- `utils/accessibility.js`
- `utils/graph-utils.js`
- `constants/proc-type-system-map.js`
- `constants/nifi-el-funcs.js`

### 11 duplicate implementations (same logic, 2+ copies)

| Function | Location 1 | Location 2+ |
|----------|-----------|-------------|
| `metricsHTML` | `utils/dom-helpers.js:60` | `ui/step-handlers.js:65`, `main.js:525` |
| `tableHTML` | `utils/dom-helpers.js:80` | `ui/step-handlers.js:81` |
| `expanderHTML` | `utils/dom-helpers.js:96` | `ui/step-handlers.js:92` |
| `classifyGroupDominantRole` | `utils/graph-utils.js:17` | `ui/tier-diagram/build-tier-data.js:39` |
| `ROLE_TIER_COLORS` | `constants/nifi-role-map.js:40` | `tier-diagram/render-nodes.js:19`, `build-tier-data.js:20`, `group-expand.js:13` |
| `ROLE_TIER_LABELS` | `constants/nifi-role-map.js:50` | `tier-diagram/build-tier-data.js:28` |
| `ROLE_TIER_ORDER` | `constants/nifi-role-map.js:37` | `tier-diagram/build-tier-data.js:15` |
| `classifyNiFiProcessor` | `constants/nifi-role-map.js:60` | `mappers/processor-classifier.js:69` |
| `wrapBatchSinkForStreaming` | `mappers/streaming-propagator.js:65` | `generators/streaming-wrapper.js:35` |
| `bfsReachable` | `utils/bfs.js:59` | `ui/tier-diagram/route-trace.js:19` |
| `NIFI_EL_FUNCS` | `constants/nifi-el-funcs.js:2` | `analyzers/resource-manifest.js:15` |
| `PACKAGE_MAP` | `constants/package-map.js:2` | `analyzers/external-systems.js:16` |

### 3 TODOs in generated output code (by design, user-visible)

- `role-fallback-templates.js:20` — `# TODO: translate NiFi routing rules`
- `utility-handlers.js:139` — `# TODO: Apply actual processing logic here`
- `transform-handlers.js:218` — `# TODO: Port Groovy logic to Python`

### Other dead code

- `utils/dom-helpers.js` — all exports orphaned (consumers have private copies)
- `core/errors.js` — `getErrorLog` exported, never called
- `utils/local-storage.js` — `safeRemoveItem` exported, never called
- `utils/debounce.js` — `throttle` exported, never called
- `security/html-sanitizer.js` — `safeInnerHTML`, `createSafeEl` exported, never called
- `main.js` — `showPanel`, `hidePanel`, `showPathToast`, `hidePathToast`, `flashNoPath` imported but never called
- `mappers/index.js:86` — `window._lastParsedNiFi = nifi` — undocumented debug artifact
- `nosql-handlers.js:132` — hardcoded `'test'` as fallback DB name

---

## 2. NOTEBOOK FORMAT & DATABRICKS COMPATIBILITY

### CRITICAL bugs (will cause runtime failures)

| # | Issue | File | Lines | Detail |
|---|-------|------|-------|--------|
| 1 | **`%pip install` missing `# MAGIC` prefix** | `step-handlers.js` | 544 | Generates `%pip install requests` as raw Python in a `type:'code'` cell → SyntaxError in Databricks |
| 2 | **`exportAsDatabricksNotebook` type mismatch** | `export-formats.js` | 27-33 | Checks `'markdown'` but generator emits `'md'` → ALL markdown cells become raw Python → SyntaxError |
| 3 | **`GetHTTP` uses `wholeTextFiles(url)`** | `nifi-databricks-map.js` | 6 | `wholeTextFiles` reads file paths, not HTTP URLs → exception |
| 4 | **`/tmp/` checkpoint paths** (10 locations) | Multiple | — | Non-persistent, break on driver restart, workers can't see driver `/tmp/` |
| 5 | **`/tmp/` file staging then `spark.read`** (6 locations) | Multiple | — | Workers can't see driver `/tmp/` on multi-node clusters |
| 6 | **`format("druid")` doesn't exist** | `nifi-databricks-map.js` | 209 | No standard Spark data source format for Druid |
| 7 | **`PutBigQueryStreaming` uses `writeStream`** | `nifi-databricks-map.js` | 201 | BigQuery connector doesn't support `writeStream` |
| 8 | **`xpath_string` not importable** from `pyspark.sql.functions` | `nifi-databricks-map.js:463`, `function-handlers.js:227` | Affects EvaluateXPath + NEL `unescapeXml` |
| 9 | **SQL injection in `__execution_log` INSERT** | `error-framework.js` | 51-58 | Error messages with quotes break the error handler SQL |

### HIGH issues

| # | Issue | Locations |
|---|-------|-----------|
| 10 | `/mnt/` paths deprecated in Unity Catalog | `nifi-databricks-map.js:5,542`, `transform-handlers.js:277,487,512,519`, `hadoop-handlers.js:64,77,100` |
| 11 | `localhost` fallbacks in templates | `messaging-handlers.js:86,101`, `controller-service.js:47` |
| 12 | `ConsumeAMQP` code fully commented out — stub only | `nifi-databricks-map.js:109` |
| 13 | Third-party packages not auto-detected for `%pip` cell | `stomp.py`, `paramiko`, `pika`, `paho-mqtt`, `websocket-client`, `grpc`, `smbclient`, `pysolr`, `pysnmp`, `geoip2`, `influxdb-client`, `exchangelib`, `requests` |
| 14 | `ListenUDP` blocking `recvfrom` hangs notebook | `nifi-databricks-map.js:242` |
| 15 | `format("org.apache.phoenix.spark")` dead on Spark 3+ | `database-handlers.js:90-92` |
| 16 | `ConsumeAzureEventHub` deprecated JVM reflection | `cloud-azure-handlers.js:48,57` |
| 17 | `EncryptContent` handler embeds key in SQL via f-string | `transform-handlers.js:201` |

### `/tmp/` locations to fix

| File | Line | Context |
|------|------|---------|
| `nifi-databricks-map.js` | 43 | ExecuteStreamCommand: `dbutils.fs.put("/tmp/{v}_cmd.sh")` |
| `nifi-databricks-map.js` | 162 | ExecuteFlumeSink: `checkpointLocation: "/tmp/checkpoint/{v}"` |
| `nifi-databricks-map.js` | 235 | FetchSFTP: `"/tmp/{v}_download"` |
| `nifi-databricks-map.js` | 237 | FetchFTP: `"/tmp/{v}_download"` |
| `nifi-databricks-map.js` | 69 | PutSFTP: `"/tmp/{v}_export"` |
| `nifi-databricks-map.js` | 169 | PutFTP: `"/tmp/{v}_export"` |
| `generators/streaming-wrapper.js` | 54 | foreachBatch: `"/tmp/checkpoints/{varName}"` |
| `mappers/streaming-propagator.js` | 85 | Same: `"/tmp/checkpoints/{varName}"` |
| `mappers/handlers/utility-handlers.js` | 52 | Wait: `"/tmp/checkpoints/wait_{signalId}"` |
| `mappers/handlers/sink-handlers.js` | 48 | SFTP sink: `"/tmp/_sftp_out.csv"` |

### `/mnt/` locations to fix

| File | Line | Context |
|------|------|---------|
| `nifi-databricks-map.js` | 5 | GetFile: `cloudFiles.schemaLocation: "/mnt/schema/{v}"` |
| `nifi-databricks-map.js` | 542 | GetHDFSEvents: `/mnt/schema/hdfs_events` |
| `transform-handlers.js` | 277 | SplitXml: `"/mnt/data/*.xml"` |
| `transform-handlers.js` | 487 | ValidateCsv: `"/mnt/data/*.csv"` |
| `transform-handlers.js` | 512 | AvroMetadata: `"/mnt/data/*.avro"` |
| `transform-handlers.js` | 519 | Parquet: `"/mnt/data/*.parquet"` |
| `hadoop-handlers.js` | 64, 77, 100 | Various HDFS handlers |

---

## 3. MAPPER ACCURACY & GAPS

### 40+ duplicate keys in `nifi-databricks-map.js`

JS objects silently overwrite earlier definitions. Key duplicates include:

| Key | First Line | Second Line | Surviving |
|-----|-----------|-------------|-----------|
| `LookupRecord` | 55 | 145 | Second |
| `ValidateRecord` | 49 | 146 | Second |
| `ConvertAvroToJSON` | 44 | 149 | Second |
| `DeleteS3Object` | 185 | 310 | Second |
| `PutKinesisStream` | 187 | 312 | Second |
| `FetchAzureDataLakeStorage` | 191 | 319 | Second |
| `PutAzureCosmosDB` | 194 | 323 | Second |
| `GetAzureEventHub` | 196 | 325 | Second |
| `PutSnowflake` | 203 | 339 | Second |
| `PutDruidRecord` | 209 | 347 | Second |
| `PutIceberg` | 215 | 355 | Second |
| `PutHudi` | 217 | 356 | Second |
| `ConvertJSONToAvro` | 150 | 369 | Second |
| `GetSplunk` | 249 | 359 | Second |
| `PutSQL` | 62 | 419 | Second |
| `ListDatabaseTables` | 164 | 418 | Second |
| + ~25 more... | | | |

### Inflated confidence scores

| Processor | Assigned | Realistic | Reason |
|-----------|----------|-----------|--------|
| `GetHTTP` | 0.90 | 0.45 | Template broken (`wholeTextFiles` can't fetch HTTP) |
| `HandleHttpResponse` (sink) | 0.92 | 0.30 | Stub that only prints |
| `CaptureChangeMySQL` | 0.92 | 0.35 | Assumes Delta CDF exists |
| `format("org.apache.phoenix.spark")` | 0.90 | 0.20 | Dead on Spark 3+ |
| `format("druid")` | 0.90 | 0.20 | Non-existent format |
| `ConsumeAzureEventHub` | 0.92 | 0.60 | Deprecated JVM reflection |
| `EvaluateXPath` | 0.90 | 0.55 | `xpath_string` not importable |
| `JoltTransformJSON` | 0.90 | 0.55 | Only handles trivial cases |
| `SplitXml` (handler) | 0.92 | 0.55 | Hardcoded `/mnt/data/*.xml` |
| `PutKinesisStream` | 0.90 | 0.55 | Requires non-standard connector |
| `ExecuteScript` (Groovy) | 0.85 | 0.40 | Requires full manual port |

### NEL function bugs

| Function | Mode | Issue |
|----------|------|-------|
| `replaceFirst` | col | Replaces ALL occurrences, not first (uses `regexp_replace`) |
| `unescapeXml` | col | Maps to non-existent `xpath_string` (wrong semantic — should decode XML entities) |
| `escapeCSV` | col | Uses `concat_ws(",", ...)` — concatenates columns instead of CSV-quoting a value |
| `unescapeCSV` | col | Uses `split(",")` — splits into array instead of unquoting |
| `math:floor` | python | Inline `import math; math.floor(...)` is a syntax error in expressions |
| `math:ceil` | python | Same inline import syntax error |
| `tonumber` | both | Duplicate handler: line 203 returns `cast("long")`, line 249 returns `cast("double")` |

### Handler-specific bugs

| Handler | Issue |
|---------|-------|
| `FlattenJson` (transform-handlers.js:246) | Recursive sub-select creates broken cross-DataFrame column references |
| `RouteOnContent` (route-handlers.js:99) | `df.subtract(df)` always produces empty unmatched set — logic bug |
| `UpdateCounter` (utility-handlers.js:169) | `df.foreach(_count)` materializes entire DataFrame; result discarded |
| `HandleHttpResponse` (sink-handlers.js:26) | Only prints a message — no actual HTTP response |
| `CaptureChangeMySQL` (source-handlers.js:130) | Reads Delta CDF, not MySQL binlog — conflates ingestion with CDC |

### Missing enterprise processors (top priority)

1. `CaptureChangePostgreSQL` — real-time PostgreSQL CDC
2. `CaptureChangeOracle` — Oracle LogMiner CDC
3. `CaptureChangeMSSQL` — SQL Server CDC
4. `GetOAuth2AccessToken` / `FetchOAuth2AccessToken` — OAuth2 tokens
5. `CalculateRecordStats` — data quality pipelines
6. `WatchDirectory` / `GetFileInfo` — operational monitoring
7. `RecordPathSetAttribute` — RecordPath expressions
8. `PrioritizeAttributeFlow` — backpressure management

---

## 4. PIPELINE & STATE INTEGRITY

### CRITICAL

| # | Issue | File | Lines |
|---|-------|------|-------|
| 1 | **PipelineOrchestrator is dead code** — `flow:loaded` never emitted, `pipeline.execute()` never runs | `main.js` | 437-442 |
| 2 | **Steps 2-5 not `await`ed in PipelineOrchestrator** — if ever invoked, steps emit `:done` before completing | `main.js` | 362, 372, 382, 392 |
| 3 | **Drag-and-drop broken** — `handleFile()` not awaited, `fileInput.files = ...` doesn't fire `change` event | `file-upload.js` | 142-149 |

### HIGH

| # | Issue | File | Lines |
|---|-------|------|-------|
| 4 | `state.valueAnalysis` stores `{html, valueAnalysis}` object instead of structured data | `main.js` | 609-622 |
| 5 | Dual `INITIAL_STATE` (main.js vs state.js) — incompatible shapes, legacy pollution | `main.js:85-100`, `state.js:10-20` |
| 6 | Steps 6-8 have no snapshot/rollback on error | `main.js` | 550-622 |
| 7 | 6 inline `onclick` attributes in rendered HTML — bypasses CSP | `step-handlers.js:574-575,671`, `final-report.js:129`, `value-analysis.js:322`, `main.js:598` |

### MEDIUM

| # | Issue | File | Lines |
|---|-------|------|-------|
| 8 | `assess` prerequisite doesn't require `analysis` | `state.js` | 84 |
| 9 | `window._lastParsedNiFi` global side-effect in pure function | `mappers/index.js` | 86 |
| 10 | Progress goes backwards: 95% → 92% | `validators/index.js` | 108→123 |
| 11 | `cfgResetBtn` restores localStorage config, not factory defaults | `main.js` | 321-341 |
| 12 | `loadSampleFile` doesn't await `parseFn()` | `sample-flows.js` | 312-313 |

---

## 5. FORMAT HANDLERS & PARSERS

### HIGH

| # | Issue | File | Lines |
|---|-------|------|-------|
| 1 | **No try/catch in any format handler** — pako/JSZip errors propagate uncaught | `format-handlers.js` | all |
| 2 | **No magic-byte sniffing** — wrong extension sends binary to text parser | `parsers/index.js` | 77-103 |
| 3 | **parseTar ignores GNU long filenames + PAX headers** — filenames truncated at 100 chars | `format-handlers.js` | 71-94 |
| 4 | **SQL semicolon split** breaks on stored procedures, comments, string literals with `;` | `format-handlers.js` | 99 |
| 5 | **DOCX `closest('w\\:p')` invalid CSS** — paragraph newline logic dead in browsers | `format-handlers.js` | 198 |
| 6 | **XLSX only reads sheet1.xml** — multi-sheet workbooks silently ignored | `format-handlers.js` | 250 |
| 7 | **XLSX sparse rows not handled** — missing cells collapse column alignment | `format-handlers.js` | 258-266 |
| 8 | **0-byte binary files pass guard** — `Uint8Array(0)` is truthy, throws inside pako | `parsers/index.js` | all binary branches |
| 9 | **No file size limit** — ZIP bombs and 100MB+ files without guard | everywhere | — |
| 10 | **Password-protected ZIPs** silently yield garbage | `format-handlers.js` | 36-44 |

### MEDIUM

| # | Issue | File | Lines |
|---|-------|------|-------|
| 11 | `_rawXml` only set for XML path — variable registry silently skips JSON | `parsers/index.js` | 165,212 |
| 12 | NiFi REST API format and `versionedFlowSnapshot` wrapper not handled | `nifi-registry-json.js` | call site |
| 13 | Controller services inside process groups not extracted (flow.xml.gz) | `nifi-xml-parser.js` | 86-89 |
| 14 | DDL parser can't handle `MAP<K,V>`, `STRUCT<...>` (Hive/Spark complex types) | `ddl-parser.js` | 85-86 |
| 15 | `_nifi` from JSON parser missing `idToName` key | `nifi-registry-json.js` | 78-91 |
| 16 | NUL `typeFlag` check in parseTar: `=== ''` is dead code (NUL decodes as `'\0'`) | `format-handlers.js` | 80 |
| 17 | Malformed octal size in tar treated as 0, corrupts all subsequent offsets | `format-handlers.js` | 75 |
| 18 | `extractXlsxData` shared strings: `sharedStrings[0]` fails when string is `''` | `format-handlers.js` | 263 |
| 19 | Inline strings (`t="inlineStr"`) in XLSX not handled | `format-handlers.js` | 260 |
| 20 | `buildDocumentFlow` regex false-positives on normal English prose | `format-handlers.js` | 283 |

### ZERO test coverage for

- `format-handlers.js`
- `nifi-registry-json.js`
- `ddl-parser.js`
- `sql-table-builder.js`
- `nel/el-evaluator.js`
- `nel/variable-registry.js`
- `parsers/index.js` (dispatch logic)

---

## 6. SUMMARY BY SEVERITY

| Severity | Count | Key Areas |
|----------|-------|-----------|
| **CRITICAL** | 6 | Dead PipelineOrchestrator, `%pip` format bug, `export-formats.js` type mismatch, broken drag-drop, `GetHTTP` template, dead cycle detection |
| **HIGH** | ~20 | `/tmp/` paths, duplicate map keys, fake Spark formats, format handler error handling, SQL injection in error logger, no magic-byte sniffing |
| **MEDIUM** | ~18 | NEL function bugs, confidence inflation, orphan modules, state inconsistencies, missing test coverage |
| **LOW** | ~8 | Dead code, cosmetic issues, edge cases |

**Total issues identified: ~52**

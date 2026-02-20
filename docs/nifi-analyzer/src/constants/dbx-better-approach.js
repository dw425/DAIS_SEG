// Databricks "better approach" patterns — where Databricks offers superior alternatives
export const DBX_BETTER_APPROACH = {
  file_polling: { nifi: 'GetFile/ListFile polling with scheduling', dbx: 'Auto Loader (cloudFiles) with file notification mode', benefit: '10-100x faster file discovery, exactly-once guarantees, schema evolution' },
  batch_loop: { nifi: 'Repeated batch processing via CRON-scheduled processors', dbx: 'Structured Streaming with trigger(availableNow=True)', benefit: 'Incremental processing, checkpoint-based exactly-once, auto-scaling' },
  schema_mgmt: { nifi: 'Schema Registry + Avro/JSON schema enforcement per processor', dbx: 'Unity Catalog schema governance with automatic schema evolution', benefit: 'Centralized governance, lineage tracking, access controls' },
  data_quality: { nifi: 'ValidateRecord + RouteOnAttribute for quality checks', dbx: 'DLT Expectations (expect, expect_or_drop, expect_or_fail)', benefit: 'Declarative quality rules, automatic quarantine, quality dashboards' },
  dedup: { nifi: 'DetectDuplicate processor with distributed cache', dbx: 'dropDuplicates() or MERGE INTO with Delta Lake', benefit: 'No external cache needed, ACID guarantees, time travel' },
  merge_small: { nifi: 'MergeContent to combine small files', dbx: 'Delta Lake Auto Optimize + Auto Compaction', benefit: 'Automatic small file compaction, no manual merge logic' },
  scheduling: { nifi: 'CRON-driven processor scheduling with backpressure', dbx: 'Databricks Workflows with task dependencies and triggers', benefit: 'DAG-based orchestration, conditional logic, cost-optimized clusters' },
  caching: { nifi: 'DistributedMapCache for lookup enrichment', dbx: 'Broadcast variables or Delta Lake lookups', benefit: 'No external cache infrastructure, automatic distribution' },
  security: { nifi: 'Per-processor credentials and NiFi Registry policies', dbx: 'Unity Catalog + Secret Scopes + identity federation', benefit: 'Centralized IAM, fine-grained ACLs, audit logging' },
  monitoring: { nifi: 'NiFi bulletins, provenance, and flow status', dbx: 'Spark UI, Ganglia, custom Databricks dashboards', benefit: 'Deep execution insights, cost tracking, ML-integrated monitoring' },
};

-- Databricks DLT Pipeline: Medallion Architecture for Synthetic Environments
-- Deploy via: databricks bundle deploy (references this in databricks.yml)
--
-- This DLT pipeline definition mirrors the Python MedallionPipeline class
-- and can be used as an alternative deployment path for production DLT.

-- ============================================================================
-- BRONZE: Raw synthetic data ingestion
-- ============================================================================

CREATE OR REFRESH STREAMING LIVE TABLE bronze_raw
COMMENT "Raw synthetic data landed as-is from the generation engine"
AS SELECT
  *,
  current_timestamp() AS _bronze_ingested_at,
  input_file_name() AS _bronze_source
FROM cloud_files(
  "${source_path}",
  "delta"
);

-- ============================================================================
-- SILVER: Cleaned and conformed
-- ============================================================================

CREATE OR REFRESH LIVE TABLE silver_cleaned (
  CONSTRAINT valid_not_all_null EXPECT (NOT (${all_null_check})) ON VIOLATION DROP ROW,
  CONSTRAINT valid_primary_key EXPECT (${pk_column} IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Cleaned and schema-enforced synthetic data"
AS SELECT
  * EXCEPT (_bronze_ingested_at, _bronze_source),
  current_timestamp() AS _silver_processed_at
FROM LIVE.bronze_raw;

-- ============================================================================
-- GOLD: Business-ready aggregations
-- ============================================================================

CREATE OR REFRESH LIVE TABLE gold_summary
COMMENT "Aggregated business-ready view for Genie dashboards and ML Serving"
AS SELECT
  * EXCEPT (_silver_processed_at),
  current_timestamp() AS _gold_published_at
FROM LIVE.silver_cleaned;

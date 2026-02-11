# Databricks notebook source
# MAGIC %md
# MAGIC # Pillar 1 — Profile Source System
# MAGIC
# MAGIC Crawls a source system via **Lakehouse Federation** and **Unity Catalog**,
# MAGIC profiles column distributions using **Foundation Model** agents, and generates
# MAGIC a **Source Blueprint** stored as a versioned Delta Table.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Lakehouse Federation connection configured in Unity Catalog
# MAGIC - Foundation Model serving endpoint active
# MAGIC - `dais-seg` wheel installed on the cluster

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/$current_user/dais-seg-*.whl --quiet

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Foreign Catalog Name")
dbutils.widgets.text("catalog", "dais_seg", "Target Catalog")
dbutils.widgets.text("schema", "blueprints", "Target Schema")
dbutils.widgets.text("sample_size", "50000", "Sample Size per Table")
dbutils.widgets.text("exclude_schemas", "information_schema,sys", "Schemas to Exclude (comma-separated)")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sample_size = int(dbutils.widgets.get("sample_size"))
exclude_schemas = [s.strip() for s in dbutils.widgets.get("exclude_schemas").split(",")]

assert source_catalog, "source_catalog widget is required"

print(f"Source catalog: {source_catalog}")
print(f"Target: {catalog}.{schema}")
print(f"Sample size: {sample_size:,}")

# COMMAND ----------

from dais_seg.config import SEGConfig, set_config

config = SEGConfig(
    catalog=catalog,
    schema=schema,
)
set_config(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Crawl Source Metadata

# COMMAND ----------

from dais_seg.profiler import CatalogCrawler, FederationConnector

connector = FederationConnector(spark, catalog)
crawler = CatalogCrawler(spark, connector)

crawl_result = crawler.crawl(
    foreign_catalog=source_catalog,
    exclude_schemas=exclude_schemas,
)

print(f"\nCrawl complete:")
print(f"  Schemas: {len(crawl_result.schemas)}")
print(f"  Tables: {len(crawl_result.tables)}")
print(f"  Columns: {crawl_result.total_columns}")
print(f"  Total rows: {crawl_result.total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Profile Distributions

# COMMAND ----------

from dais_seg.profiler import DistributionProfiler

profiler = DistributionProfiler(spark, connector, sample_size=sample_size)
profiles = profiler.profile_crawl_result(crawl_result)

print(f"\nProfiled {len(profiles)} tables")
for p in profiles:
    print(f"  {p.schema}.{p.name}: {len(p.column_profiles)} columns, {p.row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate & Save Blueprint

# COMMAND ----------

from dais_seg.profiler import BlueprintGenerator

generator = BlueprintGenerator(spark)
blueprint = generator.generate(crawl_result, profiles)
blueprint_id = generator.save_blueprint(blueprint)

print(f"\nBlueprint saved: {blueprint_id}")
print(f"  Tables: {len(blueprint['tables'])}")
print(f"  Relationships: {len(blueprint['relationships'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display blueprint summary
summary_data = [
    {
        "table": t["name"],
        "schema": t["schema"],
        "columns": len(t["columns"]),
        "row_count": t.get("row_count", 0),
        "foreign_keys": len(t.get("foreign_keys", [])),
    }
    for t in blueprint["tables"]
]

display(spark.createDataFrame(summary_data))

# COMMAND ----------

print(f"\n✅ Profiling complete. Blueprint ID: {blueprint_id}")
print(f"Next step: Run notebook 02_generate_synthetic with blueprint_id={blueprint_id}")

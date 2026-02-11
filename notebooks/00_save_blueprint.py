# Databricks notebook source
# MAGIC %md
# MAGIC # Pillar 0 — Save Blueprint from Source Loader
# MAGIC
# MAGIC Accepts a **pre-assembled blueprint JSON** from the Universal Source Loader
# MAGIC (parsed from DDL, ETL mappings, JSON/YAML schema definitions, etc.) and persists
# MAGIC it to the Blueprint Delta Table — the same format produced by notebook 01.
# MAGIC
# MAGIC This enables the Generate → Conform → Validate pipeline to run from static
# MAGIC file input without connecting to a live source database.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `dais-seg` wheel installed on the cluster

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/$current_user/dais-seg-*.whl --quiet

# COMMAND ----------

dbutils.widgets.text("blueprint_json", "", "Blueprint JSON (from Source Loader)")
dbutils.widgets.text("catalog", "dais_seg", "Catalog")
dbutils.widgets.text("schema", "blueprints", "Schema for blueprint storage")

# COMMAND ----------

import json

blueprint_json = dbutils.widgets.get("blueprint_json")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

assert blueprint_json, "blueprint_json widget is required"

blueprint = json.loads(blueprint_json)
blueprint_id = blueprint.get("blueprint_id", "unknown")

print(f"Blueprint ID: {blueprint_id}")
print(f"Source: {blueprint.get('source_system', {}).get('name', 'unknown')}")
print(f"Tables: {len(blueprint.get('tables', []))}")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

from dais_seg.config import SEGConfig, set_config

config = SEGConfig(catalog=catalog, schema=schema)
set_config(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Blueprint to Delta Table

# COMMAND ----------

from dais_seg.profiler import BlueprintGenerator

bp_saver = BlueprintGenerator(spark)
bp_saver.save_blueprint(blueprint)

print(f"✅ Blueprint {blueprint_id} saved to {catalog}.{schema}.blueprints")

# COMMAND ----------

# Verify it can be loaded back
loaded = bp_saver.load_blueprint(blueprint_id)
print(f"Verified: {len(loaded['tables'])} tables, {len(loaded['relationships'])} relationships")

for t in loaded["tables"]:
    print(f"  {t['name']}: {len(t['columns'])} columns, {t['row_count']} rows")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "blueprint_id": blueprint_id,
    "tables": len(loaded["tables"]),
    "source_name": loaded.get("source_system", {}).get("name", ""),
    "source_type": loaded.get("source_system", {}).get("type", ""),
}))

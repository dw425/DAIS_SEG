# Databricks notebook source
# MAGIC %md
# MAGIC # Pillar 4 — Validate Workspace
# MAGIC
# MAGIC Runs **like-for-like validation** across schema, data fidelity, quality
# MAGIC compliance, and pipeline integrity. Produces **green/amber/red confidence
# MAGIC scores** per table and per workspace — the decision gate for production cutover.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Medallion pipeline complete (notebook 03)
# MAGIC - `dais-seg` wheel installed on the cluster

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/$current_user/dais-seg-*.whl --quiet

# COMMAND ----------

dbutils.widgets.text("blueprint_id", "", "Blueprint ID")
dbutils.widgets.text("workspace_id", "", "Workspace ID (e.g., dais_seg.seg_team_alpha)")
dbutils.widgets.text("catalog", "dais_seg", "Catalog")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema to Validate")
dbutils.widgets.text("scale_factor", "0.1", "Scale Factor used during generation")

# COMMAND ----------

blueprint_id = dbutils.widgets.get("blueprint_id")
workspace_id = dbutils.widgets.get("workspace_id") or "default"
catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
scale_factor = float(dbutils.widgets.get("scale_factor"))

assert blueprint_id, "blueprint_id widget is required"

print(f"Workspace: {workspace_id}")
print(f"Blueprint: {blueprint_id}")
print(f"Validating: {catalog}.{gold_schema}")

# COMMAND ----------

from dais_seg.config import SEGConfig, set_config

config = SEGConfig(catalog=catalog, schema="blueprints", gold_schema=gold_schema)
set_config(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Blueprint

# COMMAND ----------

from dais_seg.profiler import BlueprintGenerator

bp_loader = BlueprintGenerator(spark)
blueprint = bp_loader.load_blueprint(blueprint_id)
print(f"Blueprint: {len(blueprint['tables'])} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Full Validation

# COMMAND ----------

from dais_seg.validator import WorkspaceValidator

validator = WorkspaceValidator(spark)
report = validator.validate(
    workspace_id=workspace_id,
    blueprint=blueprint,
    target_catalog=catalog,
    target_schema=gold_schema,
    scale_factor=scale_factor,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Confidence Scores

# COMMAND ----------

print(f"{'='*60}")
print(f"WORKSPACE CONFIDENCE: {report.confidence.overall_score:.1%} ({report.confidence.level.value.upper()})")
print(f"{'='*60}")
print(f"\n{report.confidence.summary}")

# COMMAND ----------

# Per-table breakdown
table_data = []
for t in report.table_details:
    table_data.append({
        "table": t.table_name,
        "score": f"{t.overall_score:.1%}",
        "level": t.level.value.upper(),
        "recommendations": "; ".join(t.recommendations) if t.recommendations else "None",
    })

display(spark.createDataFrame(table_data))

# COMMAND ----------

# Per-dimension breakdown for each table
for t in report.table_details:
    print(f"\n--- {t.table_name} ({t.level.value.upper()}: {t.overall_score:.1%}) ---")
    for d in t.dimensions:
        indicator = "✅" if d.level.value == "green" else ("⚠️" if d.level.value == "amber" else "❌")
        print(f"  {indicator} {d.dimension}: {d.score:.1%} (weight: {d.weight})")

# COMMAND ----------

print(f"\n✅ Validation complete: {report.confidence.level.value.upper()} ({report.confidence.overall_score:.1%})")
if report.confidence.level.value == "green":
    print("   This workspace is READY for production cutover.")
elif report.confidence.level.value == "amber":
    print("   This workspace needs attention before production cutover.")
else:
    print("   This workspace is NOT ready. Review recommendations above.")

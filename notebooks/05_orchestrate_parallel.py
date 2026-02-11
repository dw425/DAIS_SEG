# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Orchestration — Multi-Workstream Pipeline
# MAGIC
# MAGIC Spins up **N parallel workstreams**, each with its own isolated synthetic
# MAGIC workspace running the full Profile → Generate → Conform → Validate pipeline
# MAGIC simultaneously. This is the "radical parallelism" that compresses 9-month
# MAGIC migration timelines to weeks.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Source Blueprint generated (notebook 01)
# MAGIC - `dais-seg` wheel installed on the cluster
# MAGIC - Sufficient cluster resources for parallel execution

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/$current_user/dais-seg-*.whl --quiet

# COMMAND ----------

dbutils.widgets.text("blueprint_id", "", "Blueprint ID")
dbutils.widgets.text("catalog", "dais_seg", "Catalog")
dbutils.widgets.text("workstreams", "team_alpha,team_beta,team_gamma", "Workstream Names (comma-separated)")
dbutils.widgets.text("scale_factor", "0.1", "Scale Factor")
dbutils.widgets.text("max_parallel", "3", "Max Parallel Workspaces")

# COMMAND ----------

blueprint_id = dbutils.widgets.get("blueprint_id")
catalog = dbutils.widgets.get("catalog")
workstream_names = [w.strip() for w in dbutils.widgets.get("workstreams").split(",") if w.strip()]
scale_factor = float(dbutils.widgets.get("scale_factor"))
max_parallel = int(dbutils.widgets.get("max_parallel"))

assert blueprint_id, "blueprint_id is required"
assert workstream_names, "At least one workstream name is required"

print(f"Blueprint: {blueprint_id}")
print(f"Workstreams: {workstream_names}")
print(f"Scale: {scale_factor}x")
print(f"Max parallel: {max_parallel}")

# COMMAND ----------

from dais_seg.config import SEGConfig, set_config

config = SEGConfig(
    catalog=catalog,
    schema="blueprints",
    max_parallel_workspaces=max_parallel,
)
set_config(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Blueprint

# COMMAND ----------

from dais_seg.profiler import BlueprintGenerator

bp_loader = BlueprintGenerator(spark)
blueprint = bp_loader.load_blueprint(blueprint_id)
print(f"Blueprint: {len(blueprint['tables'])} tables, {len(blueprint['relationships'])} relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Workstreams

# COMMAND ----------

from dais_seg.workspace_manager.parallel_orchestrator import WorkstreamConfig

workstreams = [
    WorkstreamConfig(
        name=name,
        tables=[],  # Empty = all tables from blueprint
        scale_factor=scale_factor,
        priority=i,
    )
    for i, name in enumerate(workstream_names)
]

print(f"Configured {len(workstreams)} workstreams:")
for ws in workstreams:
    print(f"  {ws.name} (priority={ws.priority}, scale={ws.scale_factor}x)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Execute Parallel Orchestration

# COMMAND ----------

from dais_seg.workspace_manager import ParallelOrchestrator

orchestrator = ParallelOrchestrator(spark)
result = orchestrator.orchestrate(
    blueprint=blueprint,
    workstreams=workstreams,
    max_parallel=max_parallel,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Results

# COMMAND ----------

print(f"{'='*60}")
print(f"ORCHESTRATION COMPLETE")
print(f"{'='*60}")
print(f"  Total workstreams: {result.total_workstreams}")
print(f"  Completed: {result.completed}")
print(f"  Failed: {result.failed}")
print(f"  Total duration: {result.total_duration_seconds:.1f}s")

# COMMAND ----------

# Per-workstream results
ws_data = []
for ws in result.workstream_results:
    score = ""
    level = ""
    if ws.validation_report:
        score = f"{ws.validation_report.confidence.overall_score:.1%}"
        level = ws.validation_report.confidence.level.value.upper()

    ws_data.append({
        "workstream": ws.name,
        "status": ws.status,
        "workspace_id": ws.workspace_id,
        "tables_generated": ws.tables_generated,
        "tables_validated": ws.tables_validated,
        "confidence": score,
        "level": level,
        "duration_s": round(ws.duration_seconds, 1),
        "error": ws.error or "",
    })

display(spark.createDataFrame(ws_data))

# COMMAND ----------

print(f"\n✅ {result.completed}/{result.total_workstreams} workstreams completed successfully")
if result.failed > 0:
    print(f"   ⚠️ {result.failed} workstreams failed — check errors above")

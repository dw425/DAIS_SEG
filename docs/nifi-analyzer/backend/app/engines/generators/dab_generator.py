"""DAB (Databricks Asset Bundle) generator — produces a complete Databricks Asset Bundle
from parsed NiFi flow topology, assessment, and configuration.

Generates:
- databricks.yml with bundle config, variables, resources, and targets
- Notebook files per pipeline stage
- Inter-task metadata passing via dbutils.jobs.taskValues
"""

import logging
import re
from datetime import datetime, timezone

import yaml

logger = logging.getLogger(__name__)

from app.engines.generators.autoloader_generator import (
    generate_autoloader_code,
    generate_jdbc_source_code,
    generate_kafka_source_code,
    generate_streaming_trigger,
    is_autoloader_candidate,
    is_jdbc_source,
    is_kafka_source,
)
from app.engines.generators.code_scrubber import scrub_code
from app.engines.generators.processor_translators import (
    translate_execute_script,
    translate_jolt_transform,
    translate_route_on_attribute,
)
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, MappingEntry, ParseResult


def generate_dab(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig | None = None,
) -> dict:
    """Generate a complete Databricks Asset Bundle.

    Returns:
        dict with keys:
          - databricks_yml: str (YAML content for databricks.yml)
          - notebooks: dict[str, str] (filename -> content)
          - bundle_structure: dict (parsed YAML for inspection)
    """
    if config is None:
        config = DatabricksConfig()

    flow_name = _safe_key(parse_result.metadata.get("source_file", "migration"))

    # Build the bundle structure
    bundle = _build_bundle_config(flow_name, parse_result, assessment, config)

    # Generate notebook files
    notebooks = _generate_notebooks(parse_result, assessment, config, flow_name)

    # Serialize to YAML
    databricks_yml = yaml.dump(bundle, default_flow_style=False, sort_keys=False, width=120)

    logger.info("DAB generated: %d notebooks, bundle=%s", len(notebooks), flow_name)
    return {
        "databricks_yml": databricks_yml,
        "notebooks": notebooks,
        "bundle_structure": bundle,
    }


def _build_bundle_config(
    flow_name: str,
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> dict:
    """Build the complete databricks.yml structure."""
    return {
        "bundle": {
            "name": f"{flow_name}_migration",
        },
        "variables": _build_variables(parse_result, config),
        "resources": {
            "jobs": {
                "migration_job": _build_job(flow_name, parse_result, assessment, config),
            },
        },
        "targets": _build_targets(config),
    }


def _build_variables(parse_result: ParseResult, config: DatabricksConfig) -> dict:
    """Build variables block from NiFi Parameter Contexts and Controller Services."""
    variables = {}

    # Extract parameters from controller services
    for cs in parse_result.controller_services:
        cs_type = cs.type.lower()
        for key, val in cs.properties.items():
            safe_key = _safe_key(key)
            if not safe_key:
                continue

            # Detect secrets vs config
            if _is_secret_property(key):
                variables[safe_key] = {
                    "lookup": {
                        "scope": config.secret_scope,
                        "key": safe_key,
                    },
                }
            elif val and not str(val).startswith("${"):
                variables[safe_key] = {
                    "default": str(val),
                }

    # Extract from processor properties that reference NiFi parameters
    for proc in parse_result.processors:
        for key, val in proc.properties.items():
            if isinstance(val, str) and val.startswith("#{"):
                # NiFi Parameter Context reference: #{param_name}
                param_name = val.strip("#{}").strip()
                safe_key = _safe_key(param_name)
                if safe_key and safe_key not in variables:
                    if _is_secret_property(param_name):
                        variables[safe_key] = {
                            "lookup": {
                                "scope": config.secret_scope,
                                "key": safe_key,
                            },
                        }
                    else:
                        variables[safe_key] = {
                            "default": "",
                            "description": f"From NiFi parameter: {param_name}",
                        }

    # Add standard config variables
    if "catalog" not in variables:
        variables["catalog"] = {"default": config.catalog}
    if "schema" not in variables:
        variables["schema"] = {"default": config.schema_name}

    return variables


def _build_job(
    flow_name: str,
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> dict:
    """Build the job resource with tasks, clusters, and scheduling."""
    tasks = _build_tasks(flow_name, parse_result, assessment, config)
    schedule = _derive_schedule(parse_result)
    clusters = _build_job_clusters(parse_result, config)

    job: dict = {
        "name": "${bundle.name}_workflow",
        "tasks": tasks,
        "job_clusters": clusters,
        "tags": {
            "source_platform": parse_result.platform,
            "migration_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "processor_count": str(len(parse_result.processors)),
        },
        "email_notifications": {
            "on_failure": [],
        },
    }

    if schedule:
        job["schedule"] = schedule

    return job


def _build_tasks(
    flow_name: str,
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> list[dict]:
    """Build task list from assessment mappings, respecting NiFi flow DAG topology."""
    tasks = []
    task_keys_by_role: dict[str, list[str]] = {"source": [], "transform": [], "sink": [], "route": [], "process": [], "utility": []}

    # Group mappings by stage
    stages = _classify_into_stages(assessment)

    for stage_idx, (stage_name, stage_mappings) in enumerate(stages.items()):
        notebook_num = str(stage_idx + 1).zfill(2)
        task_key = f"{stage_name}__{notebook_num}"

        task: dict = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"./notebooks/{notebook_num}_{stage_name}.py",
                "base_parameters": {
                    "catalog": "${var.catalog}",
                    "schema": "${var.schema}",
                },
            },
            "job_cluster_key": "etl_cluster",
            "max_retries": 2,
            "timeout_seconds": 3600,
        }

        # Add dependencies based on stage ordering
        if stage_idx > 0:
            prev_stage_name = list(stages.keys())[stage_idx - 1]
            prev_notebook_num = str(stage_idx).zfill(2)
            prev_task_key = f"{prev_stage_name}__{prev_notebook_num}"
            task["depends_on"] = [{"task_key": prev_task_key}]

        # Check for RouteOnAttribute -> condition_task
        route_mappings = [m for m in stage_mappings if "RouteOnAttribute" in m.type]
        if route_mappings:
            route_result = translate_route_on_attribute(route_mappings[0], parse_result)
            if route_result["strategy"] == "workflow_condition":
                # Replace this task with condition_task entries
                for ct in route_result["workflow_tasks"]:
                    if task.get("depends_on"):
                        ct.setdefault("depends_on", task["depends_on"])
                    tasks.append(ct)
                continue

        tasks.append(task)

    # Add inter-task metadata passing for multi-task workflows
    if len(tasks) > 1:
        for i, task in enumerate(tasks):
            if "notebook_task" in task:
                task["notebook_task"].setdefault("base_parameters", {})
                if i > 0:
                    task["notebook_task"]["base_parameters"]["upstream_task"] = tasks[i - 1]["task_key"]

    return tasks


def _classify_into_stages(assessment: AssessmentResult) -> dict[str, list[MappingEntry]]:
    """Classify mappings into pipeline stages: ingest, transform, load."""
    stages: dict[str, list[MappingEntry]] = {}

    for mapping in assessment.mappings:
        if mapping.role == "source":
            stages.setdefault("ingest", []).append(mapping)
        elif mapping.role == "sink":
            stages.setdefault("load", []).append(mapping)
        elif mapping.role in ("route", "transform", "process"):
            stages.setdefault("transform", []).append(mapping)
        else:
            stages.setdefault("transform", []).append(mapping)

    # Ensure ordering: ingest -> transform -> load
    ordered: dict[str, list[MappingEntry]] = {}
    for key in ("ingest", "transform", "load"):
        if key in stages:
            ordered[key] = stages[key]

    # Add any remaining stages
    for key, val in stages.items():
        if key not in ordered:
            ordered[key] = val

    return ordered if ordered else {"pipeline": assessment.mappings}


def _derive_schedule(parse_result: ParseResult) -> dict | None:
    """Derive job schedule from NiFi scheduling properties."""
    for proc in parse_result.processors:
        if proc.scheduling:
            # Support both key conventions: JSON parser uses "strategy"/"period",
            # XML parser may use "schedulingStrategy"/"schedulingPeriod"
            strategy = (proc.scheduling.get("strategy", "") or proc.scheduling.get("schedulingStrategy", "")).upper()
            period = proc.scheduling.get("period", "") or proc.scheduling.get("schedulingPeriod", "")

            if strategy == "CRON_DRIVEN" and period:
                # Convert NiFi cron to Quartz cron format
                return {
                    "quartz_cron_expression": period,
                    "timezone_id": "UTC",
                    "pause_status": "UNPAUSED",
                }

            if strategy == "TIMER_DRIVEN" and period:
                cron = _period_to_cron(period)
                if cron:
                    return {
                        "quartz_cron_expression": cron,
                        "timezone_id": "UTC",
                        "pause_status": "UNPAUSED",
                    }

    return None


def _build_job_clusters(parse_result: ParseResult, config: DatabricksConfig) -> list[dict]:
    """Build job cluster configuration, mapping NiFi concurrent task limits to autoscaling."""
    # Determine max concurrency from NiFi processor settings
    max_concurrent = 2  # default min_workers
    for proc in parse_result.processors:
        if proc.scheduling:
            concurrent = proc.scheduling.get("concurrentlySchedulableTaskCount", 1)
            try:
                max_concurrent = max(max_concurrent, int(concurrent))
            except (ValueError, TypeError):
                logger.debug("Non-integer concurrent task count: %r", concurrent)

    # Cap autoscaling
    max_workers = min(max_concurrent * 2, 16)

    node_type = {
        "aws": "i3.xlarge",
        "azure": "Standard_DS3_v2",
        "gcp": "n1-standard-4",
    }.get(config.cloud_provider, "i3.xlarge")

    # Databricks Jobs API requires exactly one of num_workers or autoscale —
    # specifying both is an API error.  Use autoscale when max > min.
    cluster_sizing: dict
    if max_workers > 2:
        cluster_sizing = {
            "autoscale": {
                "min_workers": 2,
                "max_workers": max_workers,
            },
        }
    else:
        cluster_sizing = {
            "num_workers": 2,
        }

    return [
        {
            "job_cluster_key": "etl_cluster",
            "new_cluster": {
                "spark_version": f"{config.runtime_version}.x-scala2.12",
                **cluster_sizing,
                "node_type_id": node_type,
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                },
                "custom_tags": {
                    "project": "etl-migration",
                },
            },
        }
    ]


def _build_targets(config: DatabricksConfig) -> dict:
    """Build deployment targets (dev, staging, prod)."""
    return {
        "dev": {
            "mode": "development",
            "default": True,
            "resources": {
                "jobs": {
                    "migration_job": {
                        "job_clusters": [
                            {
                                "job_cluster_key": "etl_cluster",
                                "new_cluster": {
                                    "num_workers": 1,
                                },
                            }
                        ],
                    },
                },
            },
        },
        "staging": {
            "mode": "development",
        },
        "prod": {
            "mode": "production",
            "resources": {
                "jobs": {
                    "migration_job": {
                        "tags": {
                            "environment": "production",
                        },
                    },
                },
            },
        },
    }


def _generate_notebooks(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
    flow_name: str,
) -> dict[str, str]:
    """Generate notebook files for each pipeline stage."""
    notebooks = {}
    stages = _classify_into_stages(assessment)

    for stage_idx, (stage_name, stage_mappings) in enumerate(stages.items()):
        notebook_num = str(stage_idx + 1).zfill(2)
        filename = f"notebooks/{notebook_num}_{stage_name}.py"

        lines = [
            "# Databricks notebook source",
            "",
            "# MAGIC %md",
            f"# MAGIC # {stage_name.title()} Stage",
            f"# MAGIC **Generated from NiFi flow**: {flow_name}",
            f"# MAGIC **Stage**: {stage_name} ({len(stage_mappings)} processors)",
            f"# MAGIC **Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            "# COMMAND ----------",
            "",
            "# Parameters from workflow",
            'catalog = dbutils.widgets.get("catalog")',
            'schema = dbutils.widgets.get("schema")',
            'spark.sql(f"USE CATALOG {catalog}")',
            'spark.sql(f"USE SCHEMA {schema}")',
            "",
            "# COMMAND ----------",
            "",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.types import *",
            "",
            "# COMMAND ----------",
            "",
        ]

        # Inter-task metadata retrieval
        if stage_idx > 0:
            lines.extend([
                "# Retrieve upstream task metadata",
                'upstream_task = dbutils.widgets.get("upstream_task")',
                "# upstream_result = dbutils.jobs.taskValues.get(",
                '#     taskKey=upstream_task, key="output_table", debugValue="default_table"',
                "# )",
                "",
                "# COMMAND ----------",
                "",
            ])

        # Generate code for each mapping in this stage
        for mapping in stage_mappings:
            lines.append(f"# --- {mapping.name} ({mapping.type}) ---")
            lines.append("")

            code = _generate_code_for_mapping(mapping, parse_result, config)
            lines.append(code)
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        # Inter-task metadata setting
        lines.extend([
            "# Pass metadata to downstream tasks",
            f'dbutils.jobs.taskValues.set(key="output_table", value="{config.catalog}.{config.schema_name}.{stage_name}")',
            f'dbutils.jobs.taskValues.set(key="stage_status", value="complete")',
            "",
            'print(f"[DONE] {stage_name.title()} stage complete")',
        ])

        notebooks[filename] = "\n".join(lines)

    return notebooks


def _generate_code_for_mapping(
    mapping: MappingEntry,
    parse_result: ParseResult,
    config: DatabricksConfig,
) -> str:
    """Generate specialized code for a single mapping entry."""
    proc_type = mapping.type

    # Auto Loader
    if is_autoloader_candidate(proc_type):
        code = generate_autoloader_code(mapping, parse_result, config)
        trigger = generate_streaming_trigger(mapping, parse_result, config)
        return f"{code}\n\n{trigger}"

    # Kafka
    if is_kafka_source(proc_type):
        code = generate_kafka_source_code(mapping, parse_result, config)
        trigger = generate_streaming_trigger(mapping, parse_result, config)
        return f"{code}\n\n{trigger}"

    # JDBC
    if is_jdbc_source(proc_type):
        return generate_jdbc_source_code(mapping, parse_result, config)

    # RouteOnAttribute
    if "RouteOnAttribute" in proc_type:
        result = translate_route_on_attribute(mapping, parse_result)
        return result["code"]

    # JoltTransform
    if "Jolt" in proc_type:
        return translate_jolt_transform(mapping, parse_result)

    # ExecuteScript
    if any(kw in proc_type for kw in ("ExecuteScript", "ExecuteGroovyScript")):
        return translate_execute_script(mapping, parse_result)

    # Fall back to assessment-provided code
    if mapping.code:
        return scrub_code(mapping.code)

    return f"# TODO: implement {proc_type} ({mapping.name})"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:50].strip("_").lower()


def _is_secret_property(key: str) -> bool:
    """Check if a property key likely holds a secret value."""
    return bool(re.search(
        r"password|token|secret|credential|api[_.]?key|private[_.]?key|passphrase",
        key,
        re.IGNORECASE,
    ))


def _period_to_cron(period: str) -> str | None:
    """Convert a NiFi scheduling period to Quartz cron.

    Handles sec/min/hour/day units. Seconds below 60 are rounded up to 1 minute
    since Quartz cron minimum granularity is 1 minute for Databricks Jobs.
    """
    m = re.match(r"(\d+)\s*(sec|min|hour|day)", period, re.IGNORECASE)
    if not m:
        return None
    val, unit = int(m.group(1)), m.group(2).lower()
    if unit.startswith("sec"):
        # Convert seconds to minutes (minimum 1 minute for cron)
        minutes = max(1, val // 60)
        return f"0 0/{minutes} * * * ?"
    if unit.startswith("min"):
        return f"0 0/{val} * * * ?"
    if unit.startswith("hour"):
        return f"0 0 0/{val} * * ?"
    if unit.startswith("day"):
        return f"0 0 0 1/{val} * ?"
    return None

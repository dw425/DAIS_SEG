"""Workflow orchestrator — generates Databricks Workflows job definitions.

Produces a full job JSON with tasks derived from NiFi process groups / pipeline stages,
dependency graph topology, autoscaling clusters, retry policies, and schedules.
"""

import logging
import re
from datetime import datetime, timezone

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)


def generate_workflow(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig | None = None,
) -> dict:
    """Generate a Databricks Workflows job definition from the parsed flow.

    Returns dict with: name, tasks, jobClusters, schedule,
    emailNotifications, tags.
    """
    if config is None:
        config = DatabricksConfig()

    safe_name = _safe_key(
        parse_result.metadata.get("source_file", "migration")
    )

    tasks = _build_tasks(parse_result, assessment, safe_name, config)
    job_clusters = _build_job_clusters(config)
    schedule = _derive_schedule(parse_result)
    email_notifications = _build_email_notifications(parse_result, assessment)
    tags = _build_tags(parse_result)

    logger.info("Workflow orchestrator: %d tasks generated for %s", len(tasks), safe_name)
    return {
        "name": f"migration_{safe_name}",
        "tasks": tasks,
        "jobClusters": job_clusters,
        "schedule": schedule,
        "emailNotifications": email_notifications,
        "tags": tags,
    }


def _build_tasks(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    safe_name: str,
    config: DatabricksConfig,
) -> list[dict]:
    """Build task list from process groups or individual mappings."""
    tasks: list[dict] = []

    # If process groups exist, create tasks per group
    if parse_result.process_groups:
        group_order = [pg.name for pg in parse_result.process_groups]
        dep_graph = _build_dependency_graph(parse_result)

        for i, pg in enumerate(parse_result.process_groups):
            task_key = f"group_{_safe_key(pg.name)}"
            task: dict = {
                "task_key": task_key,
                "notebook_task": {
                    "notebook_path": f"/Workspace/migrations/{safe_name}/{_safe_key(pg.name)}",
                    "base_parameters": {
                        "catalog": config.catalog,
                        "schema": config.schema_name,
                    },
                },
                "job_cluster_key": "migration_cluster",
                "max_retries": 2,
                "retry_on_timeout": True,
                "timeout_seconds": 3600,
            }

            # Dependencies based on flow graph
            deps = dep_graph.get(pg.name, [])
            if deps:
                task["depends_on"] = [
                    {"task_key": f"group_{_safe_key(d)}"} for d in deps
                    if d in group_order
                ]

            tasks.append(task)
    else:
        # Fall back to per-mapping tasks
        prev_task = ""
        for i, mapping in enumerate(assessment.mappings):
            if not mapping.code:
                continue

            task_key = f"step_{i + 1}_{_safe_key(mapping.name)}"
            task: dict = {
                "task_key": task_key,
                "notebook_task": {
                    "notebook_path": f"/Workspace/migrations/{safe_name}",
                    "base_parameters": {
                        "catalog": config.catalog,
                        "schema": config.schema_name,
                    },
                },
                "job_cluster_key": "migration_cluster",
                "max_retries": 2,
                "retry_on_timeout": True,
                "timeout_seconds": 3600,
            }

            if prev_task:
                task["depends_on"] = [{"task_key": prev_task}]

            tasks.append(task)
            prev_task = task_key

    return tasks


def _build_job_clusters(config: DatabricksConfig) -> list[dict]:
    """Build job cluster configuration with autoscaling."""
    node_type = {
        "aws": "i3.xlarge",
        "azure": "Standard_DS3_v2",
        "gcp": "n1-standard-4",
    }.get(config.cloud_provider, "i3.xlarge")

    return [
        {
            "job_cluster_key": "migration_cluster",
            "new_cluster": {
                "spark_version": f"{config.runtime_version}.x-scala2.12",
                "node_type_id": node_type,
                "autoscale": {
                    "min_workers": 2,
                    "max_workers": 8,
                },
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                },
                "custom_tags": {
                    "project": "etl-migration",
                },
            },
        }
    ]


def _derive_schedule(parse_result: ParseResult) -> dict | None:
    """Derive cron schedule from ControlRate or scheduling properties."""
    for proc in parse_result.processors:
        if "ControlRate" in proc.type:
            rate = proc.properties.get("Rate", proc.properties.get("rate", ""))
            unit = proc.properties.get("Time Duration", proc.properties.get("time-duration", ""))
            cron = _rate_to_cron(rate, unit)
            if cron:
                return {
                    "quartz_cron_expression": cron,
                    "timezone_id": "UTC",
                    "pause_status": "UNPAUSED",
                }

        # Check scheduling strategy on any processor
        if proc.scheduling:
            # Support both key conventions: JSON parser uses "strategy"/"period",
            # XML parser may use "schedulingStrategy"/"schedulingPeriod"
            period = proc.scheduling.get("period", "") or proc.scheduling.get("schedulingPeriod", "")
            if period and period != "0 sec":
                cron = _period_to_cron(period)
                if cron:
                    return {
                        "quartz_cron_expression": cron,
                        "timezone_id": "UTC",
                        "pause_status": "UNPAUSED",
                    }

    return None


def _build_email_notifications(
    parse_result: ParseResult, assessment: AssessmentResult
) -> dict:
    """Build email notification config; detect PutEmail processors."""
    emails: list[str] = []

    for proc in parse_result.processors:
        if "PutEmail" in proc.type or "SendEmail" in proc.type:
            to = proc.properties.get("To", proc.properties.get("to", ""))
            if to:
                emails.extend(e.strip() for e in to.split(",") if "@" in e)

    return {
        "on_failure": emails if emails else [],
        "on_success": [],
        "no_alert_for_skipped_runs": True,
    }


def _build_tags(parse_result: ParseResult) -> dict:
    """Build job tags."""
    return {
        "platform": parse_result.platform,
        "migration_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_file": parse_result.metadata.get("source_file", "unknown"),
        "processor_count": str(len(parse_result.processors)),
    }


def _build_dependency_graph(parse_result: ParseResult) -> dict[str, list[str]]:
    """Build a dependency graph between process groups based on connections."""
    # Map processor name -> group name
    proc_to_group: dict[str, str] = {}
    for pg in parse_result.process_groups:
        for pname in pg.processors:
            proc_to_group[pname] = pg.name

    # Build cross-group dependencies
    deps: dict[str, list[str]] = {}
    for conn in parse_result.connections:
        src_group = proc_to_group.get(conn.source_name, "")
        dst_group = proc_to_group.get(conn.destination_name, "")
        if src_group and dst_group and src_group != dst_group:
            deps.setdefault(dst_group, [])
            if src_group not in deps[dst_group]:
                deps[dst_group].append(src_group)

    return deps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:50].strip("_").lower()


def _rate_to_cron(rate: str, unit: str) -> str | None:
    """Convert a rate + unit to a quartz cron expression."""
    try:
        r = int(rate)
    except (ValueError, TypeError):
        return None

    unit_lower = unit.lower() if unit else ""
    if "min" in unit_lower:
        if r <= 60:
            return f"0 0/{r} * * * ?"
    elif "hour" in unit_lower:
        if r <= 24:
            return f"0 0 0/{r} * * ?"
    elif "sec" in unit_lower:
        if r >= 60:
            mins = r // 60
            return f"0 0/{mins} * * * ?"
    elif "day" in unit_lower:
        return f"0 0 0 1/{r} * ?"

    return None


def _period_to_cron(period: str) -> str | None:
    """Convert a NiFi scheduling period string to cron."""
    m = re.match(r"(\d+)\s*(sec|min|hour|day)", period, re.IGNORECASE)
    if not m:
        return None
    val, unit = int(m.group(1)), m.group(2).lower()
    if unit.startswith("min"):
        return f"0 0/{val} * * * ?"
    if unit.startswith("hour"):
        return f"0 0 0/{val} * * ?"
    if unit.startswith("day"):
        return f"0 0 0 1/{val} * ?"
    return None

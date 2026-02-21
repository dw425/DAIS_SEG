"""Workflow generator — produces Databricks Jobs workflow definitions."""

import re

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, ParseResult


def generate_workflow(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> dict:
    """Generate a Databricks Workflows job definition."""
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", parse_result.metadata.get("source_file", "migration"))

    tasks = []
    prev_task = ""

    for i, mapping in enumerate(assessment.mappings):
        if not mapping.code:
            continue

        task_key = f"step_{i + 1}_{_safe_key(mapping.name)}"
        task = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"/Workspace/migrations/{safe_name}",
            },
        }

        if prev_task:
            task["depends_on"] = [{"task_key": prev_task}]

        tasks.append(task)
        prev_task = task_key

    return {
        "name": f"migration_{safe_name}",
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "migration_cluster",
                "new_cluster": {
                    "spark_version": f"{config.runtime_version}.x-scala2.12",
                    "num_workers": 2,
                    "node_type_id": "i3.xlarge" if config.cloud_provider == "aws" else "Standard_DS3_v2",
                },
            }
        ],
        "schedule": None,
        "email_notifications": {"on_failure": []},
    }


def _safe_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:40].strip("_").lower()

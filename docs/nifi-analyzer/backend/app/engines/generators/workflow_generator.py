"""Workflow generator — produces Databricks Jobs workflow definitions.

Uses actual NiFi connection topology to derive task dependencies instead of
building a flat linear chain. Tasks with no upstream connections run in parallel.
"""

import logging
import re

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)


def generate_workflow(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> dict:
    """Generate a Databricks Workflows job definition.

    Uses NiFi connection topology to build a proper DAG of task dependencies
    rather than a simple linear chain.
    """
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", parse_result.metadata.get("source_file", "migration"))

    # Build mapping name -> task_key lookup
    task_keys: dict[str, str] = {}
    for i, mapping in enumerate(assessment.mappings):
        if not mapping.code:
            continue
        task_keys[mapping.name] = f"step_{i + 1}_{_safe_key(mapping.name)}"

    # Build DAG dependencies from NiFi connections
    upstream_map: dict[str, set[str]] = {name: set() for name in task_keys}
    for conn in parse_result.connections:
        src = conn.source_name
        dst = conn.destination_name
        if src in task_keys and dst in task_keys:
            upstream_map[dst].add(src)

    tasks = []
    for i, mapping in enumerate(assessment.mappings):
        if not mapping.code:
            continue

        task_key = task_keys[mapping.name]
        task: dict = {
            "task_key": task_key,
            "job_cluster_key": "migration_cluster",
            "notebook_task": {
                "notebook_path": f"/Workspace/migrations/{safe_name}",
            },
        }

        # Use actual upstream dependencies from NiFi topology
        deps = upstream_map.get(mapping.name, set())
        if deps:
            task["depends_on"] = [
                {"task_key": task_keys[dep]} for dep in sorted(deps) if dep in task_keys
            ]

        tasks.append(task)

    node_type = {
        "aws": "i3.xlarge",
        "azure": "Standard_DS3_v2",
        "gcp": "n1-standard-4",
    }.get(config.cloud_provider, "i3.xlarge")

    logger.info("Workflow generated: %d tasks, DAG-based dependencies from %d connections", len(tasks), len(parse_result.connections))
    return {
        "name": f"migration_{safe_name}",
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "migration_cluster",
                "new_cluster": {
                    "spark_version": f"{config.runtime_version}.x-scala2.12",
                    "num_workers": 2,
                    "node_type_id": node_type,
                },
            }
        ],
        "schedule": None,
        "email_notifications": {"on_failure": []},
    }


def _safe_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:40].strip("_").lower()

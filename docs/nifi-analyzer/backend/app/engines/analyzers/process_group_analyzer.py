"""Process Group to modular DAB mapping analyzer.

Maps NiFi Process Groups to modular Databricks Asset Bundle definitions.
Each Process Group becomes a separate job, nested Process Groups become
nested workflow dependencies.
"""

import logging
import re

from app.models.pipeline import ParseResult
from app.models.processor import Connection, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def analyze_process_groups(parse_result: ParseResult) -> dict:
    """Map Process Groups to Databricks Asset Bundle job definitions.

    Returns:
        {
            "group_jobs": [...],
            "group_dependencies": [...],
            "dab_resources": {...},
            "summary": {...},
        }
    """
    processors = parse_result.processors
    connections = parse_result.connections
    groups = parse_result.process_groups

    logger.info("Analyzing process groups: %d group(s), %d processors", len(groups), len(processors))
    if not groups:
        # If no explicit groups, create a single job from root
        return _single_job_result(processors, connections, parse_result)

    # Build processor-to-group map
    proc_group_map = _build_proc_group_map(processors, groups)

    # Detect inter-group dependencies from connections
    group_deps = _detect_group_dependencies(connections, proc_group_map)

    # Generate job definitions for each group
    group_jobs = []
    for grp in groups:
        job = _build_group_job(grp, processors, connections, parse_result)
        group_jobs.append(job)

    # Generate DAB resources YAML structure
    dab_resources = _generate_dab_resources(group_jobs, group_deps, parse_result)

    return {
        "group_jobs": group_jobs,
        "group_dependencies": group_deps,
        "dab_resources": dab_resources,
        "summary": {
            "total_groups": len(groups),
            "total_jobs": len(group_jobs),
            "inter_group_connections": len(group_deps),
        },
    }


def _build_proc_group_map(
    processors: list[Processor],
    groups: list[ProcessGroup],
) -> dict[str, str]:
    """Map processor names to their group names."""
    mapping: dict[str, str] = {}
    group_proc_sets = {g.name: set(g.processors) for g in groups}

    for p in processors:
        if p.group and p.group != "(root)":
            mapping[p.name] = p.group
        else:
            # Check if processor is listed in a group's processors list
            for gname, procs in group_proc_sets.items():
                if p.name in procs:
                    mapping[p.name] = gname
                    break
            else:
                mapping[p.name] = "(root)"

    return mapping


def _detect_group_dependencies(
    connections: list[Connection],
    proc_group_map: dict[str, str],
) -> list[dict]:
    """Detect dependencies between Process Groups based on connections."""
    deps: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for c in connections:
        src_group = proc_group_map.get(c.source_name, "(root)")
        dst_group = proc_group_map.get(c.destination_name, "(root)")

        if src_group != dst_group:
            pair = (src_group, dst_group)
            if pair not in seen:
                seen.add(pair)
                deps.append({
                    "source_group": src_group,
                    "destination_group": dst_group,
                    "connection": f"{c.source_name} -> {c.destination_name}",
                    "relationship": c.relationship,
                })

    return deps


def _build_group_job(
    grp: ProcessGroup,
    all_processors: list[Processor],
    all_connections: list[Connection],
    parse_result: ParseResult,
) -> dict:
    """Build a DAB job definition for a single Process Group."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", grp.name).strip("_").lower()

    # Get processors in this group
    group_procs = [
        p for p in all_processors
        if p.group == grp.name or p.name in grp.processors
    ]

    # Get internal connections
    group_proc_names = {p.name for p in group_procs}
    internal_conns = [
        c for c in all_connections
        if c.source_name in group_proc_names and c.destination_name in group_proc_names
    ]

    # Build task list from processors
    tasks = []
    prev_task = ""
    for i, p in enumerate(group_procs):
        task_key = f"{safe_name}_step_{i + 1}_{_safe_key(p.name)}"
        task = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"/Workspace/migrations/{safe_name}/{_safe_key(p.name)}",
            },
        }
        if prev_task:
            task["depends_on"] = [{"task_key": prev_task}]
        tasks.append(task)
        prev_task = task_key

    return {
        "group_name": grp.name,
        "job_name": f"migration_{safe_name}",
        "processor_count": len(group_procs),
        "processors": [p.name for p in group_procs],
        "tasks": tasks,
        "internal_connections": len(internal_conns),
    }


def _generate_dab_resources(
    group_jobs: list[dict],
    group_deps: list[dict],
    parse_result: ParseResult,
) -> dict:
    """Generate Databricks Asset Bundle resources structure."""
    safe_flow = re.sub(
        r"[^a-zA-Z0-9_]", "_",
        parse_result.metadata.get("source_file", "migration"),
    ).lower()

    # Build dependency map
    dep_map: dict[str, list[str]] = {}
    for dep in group_deps:
        dst = dep["destination_group"]
        src = dep["source_group"]
        dep_map.setdefault(dst, []).append(src)

    jobs: dict[str, dict] = {}
    for gj in group_jobs:
        job_key = gj["job_name"]
        job_def = {
            "name": gj["job_name"],
            "tasks": gj["tasks"],
            "job_clusters": [
                {
                    "job_cluster_key": f"{gj['job_name']}_cluster",
                    "new_cluster": {
                        "spark_version": "15.4.x-scala2.12",
                        "num_workers": 2,
                    },
                }
            ],
        }

        # Add workflow-level dependencies
        deps_for_group = dep_map.get(gj["group_name"], [])
        if deps_for_group:
            safe_deps = [
                re.sub(r"[^a-zA-Z0-9_]", "_", d).strip("_").lower()
                for d in deps_for_group
            ]
            job_def["_depends_on_jobs"] = [f"migration_{d}" for d in safe_deps]

        jobs[job_key] = job_def

    return {
        "bundle": {
            "name": f"nifi_migration_{safe_flow}",
        },
        "resources": {
            "jobs": jobs,
        },
        "targets": {
            "dev": {"mode": "development", "default": True},
            "staging": {"mode": "development"},
            "prod": {"mode": "production"},
        },
    }


def _single_job_result(
    processors: list[Processor],
    connections: list[Connection],
    parse_result: ParseResult,
) -> dict:
    """Generate a single-job result when no Process Groups exist."""
    safe_flow = re.sub(
        r"[^a-zA-Z0-9_]", "_",
        parse_result.metadata.get("source_file", "migration"),
    ).lower()

    tasks = []
    prev_task = ""
    for i, p in enumerate(processors):
        task_key = f"step_{i + 1}_{_safe_key(p.name)}"
        task = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"/Workspace/migrations/{safe_flow}",
            },
        }
        if prev_task:
            task["depends_on"] = [{"task_key": prev_task}]
        tasks.append(task)
        prev_task = task_key

    return {
        "group_jobs": [{
            "group_name": "(root)",
            "job_name": f"migration_{safe_flow}",
            "processor_count": len(processors),
            "processors": [p.name for p in processors],
            "tasks": tasks,
            "internal_connections": len(connections),
        }],
        "group_dependencies": [],
        "dab_resources": {
            "bundle": {"name": f"nifi_migration_{safe_flow}"},
            "resources": {
                "jobs": {
                    f"migration_{safe_flow}": {
                        "name": f"migration_{safe_flow}",
                        "tasks": tasks,
                    }
                }
            },
            "targets": {
                "dev": {"mode": "development", "default": True},
                "staging": {"mode": "development"},
                "prod": {"mode": "production"},
            },
        },
        "summary": {
            "total_groups": 0,
            "total_jobs": 1,
            "inter_group_connections": 0,
        },
    }


def _safe_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)[:40].strip("_").lower()

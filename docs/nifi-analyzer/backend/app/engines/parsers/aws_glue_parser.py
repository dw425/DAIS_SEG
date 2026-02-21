"""AWS Glue JSON job definition parser.

Extracts ETL scripts, crawlers, triggers, and job parameters.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, Processor

logger = logging.getLogger(__name__)


def parse_glue(content: bytes, filename: str) -> ParseResult:
    """Parse an AWS Glue JSON job definition."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {filename}: {exc}") from exc
    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    warnings: list[Warning] = []

    # Handle single job or list
    jobs = []
    if isinstance(data, list):
        jobs = data
    elif isinstance(data, dict):
        if "Jobs" in data:
            jobs = data["Jobs"]
        elif "Job" in data:
            jobs = [data["Job"]]
        elif "JobName" in data or "Name" in data:
            jobs = [data]
        elif "Crawlers" in data:
            # Crawler definitions
            for crawler in data["Crawlers"]:
                name = crawler.get("Name", "")
                db = crawler.get("DatabaseName", "")
                targets = crawler.get("Targets", {})
                s3_targets = targets.get("S3Targets", [])
                jdbc_targets = targets.get("JdbcTargets", [])
                props = {"DatabaseName": db}
                if s3_targets:
                    props["S3Paths"] = ",".join(t.get("Path", "") for t in s3_targets)
                if jdbc_targets:
                    props["JdbcPaths"] = ",".join(t.get("Path", "") for t in jdbc_targets)
                processors.append(
                    Processor(
                        name=name,
                        type="GlueCrawler",
                        platform="aws_glue",
                        properties=props,
                    )
                )
        elif "Triggers" in data:
            for trigger in data["Triggers"]:
                name = trigger.get("Name", "")
                ttype = trigger.get("Type", "")
                actions = trigger.get("Actions", [])
                for action in actions:
                    job_name = action.get("JobName", "")
                    if name and job_name:
                        connections.append(Connection(source_name=name, destination_name=job_name))
                processors.append(
                    Processor(
                        name=name,
                        type=f"GlueTrigger_{ttype}",
                        platform="aws_glue",
                    )
                )

    for job in jobs:
        name = job.get("Name") or job.get("JobName", "")
        command = job.get("Command", {})
        script_loc = command.get("ScriptLocation", "")
        py_version = command.get("PythonVersion", "")
        job_type = command.get("Name", "glueetl")  # glueetl, gluestreaming, pythonshell

        default_args = job.get("DefaultArguments", {})
        props: dict[str, str] = {
            "ScriptLocation": script_loc,
            "PythonVersion": py_version,
            "JobType": job_type,
            "MaxRetries": str(job.get("MaxRetries", 0)),
            "Timeout": str(job.get("Timeout", 2880)),
            "WorkerType": job.get("WorkerType", "G.1X"),
            "NumberOfWorkers": str(job.get("NumberOfWorkers", 2)),
        }
        props.update({k: str(v) for k, v in default_args.items() if isinstance(v, str)})

        processors.append(
            Processor(
                name=name,
                type=f"Glue_{job_type}",
                platform="aws_glue",
                properties=props,
            )
        )

        # Connections from job
        conn_list = job.get("Connections", {}).get("Connections", [])
        for conn_name in conn_list:
            controller_services.append(
                ControllerService(
                    name=conn_name,
                    type="GlueConnection",
                    properties={},
                )
            )

    if not processors:
        warnings.append(Warning(severity="warning", message="No Glue jobs found", source=filename))

    return ParseResult(
        platform="aws_glue",
        processors=processors,
        connections=connections,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

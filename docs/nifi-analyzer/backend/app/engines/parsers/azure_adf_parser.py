"""Azure Data Factory ARM JSON template parser.

Extracts pipelines, activities (Copy, DataFlow, Notebook, etc.), datasets, linked services.
"""

import json
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, ControllerService, ProcessGroup, Processor

logger = logging.getLogger(__name__)


def parse_adf(content: bytes, filename: str) -> ParseResult:
    """Parse an Azure Data Factory ARM JSON template."""
    data = json.loads(content)

    processors: list[Processor] = []
    connections: list[Connection] = []
    controller_services: list[ControllerService] = []
    process_groups: list[ProcessGroup] = []
    warnings: list[Warning] = []

    resources = data.get("resources", [])
    if not resources and "properties" in data:
        # Single resource export
        resources = [data]

    for resource in resources:
        if not isinstance(resource, dict):
            continue

        rtype = resource.get("type", "")
        name = resource.get("name", "")
        props = resource.get("properties", {})

        if "pipelines" in rtype.lower() or rtype.endswith("/pipelines"):
            # Pipeline
            pipeline_name = name.split("/")[-1] if "/" in name else name
            process_groups.append(ProcessGroup(name=pipeline_name))

            activities = props.get("activities", [])
            for activity in activities:
                act_name = activity.get("name", "")
                act_type = activity.get("type", "")
                act_props = {}

                # Extract type-specific properties
                type_props = activity.get("typeProperties", {})
                if isinstance(type_props, dict):
                    for k, v in type_props.items():
                        if isinstance(v, str):
                            act_props[k] = v
                        elif isinstance(v, dict):
                            act_props[k] = json.dumps(v)[:200]

                processors.append(
                    Processor(
                        name=act_name,
                        type=act_type,
                        platform="azure_adf",
                        group=pipeline_name,
                        properties=act_props,
                    )
                )

                # Dependencies
                depends_on = activity.get("dependsOn", [])
                for dep in depends_on:
                    dep_name = dep.get("activity", "")
                    conditions = dep.get("dependencyConditions", ["Succeeded"])
                    if dep_name:
                        connections.append(
                            Connection(
                                source_name=dep_name,
                                destination_name=act_name,
                                relationship=",".join(conditions),
                            )
                        )

                _ = act_name  # track last activity for potential future use

        elif "linkedServices" in rtype.lower() or rtype.endswith("/linkedServices"):
            ls_name = name.split("/")[-1] if "/" in name else name
            ls_type = props.get("type", "")
            ls_props = {}
            tp = props.get("typeProperties", {})
            if isinstance(tp, dict):
                for k, v in tp.items():
                    if isinstance(v, str):
                        ls_props[k] = v
            controller_services.append(
                ControllerService(
                    name=ls_name,
                    type=ls_type,
                    properties=ls_props,
                )
            )

        elif "datasets" in rtype.lower() or rtype.endswith("/datasets"):
            ds_name = name.split("/")[-1] if "/" in name else name
            ds_type = props.get("type", "")
            processors.append(
                Processor(
                    name=ds_name,
                    type=f"Dataset_{ds_type}",
                    platform="azure_adf",
                    properties={"linkedServiceName": str(props.get("linkedServiceName", ""))},
                )
            )

    if not processors:
        warnings.append(Warning(severity="warning", message="No ADF activities found", source=filename))

    return ParseResult(
        platform="azure_adf",
        processors=processors,
        connections=connections,
        process_groups=process_groups,
        controller_services=controller_services,
        metadata={"source_file": filename},
        warnings=warnings,
    )

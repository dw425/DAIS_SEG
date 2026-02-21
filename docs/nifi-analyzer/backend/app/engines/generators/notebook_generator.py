"""Notebook generator — produces Databricks notebook cells from assessment results.

Ported from generators/notebook-generator.js.
"""

import logging
from datetime import datetime, timezone

from app.engines.generators.autoloader_generator import (
    generate_autoloader_code,
    generate_jdbc_source_code,
    generate_kafka_source_code,
    generate_streaming_trigger,
    is_autoloader_candidate,
    is_jdbc_source,
    is_kafka_source,
)
from app.engines.generators.cell_builders import (
    build_config_cell,
    build_imports_cell,
    build_setup_cell,
    build_teardown_cell,
)
from app.engines.generators.code_scrubber import scrub_code
from app.engines.generators.processor_translators import (
    translate_execute_script,
    translate_jolt_transform,
    translate_route_on_attribute,
)
from app.engines.generators.workflow_generator import generate_workflow
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, NotebookCell, NotebookResult, ParseResult

logger = logging.getLogger(__name__)


def generate_notebook(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig,
) -> NotebookResult:
    """Generate a Databricks notebook from the parsed flow and assessment."""
    cells: list[NotebookCell] = []

    # Title cell
    cells.append(
        NotebookCell(
            type="markdown",
            source=f"# Migration: {parse_result.platform.upper()} to Databricks\n\n"
            f"**Source**: {parse_result.metadata.get('source_file', 'unknown')}\n"
            f"**Platform**: {parse_result.platform}\n"
            f"**Processors**: {len(parse_result.processors)}\n"
            f"**Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"**Confidence**: {_avg_confidence(assessment):.0%}",
            label="title",
        )
    )

    # Imports cell
    imports_code = build_imports_cell(assessment)
    if imports_code:
        cells.append(NotebookCell(type="code", source=imports_code, label="imports"))

    # Config cell
    config_code = build_config_cell(config)
    cells.append(NotebookCell(type="code", source=config_code, label="config"))

    # Setup cell
    setup_code = build_setup_cell(parse_result, config)
    if setup_code:
        cells.append(NotebookCell(type="code", source=setup_code, label="setup"))

    # Pipeline cells from mappings
    cells.append(
        NotebookCell(
            type="markdown",
            source="## Pipeline Steps",
            label="pipeline_header",
        )
    )

    for i, mapping in enumerate(assessment.mappings):
        if not mapping.code:
            continue

        # Section header
        cells.append(
            NotebookCell(
                type="markdown",
                source=f"### Step {i + 1}: {mapping.name}\n"
                f"**Type**: `{mapping.type}` | **Role**: {mapping.role} | "
                f"**Confidence**: {mapping.confidence:.0%}",
                label=f"step_{i + 1}_header",
            )
        )

        # Use specialized translators for known processor types
        code = _generate_specialized_code(mapping, parse_result, config)
        if code is None:
            code = scrub_code(mapping.code)

        cells.append(
            NotebookCell(
                type="code",
                source=code,
                label=f"step_{i + 1}_{mapping.role}",
            )
        )

        # Add streaming trigger cell for Auto Loader / Kafka sources
        if is_autoloader_candidate(mapping.type) or is_kafka_source(mapping.type):
            trigger_code = generate_streaming_trigger(mapping, parse_result, config)
            cells.append(
                NotebookCell(
                    type="code",
                    source=trigger_code,
                    label=f"step_{i + 1}_trigger",
                )
            )

    # Teardown cell
    teardown_code = build_teardown_cell()
    cells.append(NotebookCell(type="code", source=teardown_code, label="teardown"))

    # Generate workflow definition
    workflow = generate_workflow(parse_result, assessment, config)

    return NotebookResult(cells=cells, workflow=workflow)


def _generate_specialized_code(
    mapping,
    parse_result: ParseResult,
    config,
) -> str | None:
    """Generate specialized code for known NiFi processor types.

    Returns None if no specialized translator applies (fall back to generic code).
    """
    proc_type = mapping.type

    # Auto Loader for file-based ingestion
    if is_autoloader_candidate(proc_type):
        return generate_autoloader_code(mapping, parse_result, config)

    # Kafka consumer
    if is_kafka_source(proc_type):
        return generate_kafka_source_code(mapping, parse_result, config)

    # JDBC source
    if is_jdbc_source(proc_type):
        return generate_jdbc_source_code(mapping, parse_result, config)

    # RouteOnAttribute
    if "RouteOnAttribute" in proc_type or "RouteOnContent" in proc_type:
        result = translate_route_on_attribute(mapping, parse_result)
        return result["code"]

    # JoltTransformJSON
    if "Jolt" in proc_type or "JoltTransform" in proc_type:
        return translate_jolt_transform(mapping, parse_result)

    # ExecuteScript / ExecuteGroovyScript
    if any(kw in proc_type for kw in ("ExecuteScript", "ExecuteGroovyScript", "ExecuteStreamCommand")):
        return translate_execute_script(mapping, parse_result)

    return None


def _avg_confidence(assessment: AssessmentResult) -> float:
    if not assessment.mappings:
        return 0.0
    return sum(m.confidence for m in assessment.mappings) / len(assessment.mappings)

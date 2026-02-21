"""Notebook generator — produces Databricks notebook cells from assessment results.

Ported from generators/notebook-generator.js.
"""

import logging
from datetime import datetime, timezone

from app.engines.generators.cell_builders import (
    build_config_cell,
    build_imports_cell,
    build_setup_cell,
    build_teardown_cell,
)
from app.engines.generators.code_scrubber import scrub_code
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

        code = scrub_code(mapping.code)

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

        cells.append(
            NotebookCell(
                type="code",
                source=code,
                label=f"step_{i + 1}_{mapping.role}",
            )
        )

    # Teardown cell
    teardown_code = build_teardown_cell()
    cells.append(NotebookCell(type="code", source=teardown_code, label="teardown"))

    # Generate workflow definition
    workflow = generate_workflow(parse_result, assessment, config)

    return NotebookResult(cells=cells, workflow=workflow)


def _avg_confidence(assessment: AssessmentResult) -> float:
    if not assessment.mappings:
        return 0.0
    return sum(m.confidence for m in assessment.mappings) / len(assessment.mappings)

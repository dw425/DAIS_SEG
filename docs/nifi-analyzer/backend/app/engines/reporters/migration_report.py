"""Migration report generator — comprehensive migration summary.

Ported from reporters/migration-report.js.
"""

import logging
from datetime import datetime, timezone

from app.engines.reporters.final_report import build_final_report

logger = logging.getLogger(__name__)
from app.engines.reporters.value_analysis import compute_value_analysis
from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    NotebookResult,
    ParseResult,
    ValidationResult,
)


def generate_report(
    parse_result: ParseResult,
    analysis: AnalysisResult,
    assessment: AssessmentResult,
    notebook: NotebookResult,
    validation: ValidationResult,
) -> dict:
    """Generate a comprehensive migration report."""
    logger.info("Generating migration report for %s", parse_result.platform if parse_result else "unknown")
    if not parse_result or not analysis or not assessment or not notebook or not validation:
        return {"error": "Incomplete pipeline data — one or more required inputs is None"}

    final = build_final_report(parse_result, analysis, assessment, validation)
    value = compute_value_analysis(parse_result, assessment)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "platform": parse_result.platform,
        "source_file": parse_result.metadata.get("source_file", ""),
        "summary": {
            "total_processors": len(parse_result.processors),
            "total_connections": len(parse_result.connections),
            "total_process_groups": len(parse_result.process_groups),
            "total_controller_services": len(parse_result.controller_services),
            "mapped_count": sum(1 for m in assessment.mappings if m.mapped),
            "unmapped_count": assessment.unmapped_count,
            "avg_confidence": sum(m.confidence for m in assessment.mappings) / max(len(assessment.mappings), 1),
            "validation_score": validation.overall_score,
            "notebook_cells": len(notebook.cells),
        },
        "final_report": final,
        "value_analysis": value,
        "security_findings": analysis.security_findings,
        "external_systems": analysis.external_systems,
        "cycles": analysis.cycles,
        "packages_required": assessment.packages,
        "warnings": [w.model_dump() for w in parse_result.warnings],
        "validation_gaps": validation.gaps,
        "validation_errors": validation.errors,
    }

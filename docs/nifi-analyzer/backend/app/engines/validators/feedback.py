"""Feedback analyzer — compute completeness and migration readiness.

Ported from validators/accelerator-feedback.js.
"""

import logging

from app.models.pipeline import NotebookResult, ParseResult

logger = logging.getLogger(__name__)


def compute_feedback(parse_result: ParseResult, notebook: NotebookResult) -> tuple[float, str]:
    """Compute completeness score and feedback details.

    Returns:
        (score, details_string)
    """
    if not parse_result or not parse_result.processors:
        return 0.0, "No parse result or processors to evaluate"
    if not notebook or not notebook.cells:
        return 0.0, "No notebook or cells to evaluate"

    total_procs = len(parse_result.processors)
    code_cells = [c for c in notebook.cells if c.type == "code" and c.label.startswith("step_")]
    generated_steps = len(code_cells)

    # Check for key aspects
    has_imports = any(c.label == "imports" for c in notebook.cells)
    has_config = any(c.label == "config" for c in notebook.cells)
    has_setup = any(c.label == "setup" for c in notebook.cells)
    has_teardown = any(c.label == "teardown" for c in notebook.cells)

    structure_score = sum([has_imports, has_config, has_setup, has_teardown]) / 4
    coverage_score = generated_steps / max(total_procs, 1)

    # Check for controller service references
    cs_count = len(parse_result.controller_services)
    code_text = "\n".join(c.source for c in code_cells)
    cs_referenced = sum(1 for cs in parse_result.controller_services if cs.name in code_text)
    cs_score = cs_referenced / max(cs_count, 1) if cs_count > 0 else 1.0

    overall = structure_score * 0.2 + coverage_score * 0.6 + cs_score * 0.2

    details = (
        f"Structure: {structure_score:.0%} | "
        f"Coverage: {generated_steps}/{total_procs} steps | "
        f"Controller Services: {cs_referenced}/{cs_count} referenced"
    )

    logger.info("Feedback score: %.2f (%s)", overall, details)
    return min(overall, 1.0), details

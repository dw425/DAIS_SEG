"""Validate router — runs validation engine on generated notebook.

Combines the existing multi-dimensional validator (intent coverage, code
quality, completeness, Delta format, checkpoints, credentials, error
handling, schema evolution) with the V6 12-point runnable checker.
"""

import dataclasses
import logging

from fastapi import APIRouter, HTTPException

from app.engines.validators import validate_notebook
from app.engines.validators.runnable_checker import check_runnable
from app.models.pipeline import NotebookResult, ParseResult
from app.models.processor import CamelModel
from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)


class ValidateRequest(CamelModel):
    parsed: ParseResult
    notebook: NotebookResult


@router.post("/validate")
def validate(req: ValidateRequest) -> dict:
    """Validate generated notebook against source flow intent.

    Returns both the multi-dimensional validation scores AND the V6
    12-point runnable quality gate.
    """
    processing_status.start("validate", len(req.parsed.processors))
    try:
        # Run existing multi-dimensional validation
        result = validate_notebook(req.parsed, req.notebook)

        # Run V6 12-point runnable checker
        runnable = check_runnable(
            [{"type": c.type, "source": c.source, "label": c.label} for c in req.notebook.cells],
            req.parsed,
        )

        processing_status.finish()

        # Build combined response
        response = result.model_dump(by_alias=True)
        response["runnableReport"] = dataclasses.asdict(runnable)

        logger.info(
            "Validation complete: overall=%.2f, runnable=%s (%d/%d checks pass)",
            result.overall_score,
            runnable.is_runnable,
            runnable.passed_count,
            runnable.passed_count + runnable.failed_count,
        )

        return response
    except Exception as exc:
        processing_status.finish()
        logger.exception("Validation error")
        raise HTTPException(status_code=500, detail=f"Validation failed: {exc}") from exc

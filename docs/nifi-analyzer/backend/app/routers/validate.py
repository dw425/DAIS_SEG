"""Validate router — runs validation engine on generated notebook."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.validators import validate_notebook
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
    """Validate generated notebook against source flow intent."""
    processing_status.start("validate", len(req.parsed.processors))
    try:
        result = validate_notebook(req.parsed, req.notebook)
        processing_status.finish()
        return result.model_dump(by_alias=True)
    except Exception as exc:
        processing_status.finish()
        logger.exception("Validation error")
        raise HTTPException(status_code=500, detail=f"Validation failed: {exc}") from exc

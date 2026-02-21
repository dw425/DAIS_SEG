"""Validate router — runs validation engine on generated notebook."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.engines.validators import validate_notebook
from app.models.pipeline import NotebookResult, ParseResult, ValidationResult

router = APIRouter()
logger = logging.getLogger(__name__)


class ValidateRequest(BaseModel):
    parse_result: ParseResult
    notebook: NotebookResult


@router.post("/validate", response_model=ValidationResult)
async def validate(req: ValidateRequest) -> ValidationResult:
    """Validate generated notebook against source flow intent."""
    try:
        return validate_notebook(req.parse_result, req.notebook)
    except Exception as exc:
        logger.exception("Validation error")
        raise HTTPException(status_code=500, detail=f"Validation failed: {exc}") from exc

"""Report router — generates migration reports."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.engines.reporters import generate_report
from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    NotebookResult,
    ParseResult,
    ValidationResult,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class ReportRequest(BaseModel):
    parse_result: ParseResult
    analysis: AnalysisResult
    assessment: AssessmentResult
    notebook: NotebookResult
    validation: ValidationResult


@router.post("/report")
async def report(req: ReportRequest) -> dict:
    """Generate a comprehensive migration report."""
    try:
        return generate_report(
            req.parse_result,
            req.analysis,
            req.assessment,
            req.notebook,
            req.validation,
        )
    except Exception as exc:
        logger.exception("Report generation error")
        raise HTTPException(status_code=500, detail=f"Report failed: {exc}") from exc

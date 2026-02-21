"""Assess router — maps source processors to Databricks equivalents."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.engines.mappers import map_to_databricks
from app.models.pipeline import AnalysisResult, AssessmentResult, ParseResult

router = APIRouter()
logger = logging.getLogger(__name__)


class AssessRequest(BaseModel):
    parse_result: ParseResult
    analysis_result: AnalysisResult


@router.post("/assess", response_model=AssessmentResult)
async def assess(req: AssessRequest) -> AssessmentResult:
    """Map processors to Databricks equivalents and return assessment."""
    try:
        return map_to_databricks(req.parse_result, req.analysis_result)
    except Exception as exc:
        logger.exception("Assessment error")
        raise HTTPException(status_code=500, detail=f"Assessment failed: {exc}") from exc

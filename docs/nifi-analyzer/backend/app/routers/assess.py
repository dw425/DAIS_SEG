"""Assess router — maps source processors to Databricks equivalents."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.mappers import map_to_databricks
from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import CamelModel
from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)


class AssessRequest(CamelModel):
    parsed: ParseResult
    analysis: AnalysisResult


@router.post("/assess")
def assess(req: AssessRequest) -> dict:
    """Map processors to Databricks equivalents and return assessment."""
    processing_status.start("assess", len(req.parsed.processors))
    try:
        result = map_to_databricks(req.parsed, req.analysis)
        processing_status.finish()
        return result.model_dump(by_alias=True)
    except Exception as exc:
        processing_status.finish()
        logger.exception("Assessment error")
        raise HTTPException(status_code=500, detail=f"Assessment failed: {exc}") from exc

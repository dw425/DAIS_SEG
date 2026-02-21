"""Analyze router — accepts ParseResult, runs analyzers, returns AnalysisResult."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.analyzers import run_analysis
from app.models.pipeline import AnalysisResult, ParseResult

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/analyze", response_model=AnalysisResult)
async def analyze(parse_result: ParseResult) -> AnalysisResult:
    """Run dependency graph, cycle detection, external systems, security scan, and stage classification."""
    try:
        return run_analysis(parse_result)
    except Exception as exc:
        logger.exception("Analysis error")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc

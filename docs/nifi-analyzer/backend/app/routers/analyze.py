"""Analyze router — accepts ParseResult, runs analyzers, returns AnalysisResult."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.analyzers import run_analysis
from app.models.pipeline import ParseResult
from app.models.processor import CamelModel
from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)


class AnalyzeRequest(CamelModel):
    parsed: ParseResult


@router.post("/analyze")
def analyze(req: AnalyzeRequest) -> dict:
    """Run dependency graph, cycle detection, external systems, security scan, and stage classification."""
    proc_count = len(req.parsed.processors)
    processing_status.start("analyze", proc_count)
    try:
        logger.info("Starting analysis: %d processors, %d connections", proc_count, len(req.parsed.connections))
        result = run_analysis(req.parsed)
        processing_status.finish()
        logger.info("Analysis complete: %d processors", proc_count)
        return result.model_dump(by_alias=True)
    except Exception as exc:
        processing_status.finish()
        logger.exception("Analysis error")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc

"""Lineage router — endpoints for data lineage and impact analysis."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.analyzers.impact_analyzer import analyze_impact
from app.engines.analyzers.lineage_tracker import track_lineage
from app.models.pipeline import ParseResult
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class LineageRequest(CamelModel):
    parsed: ParseResult


class ImpactRequest(CamelModel):
    parsed: ParseResult
    target: str


@router.post("/lineage")
async def get_lineage(req: LineageRequest) -> dict:
    """Build data lineage graph from a parsed flow."""
    try:
        result = track_lineage(req.parsed)
        return result
    except Exception as exc:
        logger.exception("Lineage tracking error")
        raise HTTPException(status_code=500, detail=f"Lineage tracking failed: {exc}") from exc


@router.post("/lineage/impact")
async def get_impact(req: ImpactRequest) -> dict:
    """Analyze downstream impact of modifying a specific processor."""
    try:
        result = analyze_impact(req.parsed, req.target)
        return result
    except Exception as exc:
        logger.exception("Impact analysis error")
        raise HTTPException(status_code=500, detail=f"Impact analysis failed: {exc}") from exc

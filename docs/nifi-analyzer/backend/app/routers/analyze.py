"""Analyze router — accepts ParseResult, runs analyzers, returns AnalysisResult.

Includes both the standard structural analysis (/analyze) and the V6 6-pass
deep analysis engine (/analyze/deep).
"""

import dataclasses
import logging

from fastapi import APIRouter, HTTPException

from app.engines.analyzers import run_analysis
from app.engines.analyzers.deep_analysis_orchestrator import run_deep_analysis
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


@router.post("/analyze/deep")
def deep_analyze(req: AnalyzeRequest) -> dict:
    """Run the V6 6-pass deep analysis engine.

    Executes:
      1. Functional analysis — flow purpose, zones, patterns, complexity
      2. Processor analysis — KB matching, categorization, unknowns
      3. Workflow analysis — topological sort, phases, critical path
      4. Upstream analysis — data sources, schemas, attribute lineage
      5. Downstream analysis — sinks, data flow, error routing, volume
      6. Line-by-line analysis — property classification, confidence scoring

    Returns the full DeepAnalysisResult as a JSON dict.
    """
    proc_count = len(req.parsed.processors)
    processing_status.start("deep_analyze", proc_count)
    try:
        logger.info("Starting 6-pass deep analysis: %d processors", proc_count)

        # Run standard analysis first (some deep passes use it)
        standard_result = run_analysis(req.parsed)

        # Run 6-pass deep analysis
        deep_result = run_deep_analysis(req.parsed, standard_result)

        processing_status.finish()
        logger.info(
            "Deep analysis complete in %.0fms: %d processors",
            deep_result.duration_ms,
            proc_count,
        )

        # Combine: return standard analysis + deep analysis overlay
        response = standard_result.model_dump(by_alias=True)
        deep_dict = dataclasses.asdict(deep_result)
        # Convert lint report to serializable dict
        if deep_result.lint is not None:
            deep_dict["lint"] = deep_result.lint.to_dict()
        response["deepAnalysis"] = deep_dict
        return response
    except Exception as exc:
        processing_status.finish()
        logger.exception("Deep analysis error")
        raise HTTPException(status_code=500, detail=f"Deep analysis failed: {exc}") from exc

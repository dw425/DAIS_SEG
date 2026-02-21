"""Generate router — produces Databricks notebook cells and workflow definitions."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.generators import generate_notebook
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, ParseResult
from app.models.processor import CamelModel
from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)


class GenerateRequest(CamelModel):
    parsed: ParseResult
    assessment: AssessmentResult
    config: DatabricksConfig = DatabricksConfig()


@router.post("/generate")
def generate(req: GenerateRequest) -> dict:
    """Generate a Databricks notebook from the parsed flow and assessment."""
    processing_status.start("generate", len(req.parsed.processors))
    try:
        result = generate_notebook(req.parsed, req.assessment, req.config)
        processing_status.finish()
        return result.model_dump(by_alias=True)
    except Exception as exc:
        processing_status.finish()
        logger.exception("Generation error")
        raise HTTPException(status_code=500, detail=f"Generation failed: {exc}") from exc

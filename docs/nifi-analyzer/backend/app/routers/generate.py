"""Generate router — produces Databricks notebook cells and workflow definitions."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.engines.generators import generate_notebook
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, NotebookResult, ParseResult

router = APIRouter()
logger = logging.getLogger(__name__)


class GenerateRequest(BaseModel):
    parse_result: ParseResult
    assessment: AssessmentResult
    config: DatabricksConfig = DatabricksConfig()


@router.post("/generate", response_model=NotebookResult)
async def generate(req: GenerateRequest) -> NotebookResult:
    """Generate a Databricks notebook from the parsed flow and assessment."""
    try:
        return generate_notebook(req.parse_result, req.assessment, req.config)
    except Exception as exc:
        logger.exception("Generation error")
        raise HTTPException(status_code=500, detail=f"Generation failed: {exc}") from exc

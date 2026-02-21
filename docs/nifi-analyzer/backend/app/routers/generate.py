"""Generate router — produces Databricks notebook cells and workflow definitions.

V6 generation (default) produces run-quality notebooks with:
- DataFrame chains wired end-to-end via topological sort
- Terminal writes for all sinks with checkpoints and triggers
- Secrets-backed configuration (dbutils.secrets.get)
- 6-pass deep analysis as documentation cells
- 12-point quality gate validation summary

Legacy V5 generation is available via use_v6=false.
"""

import dataclasses
import logging

from fastapi import APIRouter, HTTPException

from app.engines.generators import generate_notebook
from app.engines.generators.notebook_generator_v6 import (
    NotebookResultV6,
    export_as_ipynb,
    export_as_python,
    generate_notebook_v6,
)
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
    use_v6: bool = True


def _v6_to_dict(result: NotebookResultV6) -> dict:
    """Convert V6 dataclass result to JSON-serializable dict.

    Preserves the same top-level shape as V5 (cells, workflow) for
    backwards compatibility, while adding V6-specific fields.
    """
    cells = [dataclasses.asdict(c) for c in result.cells]
    deep = dataclasses.asdict(result.deep_analysis) if result.deep_analysis else None

    return {
        "cells": cells,
        "workflow": result.workflow,
        "deepAnalysis": deep,
        "validationSummary": result.validation_summary,
        "version": "v6",
        "isRunnable": result.validation_summary.get("is_runnable", False)
            if result.validation_summary else False,
    }


@router.post("/generate")
def generate(req: GenerateRequest) -> dict:
    """Generate a Databricks notebook from the parsed flow and assessment.

    By default uses the V6 generator which produces run-quality notebooks.
    Set use_v6=false to fall back to the V5 generator.
    """
    proc_count = len(req.parsed.processors)
    processing_status.start("generate", proc_count)
    try:
        if req.use_v6:
            logger.info("V6 generation: %d processors", proc_count)
            result = generate_notebook_v6(req.parsed, req.assessment)
            processing_status.finish()
            return _v6_to_dict(result)
        else:
            logger.info("V5 (legacy) generation: %d processors", proc_count)
            result = generate_notebook(req.parsed, req.assessment, req.config)
            processing_status.finish()
            return result.model_dump(by_alias=True)
    except Exception as exc:
        processing_status.finish()
        logger.exception("Generation error")
        raise HTTPException(status_code=500, detail=f"Generation failed: {exc}") from exc


@router.post("/generate/export/python")
def generate_python(req: GenerateRequest) -> dict:
    """Generate and export as Databricks .py notebook format."""
    processing_status.start("generate_python", len(req.parsed.processors))
    try:
        result = generate_notebook_v6(req.parsed, req.assessment)
        py_content = export_as_python(result)
        processing_status.finish()
        return {"content": py_content, "filename": "migration.py"}
    except Exception as exc:
        processing_status.finish()
        logger.exception("Python export error")
        raise HTTPException(status_code=500, detail=f"Python export failed: {exc}") from exc


@router.post("/generate/export/ipynb")
def generate_ipynb(req: GenerateRequest) -> dict:
    """Generate and export as Jupyter .ipynb notebook format."""
    processing_status.start("generate_ipynb", len(req.parsed.processors))
    try:
        result = generate_notebook_v6(req.parsed, req.assessment)
        ipynb_content = export_as_ipynb(result)
        processing_status.finish()
        return {"content": ipynb_content, "filename": "migration.ipynb"}
    except Exception as exc:
        processing_status.finish()
        logger.exception("ipynb export error")
        raise HTTPException(status_code=500, detail=f"ipynb export failed: {exc}") from exc

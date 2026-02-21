"""Pipeline router — full 8-step pipeline execution (batch and streaming)."""

import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app.config import settings
from app.engines.analyzers import run_analysis
from app.engines.generators import generate_notebook
from app.engines.mappers import map_to_databricks
from app.engines.parsers import parse_flow
from app.engines.validators import validate_notebook
from app.models.config import DatabricksConfig
from app.models.processor import CamelModel
from app.routers.report import ReportRequest, _build_final, _build_migration, _build_value
from app.utils.streaming import StreamingPipelineRunner

router = APIRouter()
logger = logging.getLogger(__name__)


class PipelineStepResult(CamelModel):
    """Timing information for a single pipeline step."""
    step: str
    duration_ms: float
    status: str


class PipelineResponse(CamelModel):
    """Combined result from the full 8-step pipeline."""
    parsed: dict = {}
    analysis: dict = {}
    assessment: dict = {}
    notebook: dict = {}
    validation: dict = {}
    report_migration: dict = {}
    report_final: dict = {}
    report_value: dict = {}
    steps: list[PipelineStepResult] = []
    total_duration_ms: float = 0


@router.post("/pipeline/run")
async def pipeline_run(file: UploadFile) -> dict:
    """Accept a file upload and run the full 8-step pipeline, returning combined results."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    content = await file.read()
    if len(content) > settings.MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.MAX_FILE_SIZE} byte limit")

    results: dict[str, Any] = {}
    step_timings: list[PipelineStepResult] = []
    t_total = time.monotonic()

    try:
        # Step 1: Parse
        t0 = time.monotonic()
        parsed = parse_flow(content, file.filename)
        results["parsed"] = parsed.model_dump(by_alias=True)
        step_timings.append(PipelineStepResult(step="parse", duration_ms=_elapsed(t0), status="ok"))

        # Step 2: Analyze
        t0 = time.monotonic()
        analysis = run_analysis(parsed)
        results["analysis"] = analysis.model_dump(by_alias=True)
        step_timings.append(PipelineStepResult(step="analyze", duration_ms=_elapsed(t0), status="ok"))

        # Step 3: Assess
        t0 = time.monotonic()
        assessment = map_to_databricks(parsed, analysis)
        results["assessment"] = assessment.model_dump(by_alias=True)
        step_timings.append(PipelineStepResult(step="assess", duration_ms=_elapsed(t0), status="ok"))

        # Step 4: Generate
        t0 = time.monotonic()
        notebook = generate_notebook(parsed, assessment, DatabricksConfig())
        results["notebook"] = notebook.model_dump(by_alias=True)
        step_timings.append(PipelineStepResult(step="generate", duration_ms=_elapsed(t0), status="ok"))

        # Step 5: Validate
        t0 = time.monotonic()
        validation = validate_notebook(parsed, notebook)
        results["validation"] = validation.model_dump(by_alias=True)
        step_timings.append(PipelineStepResult(step="validate", duration_ms=_elapsed(t0), status="ok"))

        # Step 6: Migration Report
        t0 = time.monotonic()
        req = ReportRequest(type="migration", parsed=parsed, analysis=analysis, assessment=assessment)
        results["report_migration"] = _build_migration(req)
        step_timings.append(PipelineStepResult(step="report_migration", duration_ms=_elapsed(t0), status="ok"))

        # Step 7: Final Report
        t0 = time.monotonic()
        req = ReportRequest(
            type="final", parsed=parsed, analysis=analysis,
            assessment=assessment, validation=validation,
        )
        results["report_final"] = _build_final(req)
        step_timings.append(PipelineStepResult(step="report_final", duration_ms=_elapsed(t0), status="ok"))

        # Step 8: Value Analysis
        t0 = time.monotonic()
        req = ReportRequest(type="value", parsed=parsed, assessment=assessment)
        results["report_value"] = _build_value(req)
        step_timings.append(PipelineStepResult(step="report_value", duration_ms=_elapsed(t0), status="ok"))

    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid input: {exc}") from exc
    except Exception as exc:
        logger.exception("Pipeline run failed")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {exc}") from exc

    total_ms = _elapsed(t_total)
    response = PipelineResponse(
        **results,
        steps=step_timings,
        total_duration_ms=total_ms,
    )
    return response.model_dump(by_alias=True)


@router.post("/pipeline/run-streaming")
async def pipeline_run_streaming(file: UploadFile) -> StreamingResponse:
    """Accept a file upload and stream SSE progress events for each pipeline step."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    content = await file.read()
    if len(content) > settings.MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.MAX_FILE_SIZE} byte limit")

    runner = StreamingPipelineRunner(content, file.filename)

    return StreamingResponse(
        runner.run(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _elapsed(t0: float) -> float:
    """Return elapsed milliseconds since *t0*."""
    return round((time.monotonic() - t0) * 1000, 1)

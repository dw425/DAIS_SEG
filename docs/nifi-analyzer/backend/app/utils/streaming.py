"""Server-Sent Events (SSE) helpers for streaming pipeline progress."""

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

logger = logging.getLogger(__name__)


def format_sse(event: str, data: dict[str, Any]) -> str:
    """Format a single SSE frame.

    Returns a string like::

        event: step_complete
        data: {"step": "parse", "status": "ok"}

    """
    payload = json.dumps(data, default=str)
    lines = [f"event: {event}", f"data: {payload}", "", ""]
    return "\n".join(lines)


class StreamingPipelineRunner:
    """Yields SSE events as it executes each pipeline step sequentially.

    Usage::

        runner = StreamingPipelineRunner(content, filename)
        async for chunk in runner.run():
            yield chunk  # send to StreamingResponse
    """

    STEPS = [
        "parse",
        "analyze",
        "assess",
        "generate",
        "validate",
        "report_migration",
        "report_final",
        "report_value",
    ]

    def __init__(self, content: bytes, filename: str) -> None:
        self.content = content
        self.filename = filename

    async def run(self) -> AsyncIterator[str]:
        """Execute the 8-step pipeline, yielding SSE events."""
        from app.engines.analyzers import run_analysis
        from app.engines.generators import generate_notebook
        from app.engines.mappers import map_to_databricks
        from app.engines.parsers import parse_flow
        from app.engines.validators import validate_notebook
        from app.models.config import DatabricksConfig
        from app.routers.report import _build_final, _build_migration, _build_value, ReportRequest

        results: dict[str, Any] = {}

        for step in self.STEPS:
            yield format_sse("step_start", {"step": step})
            t0 = time.monotonic()
            try:
                if step == "parse":
                    parsed = parse_flow(self.content, self.filename)
                    results["parsed"] = parsed.model_dump(by_alias=True)

                elif step == "analyze":
                    analysis = run_analysis(parsed)
                    results["analysis"] = analysis.model_dump(by_alias=True)

                elif step == "assess":
                    assessment = map_to_databricks(parsed, analysis)
                    results["assessment"] = assessment.model_dump(by_alias=True)

                elif step == "generate":
                    notebook = generate_notebook(parsed, assessment, DatabricksConfig())
                    results["notebook"] = notebook.model_dump(by_alias=True)

                elif step == "validate":
                    validation = validate_notebook(parsed, notebook)
                    results["validation"] = validation.model_dump(by_alias=True)

                elif step == "report_migration":
                    req = ReportRequest(
                        type="migration", parsed=parsed, analysis=analysis, assessment=assessment,
                    )
                    results["report_migration"] = _build_migration(req)

                elif step == "report_final":
                    req = ReportRequest(
                        type="final", parsed=parsed, analysis=analysis,
                        assessment=assessment, validation=validation,
                    )
                    results["report_final"] = _build_final(req)

                elif step == "report_value":
                    req = ReportRequest(
                        type="value", parsed=parsed, assessment=assessment,
                    )
                    results["report_value"] = _build_value(req)

                elapsed = round((time.monotonic() - t0) * 1000, 1)
                yield format_sse("step_complete", {"step": step, "duration_ms": elapsed, "status": "ok"})

            except Exception as exc:
                elapsed = round((time.monotonic() - t0) * 1000, 1)
                logger.exception("Pipeline step '%s' failed", step)
                yield format_sse("step_error", {
                    "step": step, "duration_ms": elapsed, "error": str(exc),
                })
                break  # abort remaining steps

        yield format_sse("pipeline_complete", {"results": results})

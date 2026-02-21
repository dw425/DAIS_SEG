"""Export router — file download endpoints for notebooks, DLT, tests, workflows, and DAB bundles.

Each endpoint accepts the appropriate request model and returns a streaming
file download with the correct Content-Type and Content-Disposition headers.
"""

import json
import logging
import zipfile
from io import BytesIO

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.engines.generators.dab_generator import generate_dab
from app.engines.generators.dlt_generator import generate_dlt_pipeline
from app.engines.generators.notebook_generator import generate_notebook
from app.engines.generators.test_generator import generate_tests
from app.engines.generators.workflow_orchestrator import (
    generate_workflow as generate_workflow_def,
)
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, NotebookResult, ParseResult
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Request models ──────────────────────────────────────────────────────

class NotebookExportRequest(CamelModel):
    notebook: NotebookResult


class PipelineExportRequest(CamelModel):
    parsed: ParseResult
    assessment: AssessmentResult
    config: DatabricksConfig = DatabricksConfig()


# ── POST /api/export/notebook ───────────────────────────────────────────

@router.post("/export/notebook")
async def export_notebook(req: NotebookExportRequest) -> StreamingResponse:
    """Export a generated notebook as a .py file download."""
    try:
        lines: list[str] = []
        lines.append("# Databricks notebook source")
        lines.append("")

        for cell in req.notebook.cells:
            if cell.type == "markdown":
                lines.append("# MAGIC %md")
                for md_line in cell.source.split("\n"):
                    lines.append(f"# MAGIC {md_line}")
            else:
                lines.append(cell.source)
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        content = "\n".join(lines).encode("utf-8")
        return _stream_file(content, "migration_notebook.py", "text/x-python")

    except Exception as exc:
        logger.exception("Notebook export error")
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}") from exc


# ── POST /api/export/dlt ────────────────────────────────────────────────

@router.post("/export/dlt")
async def export_dlt(req: PipelineExportRequest) -> StreamingResponse:
    """Export a DLT pipeline as a .py file download."""
    try:
        result = generate_dlt_pipeline(req.parsed, req.assessment, req.config)
        cells = result.get("cells", [])

        lines: list[str] = []
        lines.append("# Databricks notebook source — Delta Live Tables Pipeline")
        lines.append("")

        for cell in cells:
            if cell.get("type") == "markdown":
                lines.append("# MAGIC %md")
                for md_line in cell["source"].split("\n"):
                    lines.append(f"# MAGIC {md_line}")
            else:
                lines.append(cell["source"])
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        content = "\n".join(lines).encode("utf-8")
        return _stream_file(content, "dlt_pipeline.py", "text/x-python")

    except Exception as exc:
        logger.exception("DLT export error")
        raise HTTPException(status_code=500, detail=f"DLT export failed: {exc}") from exc


# ── POST /api/export/tests ──────────────────────────────────────────────

@router.post("/export/tests")
async def export_tests(req: PipelineExportRequest) -> StreamingResponse:
    """Export generated pytest tests as a .py file download."""
    try:
        test_content = generate_tests(req.parsed, req.assessment)
        content = test_content.encode("utf-8")
        return _stream_file(content, "test_migration.py", "text/x-python")

    except Exception as exc:
        logger.exception("Test export error")
        raise HTTPException(status_code=500, detail=f"Test export failed: {exc}") from exc


# ── POST /api/export/workflow ───────────────────────────────────────────

@router.post("/export/workflow")
async def export_workflow(req: PipelineExportRequest) -> StreamingResponse:
    """Export a Databricks Workflows job definition as a .json file download."""
    try:
        workflow = generate_workflow_def(req.parsed, req.assessment, req.config)
        content = json.dumps(workflow, indent=2).encode("utf-8")
        return _stream_file(content, "workflow.json", "application/json")

    except Exception as exc:
        logger.exception("Workflow export error")
        raise HTTPException(status_code=500, detail=f"Workflow export failed: {exc}") from exc


# ── POST /api/export/dab ──────────────────────────────────────────────

@router.post("/export/dab")
async def export_dab(req: PipelineExportRequest) -> StreamingResponse:
    """Export a Databricks Asset Bundle as a ZIP file download.

    Returns a ZIP containing:
      - databricks.yml (bundle configuration)
      - notebooks/01_ingest.py, 02_transform.py, etc.
      - resources/ (supporting files)
    """
    try:
        dab_result = generate_dab(req.parsed, req.assessment, req.config)

        # Build ZIP in memory
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # databricks.yml
            zf.writestr("databricks.yml", dab_result["databricks_yml"])

            # Notebook files
            for filename, content in dab_result.get("notebooks", {}).items():
                zf.writestr(filename, content)

            # Resources directory with a placeholder README
            zf.writestr(
                "resources/README.md",
                "# Resources\n\nPlace supporting files (schemas, configs, etc.) in this directory.\n",
            )

            # Include the bundle structure as JSON for reference
            zf.writestr(
                "resources/bundle_structure.json",
                json.dumps(dab_result["bundle_structure"], indent=2),
            )

        zip_buffer.seek(0)
        content = zip_buffer.getvalue()

        return _stream_file(content, "databricks_asset_bundle.zip", "application/zip")

    except Exception as exc:
        logger.exception("DAB export error")
        raise HTTPException(status_code=500, detail=f"DAB export failed: {exc}") from exc


# ── Helper ──────────────────────────────────────────────────────────────

def _stream_file(
    content: bytes, filename: str, media_type: str
) -> StreamingResponse:
    """Wrap bytes as a StreamingResponse with download headers."""
    return StreamingResponse(
        BytesIO(content),
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(content)),
        },
    )

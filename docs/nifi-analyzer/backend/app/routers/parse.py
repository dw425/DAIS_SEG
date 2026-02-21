"""Parse router — accepts file upload, detects format, returns ParseResult."""

import logging

from fastapi import APIRouter, HTTPException, UploadFile

from app.config import settings
from app.engines.parsers import parse_flow
from app.models.pipeline import ParseResult

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/parse")
async def parse_file(file: UploadFile) -> dict:
    """Accept an ETL definition file, detect its format, and parse it."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    content = await file.read()
    if len(content) > settings.MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.MAX_FILE_SIZE} byte limit")

    try:
        result = parse_flow(content, file.filename)
        return result.model_dump(by_alias=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Parse error for %s", file.filename)
        raise HTTPException(status_code=500, detail=f"Parse failed: {exc}") from exc

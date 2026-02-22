"""NiFi connect router — endpoints for live NiFi instance connection and extraction."""

import logging

from fastapi import APIRouter, HTTPException

from app.engines.parsers.nifi_live_extractor import (
    connect,
    extract_flow,
    is_available,
    list_process_groups,
)
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class NiFiConnectRequest(CamelModel):
    nifi_url: str
    username: str | None = None
    password: str | None = None
    token: str | None = None
    verify_ssl: bool = True


class NiFiExtractRequest(CamelModel):
    nifi_url: str
    process_group_id: str = "root"
    username: str | None = None
    password: str | None = None
    token: str | None = None
    verify_ssl: bool = True


@router.get("/nifi/status")
async def nifi_status() -> dict:
    """Check if NiPyApi is available."""
    return {"available": is_available()}


@router.post("/nifi/connect")
async def nifi_connect(req: NiFiConnectRequest) -> dict:
    """Test connection to a NiFi instance."""
    if not is_available():
        raise HTTPException(
            status_code=501,
            detail="NiPyApi is not installed. Install with: pip install 'etl-migration-platform[nifi]'",
        )
    try:
        result = connect(
            nifi_url=req.nifi_url,
            username=req.username,
            password=req.password,
            token=req.token,
            verify_ssl=req.verify_ssl,
        )
        return result
    except Exception as exc:
        logger.exception("NiFi connection error")
        raise HTTPException(status_code=500, detail=f"Connection failed: {exc}") from exc


@router.post("/nifi/process-groups")
async def nifi_process_groups(req: NiFiConnectRequest) -> dict:
    """List available process groups from a NiFi instance."""
    if not is_available():
        raise HTTPException(
            status_code=501,
            detail="NiPyApi is not installed.",
        )
    try:
        groups = list_process_groups(
            nifi_url=req.nifi_url,
            username=req.username,
            password=req.password,
            token=req.token,
            verify_ssl=req.verify_ssl,
        )
        return {"processGroups": groups}
    except Exception as exc:
        logger.exception("NiFi process group listing error")
        raise HTTPException(status_code=500, detail=f"Failed to list process groups: {exc}") from exc


@router.post("/nifi/extract")
async def nifi_extract(req: NiFiExtractRequest) -> dict:
    """Extract flow from a live NiFi instance.

    Returns the same ParseResult shape as the file upload parser.
    """
    if not is_available():
        raise HTTPException(
            status_code=501,
            detail="NiPyApi is not installed.",
        )
    try:
        result = extract_flow(
            nifi_url=req.nifi_url,
            process_group_id=req.process_group_id,
            username=req.username,
            password=req.password,
            token=req.token,
            verify_ssl=req.verify_ssl,
        )
        return result.model_dump(by_alias=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("NiFi extraction error")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {exc}") from exc

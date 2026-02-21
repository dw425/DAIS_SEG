"""Flow versioning router — snapshot, list, and diff flow versions."""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.db.models.enterprise import FlowVersion
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class VersionSnapshot(CamelModel):
    """Request body for saving a version snapshot."""
    label: str = ""
    parsed: dict = {}
    analysis: dict | None = None
    assessment: dict | None = None
    notebook: dict | None = None


class VersionMeta(CamelModel):
    """Returned metadata for a single version."""
    version_id: str
    label: str
    created_at: str
    processor_count: int = 0
    connection_count: int = 0


class DiffResult(CamelModel):
    """Result of diffing two versions."""
    added_processors: list[str] = []
    removed_processors: list[str] = []
    modified_processors: list[str] = []
    added_connections: int = 0
    removed_connections: int = 0
    v1_label: str = ""
    v2_label: str = ""


@router.post("/projects/{project_id}/versions")
async def save_version(project_id: str, snapshot: VersionSnapshot, db: Session = Depends(get_db)) -> dict:
    """Save a snapshot of the current flow as a new version."""
    version_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    processors = snapshot.parsed.get("processors", [])
    connections = snapshot.parsed.get("connections", [])

    # Compute label — count existing versions for this project
    existing_count = db.query(FlowVersion).filter(FlowVersion.project_id == project_id).count()
    label = snapshot.label or f"v{existing_count + 1}"

    row = FlowVersion(
        id=version_id,
        project_id=project_id,
        label=label,
        processor_count=len(processors),
        connection_count=len(connections),
        data=snapshot.model_dump(),
        created_at=now,
    )
    db.add(row)
    db.commit()

    logger.info("Saved version %s for project %s", version_id, project_id)
    return {"versionId": version_id, "createdAt": now.isoformat()}


@router.get("/projects/{project_id}/versions")
async def list_versions(project_id: str, db: Session = Depends(get_db)) -> dict:
    """List all saved versions for a project."""
    rows = db.query(FlowVersion).filter(FlowVersion.project_id == project_id).all()
    meta = [
        {
            "versionId": v.id,
            "label": v.label,
            "createdAt": v.created_at.isoformat() if v.created_at else None,
            "processorCount": v.processor_count,
            "connectionCount": v.connection_count,
        }
        for v in rows
    ]
    return {"versions": meta}


@router.get("/projects/{project_id}/versions/diff")
async def diff_versions(project_id: str, v1: str, v2: str, db: Session = Depends(get_db)) -> dict:
    """Compute the diff between two versions."""
    ver1 = db.query(FlowVersion).filter(FlowVersion.id == v1, FlowVersion.project_id == project_id).first()
    ver2 = db.query(FlowVersion).filter(FlowVersion.id == v2, FlowVersion.project_id == project_id).first()

    if not ver1 or not ver2:
        raise HTTPException(status_code=404, detail="One or both versions not found")

    p1_names = {p.get("name", "") for p in (ver1.data or {}).get("parsed", {}).get("processors", [])}
    p2_names = {p.get("name", "") for p in (ver2.data or {}).get("parsed", {}).get("processors", [])}

    c1_count = len((ver1.data or {}).get("parsed", {}).get("connections", []))
    c2_count = len((ver2.data or {}).get("parsed", {}).get("connections", []))

    # Detect modified processors (same name, different properties)
    p1_map = {p.get("name", ""): p for p in (ver1.data or {}).get("parsed", {}).get("processors", [])}
    p2_map = {p.get("name", ""): p for p in (ver2.data or {}).get("parsed", {}).get("processors", [])}
    common = p1_names & p2_names
    modified = [n for n in common if p1_map.get(n) != p2_map.get(n)]

    return {
        "addedProcessors": list(p2_names - p1_names),
        "removedProcessors": list(p1_names - p2_names),
        "modifiedProcessors": modified,
        "addedConnections": max(0, c2_count - c1_count),
        "removedConnections": max(0, c1_count - c2_count),
        "v1Label": ver1.label,
        "v2Label": ver2.label,
    }

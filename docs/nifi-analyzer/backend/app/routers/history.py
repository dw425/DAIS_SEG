"""Run history router — track and retrieve past pipeline runs."""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import RunHistory
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class RunRecord(CamelModel):
    """Request body for recording a pipeline run."""
    file_name: str
    platform: str = "unknown"
    processor_count: int = 0
    status: str = "completed"
    duration_ms: int = 0
    steps_completed: list[str] = []
    result_summary: dict = {}


@router.post("/history")
async def record_run(body: RunRecord, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Record a completed pipeline run."""
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    row = RunHistory(
        id=run_id,
        filename=body.file_name,
        platform=body.platform,
        processor_count=body.processor_count,
        status=body.status,
        duration_ms=body.duration_ms,
        steps_completed=body.steps_completed,
        result_summary=body.result_summary,
        created_at=now,
    )
    db.add(row)
    db.commit()

    logger.info("Run %s recorded: %s", run_id, body.file_name)
    return {
        "id": run_id,
        "fileName": body.file_name,
        "platform": body.platform,
        "processorCount": body.processor_count,
        "status": body.status,
        "durationMs": body.duration_ms,
        "stepsCompleted": body.steps_completed,
        "resultSummary": body.result_summary,
        "createdAt": now.isoformat(),
    }


@router.get("/history")
async def list_runs(limit: int = 50, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List past pipeline runs with pagination."""
    total = db.query(RunHistory).count()
    rows = (
        db.query(RunHistory)
        .order_by(RunHistory.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    runs = [
        {
            "id": r.id,
            "fileName": r.filename,
            "platform": r.platform,
            "processorCount": r.processor_count,
            "status": r.status,
            "durationMs": r.duration_ms,
            "stepsCompleted": r.steps_completed or [],
            "resultSummary": r.result_summary or {},
            "createdAt": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {"runs": runs, "total": total}


@router.get("/history/{run_id}")
async def get_run(run_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Get detailed information about a specific run."""
    r = db.query(RunHistory).filter(RunHistory.id == run_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "id": r.id,
        "fileName": r.filename,
        "platform": r.platform,
        "processorCount": r.processor_count,
        "status": r.status,
        "durationMs": r.duration_ms,
        "stepsCompleted": r.steps_completed or [],
        "resultSummary": r.result_summary or {},
        "createdAt": r.created_at.isoformat() if r.created_at else None,
    }

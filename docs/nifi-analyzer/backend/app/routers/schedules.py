"""Scheduled runs router — CRUD for scheduled pipeline runs."""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import Schedule
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class ScheduleCreate(CamelModel):
    """Create a scheduled run."""
    name: str = Field(..., min_length=1, max_length=200)
    cron: str = Field(default="0 0 * * *", max_length=100)
    project_id: str = ""
    enabled: bool = True

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have exactly 5 fields")
        return v


def _estimate_next_run(cron: str) -> datetime:
    """Estimate next run from a cron expression using basic heuristic.

    This does not implement full cron parsing — it inspects the minute and hour
    fields to produce a reasonable next-run timestamp for display purposes.
    """
    parts = cron.strip().split()
    now = datetime.now(timezone.utc)

    if len(parts) < 5:
        # Fallback: next day at midnight
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    minute_field, hour_field = parts[0], parts[1]

    # Parse minute
    try:
        target_minute = int(minute_field)
    except ValueError:
        target_minute = 0

    # Parse hour
    try:
        target_hour = int(hour_field)
    except ValueError:
        target_hour = 0

    candidate = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)

    return candidate


@router.post("/schedules")
async def create_schedule(body: ScheduleCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Create a new scheduled run."""
    schedule_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    next_run = _estimate_next_run(body.cron) if body.enabled else None

    row = Schedule(
        id=schedule_id,
        name=body.name,
        cron=body.cron,
        project_id=body.project_id,
        enabled=body.enabled,
        last_run=None,
        next_run=next_run,
        created_at=now,
    )
    db.add(row)
    db.commit()

    return {
        "id": schedule_id,
        "name": body.name,
        "cron": body.cron,
        "projectId": body.project_id,
        "enabled": body.enabled,
        "createdAt": now.isoformat(),
        "lastRun": None,
        "nextRun": next_run.isoformat() if next_run else None,
    }


@router.get("/schedules")
async def list_schedules(limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List all scheduled runs."""
    rows = db.query(Schedule).offset(offset).limit(limit).all()
    schedules = [
        {
            "id": r.id,
            "name": r.name,
            "cron": r.cron,
            "projectId": r.project_id,
            "enabled": r.enabled,
            "createdAt": r.created_at.isoformat() if r.created_at else None,
            "lastRun": r.last_run.isoformat() if r.last_run else None,
            "nextRun": r.next_run.isoformat() if r.next_run else None,
        }
        for r in rows
    ]
    return {"schedules": schedules}


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Delete a scheduled run."""
    row = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Schedule not found")
    db.delete(row)
    db.commit()
    return {"deleted": schedule_id}

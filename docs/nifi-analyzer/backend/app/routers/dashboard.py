"""Portfolio dashboard router — aggregate stats across all runs."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import RunHistory
from app.db.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/dashboard")
async def get_dashboard(db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Return aggregate stats for the portfolio dashboard."""
    total_runs = db.query(RunHistory).count()
    completed = db.query(RunHistory).filter(RunHistory.status == "completed").count()
    failed = db.query(RunHistory).filter(RunHistory.status == "failed").count()

    total_processors = (
        db.query(func.coalesce(func.sum(RunHistory.processor_count), 0)).scalar()
    )

    # Platform breakdown
    platform_rows = (
        db.query(RunHistory.platform, func.count(RunHistory.id))
        .group_by(RunHistory.platform)
        .all()
    )
    platform_counts = {plat or "unknown": cnt for plat, cnt in platform_rows}

    # Average duration
    avg_duration = (
        db.query(func.avg(RunHistory.duration_ms))
        .filter(RunHistory.duration_ms > 0)
        .scalar()
    )
    avg_duration = int(avg_duration) if avg_duration else 0

    # Recent runs (last 5)
    recent_rows = (
        db.query(RunHistory)
        .order_by(RunHistory.created_at.desc())
        .limit(5)
        .all()
    )
    recent = [
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
        for r in recent_rows
    ]

    return {
        "totalRuns": total_runs,
        "completedRuns": completed,
        "failedRuns": failed,
        "totalProcessors": total_processors,
        "platformBreakdown": platform_counts,
        "avgDurationMs": avg_duration,
        "successRate": round(completed / total_runs * 100, 1) if total_runs > 0 else 0,
        "recentRuns": recent,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
    }

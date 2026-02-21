"""Audit router — query audit logs (admin only)."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.auth.rbac import require_role
from app.db.engine import get_db
from app.db.models.audit import AuditLog
from app.db.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)


def _audit_to_dict(log: AuditLog) -> dict:
    return {
        "id": log.id,
        "user_id": log.user_id,
        "action": log.action,
        "resource_type": log.resource_type,
        "resource_id": log.resource_id,
        "details": json.loads(log.details) if log.details and log.details != "{}" else {},
        "ip_address": log.ip_address,
        "created_at": log.created_at.isoformat() if log.created_at else None,
    }


@router.get("/audit")
def list_audit_logs(
    action: Optional[str] = Query(None, description="Filter by action"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
) -> dict:
    """List audit logs with optional filters. Admin only."""
    query = db.query(AuditLog).order_by(AuditLog.created_at.desc())

    if action:
        query = query.filter(AuditLog.action == action)
    if resource_type:
        query = query.filter(AuditLog.resource_type == resource_type)
    if user_id:
        query = query.filter(AuditLog.user_id == user_id)

    total = query.count()
    logs = query.offset(offset).limit(limit).all()

    return {
        "logs": [_audit_to_dict(log) for log in logs],
        "total": total,
        "limit": limit,
        "offset": offset,
    }

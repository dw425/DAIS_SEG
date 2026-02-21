"""Shareable links router — generate and resolve short shareable links."""

import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import ShareLink
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class ShareCreate(CamelModel):
    """Create a shareable link."""
    target_type: str = "project"
    target_id: str = ""
    expires_hours: int = 168  # 7 days default


@router.post("/shares")
async def create_share(body: ShareCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Generate a shareable link token."""
    token = secrets.token_urlsafe(16)
    now = datetime.now(timezone.utc)

    row = ShareLink(
        token=token,
        target_type=body.target_type,
        target_id=body.target_id,
        expires_hours=body.expires_hours,
        created_at=now,
    )
    db.add(row)
    db.commit()

    return {
        "token": token,
        "targetType": body.target_type,
        "targetId": body.target_id,
        "createdAt": now.isoformat(),
        "expiresHours": body.expires_hours,
    }


@router.get("/shares/{token}")
async def resolve_share(token: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Resolve a shareable link token."""
    row = db.query(ShareLink).filter(ShareLink.token == token).first()
    if not row:
        raise HTTPException(status_code=404, detail="Share link not found or expired")

    # Check expiration
    if row.created_at and row.expires_hours:
        expires_at = row.created_at + timedelta(hours=row.expires_hours)
        if datetime.now(timezone.utc) > expires_at:
            raise HTTPException(status_code=404, detail="Share link not found or expired")

    return {
        "token": row.token,
        "targetType": row.target_type,
        "targetId": row.target_id,
        "createdAt": row.created_at.isoformat() if row.created_at else None,
        "expiresHours": row.expires_hours,
    }


@router.get("/shares")
async def list_shares(limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List all active shareable links."""
    rows = db.query(ShareLink).offset(offset).limit(limit).all()
    shares = [
        {
            "token": r.token,
            "targetType": r.target_type,
            "targetId": r.target_id,
            "createdAt": r.created_at.isoformat() if r.created_at else None,
            "expiresHours": r.expires_hours,
        }
        for r in rows
    ]
    return {"shares": shares}

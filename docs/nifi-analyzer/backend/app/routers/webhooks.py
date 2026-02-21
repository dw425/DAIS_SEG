"""Webhooks router — manage webhook subscriptions for pipeline events."""

import ipaddress
import logging
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import Webhook
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class WebhookCreate(CamelModel):
    """Create a webhook subscription."""
    url: str
    events: list[str] = ["run.completed"]
    secret: str = ""


def _validate_webhook_url(url: str) -> None:
    """Validate that the webhook URL is HTTPS and not targeting a private IP."""
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise HTTPException(status_code=400, detail="Webhook URL must use HTTPS")
    host = parsed.hostname
    if not host:
        raise HTTPException(status_code=400, detail="Invalid webhook URL")
    try:
        addr = ipaddress.ip_address(host)
        if addr.is_private or addr.is_loopback or addr.is_reserved:
            raise HTTPException(status_code=400, detail="Webhook URL must not target private/reserved IPs")
    except ValueError:
        pass  # hostname is a domain name, not an IP — allowed


@router.post("/webhooks")
async def create_webhook(body: WebhookCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Register a new webhook."""
    _validate_webhook_url(body.url)
    webhook_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    row = Webhook(
        id=webhook_id,
        url=body.url,
        events=body.events,
        secret=body.secret or None,
        active=True,
        last_triggered=None,
        created_at=now,
    )
    db.add(row)
    db.commit()
    return {
        "id": webhook_id,
        "url": body.url,
        "events": body.events,
        "createdAt": now.isoformat(),
        "lastTriggered": None,
    }


@router.get("/webhooks")
async def list_webhooks(limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List all webhook subscriptions."""
    rows = db.query(Webhook).offset(offset).limit(limit).all()
    webhooks = [
        {
            "id": r.id,
            "url": r.url,
            "events": r.events or [],
            "createdAt": r.created_at.isoformat() if r.created_at else None,
            "lastTriggered": r.last_triggered.isoformat() if r.last_triggered else None,
        }
        for r in rows
    ]
    return {"webhooks": webhooks}


@router.delete("/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Delete a webhook subscription."""
    row = db.query(Webhook).filter(Webhook.id == webhook_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Webhook not found")
    db.delete(row)
    db.commit()
    return {"deleted": webhook_id}

"""API Keys router — CRUD for API keys."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.api_key import generate_api_key
from app.auth.dependencies import get_current_user
from app.auth.rbac import require_role
from app.db.engine import get_db
from app.db.models.api_key import APIKey
from app.db.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Schemas ──


class APIKeyCreate(BaseModel):
    name: str
    scopes: str = "read"
    expires_in_days: int | None = None  # None = never expires


# ── Endpoints ──


@router.post("/api-keys", status_code=status.HTTP_201_CREATED)
def create_api_key(
    body: APIKeyCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Generate a new API key for the current user."""
    full_key, key_hash, prefix = generate_api_key()

    expires_at = None
    if body.expires_in_days:
        from datetime import timedelta
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    api_key = APIKey(
        user_id=user.id,
        name=body.name,
        key_hash=key_hash,
        prefix=prefix,
        scopes=body.scopes,
        expires_at=expires_at,
    )
    db.add(api_key)
    db.commit()
    db.refresh(api_key)

    logger.info("API key created: %s for user %s", body.name, user.email)

    return {
        "api_key": {
            "id": api_key.id,
            "name": api_key.name,
            "key": full_key,  # Only shown once!
            "prefix": api_key.prefix,
            "scopes": api_key.scopes,
            "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
            "created_at": api_key.created_at.isoformat() if api_key.created_at else None,
        }
    }


@router.get("/api-keys")
def list_api_keys(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """List API keys for the current user (admins see all)."""
    if user.role == "admin":
        keys = db.query(APIKey).order_by(APIKey.created_at.desc()).all()
    else:
        keys = db.query(APIKey).filter(APIKey.user_id == user.id).order_by(APIKey.created_at.desc()).all()

    return {
        "api_keys": [
            {
                "id": k.id,
                "user_id": k.user_id,
                "name": k.name,
                "prefix": k.prefix,
                "scopes": k.scopes,
                "is_active": k.is_active,
                "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
                "expires_at": k.expires_at.isoformat() if k.expires_at else None,
                "created_at": k.created_at.isoformat() if k.created_at else None,
            }
            for k in keys
        ]
    }


@router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_api_key(
    key_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """Revoke (deactivate) an API key."""
    api_key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not api_key:
        raise HTTPException(status_code=404, detail="API key not found")
    if user.role != "admin" and api_key.user_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized")

    api_key.is_active = False
    db.commit()
    logger.info("API key revoked: %s by %s", key_id, user.email)

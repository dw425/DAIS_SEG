"""API key generation and validation."""

import hashlib
import secrets
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.db.models.api_key import APIKey
from app.db.models.user import User


def generate_api_key() -> tuple[str, str, str]:
    """Generate a new API key.

    Returns (full_key, key_hash, prefix) where:
    - full_key is shown once to the user
    - key_hash is stored in DB
    - prefix is the first 8 chars for display
    """
    raw_key = f"etl_{secrets.token_urlsafe(32)}"
    prefix = raw_key[:8]
    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return raw_key, key_hash, prefix


def validate_api_key(key: str, db: Session) -> APIKey | None:
    """Validate an API key string. Returns the APIKey record or None."""
    key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
    api_key = db.query(APIKey).filter(APIKey.key_hash == key_hash, APIKey.is_active.is_(True)).first()

    if not api_key:
        return None

    # Check expiration
    if api_key.expires_at and api_key.expires_at < datetime.now(timezone.utc):
        return None

    # Update last_used_at
    api_key.last_used_at = datetime.now(timezone.utc)
    db.commit()

    return api_key


def get_user_from_api_key(request: Request, db: Session = Depends(get_db)) -> User | None:
    """Extract and validate API key from X-API-Key header, return associated User."""
    api_key_header = request.headers.get("X-API-Key", "")
    if not api_key_header:
        return None

    api_key_record = validate_api_key(api_key_header, db)
    if not api_key_record:
        return None

    user = db.query(User).filter(User.id == api_key_record.user_id).first()
    return user if user and user.is_active else None

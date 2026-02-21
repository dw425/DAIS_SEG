"""FastAPI dependencies for authentication."""

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.jwt_handler import verify_token
from app.db.engine import get_db
from app.db.models.user import User


def _extract_token(request: Request) -> str | None:
    """Extract Bearer token from Authorization header or API key from X-API-Key."""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    return None


def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    """Return the authenticated User. Raises 401 if no valid token is provided."""
    token = _extract_token(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    return user


def get_optional_user(request: Request, db: Session = Depends(get_db)) -> User | None:
    """Optionally extract user from token — returns None for public endpoints."""
    token = _extract_token(request)
    if not token:
        return None

    payload = verify_token(token)
    if not payload:
        return None

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        return None

    return user

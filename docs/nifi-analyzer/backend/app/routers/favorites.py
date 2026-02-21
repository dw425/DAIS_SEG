"""Favorites router — bookmark flows and projects."""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import Favorite
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class FavoriteCreate(CamelModel):
    """Create a favorite/bookmark."""
    target_type: str = "project"
    target_id: str = ""
    label: str = ""


@router.post("/favorites")
async def create_favorite(body: FavoriteCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Add a favorite."""
    fav_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    row = Favorite(
        id=fav_id,
        target_type=body.target_type,
        target_id=body.target_id,
        label=body.label,
        created_at=now,
    )
    db.add(row)
    db.commit()
    return {
        "id": fav_id,
        "targetType": body.target_type,
        "targetId": body.target_id,
        "label": body.label,
        "createdAt": now.isoformat(),
    }


@router.get("/favorites")
async def list_favorites(limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List all favorites."""
    rows = db.query(Favorite).offset(offset).limit(limit).all()
    favorites = [
        {
            "id": r.id,
            "targetType": r.target_type,
            "targetId": r.target_id,
            "label": r.label,
            "createdAt": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {"favorites": favorites}


@router.delete("/favorites/{fav_id}")
async def delete_favorite(fav_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Remove a favorite."""
    row = db.query(Favorite).filter(Favorite.id == fav_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Favorite not found")
    db.delete(row)
    db.commit()
    return {"deleted": fav_id}

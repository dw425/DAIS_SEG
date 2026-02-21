"""Tags router — manage tags for flows and projects."""

import logging
import uuid
from datetime import datetime, timezone

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import Tag
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class TagCreate(CamelModel):
    """Create a tag."""
    name: str = Field(..., min_length=1, max_length=50)
    color: str = Field(default="#3B82F6", pattern=r"^#[0-9a-fA-F]{6}$")
    target_type: Literal["project", "flow", "processor"] = "project"
    target_id: str = Field(default="", max_length=200)


@router.post("/tags")
async def create_tag(body: TagCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Create a new tag."""
    tag_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    row = Tag(
        id=tag_id,
        name=body.name,
        color=body.color,
        target_type=body.target_type,
        target_id=body.target_id,
        created_at=now,
    )
    db.add(row)
    db.commit()
    return {
        "id": tag_id,
        "name": body.name,
        "color": body.color,
        "targetType": body.target_type,
        "targetId": body.target_id,
        "createdAt": now.isoformat(),
    }


@router.get("/tags")
async def list_tags(target_type: str = "", target_id: str = "", limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List tags, optionally filtered."""
    q = db.query(Tag)
    if target_type:
        q = q.filter(Tag.target_type == target_type)
    if target_id:
        q = q.filter(Tag.target_id == target_id)
    rows = q.offset(offset).limit(limit).all()
    tags = [
        {
            "id": r.id,
            "name": r.name,
            "color": r.color,
            "targetType": r.target_type,
            "targetId": r.target_id,
            "createdAt": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {"tags": tags}


@router.delete("/tags/{tag_id}")
async def delete_tag(tag_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Delete a tag."""
    row = db.query(Tag).filter(Tag.id == tag_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Tag not found")
    db.delete(row)
    db.commit()
    return {"deleted": tag_id}

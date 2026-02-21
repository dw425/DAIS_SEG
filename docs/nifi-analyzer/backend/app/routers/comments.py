"""Comments router — CRUD for annotations and threaded comments."""

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.enterprise import Comment
from app.db.models.user import User
from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


def _strip_html(text: str) -> str:
    """Remove HTML tags from text to prevent XSS."""
    return re.sub(r"<[^>]+>", "", text)


class CommentCreate(CamelModel):
    """Request body for creating a comment."""
    text: str = Field(..., min_length=1, max_length=5000)
    author: str = Field(default="Anonymous", max_length=100)
    target_type: Literal["processor", "connection", "group", "flow"] = "processor"
    target_id: str = Field(default="", max_length=200)
    parent_id: str | None = None  # For threaded replies


@router.post("/comments")
async def create_comment(body: CommentCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Create a new comment or reply."""
    comment_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    row = Comment(
        id=comment_id,
        text=_strip_html(body.text),
        author=body.author,
        target_type=body.target_type,
        target_id=body.target_id,
        parent_id=body.parent_id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()

    logger.info("Comment %s created by %s", comment_id, body.author)
    return {
        "id": comment_id,
        "text": body.text,
        "author": body.author,
        "targetType": body.target_type,
        "targetId": body.target_id,
        "parentId": body.parent_id,
        "createdAt": now.isoformat(),
        "updatedAt": now.isoformat(),
    }


@router.get("/comments")
async def list_comments(target_type: str = "", target_id: str = "", limit: int = 100, offset: int = 0, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """List comments, optionally filtered by target."""
    q = db.query(Comment)
    if target_type:
        q = q.filter(Comment.target_type == target_type)
    if target_id:
        q = q.filter(Comment.target_id == target_id)
    rows = q.offset(offset).limit(limit).all()
    comments = [
        {
            "id": r.id,
            "text": r.text,
            "author": r.author,
            "targetType": r.target_type,
            "targetId": r.target_id,
            "parentId": r.parent_id,
            "createdAt": r.created_at.isoformat() if r.created_at else None,
            "updatedAt": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rows
    ]
    return {"comments": comments}


@router.delete("/comments/{comment_id}")
async def delete_comment(comment_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)) -> dict:
    """Delete a comment by ID."""
    row = db.query(Comment).filter(Comment.id == comment_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Comment not found")
    # Also remove replies
    db.query(Comment).filter(Comment.parent_id == comment_id).delete()
    db.delete(row)
    db.commit()
    logger.info("Comment %s deleted", comment_id)
    return {"deleted": comment_id}

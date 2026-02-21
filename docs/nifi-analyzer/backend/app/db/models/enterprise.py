"""Enterprise feature models — persisted to SQLite instead of in-memory stores."""

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, JSON, String, Text

from app.db.base import Base


class Comment(Base):
    __tablename__ = "comments"

    id = Column(String, primary_key=True)
    target_type = Column(String, nullable=True)
    target_id = Column(String, nullable=True)
    parent_id = Column(String, nullable=True)
    author = Column(String, default="anonymous")
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class RunHistory(Base):
    """Lightweight run history for the history/dashboard routers.

    Separate from PipelineRun in project.py which is tied to projects/users.
    """

    __tablename__ = "run_history"

    id = Column(String, primary_key=True)
    filename = Column(String)
    platform = Column(String)
    processor_count = Column(Integer, default=0)
    status = Column(String, default="completed")
    duration_ms = Column(Integer, default=0)
    steps_completed = Column(JSON, default=list)
    result_summary = Column(JSON, default=dict)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Favorite(Base):
    __tablename__ = "favorites"

    id = Column(String, primary_key=True)
    target_type = Column(String)
    target_id = Column(String)
    label = Column(String, default="")
    user_id = Column(String, default="anonymous")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Schedule(Base):
    __tablename__ = "schedules"

    id = Column(String, primary_key=True)
    name = Column(String)
    cron = Column(String)
    project_id = Column(String, nullable=True)
    enabled = Column(Boolean, default=True)
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ShareLink(Base):
    __tablename__ = "share_links"

    token = Column(String, primary_key=True)
    target_type = Column(String, default="project")
    target_id = Column(String, default="")
    expires_hours = Column(Integer, default=168)
    created_by = Column(String, default="anonymous")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Tag(Base):
    __tablename__ = "tags"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    color = Column(String, default="#3b82f6")
    target_type = Column(String, nullable=True)
    target_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Webhook(Base):
    __tablename__ = "webhooks"

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    events = Column(JSON, default=list)
    secret = Column(String, nullable=True)
    active = Column(Boolean, default=True)
    last_triggered = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class FlowVersion(Base):
    __tablename__ = "flow_versions"

    id = Column(String, primary_key=True)
    project_id = Column(String, nullable=False)
    label = Column(String)
    processor_count = Column(Integer, default=0)
    connection_count = Column(Integer, default=0)
    data = Column(JSON)
    created_by = Column(String, default="anonymous")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

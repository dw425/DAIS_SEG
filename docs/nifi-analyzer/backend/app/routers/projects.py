"""Projects router — CRUD for projects and pipeline runs."""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.db.engine import get_db
from app.db.models.project import PipelineRun, Project
from app.db.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Schemas ──


class ProjectCreate(BaseModel):
    name: str
    description: str = ""
    platform: str = "auto"


class ProjectUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    platform: str | None = None


def _project_to_dict(p: Project) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "description": p.description,
        "platform": p.platform,
        "owner_id": p.owner_id,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "updated_at": p.updated_at.isoformat() if p.updated_at else None,
        "run_count": len(p.runs) if p.runs else 0,
    }


def _run_to_dict(r: PipelineRun) -> dict:
    return {
        "id": r.id,
        "project_id": r.project_id,
        "user_id": r.user_id,
        "status": r.status,
        "step_results": json.loads(r.step_results) if r.step_results else {},
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
    }


# ── Endpoints ──


@router.post("/projects", status_code=status.HTTP_201_CREATED)
def create_project(
    body: ProjectCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Create a new project."""
    project = Project(
        name=body.name,
        description=body.description,
        platform=body.platform,
        owner_id=user.id,
    )
    db.add(project)
    db.commit()
    db.refresh(project)
    logger.info("Project created: %s by %s", project.name, user.email)
    return {"project": _project_to_dict(project)}


@router.get("/projects")
def list_projects(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """List all projects visible to the current user."""
    if user.role == "admin":
        projects = db.query(Project).order_by(Project.updated_at.desc()).all()
    else:
        projects = (
            db.query(Project)
            .filter(Project.owner_id == user.id)
            .order_by(Project.updated_at.desc())
            .all()
        )
    return {"projects": [_project_to_dict(p) for p in projects]}


@router.get("/projects/{project_id}")
def get_project(
    project_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Get a single project by ID."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.role != "admin" and project.owner_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized to view this project")
    return {"project": _project_to_dict(project)}


@router.put("/projects/{project_id}")
def update_project(
    project_id: str,
    body: ProjectUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Update a project."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.role != "admin" and project.owner_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized to update this project")

    if body.name is not None:
        project.name = body.name
    if body.description is not None:
        project.description = body.description
    if body.platform is not None:
        project.platform = body.platform

    db.commit()
    db.refresh(project)
    return {"project": _project_to_dict(project)}


@router.delete("/projects/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(
    project_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """Delete a project and all its runs."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.role != "admin" and project.owner_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this project")

    db.delete(project)
    db.commit()
    logger.info("Project deleted: %s by %s", project_id, user.email)


@router.get("/projects/{project_id}/runs")
def list_runs(
    project_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """List all pipeline runs for a project."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.role != "admin" and project.owner_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized")

    runs = (
        db.query(PipelineRun)
        .filter(PipelineRun.project_id == project_id)
        .order_by(PipelineRun.created_at.desc())
        .all()
    )
    return {"runs": [_run_to_dict(r) for r in runs]}

"""FastAPI application entry point for the Universal ETL Migration Platform."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.config_validator import validate_config
from app.db import init_db
from app.middleware.audit_middleware import AuditMiddleware
from app.middleware.rate_limiter import RateLimiterMiddleware
from app.routers import admin, analyze, assess, export, generate, parse, report, validate
from app.routers import api_keys as api_keys_router
from app.routers import audit as audit_router
from app.routers import auth as auth_router
from app.routers import comments, compare, dashboard, export_pdf, favorites
from app.routers import health as health_router
from app.routers import history, pipeline as pipeline_router
from app.routers import projects as projects_router
from app.routers import lineage as lineage_router
from app.routers import schedules, shares, tags, versions, webhooks, ws
from app.utils.logging import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: runs startup logic before yield, shutdown after."""
    setup_logging(settings.LOG_LEVEL)
    logger.info("ETL Migration Platform v6.0.0 started")

    # Initialize database tables
    init_db()
    logger.info("Database initialized")

    # Ensure admin user exists
    from app.db.engine import SessionLocal
    from app.db.models.user import User as UserModel
    from app.auth.password import hash_password

    with SessionLocal() as db:
        admin = db.query(UserModel).filter(UserModel.email == "admin").first()
        if not admin:
            db.add(UserModel(
                id="admin-001",
                email="admin",
                name="Admin",
                hashed_password=hash_password("1234@ABCd"),
                role="admin",
                is_active=True,
            ))
            db.commit()
            logger.info("Admin user created (admin / 1234@ABCd)")

    # Validate configuration at startup
    config_status = validate_config()
    if config_status["errors"]:
        logger.warning(
            "Startup config validation found %d issue(s) — see logs above",
            len(config_status["errors"]),
        )
    else:
        logger.info("Startup config validation passed")

    yield


app = FastAPI(
    title="Universal ETL Migration Platform",
    version="6.0.0",
    description="Converts any ETL tool definition to Databricks notebooks and workflows.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-API-Key"],
)

app.add_middleware(RateLimiterMiddleware, requests_per_minute=60)
app.add_middleware(AuditMiddleware)

app.include_router(parse.router, prefix="/api", tags=["parse"])
app.include_router(analyze.router, prefix="/api", tags=["analyze"])
app.include_router(assess.router, prefix="/api", tags=["assess"])
app.include_router(generate.router, prefix="/api", tags=["generate"])
app.include_router(validate.router, prefix="/api", tags=["validate"])
app.include_router(report.router, prefix="/api", tags=["report"])
app.include_router(admin.router, prefix="/api", tags=["admin"])
app.include_router(pipeline_router.router, prefix="/api", tags=["pipeline"])
app.include_router(health_router.router, prefix="/api", tags=["health"])
app.include_router(export.router, prefix="/api", tags=["export"])
app.include_router(versions.router, prefix="/api", tags=["versions"])
app.include_router(compare.router, prefix="/api", tags=["compare"])
app.include_router(comments.router, prefix="/api", tags=["comments"])
app.include_router(history.router, prefix="/api", tags=["history"])
app.include_router(export_pdf.router, prefix="/api", tags=["export-pdf"])
app.include_router(dashboard.router, prefix="/api", tags=["dashboard"])
app.include_router(schedules.router, prefix="/api", tags=["schedules"])
app.include_router(webhooks.router, prefix="/api", tags=["webhooks"])
app.include_router(tags.router, prefix="/api", tags=["tags"])
app.include_router(favorites.router, prefix="/api", tags=["favorites"])
app.include_router(shares.router, prefix="/api", tags=["shares"])
app.include_router(lineage_router.router, prefix="/api", tags=["lineage"])
app.include_router(ws.router, tags=["websocket"])

# Enterprise routers
app.include_router(auth_router.router, prefix="/api", tags=["auth"])
app.include_router(projects_router.router, prefix="/api", tags=["projects"])
app.include_router(audit_router.router, prefix="/api", tags=["audit"])
app.include_router(api_keys_router.router, prefix="/api", tags=["api-keys"])

# Serve frontend static files (production build)
_frontend_dist = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if _frontend_dist.is_dir():
    from starlette.responses import FileResponse

    # Serve built assets
    app.mount("/assets", StaticFiles(directory=str(_frontend_dist / "assets")), name="static")

    # SPA fallback — must be AFTER all API routes
    @app.get("/")
    @app.get("/login")
    @app.get("/register")
    @app.get("/dashboard")
    @app.get("/pipeline")
    @app.get("/admin")
    async def serve_spa() -> FileResponse:
        """Serve the SPA index.html for known frontend routes."""
        return FileResponse(str(_frontend_dist / "index.html"))

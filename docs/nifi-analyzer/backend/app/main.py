"""FastAPI application entry point for the Universal ETL Migration Platform."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import admin, analyze, assess, generate, parse, report, validate
from app.utils.logging import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: runs startup logic before yield, shutdown after."""
    setup_logging(settings.LOG_LEVEL)
    logger.info("ETL Migration Platform v5.0.0 started")
    yield


app = FastAPI(
    title="Universal ETL Migration Platform",
    version="5.0.0",
    description="Converts any ETL tool definition to Databricks notebooks and workflows.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(parse.router, prefix="/api", tags=["parse"])
app.include_router(analyze.router, prefix="/api", tags=["analyze"])
app.include_router(assess.router, prefix="/api", tags=["assess"])
app.include_router(generate.router, prefix="/api", tags=["generate"])
app.include_router(validate.router, prefix="/api", tags=["validate"])
app.include_router(report.router, prefix="/api", tags=["report"])
app.include_router(admin.router, prefix="/api", tags=["admin"])

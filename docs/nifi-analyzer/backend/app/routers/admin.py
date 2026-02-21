"""Admin router — health check, logs, platform list."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from app.utils.logging import get_recent_errors

router = APIRouter()
logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = [
    {"id": "nifi", "name": "Apache NiFi", "formats": [".xml", ".json"]},
    {"id": "ssis", "name": "SQL Server Integration Services", "formats": [".dtsx"]},
    {"id": "informatica", "name": "Informatica PowerCenter", "formats": [".xml"]},
    {"id": "talend", "name": "Talend", "formats": [".zip", ".item"]},
    {"id": "airflow", "name": "Apache Airflow", "formats": [".py"]},
    {"id": "dbt", "name": "dbt", "formats": [".json", ".sql", ".yml"]},
    {"id": "oracle_odi", "name": "Oracle ODI", "formats": [".xml"]},
    {"id": "snowflake", "name": "Snowflake SQL", "formats": [".sql"]},
    {"id": "azure_adf", "name": "Azure Data Factory", "formats": [".json"]},
    {"id": "aws_glue", "name": "AWS Glue", "formats": [".json"]},
    {"id": "matillion", "name": "Matillion", "formats": [".json"]},
    {"id": "pentaho", "name": "Pentaho", "formats": [".ktr", ".kjb"]},
    {"id": "datastage", "name": "IBM DataStage", "formats": [".dsx"]},
    {"id": "fivetran", "name": "Fivetran", "formats": [".json"]},
    {"id": "airbyte", "name": "Airbyte", "formats": [".json"]},
    {"id": "prefect", "name": "Prefect", "formats": [".py"]},
    {"id": "dagster", "name": "Dagster", "formats": [".py"]},
    {"id": "stitch", "name": "Stitch Data", "formats": [".json"]},
    {"id": "sql", "name": "Generic SQL", "formats": [".sql"]},
    {"id": "spark", "name": "Apache Spark", "formats": [".py"]},
]


@router.get("/admin/health")
async def health() -> dict:
    return {
        "status": "healthy",
        "version": "5.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "platforms_supported": len(SUPPORTED_PLATFORMS),
    }


@router.get("/admin/logs")
async def logs() -> dict:
    return {"errors": get_recent_errors()}


@router.get("/platforms")
async def platforms() -> list[dict]:
    return SUPPORTED_PLATFORMS

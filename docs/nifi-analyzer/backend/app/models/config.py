"""Databricks target configuration model."""

from typing import Literal

from pydantic import BaseModel


class DatabricksConfig(BaseModel):
    """User-specified Databricks workspace and deployment configuration."""

    catalog: str = "main"
    schema_name: str = "default"
    cloud_provider: Literal["aws", "azure", "gcp"] = "aws"
    compute_type: Literal["jobs_compute", "all_purpose", "serverless"] = "jobs_compute"
    runtime_version: str = "15.4"
    secret_scope: str = "etl-migration"
    volume_path: str = "/Volumes/main/default/landing"  # Override per environment (dev/staging/prod)
    use_unity_catalog: bool = True
    use_dlt: bool = False
    streaming_enabled: bool = True

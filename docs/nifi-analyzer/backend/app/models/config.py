"""Databricks target configuration model."""

from pydantic import BaseModel


class DatabricksConfig(BaseModel):
    """User-specified Databricks workspace and deployment configuration."""

    catalog: str = "main"
    schema_name: str = "default"
    cloud_provider: str = "aws"  # aws | azure | gcp
    compute_type: str = "jobs_compute"  # jobs_compute | all_purpose | serverless
    runtime_version: str = "15.4"
    secret_scope: str = "etl-migration"
    volume_path: str = "/Volumes/main/default/landing"
    use_unity_catalog: bool = True
    use_dlt: bool = False
    streaming_enabled: bool = True

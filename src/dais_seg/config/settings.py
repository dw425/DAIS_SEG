"""Central configuration for Synthetic Environment Generation framework."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SEGConfig:
    """Configuration for the SEG framework.

    Reads from environment variables with SEG_ prefix, or accepts explicit values.
    Designed to work both inside Databricks notebooks (via widgets/secrets)
    and in the Databricks App (via app.yaml env vars).
    """

    # Databricks connection
    databricks_host: str = ""
    databricks_token: str = ""

    # Unity Catalog
    catalog: str = "dais_seg"
    schema: str = "blueprints"
    blueprint_table: str = "source_blueprints"

    # SQL Warehouse
    warehouse_id: str = ""

    # Synthetic generation
    default_scale_factor: float = 1.0
    max_rows_per_table: int = 10_000_000
    preserve_referential_integrity: bool = True
    synthetic_workspace_prefix: str = "seg"

    # Medallion target
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # Validation thresholds
    schema_match_threshold: float = 1.0
    distribution_ks_threshold: float = 0.05
    null_ratio_tolerance: float = 0.01
    row_count_tolerance: float = 0.05

    # Confidence scoring
    confidence_green: float = 0.90
    confidence_amber: float = 0.70

    # Foundation Model
    model_serving_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"

    # Workspace manager
    max_parallel_workspaces: int = 5

    # Job execution (for Streamlit app → notebook submission)
    cluster_id: str = ""
    notebook_base_path: str = "/Workspace/Repos/DAIS_SEG/notebooks"

    @classmethod
    def from_env(cls) -> SEGConfig:
        """Load configuration from environment variables."""
        return cls(
            databricks_host=os.getenv("DATABRICKS_HOST", ""),
            databricks_token=os.getenv("DATABRICKS_TOKEN", ""),
            catalog=os.getenv("SEG_CATALOG", "dais_seg"),
            schema=os.getenv("SEG_SCHEMA", "blueprints"),
            warehouse_id=os.getenv("SEG_WAREHOUSE_ID", ""),
            model_serving_endpoint=os.getenv(
                "SEG_MODEL_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct"
            ),
            max_parallel_workspaces=int(os.getenv("SEG_MAX_PARALLEL_WORKSPACES", "5")),
            cluster_id=os.getenv("SEG_CLUSTER_ID", ""),
            notebook_base_path=os.getenv(
                "SEG_NOTEBOOK_PATH", "/Workspace/Repos/DAIS_SEG/notebooks"
            ),
        )

    @classmethod
    def from_databricks_widgets(cls, dbutils) -> SEGConfig:
        """Load configuration from Databricks notebook widgets."""
        def widget(name: str, default: str = "") -> str:
            try:
                return dbutils.widgets.get(name)
            except Exception:
                return default

        return cls(
            catalog=widget("catalog", "dais_seg"),
            schema=widget("schema", "blueprints"),
            warehouse_id=widget("warehouse_id"),
            model_serving_endpoint=widget(
                "model_endpoint", "databricks-meta-llama-3-1-70b-instruct"
            ),
        )


_config: Optional[SEGConfig] = None


def get_config() -> SEGConfig:
    """Get or create the singleton configuration."""
    global _config
    if _config is None:
        _config = SEGConfig.from_env()
    return _config


def set_config(config: SEGConfig) -> None:
    """Override the global configuration."""
    global _config
    _config = config

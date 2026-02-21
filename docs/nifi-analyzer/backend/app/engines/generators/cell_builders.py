"""Cell builder utilities for notebook generation."""

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, ParseResult


def build_imports_cell(assessment: AssessmentResult) -> str:
    """Build the imports cell from assessment packages.

    Uses ``import pyspark.sql.functions as F`` so that both ``F.col()``
    (processor_translators style) and bare ``col()`` (YAML template style)
    work without conflict.  A selective wildcard import is kept for
    convenience of YAML-generated code that references bare names.
    """
    imports: set[str] = set()
    # Qualified alias — required by processor_translators (F.col, F.when, …)
    imports.add("from pyspark.sql import functions as F")
    # Wildcard — keeps YAML-template code that uses bare col()/lit() working
    imports.add("from pyspark.sql.functions import col, lit, when, coalesce, regexp_replace, from_json, sha2, current_timestamp")
    imports.add("from pyspark.sql.types import *")

    for pkg in assessment.packages:
        if pkg.startswith("from ") or pkg.startswith("import "):
            imports.add(pkg)
        else:
            imports.add(f"import {pkg}")

    return "\n".join(sorted(imports))


def build_config_cell(config: DatabricksConfig) -> str:
    """Build the configuration cell."""
    return f'''# Configuration
catalog = "{config.catalog}"
schema = "{config.schema_name}"
secret_scope = "{config.secret_scope}"
volume_path = "{config.volume_path}"

# Set catalog context
spark.sql(f"USE CATALOG {{catalog}}")
spark.sql(f"USE SCHEMA {{schema}}")

print(f"[CONFIG] catalog={{catalog}}, schema={{schema}}, cloud={{'{config.cloud_provider}'}}")'''


def build_setup_cell(parse_result: ParseResult, config: DatabricksConfig) -> str:
    """Build the setup/initialization cell."""
    lines = [
        "# Setup: create schema and checkpoint locations if needed",
        'spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")',
    ]

    if config.use_unity_catalog:
        lines.append('spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")')
        lines.append('spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")')

    lines.append('\nprint("[SETUP] Schema and volumes initialized")')
    return "\n".join(lines)


def build_teardown_cell() -> str:
    """Build the teardown/cleanup cell."""
    return """# Teardown: cleanup temporary views and display summary
for table_name in spark.catalog.listTables():
    if table_name.name.startswith("tmp_"):
        spark.catalog.dropTempView(table_name.name)

print("[DONE] Pipeline execution complete")"""

"""DLT generator — produces Delta Live Tables pipeline from parse + assessment results.

Generates @dlt.table, @dlt.view, and @dlt.expect decorators based on NiFi flow topology.
"""

import logging
import re
from datetime import datetime, timezone

from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)


def generate_dlt_pipeline(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: DatabricksConfig | None = None,
) -> dict:
    """Generate a complete Delta Live Tables pipeline from parsed flow and assessment.

    Returns:
        dict with keys:
          - cells: list of {type, source, label}
          - pipelineConfig: dict with DLT pipeline settings
    """
    if config is None:
        config = DatabricksConfig()

    cells: list[dict] = []
    source_tables: list[str] = []
    transform_views: list[str] = []
    sink_tables: list[str] = []

    # Header cell
    cells.append({
        "type": "markdown",
        "source": (
            "# Delta Live Tables Pipeline\n\n"
            f"**Source Platform**: {parse_result.platform}\n"
            f"**Processors**: {len(parse_result.processors)}\n"
            f"**Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        ),
        "label": "dlt_header",
    })

    # Imports cell
    cells.append({
        "type": "code",
        "source": (
            "import dlt\n"
            "from pyspark.sql.functions import *\n"
            "from pyspark.sql.types import *\n"
        ),
        "label": "dlt_imports",
    })

    # Process each mapping
    for i, mapping in enumerate(assessment.mappings):
        safe_name = _safe_table_name(mapping.name)

        if mapping.role == "source":
            cell = _build_source_cell(mapping, safe_name, i, config)
            source_tables.append(safe_name)
        elif mapping.role == "sink":
            cell = _build_sink_cell(mapping, safe_name, i, source_tables, transform_views)
            sink_tables.append(safe_name)
        elif mapping.role == "route":
            cell = _build_route_cell(mapping, safe_name, i, source_tables, transform_views)
            transform_views.append(safe_name)
        else:
            cell = _build_transform_cell(mapping, safe_name, i, source_tables, transform_views)
            transform_views.append(safe_name)

        cells.append(cell)

    # DQ expectations for validators
    dq_cells = _build_dq_expectation_cells(parse_result, assessment)
    cells.extend(dq_cells)

    # Pipeline configuration
    pipeline_config = _build_pipeline_config(parse_result, config)

    logger.info("DLT pipeline: %d cells, sources=%d, transforms=%d, sinks=%d", len(cells), len(source_tables), len(transform_views), len(sink_tables))
    return {
        "cells": cells,
        "pipelineConfig": pipeline_config,
    }


def _build_source_cell(
    mapping: MappingEntry, safe_name: str, index: int, config: DatabricksConfig
) -> dict:
    """Build a @dlt.table cell for a source processor with streaming read."""
    source_format = _infer_source_format(mapping)
    source_path = _extract_source_path(mapping)

    code_lines = [
        f'@dlt.table(',
        f'    name="{safe_name}",',
        f'    comment="Source: {mapping.type} - {_escape(mapping.name)}"',
        f')',
        f'def {safe_name}():',
        f'    """Streaming read from {mapping.type}."""',
    ]

    if source_format == "kafka":
        code_lines.extend([
            f'    return (',
            f'        spark.readStream',
            f'            .format("kafka")',
            f'            .option("kafka.bootstrap.servers", spark.conf.get("source.kafka.servers", "UNSET_kafka_bootstrap_servers"))  # USER ACTION: set actual Kafka bootstrap servers',
            f'            .option("subscribe", "{source_path or "topic"}")',
            f'            .option("startingOffsets", "earliest")',
            f'            .load()',
            f'            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp")',
            f'    )',
        ])
    elif source_format in ("delta", "parquet", "csv", "json", "avro"):
        path = source_path or f"/Volumes/{config.catalog}/{config.schema_name}/landing/{safe_name}"
        code_lines.extend([
            f'    return (',
            f'        spark.readStream',
            f'            .format("cloudFiles")',
            f'            .option("cloudFiles.format", "{source_format}")',
            f'            .option("cloudFiles.inferColumnTypes", "true")',
            f'            .load("{path}")',
            f'    )',
        ])
    elif source_format == "jdbc":
        code_lines.extend([
            f'    return (',
            f'        spark.read',
            f'            .format("jdbc")',
            f'            .option("url", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_url"))',
            f'            .option("dbtable", "{source_path or safe_name}")',
            f'            .option("user", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_user"))',
            f'            .option("password", dbutils.secrets.get(scope="{config.secret_scope}", key="jdbc_password"))',
            f'            .load()',
            f'    )',
        ])
    else:
        code_lines.extend([
            f'    # USER ACTION: configure source for {mapping.type}',
            f'    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/{config.catalog}/{config.schema_name}/landing/{safe_name}")',
        ])

    return {
        "type": "code",
        "source": "\n".join(code_lines),
        "label": f"source_{index}_{safe_name}",
    }


def _build_transform_cell(
    mapping: MappingEntry,
    safe_name: str,
    index: int,
    source_tables: list[str],
    transform_views: list[str],
) -> dict:
    """Build a @dlt.view cell for a transform processor."""
    upstream = _pick_upstream(source_tables, transform_views)

    code_lines = [
        f'@dlt.view(',
        f'    name="{safe_name}",',
        f'    comment="Transform: {mapping.type} - {_escape(mapping.name)}"',
        f')',
        f'def {safe_name}():',
        f'    """Transformation step: {mapping.type}."""',
        f'    df = dlt.read_stream("{upstream}")',
    ]

    # Generate transformation logic based on processor type
    transform_code = _generate_transform_logic(mapping)
    code_lines.extend(transform_code)
    code_lines.append("    return df")

    return {
        "type": "code",
        "source": "\n".join(code_lines),
        "label": f"transform_{index}_{safe_name}",
    }


def _build_sink_cell(
    mapping: MappingEntry,
    safe_name: str,
    index: int,
    source_tables: list[str],
    transform_views: list[str],
) -> dict:
    """Build a @dlt.table cell for a sink processor (materialized Delta table)."""
    upstream = _pick_upstream(source_tables, transform_views)

    code_lines = [
        f'@dlt.table(',
        f'    name="{safe_name}",',
        f'    comment="Sink: {mapping.type} - {_escape(mapping.name)}",',
        f'    table_properties={{',
        f'        "quality": "gold",',
        f'        "pipelines.autoOptimize.zOrderCols": "id"',
        f'    }}',
        f')',
        f'def {safe_name}():',
        f'    """Materialized sink table from {mapping.type}."""',
        f'    return dlt.read_stream("{upstream}")',
    ]

    return {
        "type": "code",
        "source": "\n".join(code_lines),
        "label": f"sink_{index}_{safe_name}",
    }


def _build_route_cell(
    mapping: MappingEntry,
    safe_name: str,
    index: int,
    source_tables: list[str] | None = None,
    transform_views: list[str] | None = None,
) -> dict:
    """Build DLT cells for routing processors (RouteOnAttribute, etc.)."""
    conditions = _extract_route_conditions(mapping)
    upstream = _pick_upstream(source_tables or [], transform_views or [])

    code_lines = [
        f'@dlt.view(',
        f'    name="{safe_name}",',
        f'    comment="Route: {mapping.type} - {_escape(mapping.name)}"',
        f')',
        f'def {safe_name}():',
        f'    """Routing logic from {mapping.type}."""',
        f'    df = dlt.read_stream("{upstream}")',
    ]

    if conditions:
        for cond_name, cond_expr in conditions.items():
            spark_expr = _nel_to_spark_expr(cond_expr)
            code_lines.append(f'    # Route: {cond_name} -> {cond_expr}')
            code_lines.append(f'    df = df.filter({spark_expr})')
    else:
        code_lines.append('    # TODO: add routing conditions')

    code_lines.append('    return df')

    return {
        "type": "code",
        "source": "\n".join(code_lines),
        "label": f"route_{index}_{safe_name}",
    }


def _build_dq_expectation_cells(
    parse_result: ParseResult, assessment: AssessmentResult
) -> list[dict]:
    """Build DLT expectation cells from ValidateRecord and RouteOnAttribute processors."""
    cells: list[dict] = []

    for mapping in assessment.mappings:
        proc_type = mapping.type
        safe_name = _safe_table_name(mapping.name)

        if "ValidateRecord" in proc_type or "Validate" in proc_type:
            cells.append({
                "type": "code",
                "source": (
                    f'@dlt.expect_or_drop("valid_{safe_name}", "{safe_name}_valid IS NOT NULL")\n'
                ),
                "label": f"dq_validate_{safe_name}",
            })
        elif "RouteOnAttribute" in proc_type:
            conditions = _extract_route_conditions(mapping)
            for cond_name, cond_expr in conditions.items():
                spark_expr = _nel_to_spark_expr(cond_expr)
                cells.append({
                    "type": "code",
                    "source": (
                        f'@dlt.expect("route_{_safe_table_name(cond_name)}", {spark_expr})\n'
                    ),
                    "label": f"dq_route_{safe_name}_{_safe_table_name(cond_name)}",
                })

    return cells


def _build_pipeline_config(parse_result: ParseResult, config: DatabricksConfig) -> dict:
    """Build the DLT pipeline configuration."""
    safe_name = re.sub(
        r"[^a-zA-Z0-9_-]", "_",
        parse_result.metadata.get("source_file", "migration"),
    )

    has_streaming = any(
        p.type.startswith(("Consume", "Listen", "Get"))
        for p in parse_result.processors
    )

    return {
        "name": f"dlt_pipeline_{safe_name}",
        "catalog": config.catalog,
        "schema": config.schema_name,
        "target": f"{config.catalog}.{config.schema_name}",
        "continuous": has_streaming,
        "development": True,
        "configuration": {
            "pipelines.trigger.interval": "5 seconds" if has_streaming else "1 hour",
        },
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 4,
                    "mode": "ENHANCED",
                },
            }
        ],
        "libraries": [
            {"notebook": {"path": f"/Workspace/migrations/dlt_{safe_name}"}}
        ],
        "channel": "PREVIEW",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_table_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "t_" + s
    return s.lower()


def _escape(s: str) -> str:
    return s.replace('"', '\\"').replace("'", "\\'")


def _infer_source_format(mapping: MappingEntry) -> str:
    t = mapping.type.lower()
    if "kafka" in t:
        return "kafka"
    if "s3" in t or "hdfs" in t or "file" in t:
        notes = (mapping.notes + mapping.code).lower()
        if "parquet" in notes:
            return "parquet"
        if "avro" in notes:
            return "avro"
        if "csv" in notes:
            return "csv"
        return "json"
    if "database" in t or "sql" in t or "jdbc" in t or "execute" in t.lower():
        return "jdbc"
    if "delta" in t:
        return "delta"
    return "json"


def _extract_source_path(mapping: MappingEntry) -> str:
    code = mapping.code
    m = re.search(r'\.load\(["\']([^"\']+)["\']\)', code)
    if m:
        return m.group(1)
    m = re.search(r'path\s*=\s*["\']([^"\']+)["\']', code, re.IGNORECASE)
    if m:
        return m.group(1)
    return ""


def _pick_upstream(source_tables: list[str], transform_views: list[str]) -> str:
    if transform_views:
        return transform_views[-1]
    if source_tables:
        return source_tables[-1]
    return "UNSET_upstream_table"


def _extract_route_conditions(mapping: MappingEntry) -> dict[str, str]:
    conditions: dict[str, str] = {}
    code = mapping.code + "\n" + mapping.notes
    # Look for NiFi Expression Language patterns
    for m in re.finditer(
        r'["\'](\w+)["\']\s*[:=]\s*["\']([^"\']+)["\']', code
    ):
        key, val = m.group(1), m.group(2)
        if key.lower() not in ("name", "type", "role", "category"):
            conditions[key] = val
    return conditions


def _nel_to_spark_expr(nel_expr: str) -> str:
    """Convert NiFi Expression Language to Spark SQL expression (best effort)."""
    expr = nel_expr
    expr = re.sub(r'\$\{([^}:]+):isEmpty\(\)\}', r'col("\1").isNull()', expr)
    expr = re.sub(r'\$\{([^}:]+):equals\(["\']?([^"\')}]+)["\']?\)\}',
                  r'col("\1") == "\2"', expr)
    expr = re.sub(r'\$\{([^}]+)\}', r'col("\1")', expr)
    if not expr.startswith("col("):
        expr = f'"{expr}"'
    return expr


def _generate_transform_logic(mapping: MappingEntry) -> list[str]:
    """Generate transformation logic based on processor type."""
    lines: list[str] = []
    t = mapping.type
    safe = _safe_table_name(mapping.name)

    if "Jolt" in t or "JoltTransform" in t:
        # Delegate to processor_translators.translate_jolt_transform
        lines.append('    # JoltTransformJSON — structural JSON transformation')
        lines.append('    # Uses Jolt spec from processor properties')
        lines.append('    from app.engines.generators.processor_translators import translate_jolt_transform')
        lines.append(f'    # See translate_jolt_transform() for full implementation')
        lines.append('    df = df.select("*")  # USER ACTION: wire Jolt spec from processor properties')
    elif "Replace" in t:
        lines.append('    # ReplaceText — apply text replacement')
        lines.append('    df = df.withColumn("value", regexp_replace(col("value"), "old", "new"))')
    elif "Convert" in t:
        lines.append('    # ConvertRecord — cast columns to target schema types')
        lines.append(f'    # USER ACTION: define target_schema for {mapping.name}')
        lines.append('    # target_schema = StructType([StructField("col", StringType())])')
        lines.append('    # df = df.select([col(c.name).cast(c.dataType) for c in target_schema])')
        lines.append('    df = df.select("*")')
    elif "EvaluateJsonPath" in t:
        lines.append('    # EvaluateJsonPath — extract JSON fields')
        lines.append('    df = df.withColumn("parsed", from_json(col("value"), schema))')
    elif "Split" in t:
        lines.append('    # SplitRecord / SplitContent — explode array column into rows')
        lines.append(f'    # USER ACTION: replace "items" with the actual array column name')
        lines.append('    df = df.withColumn("item", explode(col("items"))).drop("items")')
    elif "Merge" in t:
        lines.append('    # MergeContent — aggregate rows back into a collected array')
        lines.append(f'    # USER ACTION: set the correct group key and value columns')
        lines.append('    df = df.groupBy("id").agg(collect_list("value").alias("merged_values"))')
    elif "UpdateAttribute" in t or "UpdateRecord" in t:
        lines.append('    # UpdateAttribute/Record — add or modify columns')
        lines.append(f'    # USER ACTION: add withColumn calls for each attribute update')
        lines.append('    # Example: df = df.withColumn("new_col", lit("value"))')
        lines.append('    df = df.select("*")')
    elif "Lookup" in t:
        lines.append('    # LookupRecord — enrich with reference data via join')
        lines.append(f'    # USER ACTION: replace "ref_table" and join keys')
        lines.append('    # ref_df = dlt.read("ref_table")')
        lines.append('    # df = df.join(ref_df, df["lookup_key"] == ref_df["ref_key"], "left")')
        lines.append('    df = df.select("*")')
    elif "Encrypt" in t or "Hash" in t:
        lines.append('    df = df.withColumn("hashed", sha2(col("value"), 256))')
    elif "Compress" in t:
        lines.append('    # Compression handled by sink format settings')
        lines.append('    df = df.select("*")')
    elif "ExecuteScript" in t or "ExecuteGroovyScript" in t or "ExecuteStreamCommand" in t:
        lines.append(f'    # {t} — custom script execution')
        lines.append('    # USER ACTION: Port NiFi script logic to PySpark UDF')
        lines.append('    from pyspark.sql.types import StringType')
        lines.append('    @udf(returnType=StringType())')
        lines.append(f'    def udf_{safe}(value):')
        lines.append(f'        """Ported from NiFi {t}."""')
        lines.append(f'        # USER ACTION: Port script logic to Python')
        lines.append('        return value')
        lines.append(f'    df = df.withColumn("result", udf_{safe}(col("value")))')
    elif "RouteOnAttribute" in t:
        lines.append('    # RouteOnAttribute — filter rows by condition')
        conditions = _extract_route_conditions(mapping)
        if conditions:
            first_cond = next(iter(conditions.values()))
            spark_expr = _nel_to_spark_expr(first_cond)
            lines.append(f'    df = df.filter({spark_expr})')
        else:
            lines.append('    # USER ACTION: add filter condition')
            lines.append('    df = df.select("*")')
    elif mapping.code and mapping.code.strip() and not mapping.code.startswith("# UNMAPPED"):
        lines.append(f'    # Custom logic from {mapping.type}')
        for cl in mapping.code.strip().split("\n"):
            lines.append(f'    # {cl}')
    else:
        lines.append(f'    # USER ACTION: implement {mapping.type} transformation')
        lines.append('    df = df.select("*")')

    return lines

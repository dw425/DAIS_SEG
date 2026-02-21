"""Mapper dispatcher — routes to platform-specific mapper."""

import logging

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)

# All registered platform mappers
_MAPPER_MAP = {
    "ssis": "app.engines.mappers.ssis_mapper",
    "informatica": "app.engines.mappers.informatica_mapper",
    "talend": "app.engines.mappers.talend_mapper",
    "airflow": "app.engines.mappers.airflow_mapper",
    "dbt": "app.engines.mappers.dbt_mapper",
    "azure_adf": "app.engines.mappers.azure_adf_mapper",
    "aws_glue": "app.engines.mappers.aws_glue_mapper",
    "pentaho": "app.engines.mappers.pentaho_mapper",
    "snowflake": "app.engines.mappers.snowflake_mapper",
    "datastage": "app.engines.mappers.datastage_mapper",
    "apache_beam": "app.engines.mappers.beam_mapper",
    "matillion": "app.engines.mappers.matillion_mapper",
    "oracle": "app.engines.mappers.oracle_mapper",
    "spark": "app.engines.mappers.spark_mapper",
    "sql": "app.engines.mappers.sql_mapper",
    "prefect": "app.engines.mappers.prefect_mapper",
    "luigi": "app.engines.mappers.luigi_mapper",
    "airbyte": "app.engines.mappers.airbyte_mapper",
    "fivetran": "app.engines.mappers.fivetran_mapper",
    "dagster": "app.engines.mappers.dagster_mapper",
    "stitch": "app.engines.mappers.stitch_mapper",
}


def map_to_databricks(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    """Map processors to Databricks equivalents based on platform."""
    platform = parse_result.platform

    if platform == "nifi":
        from app.engines.mappers.nifi_mapper import map_nifi

        return map_nifi(parse_result, analysis_result)

    # Try platform-specific mapper
    fallback_reason = ""
    mapper_module = _MAPPER_MAP.get(platform)
    if mapper_module:
        try:
            import importlib

            mod = importlib.import_module(mapper_module)
        except ImportError:
            fallback_reason = (
                f"Specialized mapper '{mapper_module}' not installed; "
                f"falling back to generic YAML mapping for platform '{platform}'"
            )
            logger.warning("Mapper module %s not found, using generic fallback", mapper_module)
        else:
            try:
                map_fn = getattr(mod, "map_platform")
            except AttributeError:
                fallback_reason = (
                    f"Mapper module '{mapper_module}' loaded but missing 'map_platform' "
                    f"function — using generic fallback"
                )
                logger.error(
                    "Mapper module %s loaded but missing 'map_platform' function — "
                    "this is likely a bug in the mapper implementation",
                    mapper_module,
                )
            else:
                return map_fn(parse_result, analysis_result)

    # Fallback: use base_mapper generic
    from app.engines.mappers.base_mapper import map_platform_generic

    result = map_platform_generic(platform, parse_result, analysis_result)

    # Surface the fallback reason so callers know the specialized mapper was skipped
    if fallback_reason:
        from app.models.pipeline import MappingEntry

        result.mappings.insert(
            0,
            MappingEntry(
                name="__dispatcher_warning__",
                type="dispatcher",
                role="utility",
                category="Warning",
                mapped=False,
                confidence=0.0,
                code="",
                notes=fallback_reason,
            ),
        )

    return result

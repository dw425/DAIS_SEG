"""Mapper dispatcher — routes to platform-specific mapper."""

import logging

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)


def map_to_databricks(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    """Map processors to Databricks equivalents based on platform."""
    platform = parse_result.platform

    if platform == "nifi":
        from app.engines.mappers.nifi_mapper import map_nifi

        return map_nifi(parse_result, analysis_result)

    # For other platforms, use the generic mapper
    return _generic_map(parse_result, analysis_result)


def _generic_map(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    """Generic mapper for platforms without specialized mapping."""
    platform = parse_result.platform
    mappings: list[MappingEntry] = []
    packages: set[str] = set()
    unmapped = 0

    # Load platform-specific mapper if available
    mapper_map = {
        "ssis": "app.engines.mappers.ssis_mapper",
        "informatica": "app.engines.mappers.informatica_mapper",
        "talend": "app.engines.mappers.talend_mapper",
        "airflow": "app.engines.mappers.airflow_mapper",
        "dbt": "app.engines.mappers.dbt_mapper",
        "azure_adf": "app.engines.mappers.azure_adf_mapper",
        "aws_glue": "app.engines.mappers.aws_glue_mapper",
        "pentaho": "app.engines.mappers.pentaho_mapper",
        "snowflake": "app.engines.mappers.snowflake_mapper",
    }

    mapper_module = mapper_map.get(platform)
    if mapper_module:
        try:
            import importlib

            mod = importlib.import_module(mapper_module)
            return mod.map_platform(parse_result, analysis_result)
        except (ImportError, AttributeError):
            logger.warning("Mapper for %s not fully implemented, using fallback", platform)

    # Fallback: basic mapping
    for p in parse_result.processors:
        role = _infer_role(p.type)
        mappings.append(
            MappingEntry(
                name=p.name,
                type=p.type,
                role=role,
                mapped=True,
                confidence=0.5,
                code=f"# {platform} {p.type}: {p.name} -- manual migration required",
                notes=f"Platform '{platform}' processor; review for Databricks equivalent",
            )
        )

    return AssessmentResult(
        mappings=mappings,
        packages=sorted(packages),
        unmapped_count=unmapped,
    )


def _infer_role(proc_type: str) -> str:
    """Infer a basic role from processor type name."""
    t = proc_type.lower()
    if any(w in t for w in ("source", "input", "read", "get", "fetch", "consume", "listen", "query")):
        return "source"
    if any(w in t for w in ("sink", "output", "write", "put", "publish", "send", "insert")):
        return "sink"
    if any(w in t for w in ("route", "filter", "distribute")):
        return "route"
    if any(w in t for w in ("transform", "convert", "replace", "map", "merge", "split")):
        return "transform"
    return "utility"

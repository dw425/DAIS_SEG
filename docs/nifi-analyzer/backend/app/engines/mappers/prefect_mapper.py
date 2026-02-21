"""Prefect to Databricks mapper."""

from app.engines.mappers.base_mapper import map_platform_generic
from app.models.pipeline import AnalysisResult, AssessmentResult, ParseResult


def map_platform(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    return map_platform_generic("prefect", parse_result, analysis_result)

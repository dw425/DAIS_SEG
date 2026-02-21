"""SSIS to Databricks mapper."""

from pathlib import Path

import yaml

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

_YAML_PATH = Path(__file__).parent.parent.parent / "constants" / "processor_maps" / "ssis_databricks.yaml"
_cache: dict | None = None


def _load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    if _YAML_PATH.exists():
        with open(_YAML_PATH) as f:
            _cache = yaml.safe_load(f) or {}
    else:
        _cache = {}
    return _cache


def map_platform(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    table = _load()
    mappings: list[MappingEntry] = []
    packages: set[str] = set()
    unmapped = 0

    for p in parse_result.processors:
        entry = table.get(p.type)
        if entry:
            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=entry.get("role", "transform"),
                    mapped=True,
                    confidence=entry.get("confidence", 0.7),
                    code=entry.get("template", ""),
                    notes=entry.get("notes", ""),
                )
            )
        else:
            unmapped += 1
            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role="transform",
                    mapped=False,
                    confidence=0.0,
                    code=f"# UNMAPPED: SSIS {p.type}",
                    notes="Manual migration required",
                )
            )

    return AssessmentResult(mappings=mappings, packages=sorted(packages), unmapped_count=unmapped)

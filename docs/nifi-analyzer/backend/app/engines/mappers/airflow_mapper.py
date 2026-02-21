"""Airflow to Databricks mapper."""

from pathlib import Path

import yaml

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

_YAML_PATH = Path(__file__).parent.parent.parent / "constants" / "processor_maps" / "airflow_databricks.yaml"
_cache: dict | None = None


def _load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    _cache = yaml.safe_load(open(_YAML_PATH)) if _YAML_PATH.exists() else {}
    return _cache


def map_platform(pr: ParseResult, ar: AnalysisResult) -> AssessmentResult:
    t = _load()
    m = []
    u = 0
    for p in pr.processors:
        e = t.get(p.type)
        if e:
            m.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=e.get("role", "transform"),
                    mapped=True,
                    confidence=e.get("confidence", 0.7),
                    code=e.get("template", ""),
                    notes=e.get("notes", ""),
                )
            )
        else:
            u += 1
            m.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role="transform",
                    mapped=False,
                    confidence=0.0,
                    code=f"# UNMAPPED: Airflow {p.type}",
                    notes="Manual migration required",
                )
            )
    return AssessmentResult(mappings=m, packages=[], unmapped_count=u)

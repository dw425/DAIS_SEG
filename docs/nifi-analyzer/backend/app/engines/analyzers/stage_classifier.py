"""Classify processors into pipeline stages.

Ported from stage-classifier.js.
"""

import re

from app.models.processor import Processor

_SOURCE_RE = re.compile(r"^(Get|List|Consume|Listen|Fetch|Query|Scan|Select|Generate)", re.I)
_SINK_RE = re.compile(r"^(Put|Publish|Send|Post|Insert|Write)", re.I)
_ROUTE_RE = re.compile(r"^(Route|Distribute|Detect)", re.I)
_TRANSFORM_RE = re.compile(
    r"(Convert|Replace|Update|Jolt|Extract|Split|Merge|Compress|Encrypt|Hash|Transform|Attribute)",
    re.I,
)
_UTILITY_RE = re.compile(r"(Log|Debug|Count|Control|Wait|Notify|ControlRate)", re.I)

STAGE_DEFS = [
    {"id": "ingestion", "label": "INGESTION", "color": "#3B82F6"},
    {"id": "extraction", "label": "EXTRACTION", "color": "#8B5CF6"},
    {"id": "routing", "label": "ROUTING", "color": "#EAB308"},
    {"id": "enrichment", "label": "ENRICHMENT", "color": "#06B6D4"},
    {"id": "transformation", "label": "TRANSFORMATION", "color": "#A855F7"},
    {"id": "loading", "label": "LOADING", "color": "#21C354"},
    {"id": "monitoring", "label": "MONITORING", "color": "#808495"},
    {"id": "processing", "label": "PROCESSING", "color": "#6366F1"},
]


def _classify_type(proc_type: str) -> str:
    """Classify a processor type into a stage id."""
    if _SOURCE_RE.match(proc_type):
        return "ingestion"
    if proc_type in ("EvaluateJsonPath", "EvaluateXPath", "ExtractText", "ExtractGrok"):
        return "extraction"
    if _ROUTE_RE.match(proc_type):
        return "routing"
    if proc_type in ("InvokeHTTP", "LookupRecord", "HandleHttpRequest"):
        return "enrichment"
    if _SINK_RE.match(proc_type):
        return "loading"
    if _UTILITY_RE.search(proc_type):
        return "monitoring"
    if _TRANSFORM_RE.search(proc_type):
        return "transformation"
    return "processing"


def classify_stages(processors: list[Processor]) -> list[dict]:
    """Classify processors into pipeline stages.

    Returns list of stage dicts with 'id', 'label', 'color', 'processors'.
    """
    buckets: dict[str, list[str]] = {}

    for p in processors:
        stage_id = _classify_type(p.type)
        buckets.setdefault(stage_id, []).append(p.name)

    result: list[dict] = []
    for stage_def in STAGE_DEFS:
        procs = buckets.get(stage_def["id"])
        if procs:
            result.append(
                {
                    "id": stage_def["id"],
                    "label": stage_def["label"],
                    "color": stage_def["color"],
                    "processors": procs,
                }
            )

    return result

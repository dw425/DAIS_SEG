"""NiFi processor mapper — maps 250+ NiFi processor types to Databricks equivalents.

Loads mapping from YAML config file (nifi_databricks.yaml).
"""

import logging
from pathlib import Path

import yaml

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)

_YAML_PATH = Path(__file__).parent.parent.parent / "constants" / "processor_maps" / "nifi_databricks.yaml"

# Lazy-loaded mapping cache
_mapping_cache: dict | None = None


def _load_mapping() -> dict:
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache
    if _YAML_PATH.exists():
        with open(_YAML_PATH) as f:
            raw = yaml.safe_load(f) or {}
        # Handle both formats:
        #   1. Direct dict keyed by type: {GetFile: {...}, ...}
        #   2. List under 'mappings' key: {mappings: [{type: "GetFile", ...}, ...]}
        if "mappings" in raw and isinstance(raw["mappings"], list):
            _mapping_cache = {}
            for entry in raw["mappings"]:
                if isinstance(entry, dict) and "type" in entry:
                    _mapping_cache[entry["type"]] = entry
        elif isinstance(raw, dict):
            _mapping_cache = raw
        else:
            _mapping_cache = {}
    else:
        logger.warning("NiFi mapping YAML not found at %s", _YAML_PATH)
        _mapping_cache = {}
    return _mapping_cache


def _classify_role(proc_type: str) -> str:
    """Classify NiFi processor into a role."""
    t = proc_type
    _source_prefixes = ("Get", "List", "Consume", "Listen", "Fetch", "Query", "Scan", "Select", "Generate")
    if any(t.startswith(p) for p in _source_prefixes):
        return "source"
    if any(t.startswith(p) for p in ("Put", "Publish", "Send", "Post", "Insert", "Write", "Delete")):
        return "sink"
    if any(t.startswith(p) for p in ("Route", "Distribute", "Detect")):
        return "route"
    if any(w in t for w in ("Convert", "Replace", "Update", "Jolt", "Extract", "Split", "Merge", "Transform")):
        return "transform"
    if any(w in t for w in ("Log", "Debug", "Count", "Wait", "Notify", "ControlRate")):
        return "utility"
    if any(w in t for w in ("Execute", "Invoke")):
        return "process"
    return "transform"


def map_nifi(parse_result: ParseResult, analysis_result: AnalysisResult) -> AssessmentResult:
    """Map NiFi processors to Databricks equivalents."""
    mapping_table = _load_mapping()
    mappings: list[MappingEntry] = []
    packages: set[str] = set()
    unmapped = 0

    for p in parse_result.processors:
        entry = mapping_table.get(p.type)
        role = _classify_role(p.type)

        if entry:
            code = entry.get("template", "")
            # Substitute basic placeholders
            code = code.replace("{v}", _safe_var(p.name))
            code = code.replace("{in}", _safe_var(p.name))

            for pkg in entry.get("imports", []):
                if pkg:
                    packages.add(pkg)

            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=role,
                    mapped=True,
                    confidence=entry.get("confidence", 0.9),
                    code=code,
                    notes=entry.get("notes", ""),
                )
            )
        else:
            unmapped += 1
            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=role,
                    mapped=False,
                    confidence=0.0,
                    code=f"# UNMAPPED: NiFi {p.type} ({p.name}) -- manual migration required",
                    notes=f"No Databricks mapping found for {p.type}",
                )
            )

    return AssessmentResult(
        mappings=mappings,
        packages=sorted(packages),
        unmapped_count=unmapped,
    )


def _safe_var(name: str) -> str:
    """Convert a processor name to a safe Python variable name."""
    import re

    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()

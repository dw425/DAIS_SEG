"""Compatibility matrix — per-processor compatibility assessment.

Produces a structured matrix showing:
- Source processor type
- Target Databricks equivalent
- Compatibility level: native, partial, manual, unsupported
- Gap details and manual intervention steps

Inspired by Databricks Labs' Lakebridge assessment format.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Compatibility levels
NATIVE = "native"       # Direct equivalent, fully automated
PARTIAL = "partial"     # Equivalent exists but needs configuration
MANUAL = "manual"       # Requires custom code or manual migration
UNSUPPORTED = "unsupported"  # No equivalent, must redesign


def _load_processor_map() -> dict[str, dict]:
    """Load the NiFi-to-Databricks processor mapping YAML."""
    yaml_path = Path(__file__).resolve().parent.parent.parent / "constants" / "processor_maps" / "nifi_databricks.yaml"
    if not yaml_path.exists():
        return {}
    try:
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        return {m["type"]: m for m in data.get("mappings", [])}
    except Exception:
        return {}


def compute_compatibility_matrix(
    parsed: ParseResult,
    assessment: AssessmentResult,
) -> dict:
    """Build a compatibility matrix for all processor types in the flow.

    Returns:
    {
        "entries": [...],
        "summary": {native: N, partial: N, manual: N, unsupported: N},
        "byCategory": {category: {native: N, ...}, ...},
    }
    """
    proc_map = _load_processor_map()
    entries: list[dict] = []
    seen_types: set[str] = set()

    # Build assessment lookup
    assessment_by_name: dict[str, dict] = {}
    for m in assessment.mappings:
        assessment_by_name[m.name] = {
            "type": m.type,
            "role": m.role,
            "category": m.category,
            "mapped": m.mapped,
            "confidence": m.confidence,
            "notes": m.notes,
        }

    for proc in parsed.processors:
        if proc.type in seen_types:
            continue
        seen_types.add(proc.type)

        mapping = assessment_by_name.get(proc.name, {})
        yaml_entry = proc_map.get(proc.type, {})

        # Determine compatibility level
        confidence = mapping.get("confidence", 0)
        is_mapped = mapping.get("mapped", False)

        if is_mapped and confidence >= 0.9:
            level = NATIVE
            gap = ""
            manual_steps = []
        elif is_mapped and confidence >= 0.7:
            level = PARTIAL
            gap = mapping.get("notes", "Some configuration adjustments needed")
            manual_steps = ["Review generated code for correctness", "Adjust configuration parameters"]
        elif is_mapped and confidence > 0:
            level = MANUAL
            gap = mapping.get("notes", "Significant manual review required")
            manual_steps = [
                "Review generated code structure",
                "Implement custom transformation logic",
                "Add error handling",
                "Write unit tests",
            ]
        else:
            level = UNSUPPORTED
            gap = f"No Databricks equivalent found for {proc.type}"
            manual_steps = [
                f"Design custom PySpark logic to replace {proc.type}",
                "Implement and test transformation",
                "Validate output matches original behavior",
            ]

        target_equiv = yaml_entry.get("category", mapping.get("category", ""))
        if not target_equiv and is_mapped:
            target_equiv = "Spark SQL / PySpark"

        entries.append({
            "processorType": proc.type,
            "sourceCategory": mapping.get("role", yaml_entry.get("role", "unknown")),
            "targetEquivalent": target_equiv,
            "compatibilityLevel": level,
            "gapDetails": gap,
            "manualSteps": manual_steps,
        })

    # Compute summary
    summary = {
        NATIVE: sum(1 for e in entries if e["compatibilityLevel"] == NATIVE),
        PARTIAL: sum(1 for e in entries if e["compatibilityLevel"] == PARTIAL),
        MANUAL: sum(1 for e in entries if e["compatibilityLevel"] == MANUAL),
        UNSUPPORTED: sum(1 for e in entries if e["compatibilityLevel"] == UNSUPPORTED),
    }

    # Group by category
    by_category: dict[str, dict[str, int]] = {}
    for e in entries:
        cat = e["sourceCategory"] or "other"
        if cat not in by_category:
            by_category[cat] = {NATIVE: 0, PARTIAL: 0, MANUAL: 0, UNSUPPORTED: 0}
        by_category[cat][e["compatibilityLevel"]] += 1

    return {
        "entries": entries,
        "summary": summary,
        "byCategory": by_category,
    }

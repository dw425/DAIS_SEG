"""Confidence calibrator — adjust raw YAML confidence based on flow context.

Refines mapping confidence scores by examining processor properties,
NEL expression complexity, external system dependencies, connection
fan-out, and cycle membership.
"""

from __future__ import annotations

import logging
import re

from app.models.pipeline import AnalysisResult, MappingEntry
from app.models.processor import Processor

logger = logging.getLogger(__name__)

_NEL_EXPR_RE = re.compile(r"\$\{[^}]+\}")
_TEMPLATE_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


def calibrate_confidence(
    mapping: MappingEntry,
    processor: Processor,
    analysis: AnalysisResult,
) -> tuple[float, list[str]]:
    """Adjust raw YAML confidence based on flow-context signals.

    Returns (adjusted_confidence, list_of_adjustment_reasons).
    """
    confidence = mapping.confidence
    reasons: list[str] = []

    # --- 1. Property completeness ---
    code = mapping.code or ""
    placeholders = set(_TEMPLATE_PLACEHOLDER_RE.findall(code))
    proc_prop_keys = {k.lower().replace(" ", "_") for k in processor.properties}

    if placeholders:
        matched = placeholders & proc_prop_keys
        if matched:
            confidence += 0.05
            reasons.append(
                f"+0.05 property completeness: {len(matched)}/{len(placeholders)} "
                f"placeholders matched ({', '.join(sorted(matched)[:3])})"
            )
        missing_required = placeholders - proc_prop_keys
        if missing_required:
            confidence -= 0.10
            reasons.append(
                f"-0.10 missing required properties: "
                f"{', '.join(sorted(missing_required)[:3])}"
            )

    # --- 2. NEL expression complexity ---
    all_values = " ".join(str(v) for v in processor.properties.values() if v)
    nel_expressions = _NEL_EXPR_RE.findall(all_values)
    nel_count = len(nel_expressions)
    if nel_count > 3:
        confidence -= 0.05
        reasons.append(
            f"-0.05 NEL expression complexity: {nel_count} expressions found"
        )

    # --- 3. External system dependency ---
    ext_system_keys = {s.get("key", "") for s in analysis.external_systems}
    proc_type_lower = processor.type.lower()
    for key in ext_system_keys:
        if key and key in proc_type_lower:
            confidence -= 0.05
            reasons.append(
                f"-0.05 external system dependency: processor references '{key}'"
            )
            break

    # --- 4. Connection fan-out ---
    fan_out = analysis.dependency_graph.get("fan_out", {})
    proc_fan_out = fan_out.get(processor.name, 0)
    if proc_fan_out > 5:
        confidence -= 0.03
        reasons.append(
            f"-0.03 high connection fan-out: {proc_fan_out} outgoing connections"
        )

    # --- 5. Cycle membership ---
    for cycle in analysis.cycles:
        if processor.name in cycle:
            confidence -= 0.10
            reasons.append(
                f"-0.10 cycle membership: processor is in cycle "
                f"with {', '.join(c for c in cycle if c != processor.name)}"
            )
            break

    # Clamp to [0.0, 1.0]
    adjusted = max(0.0, min(1.0, round(confidence, 2)))
    if reasons:
        logger.debug("Calibrated %s: %.2f -> %.2f (%d adjustments)", mapping.name, mapping.confidence, adjusted, len(reasons))
    return adjusted, reasons


def calibrate_all(
    mappings: list[MappingEntry],
    processors: list[Processor],
    analysis: AnalysisResult,
) -> list[MappingEntry]:
    """Apply calibration to all mappings, returning updated copies."""
    logger.info("Calibrating confidence for %d mappings", len(mappings))
    proc_by_name: dict[str, Processor] = {p.name: p for p in processors}
    calibrated: list[MappingEntry] = []

    for m in mappings:
        proc = proc_by_name.get(m.name)
        if proc is None:
            calibrated.append(m)
            continue
        adj_conf, reasons = calibrate_confidence(m, proc, analysis)
        updated = m.model_copy(
            update={
                "confidence": adj_conf,
                "notes": (m.notes + " | " if m.notes else "")
                + "; ".join(reasons) if reasons else m.notes,
            }
        )
        calibrated.append(updated)

    return calibrated

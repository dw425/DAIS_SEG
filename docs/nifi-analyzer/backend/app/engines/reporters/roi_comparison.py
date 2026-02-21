"""ROI comparison — lift-and-shift vs refactor migration scenarios.

Computes cost, timeline, and risk for two migration approaches and provides
a recommendation based on the analysis.
"""

import logging
import re

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Hours per processor by role/category
_ROLE_EFFORT: dict[str, float] = {
    "source": 2.0,
    "ingestion": 2.0,
    "transform": 4.0,
    "transformation": 4.0,
    "process": 6.0,
    "processing": 6.0,
    "route": 3.0,
    "routing": 3.0,
    "sink": 2.0,
    "loading": 2.0,
    "utility": 1.0,
    "monitoring": 1.0,
    "enrichment": 4.0,
    "extraction": 3.0,
}

_DEFAULT_RATE = 150  # USD per hour


def _classify_role(proc_type: str) -> str:
    """Quick role classification for effort weighting."""
    t = proc_type.lower()
    if re.match(r"(get|list|consume|listen|fetch|query|scan|select|generate)", t):
        return "source"
    if re.match(r"(put|publish|send|post|insert|write)", t):
        return "sink"
    if re.match(r"(route|distribute|detect)", t):
        return "route"
    if re.search(r"(convert|replace|update|jolt|extract|split|merge|compress|encrypt|hash|transform)", t):
        return "transform"
    if re.search(r"(log|debug|count|control|wait|notify)", t):
        return "utility"
    return "process"


def compute_roi_comparison(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    rate: float = _DEFAULT_RATE,
) -> dict:
    """Compute ROI comparison between lift-and-shift and refactor approaches.

    Returns dict with liftAndShift, refactor, recommendation, and breakEvenMonths.
    """
    mappings = assessment.mappings
    total = len(mappings)
    if total == 0:
        return _empty_result()

    auto_mapped = [m for m in mappings if m.mapped and m.confidence >= 0.9]
    semi_mapped = [m for m in mappings if m.mapped and m.confidence < 0.9]
    unmapped = [m for m in mappings if not m.mapped]

    # --- Lift-and-shift scenario ---
    # Auto-mapped: base role effort * 0.25 (mostly automated)
    auto_hours = 0.0
    for m in auto_mapped:
        role = m.role.lower() if m.role else _classify_role(m.type)
        base = _ROLE_EFFORT.get(role, 4.0)
        auto_hours += base * 0.25

    # Semi-mapped: base role effort * 0.75
    semi_hours = 0.0
    for m in semi_mapped:
        role = m.role.lower() if m.role else _classify_role(m.type)
        base = _ROLE_EFFORT.get(role, 4.0)
        semi_hours += base * 0.75

    # Unmapped: base role effort * 2.0 (fully manual + research)
    manual_hours = 0.0
    for m in unmapped:
        role = _classify_role(m.type)
        base = _ROLE_EFFORT.get(role, 4.0)
        manual_hours += base * 2.0

    ls_total_hours = auto_hours + semi_hours + manual_hours
    ls_cost = round(ls_total_hours * rate)
    ls_weeks = max(1, round(ls_total_hours / 40))  # 40h work week
    ls_risk = round(len(unmapped) / max(total, 1), 3)

    ls_description = (
        f"Direct migration: {len(auto_mapped)} auto-mapped ({auto_hours:.0f}h), "
        f"{len(semi_mapped)} semi-automated ({semi_hours:.0f}h), "
        f"{len(unmapped)} manual ({manual_hours:.0f}h). "
        f"Total: {ls_total_hours:.0f} hours."
    )

    # --- Refactor scenario ---
    # All processors rebuilt from scratch with Databricks-native patterns
    refactor_hours = 0.0
    for m in mappings:
        role = m.role.lower() if m.role else _classify_role(m.type)
        base = _ROLE_EFFORT.get(role, 4.0)
        refactor_hours += base  # Full base effort for each processor

    rf_cost = round(refactor_hours * rate)
    rf_weeks = max(1, round(total / 20))  # ~20 processors per week
    # Refactor risk is not zero — it scales with unmapped processors (migration
    # unknowns), total flow size (coordination overhead), and external system
    # integrations.  Formula: base 0.05 + unmapped fraction * 0.15 + size penalty,
    # capped at 0.8.
    unmapped_count = len(unmapped)
    rf_risk = 0.05 + (unmapped_count / max(total, 1)) * 0.15 + min(total / 500, 0.2)
    rf_risk = round(min(rf_risk, 0.8), 3)

    rf_description = (
        f"Full redesign for Databricks-native patterns. "
        f"{total} processors rebuilt ({refactor_hours:.0f}h). "
        f"Lower risk but higher upfront investment."
    )

    logger.info("ROI comparison: L&S=$%d (%d wks), Refactor=$%d (%d wks)", ls_cost, ls_weeks, rf_cost, rf_weeks)
    # --- Recommendation ---
    if ls_cost < rf_cost * 0.7:
        recommendation = (
            "Lift-and-shift recommended: significantly lower cost with acceptable risk. "
            f"Saves ${rf_cost - ls_cost:,} vs refactor."
        )
    elif ls_risk > 0.3:
        recommendation = (
            "Refactor recommended: high unmapped percentage ({:.0%}) makes lift-and-shift risky. "
            "Rebuilding ensures long-term maintainability.".format(ls_risk)
        )
    elif rf_cost < ls_cost * 1.3:
        recommendation = (
            "Refactor recommended: similar cost to lift-and-shift but with lower risk "
            "and better long-term architecture."
        )
    else:
        recommendation = (
            "Hybrid approach recommended: lift-and-shift auto-mapped processors, "
            "refactor complex/unmapped ones for optimal cost-risk balance."
        )

    # Break-even: months until operational savings offset migration cost difference
    # Assume refactor saves 20% monthly ops vs lift-and-shift
    monthly_ops_saving = max(ls_cost * 0.02, 500)  # At least $500/mo savings
    cost_diff = abs(rf_cost - ls_cost)
    break_even = max(1, round(cost_diff / monthly_ops_saving)) if monthly_ops_saving > 0 else 12

    return {
        "liftAndShift": {
            "cost": ls_cost,
            "weeks": ls_weeks,
            "risk": ls_risk,
            "hours": round(ls_total_hours, 1),
            "description": ls_description,
        },
        "refactor": {
            "cost": rf_cost,
            "weeks": rf_weeks,
            "risk": rf_risk,
            "hours": round(refactor_hours, 1),
            "description": rf_description,
        },
        "recommendation": recommendation,
        "breakEvenMonths": break_even,
        "processorCount": total,
        "automationRate": round(len(auto_mapped) / max(total, 1) * 100, 1),
    }


def _empty_result() -> dict:
    """Return an empty ROI comparison when no processors are present."""
    return {
        "liftAndShift": {"cost": 0, "weeks": 0, "risk": 0, "hours": 0, "description": "No processors to migrate."},
        "refactor": {"cost": 0, "weeks": 0, "risk": 0, "hours": 0, "description": "No processors to migrate."},
        "recommendation": "No processors found in flow.",
        "breakEvenMonths": 0,
        "processorCount": 0,
        "automationRate": 0,
    }

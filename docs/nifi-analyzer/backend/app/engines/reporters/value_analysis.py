"""Value analysis — estimate migration effort and value.

Ported from reporters/value-analysis.js.
"""

import logging

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Per-processor effort estimates (hours) by migration category.
#
# These estimates are based on observed migration timelines across NiFi-to-Databricks
# projects and represent median effort per processor:
#
#   - AUTO:     Review and validate auto-generated code, run integration test.
#   - SEMI:     Adjust generated mapping, resolve ambiguities, write custom glue code.
#   - MANUAL:   Full research, design, implement, and test from scratch.
#   - BASELINE: Average effort per processor without any tooling assistance.
EFFORT_HOURS_PER_PROCESSOR: dict[str, float] = {
    "auto": 0.5,      # Fully automated — quick review & validation only
    "semi": 2.0,      # Semi-automated — manual refinement of generated mapping
    "manual": 8.0,    # Fully manual — research, implement, and test from scratch
    "baseline": 6.0,  # No-tool baseline — average manual migration per processor
}


def compute_value_analysis(parse_result: ParseResult, assessment: AssessmentResult) -> dict:
    """Compute migration value analysis metrics.

    Estimates total migration effort by classifying each processor into one of
    three categories (auto, semi-automated, manual) based on mapping confidence,
    then applies per-processor hour estimates from ``EFFORT_HOURS_PER_PROCESSOR``.

    The estimation methodology:
      - **Auto** (confidence >= 0.9): Generated code needs only review/validation.
      - **Semi** (0.7 <= confidence < 0.9): Partial mapping exists but needs manual
        refinement — custom logic, ambiguity resolution, or glue code.
      - **Manual** (confidence < 0.7): No reliable mapping — requires full research,
        design, implementation, and testing.
      - **Baseline**: What the same migration would cost without tooling, used to
        compute ROI and hours saved.
    """
    total = len(assessment.mappings)
    auto_migratable = sum(1 for m in assessment.mappings if m.confidence >= 0.9)
    semi_auto = sum(1 for m in assessment.mappings if 0.7 <= m.confidence < 0.9)
    manual = total - auto_migratable - semi_auto

    # Effort estimates (hours) — sourced from EFFORT_HOURS_PER_PROCESSOR
    auto_hours = auto_migratable * EFFORT_HOURS_PER_PROCESSOR["auto"]
    semi_hours = semi_auto * EFFORT_HOURS_PER_PROCESSOR["semi"]
    manual_hours = manual * EFFORT_HOURS_PER_PROCESSOR["manual"]
    total_hours = auto_hours + semi_hours + manual_hours

    # Effort without tool (baseline)
    baseline_hours = total * EFFORT_HOURS_PER_PROCESSOR["baseline"]
    hours_saved = max(baseline_hours - total_hours, 0)

    automation_rate = round(auto_migratable / max(total, 1) * 100, 1)
    logger.info("Value analysis: %d total, %d auto, %d semi, %d manual (%.1f%% automation)", total, auto_migratable, semi_auto, manual, automation_rate)
    return {
        "processors": {
            "total": total,
            "auto_migratable": auto_migratable,
            "semi_automated": semi_auto,
            "manual_required": manual,
        },
        "effort_hours": {
            "automated": round(auto_hours, 1),
            "semi_automated": round(semi_hours, 1),
            "manual": round(manual_hours, 1),
            "total": round(total_hours, 1),
            "baseline_without_tool": round(baseline_hours, 1),
            "hours_saved": round(hours_saved, 1),
        },
        "automation_rate": round(auto_migratable / max(total, 1) * 100, 1),
        "roi_multiplier": round(baseline_hours / max(total_hours, 1), 1),
    }

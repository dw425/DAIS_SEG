"""Value analysis — estimate migration effort and value.

Ported from reporters/value-analysis.js.
"""

from app.models.pipeline import AssessmentResult, ParseResult


def compute_value_analysis(parse_result: ParseResult, assessment: AssessmentResult) -> dict:
    """Compute migration value analysis metrics."""
    total = len(assessment.mappings)
    auto_migratable = sum(1 for m in assessment.mappings if m.confidence >= 0.9)
    semi_auto = sum(1 for m in assessment.mappings if 0.7 <= m.confidence < 0.9)
    manual = total - auto_migratable - semi_auto

    # Effort estimates (hours)
    auto_hours = auto_migratable * 0.5
    semi_hours = semi_auto * 2.0
    manual_hours = manual * 8.0
    total_hours = auto_hours + semi_hours + manual_hours

    # Effort without tool (baseline)
    baseline_hours = total * 6.0
    hours_saved = max(baseline_hours - total_hours, 0)

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

"""Final report builder — executive summary of migration.

Enhanced with Lakebridge-inspired compatibility matrix and effort estimation.
"""

import logging

from app.models.pipeline import AnalysisResult, AssessmentResult, ParseResult, ValidationResult

logger = logging.getLogger(__name__)


def build_final_report(
    parse_result: ParseResult,
    analysis: AnalysisResult,
    assessment: AssessmentResult,
    validation: ValidationResult,
) -> dict:
    """Build the final executive report."""
    total = len(assessment.mappings)
    mapped = sum(1 for m in assessment.mappings if m.mapped)
    high_conf = sum(1 for m in assessment.mappings if m.confidence >= 0.9)
    med_conf = sum(1 for m in assessment.mappings if 0.7 <= m.confidence < 0.9)
    low_conf = sum(1 for m in assessment.mappings if m.confidence < 0.7 and m.mapped)

    # Readiness classification
    avg_conf = sum(m.confidence for m in assessment.mappings) / max(total, 1)
    if avg_conf >= 0.9 and assessment.unmapped_count == 0:
        readiness = "GREEN"
        readiness_label = "Ready for migration"
    elif avg_conf >= 0.7:
        readiness = "AMBER"
        readiness_label = "Migration feasible with manual review"
    else:
        readiness = "RED"
        readiness_label = "Significant manual effort required"

    # Enhanced reporting: compatibility matrix and effort estimation
    compatibility = {}
    effort = {}
    try:
        from app.engines.reporters.compatibility_matrix import compute_compatibility_matrix
        compatibility = compute_compatibility_matrix(parse_result, assessment)
    except Exception as exc:
        logger.warning("Compatibility matrix generation failed: %s", exc)

    try:
        from app.engines.reporters.effort_estimator import compute_effort_estimate
        effort = compute_effort_estimate(parse_result, assessment)
    except Exception as exc:
        logger.warning("Effort estimation failed: %s", exc)

    return {
        "readiness": readiness,
        "readiness_label": readiness_label,
        "confidence_breakdown": {
            "high": high_conf,
            "medium": med_conf,
            "low": low_conf,
            "unmapped": assessment.unmapped_count,
        },
        "coverage_pct": round(mapped / max(total, 1) * 100, 1),
        "risk_factors": _identify_risks(analysis, assessment),
        "recommendations": _build_recommendations(parse_result, analysis, assessment),
        "compatibility_matrix": compatibility,
        "effort_estimate": effort,
    }


def _identify_risks(analysis: AnalysisResult, assessment: AssessmentResult) -> list[dict]:
    risks: list[dict] = []
    if analysis.cycles:
        risks.append(
            {
                "severity": "HIGH",
                "risk": "Cycles detected in flow graph",
                "count": len(analysis.cycles),
            }
        )
    if analysis.security_findings:
        critical = sum(1 for f in analysis.security_findings if f.get("severity") == "CRITICAL")
        if critical:
            risks.append({"severity": "CRITICAL", "risk": "Security vulnerabilities found", "count": critical})
    if assessment.unmapped_count > 0:
        risks.append(
            {
                "severity": "MEDIUM",
                "risk": "Unmapped processors require manual migration",
                "count": assessment.unmapped_count,
            }
        )
    return risks


def _build_recommendations(
    parse_result: ParseResult,
    analysis: AnalysisResult,
    assessment: AssessmentResult,
) -> list[str]:
    recs: list[str] = []
    if assessment.unmapped_count > 0:
        recs.append(f"Review {assessment.unmapped_count} unmapped processor(s) for manual Databricks equivalent")
    if analysis.security_findings:
        recs.append("Address security findings before deploying to production")
    if analysis.external_systems:
        recs.append(f"Configure connectivity for {len(analysis.external_systems)} external system(s)")
    if len(parse_result.controller_services) > 0:
        recs.append("Map controller services to Databricks Secret Scopes and connection pools")
    recs.append("Run generated notebook in a dev workspace before production deployment")
    return recs

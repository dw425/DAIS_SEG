"""Validation report generator — structured output from validation results.

Produces an actionable report with severity classification, remediation
steps, and a migration-readiness summary that can be consumed by the
frontend or exported as JSON/Markdown.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.engines.validators.score_engine import is_vacuous
from app.models.pipeline import ValidationResult

logger = logging.getLogger(__name__)


def _classify_severity(score: float) -> str:
    """Map a 0-1 score to a human severity label."""
    if score >= 0.9:
        return "pass"
    if score >= 0.7:
        return "warning"
    return "critical"


_REMEDIATION_HINTS: dict[str, str] = {
    "intent_coverage": "Add generated code cells for each missing source processor.",
    "code_quality": "Fix syntax errors and remove anti-patterns (collect, toPandas, polling loops).",
    "completeness": "Ensure all notebook structural cells (imports, config, setup, teardown) exist.",
    "delta_format": "Replace non-Delta write formats with .format('delta') or .saveAsTable().",
    "checkpoint_coverage": "Add .option('checkpointLocation', '/Volumes/<catalog>/<schema>/checkpoints/<stream_name>') to every writeStream call.",
    "credential_security": "Migrate hardcoded passwords/tokens to dbutils.secrets.get().",
    "error_handling": "Wrap JDBC operations in try/except blocks with proper logging.",
    "schema_evolution": "Set cloudFiles.schemaLocation for every Auto Loader source.",
}


def generate_validation_report(result: ValidationResult) -> dict[str, Any]:
    """Build a structured validation report from a ValidationResult.

    Returns a dict with:
      - generated_at: ISO timestamp
      - overall_score: float 0-1
      - readiness: "ready" | "needs_work" | "not_ready"
      - dimension_reports: list of per-dimension detail dicts
      - critical_issues: list of issues needing immediate attention
      - error_count: total error count
      - gap_count: total gap count
      - summary: human-readable summary string
    """
    dimension_reports: list[dict[str, Any]] = []
    critical_issues: list[dict[str, str]] = []
    warning_issues: list[dict[str, str]] = []

    for s in result.scores:
        severity = _classify_severity(s.score)
        vacuous = is_vacuous(s)
        entry: dict[str, Any] = {
            "dimension": s.dimension,
            "score": round(s.score, 3),
            "percentage": round(s.score * 100, 1),
            "severity": severity,
            "details": s.details,
            "applicable": not vacuous,
        }
        if severity != "pass" and not vacuous:
            hint = _REMEDIATION_HINTS.get(s.dimension, "Review and address dimension-specific issues.")
            entry["remediation"] = hint
            issue_entry = {
                "dimension": s.dimension,
                "severity": severity,
                "message": s.details,
                "remediation": hint,
            }
            if severity == "critical":
                critical_issues.append(issue_entry)
            else:
                warning_issues.append(issue_entry)
        dimension_reports.append(entry)

    # Readiness classification
    if result.overall_score >= 0.85:
        readiness = "ready"
    elif result.overall_score >= 0.6:
        readiness = "needs_work"
    else:
        readiness = "not_ready"

    # Build human summary
    pass_count = sum(1 for d in dimension_reports if d["severity"] == "pass" and d["applicable"])
    warn_count = sum(1 for d in dimension_reports if d["severity"] == "warning")
    crit_count = sum(1 for d in dimension_reports if d["severity"] == "critical")
    applicable_count = sum(1 for d in dimension_reports if d["applicable"])

    summary = (
        f"Validation scored {result.overall_score:.0%} overall. "
        f"{pass_count}/{applicable_count} applicable dimensions passed, "
        f"{warn_count} warning(s), {crit_count} critical issue(s). "
        f"Migration readiness: {readiness.upper().replace('_', ' ')}."
    )

    logger.info("Validation report: readiness=%s, score=%.0f%%, %d critical, %d warnings", readiness, result.overall_score * 100, crit_count, warn_count)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_score": result.overall_score,
        "readiness": readiness,
        "dimension_reports": dimension_reports,
        "critical_issues": critical_issues,
        "warning_issues": warning_issues,
        "error_count": len(result.errors),
        "gap_count": len(result.gaps),
        "errors": result.errors,
        "gaps": result.gaps,
        "summary": summary,
    }


__all__ = ["generate_validation_report"]

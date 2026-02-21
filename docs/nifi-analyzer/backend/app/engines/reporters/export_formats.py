"""Export format utilities — convert reports to various output formats."""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def to_json(report: dict) -> str:
    """Export report as formatted JSON."""
    return json.dumps(report, indent=2, default=str)


def to_markdown(report: dict) -> str:
    """Export report as Markdown."""
    lines: list[str] = []
    summary = report.get("summary", {})

    lines.append(f"# Migration Report: {report.get('platform', 'Unknown').upper()}")
    lines.append(f"\n**Generated**: {report.get('generated_at', '')}")
    lines.append(f"**Source**: {report.get('source_file', '')}\n")

    lines.append("## Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    for k, v in summary.items():
        lines.append(f"| {k.replace('_', ' ').title()} | {v} |")

    final = report.get("final_report", {})
    if final:
        lines.append(f"\n## Readiness: {final.get('readiness', 'N/A')} - {final.get('readiness_label', '')}\n")

    risks = final.get("risk_factors", [])
    if risks:
        lines.append("## Risk Factors\n")
        for r in risks:
            lines.append(f"- **{r.get('severity', '')}**: {r.get('risk', '')} (count: {r.get('count', 0)})")

    recs = final.get("recommendations", [])
    if recs:
        lines.append("\n## Recommendations\n")
        for r in recs:
            lines.append(f"- {r}")

    return "\n".join(lines)


def to_csv_rows(report: dict) -> list[dict[str, Any]]:
    """Export assessment mappings as CSV-compatible rows.

    Extracts processor-level mapping data from the report's assessment
    section and returns one row per processor with standardized fields.

    The report dict is expected to contain an ``assessment`` key with a
    ``mappings`` list, where each mapping has name, type, confidence,
    mapped, category, role, and notes fields (matching ``MappingEntry``).
    """
    assessment = report.get("assessment", {})
    mappings = assessment.get("mappings", [])
    if not mappings:
        return []

    logger.info("CSV export: %d mappings to rows", len(mappings))
    rows: list[dict[str, Any]] = []
    for m in mappings:
        # Support both dict and object-style access
        get = m.get if isinstance(m, dict) else lambda k, d=None: getattr(m, k, d)
        rows.append({
            "name": get("name", ""),
            "type": get("type", ""),
            "confidence": get("confidence", 0),
            "mapped": get("mapped", False),
            "category": get("category", ""),
            "role": get("role", ""),
            "notes": get("notes", ""),
        })
    return rows

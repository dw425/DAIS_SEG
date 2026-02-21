"""Export format utilities — convert reports to various output formats."""

import json
from typing import Any


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
    """Export assessment mappings as CSV-compatible rows."""
    mappings = report.get("summary", {})
    # For actual CSV we'd extract from the full assessment
    return [{"key": k, "value": v} for k, v in mappings.items()]

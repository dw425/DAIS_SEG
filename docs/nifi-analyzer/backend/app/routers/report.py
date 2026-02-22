"""Report router — generates migration reports, final reports, and value analyses."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from app.engines.reporters.final_report import build_final_report
from app.engines.reporters.value_analysis import compute_value_analysis
from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    NotebookResult,
    ParseResult,
    ValidationResult,
)
from app.models.processor import CamelModel
from app.engines.reporters.roi_comparison import compute_roi_comparison
from app.engines.reporters.tco_calculator import compute_tco
from app.engines.reporters.fte_impact import compute_fte_impact
from app.engines.reporters.license_savings import compute_license_savings
from app.engines.reporters.monte_carlo_roi import simulate_roi
from app.engines.reporters.benchmarks import get_benchmarks
from app.engines.analyzers.complexity_scorer import score_complexity
from app.engines.reporters.compatibility_matrix import compute_compatibility_matrix
from app.engines.reporters.effort_estimator import compute_effort_estimate

from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)


class ReportRequest(CamelModel):
    type: str = "migration"
    parsed: ParseResult | None = None
    analysis: AnalysisResult | None = None
    assessment: AssessmentResult | None = None
    notebook: NotebookResult | None = None
    validation: ValidationResult | None = None
    report: dict | None = None


@router.post("/report")
def report(req: ReportRequest) -> dict:
    """Generate a report based on type: migration, final, or value."""
    proc_count = len(req.parsed.processors) if req.parsed else 0
    processing_status.start(f"report_{req.type}", proc_count)
    try:
        if req.type == "final":
            result = _build_final(req)
        elif req.type == "value":
            result = _build_value(req)
        elif req.type == "roi":
            result = _build_roi(req)
        elif req.type == "compatibility":
            result = _build_compatibility(req)
        elif req.type == "effort":
            result = _build_effort(req)
        else:
            result = _build_migration(req)
        processing_status.finish()
        return result
    except Exception as exc:
        processing_status.finish()
        logger.exception("Report generation error for type=%s", req.type)
        raise HTTPException(status_code=500, detail=f"Report failed: {exc}") from exc


def _build_migration(req: ReportRequest) -> dict:
    """Build migration report: gap playbook, risk matrix, timeline, summary."""
    assessment = req.assessment
    analysis = req.analysis

    if not assessment:
        return {"gapPlaybook": [], "riskMatrix": {}, "estimatedTimeline": [], "summary": "No assessment data available."}

    mappings = assessment.mappings
    total = len(mappings)
    mapped = sum(1 for m in mappings if m.mapped)
    unmapped = assessment.unmapped_count
    avg_conf = sum(m.confidence for m in mappings) / max(total, 1)

    # Gap playbook — unmapped and low-confidence processors
    gap_playbook: list[dict] = []
    for m in mappings:
        if not m.mapped:
            gap_playbook.append({
                "processor": m.name,
                "severity": "critical",
                "gap": f"No Databricks equivalent found for {m.type}",
                "remediation": "Implement custom logic or use a generic Spark transformation",
            })
        elif m.confidence < 0.7:
            gap_playbook.append({
                "processor": m.name,
                "severity": "high",
                "gap": f"Low confidence mapping ({m.confidence:.0%}) for {m.type}",
                "remediation": m.notes or "Review generated code and adjust manually",
            })
        elif m.confidence < 0.9:
            gap_playbook.append({
                "processor": m.name,
                "severity": "medium",
                "gap": f"Moderate confidence mapping ({m.confidence:.0%}) for {m.type}",
                "remediation": m.notes or "Verify output matches expected behavior",
            })

    # Risk matrix
    risk_matrix = {
        "critical": sum(1 for g in gap_playbook if g["severity"] == "critical"),
        "high": sum(1 for g in gap_playbook if g["severity"] == "high"),
        "medium": sum(1 for g in gap_playbook if g["severity"] == "medium"),
        "low": sum(1 for m in mappings if m.confidence >= 0.9 and m.confidence < 0.95),
    }

    # Add security and cycle risks
    if analysis:
        risk_matrix["security"] = len(analysis.security_findings)
        risk_matrix["cycles"] = len(analysis.cycles)

    # Timeline
    auto_weeks = max(1, mapped // 50)
    manual_weeks = max(1, unmapped * 2) if unmapped > 0 else 0
    timeline = [
        {"phase": "Environment Setup & Configuration", "weeks": 1},
        {"phase": f"Automated Migration ({mapped} processors)", "weeks": auto_weeks},
    ]
    if manual_weeks > 0:
        timeline.append({"phase": f"Manual Migration ({unmapped} processors)", "weeks": manual_weeks})
    timeline.extend([
        {"phase": "Integration Testing & Validation", "weeks": 2},
        {"phase": "UAT & Production Deployment", "weeks": 1},
    ])

    summary = (
        f"Migration analysis for {total} processors: {mapped} mapped ({avg_conf:.0%} avg confidence), "
        f"{unmapped} unmapped. {len(gap_playbook)} gaps identified."
    )

    return {
        "gapPlaybook": gap_playbook,
        "riskMatrix": risk_matrix,
        "estimatedTimeline": timeline,
        "summary": summary,
    }


def _build_final(req: ReportRequest) -> dict:
    """Build final executive report with sections and export support."""
    parsed = req.parsed
    analysis = req.analysis
    assessment = req.assessment

    now = datetime.now(timezone.utc).isoformat()

    if not parsed or not assessment:
        return {
            "executiveSummary": "Insufficient data to generate final report.",
            "sections": [],
            "exportFormats": ["json", "markdown", "text"],
            "generatedAt": now,
            "rawJson": {},
        }

    # Use existing final report builder for the raw data
    validation = req.validation or ValidationResult()
    raw = build_final_report(parsed, analysis or AnalysisResult(), assessment, validation)

    total = len(assessment.mappings)
    mapped = sum(1 for m in assessment.mappings if m.mapped)

    exec_summary = (
        f"Migration readiness: {raw['readiness']} — {raw['readiness_label']}.\n\n"
        f"Platform: {parsed.platform} | Processors: {total} | "
        f"Mapped: {mapped} ({raw['coverage_pct']}% coverage)\n\n"
        f"Average confidence: {sum(m.confidence for m in assessment.mappings) / max(total, 1):.0%}"
    )

    sections = [
        {
            "title": "Confidence Breakdown",
            "content": (
                f"High confidence (>=90%): {raw['confidence_breakdown']['high']}\n"
                f"Medium confidence (70-89%): {raw['confidence_breakdown']['medium']}\n"
                f"Low confidence (<70%): {raw['confidence_breakdown']['low']}\n"
                f"Unmapped: {raw['confidence_breakdown']['unmapped']}"
            ),
        },
    ]

    if raw.get("risk_factors"):
        risk_text = "\n".join(
            f"[{r['severity']}] {r['risk']} (count: {r.get('count', 'N/A')})"
            for r in raw["risk_factors"]
        )
        sections.append({"title": "Risk Factors", "content": risk_text})

    if raw.get("recommendations"):
        rec_text = "\n".join(f"- {r}" for r in raw["recommendations"])
        sections.append({"title": "Recommendations", "content": rec_text})

    if analysis and analysis.external_systems:
        ext_text = "\n".join(
            f"- {s.get('name', s.get('type', 'Unknown'))}: {s.get('protocol', 'N/A')}"
            for s in analysis.external_systems
        )
        sections.append({"title": "External Systems", "content": ext_text})

    return {
        "executiveSummary": exec_summary,
        "sections": sections,
        "exportFormats": ["json", "markdown", "text"],
        "generatedAt": now,
        "rawJson": raw,
    }


def _build_value(req: ReportRequest) -> dict:
    """Build value analysis: droppable processors, complexity, ROI, roadmap."""
    parsed = req.parsed
    assessment = req.assessment

    if not parsed or not assessment:
        return {
            "droppableProcessors": [],
            "complexityBreakdown": {},
            "roiEstimate": {"costSavings": 0, "timeSavings": 0, "riskReduction": 0},
            "implementationRoadmap": [],
        }

    raw = compute_value_analysis(parsed, assessment)

    # Build droppable processors list
    # NiFi-specific utility processor types not needed in Databricks.
    # For other source platforms (e.g. Airflow, SSIS), this set would need to be
    # replaced with platform-specific droppable types.
    droppable_types = {"LogMessage", "LogAttribute", "ControlRate", "MonitorActivity", "DebugFlow"}
    droppable_roles = {"logging", "monitoring", "funnel", "noop", "utility"}

    droppable: list[dict] = []
    seen: set[str] = set()
    for m in assessment.mappings:
        key = f"{m.name}:{m.type}"
        if key in seen:
            continue

        # NiFi-specific utility processor types — can be dropped entirely
        if m.type in droppable_types:
            seen.add(key)
            droppable.append({
                "name": m.name,
                "type": m.type,
                "reason": f"NiFi-specific utility ({m.type}) — no migration needed in Databricks",
                "savingsHours": 4,
            })
        # Processors with utility/logging/monitoring roles
        elif m.role.lower() in droppable_roles:
            seen.add(key)
            droppable.append({
                "name": m.name,
                "type": m.type,
                "reason": f"Platform utility ({m.role}) — not needed in Databricks",
                "savingsHours": 4,
            })
        # High-confidence mapped processors — fully automated
        elif m.confidence >= 0.95 and m.mapped:
            seen.add(key)
            droppable.append({
                "name": m.name,
                "type": m.type,
                "reason": "Fully automated — direct Databricks equivalent available",
                "savingsHours": 6,  # hours saved vs manual migration
            })

    # Complexity breakdown
    complexity = {
        "auto_migratable": raw["processors"]["auto_migratable"],
        "semi_automated": raw["processors"]["semi_automated"],
        "manual_required": raw["processors"]["manual_required"],
    }

    # ROI estimate
    hours_saved = raw["effort_hours"]["hours_saved"]
    cost_per_hour = 150  # avg developer cost
    roi = {
        "costSavings": round(hours_saved * cost_per_hour),
        "timeSavings": round(hours_saved),
        "riskReduction": min(round(raw["automation_rate"]), 100),
    }

    # Implementation roadmap
    roadmap = [
        {
            "phase": "Phase 1: Automated Migration",
            "tasks": [
                f"Deploy {raw['processors']['auto_migratable']} auto-migrated processors",
                "Run generated notebooks in dev workspace",
                "Validate output against source flow",
            ],
            "weeks": max(1, raw["processors"]["auto_migratable"] // 50),
        },
    ]
    if raw["processors"]["semi_automated"] > 0:
        roadmap.append({
            "phase": "Phase 2: Semi-Automated Review",
            "tasks": [
                f"Review {raw['processors']['semi_automated']} semi-automated mappings",
                "Adjust generated code where needed",
                "Run integration tests",
            ],
            "weeks": max(1, raw["processors"]["semi_automated"] // 10),
        })
    if raw["processors"]["manual_required"] > 0:
        roadmap.append({
            "phase": "Phase 3: Manual Migration",
            "tasks": [
                f"Implement {raw['processors']['manual_required']} manual processors",
                "Build custom Spark transformations",
                "Unit test each component",
            ],
            "weeks": max(1, raw["processors"]["manual_required"] // 5),
        })
    roadmap.append({
        "phase": "Final: Testing & Deployment",
        "tasks": [
            "End-to-end pipeline testing",
            "Performance benchmarking",
            "Production deployment",
        ],
        "weeks": 2,
    })

    return {
        "droppableProcessors": droppable,
        "complexityBreakdown": complexity,
        "roiEstimate": roi,
        "implementationRoadmap": roadmap,
    }


def _build_roi(req: ReportRequest) -> dict:
    """Build comprehensive ROI analysis combining all ROI modules."""
    parsed = req.parsed
    assessment = req.assessment
    analysis = req.analysis

    if not parsed or not assessment:
        return {
            "roiComparison": {},
            "tco": {},
            "fteImpact": {},
            "licenseSavings": {},
            "monteCarlo": {},
            "benchmarks": {},
            "complexity": {},
            "summary": "Insufficient data for ROI analysis.",
        }

    # Run all ROI computations
    roi = compute_roi_comparison(parsed, assessment)
    tco = compute_tco(parsed, assessment)
    fte = compute_fte_impact(assessment, parse_result=parsed)
    license_info = compute_license_savings(parsed)
    mc = simulate_roi(assessment, n_simulations=10_000)
    bench = get_benchmarks(parsed, assessment)

    # Complexity scoring (requires analysis)
    complexity = {}
    if analysis:
        complexity = score_complexity(parsed, analysis)

    # Executive summary
    total = len(assessment.mappings)
    mapped = sum(1 for m in assessment.mappings if m.mapped)
    summary = (
        f"ROI analysis for {total} processors ({mapped} mapped). "
        f"Lift-and-shift: ${roi['liftAndShift']['cost']:,} / {roi['liftAndShift']['weeks']}wk. "
        f"Refactor: ${roi['refactor']['cost']:,} / {roi['refactor']['weeks']}wk. "
        f"Monte Carlo P50: ${mc['p50']:,} (P10-P90: ${mc['p10']:,}-${mc['p90']:,}). "
        f"TCO savings Year 3: ${tco['savingsYear3']:,}. "
        f"Team: {fte['totalFTEMonths']:.1f} FTE-months. "
        f"{bench['flowSize']} flow, est. {bench['estimatedWeeks']} weeks."
    )

    return {
        "roiComparison": roi,
        "tco": tco,
        "fteImpact": fte,
        "licenseSavings": license_info,
        "monteCarlo": mc,
        "benchmarks": bench,
        "complexity": complexity,
        "summary": summary,
    }


def _build_compatibility(req: ReportRequest) -> dict:
    """Build compatibility matrix report."""
    parsed = req.parsed
    assessment = req.assessment
    if not parsed or not assessment:
        return {"entries": [], "summary": {}, "byCategory": {}}
    return compute_compatibility_matrix(parsed, assessment)


def _build_effort(req: ReportRequest) -> dict:
    """Build effort estimation report."""
    parsed = req.parsed
    assessment = req.assessment
    if not parsed or not assessment:
        return {"entries": [], "totalHours": 0, "criticalPathHours": 0,
                "skillRequirements": {}, "teamSizeRecommendation": 1, "estimatedWeeks": 1}
    return compute_effort_estimate(parsed, assessment)

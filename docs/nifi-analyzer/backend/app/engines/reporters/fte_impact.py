"""FTE impact calculator — estimate staffing needs by role and phase.

Breaks down migration effort into role-specific allocations and phased
delivery timelines.
"""

import logging

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)


def compute_fte_impact(assessment: AssessmentResult, parse_result: ParseResult | None = None) -> dict:
    """Compute FTE impact analysis by role and phase.

    Args:
        assessment: The mapping/assessment result with processor mappings.
        parse_result: The parsed flow result, used to determine the actual
            process group count. If not provided, falls back to counting
            distinct categories from the assessment mappings.

    Returns dict with roles, totalHours, totalFTEMonths, and phases.
    """
    mappings = assessment.mappings
    total = len(mappings)
    if total == 0:
        return _empty_fte()

    auto_mapped = [m for m in mappings if m.mapped and m.confidence >= 0.9]
    semi_mapped = [m for m in mappings if m.mapped and m.confidence < 0.9]
    unmapped = [m for m in mappings if not m.mapped]

    # Count process groups for architect effort — prefer the actual parsed
    # process group list over the category-based heuristic.
    if parse_result and parse_result.process_groups:
        group_count = max(len(parse_result.process_groups), 1)
    else:
        groups = set()
        for m in mappings:
            groups.add(m.category or "default")
        group_count = max(len(groups), 1)

    # --- Data Engineer: review auto-mapped processors ---
    de_hours = len(auto_mapped) * 1.0  # 1h each review
    de_tasks = [
        f"Review {len(auto_mapped)} auto-mapped processor translations",
        "Validate generated Spark/SQL code against source logic",
        "Configure Databricks job scheduling parameters",
    ]

    # --- Senior Data Engineer: handle semi-automated processors ---
    sde_hours = len(semi_mapped) * 4.0  # 4h each
    sde_tasks = [
        f"Refine {len(semi_mapped)} semi-automated processor mappings",
        "Implement custom transformation logic where needed",
        "Resolve mapping ambiguities and optimize code",
    ]

    # --- Architect: flow redesign for unmapped + overall architecture ---
    arch_hours = group_count * 8.0 + len(unmapped) * 2.0  # 8h per group + 2h per unmapped
    arch_tasks = [
        f"Redesign {group_count} process group(s) for Databricks architecture",
        f"Define migration strategy for {len(unmapped)} unmapped processors",
        "Design medallion architecture and data flow patterns",
        "Create integration patterns for external systems",
    ]

    # --- QA Engineer: testing ---
    qa_hours = total * 2.0  # 2h per processor for integration tests
    qa_tasks = [
        f"Write integration tests for {total} migrated processors",
        "Perform data validation and regression testing",
        "Execute end-to-end pipeline validation",
        "Document test results and known issues",
    ]

    # Assemble roles
    roles = [
        {
            "role": "Data Engineer",
            "hours": round(de_hours, 1),
            "fte_months": round(de_hours / 160, 2),  # 160h = 1 FTE-month
            "tasks": de_tasks,
        },
        {
            "role": "Senior Data Engineer",
            "hours": round(sde_hours, 1),
            "fte_months": round(sde_hours / 160, 2),
            "tasks": sde_tasks,
        },
        {
            "role": "Solutions Architect",
            "hours": round(arch_hours, 1),
            "fte_months": round(arch_hours / 160, 2),
            "tasks": arch_tasks,
        },
        {
            "role": "QA Engineer",
            "hours": round(qa_hours, 1),
            "fte_months": round(qa_hours / 160, 2),
            "tasks": qa_tasks,
        },
    ]

    total_hours = de_hours + sde_hours + arch_hours + qa_hours
    total_fte_months = total_hours / 160
    logger.info("FTE impact: %.1f total hours, %.2f FTE-months", total_hours, total_fte_months)

    # --- Phases ---
    phases = [
        {
            "phase": "Discovery & Architecture",
            "roles": ["Solutions Architect"],
            "weeks": max(1, round(arch_hours / 40)),
        },
        {
            "phase": "Automated Migration",
            "roles": ["Data Engineer"],
            "weeks": max(1, round(de_hours / 40)),
        },
    ]
    if sde_hours > 0:
        phases.append({
            "phase": "Semi-Automated Refinement",
            "roles": ["Senior Data Engineer", "Data Engineer"],
            "weeks": max(1, round(sde_hours / 40)),
        })
    if len(unmapped) > 0:
        phases.append({
            "phase": "Manual Implementation",
            "roles": ["Senior Data Engineer", "Solutions Architect"],
            "weeks": max(1, round(len(unmapped) * 4 / 40)),
        })
    phases.append({
        "phase": "Testing & Validation",
        "roles": ["QA Engineer", "Data Engineer"],
        "weeks": max(2, round(qa_hours / 40)),
    })
    phases.append({
        "phase": "Deployment & Handover",
        "roles": ["Solutions Architect", "Data Engineer"],
        "weeks": 1,
    })

    return {
        "roles": roles,
        "totalHours": round(total_hours, 1),
        "totalFTEMonths": round(total_fte_months, 2),
        "phases": phases,
        "summary": {
            "autoMapped": len(auto_mapped),
            "semiMapped": len(semi_mapped),
            "unmapped": len(unmapped),
            "processGroups": group_count,
        },
    }


def _empty_fte() -> dict:
    """Return empty FTE impact when no processors are present."""
    return {
        "roles": [],
        "totalHours": 0,
        "totalFTEMonths": 0,
        "phases": [],
        "summary": {"autoMapped": 0, "semiMapped": 0, "unmapped": 0, "processGroups": 0},
    }

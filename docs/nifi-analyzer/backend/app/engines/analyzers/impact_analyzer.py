"""Impact analyzer — determine blast radius when a processor changes.

Given a target processor, computes downstream impact, percentage of
the overall flow affected, risk level, and which sinks are impacted.
"""

from __future__ import annotations

import logging

from app.engines.analyzers.lineage_tracker import build_lineage_graph, get_downstream
from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)


def analyze_impact(parse_result: ParseResult, target: str) -> dict:
    """Analyze the downstream impact of modifying a given processor.

    Returns:
        {
            target: str,
            affectedProcessors: list[str],
            affectedCount: int,
            totalProcessors: int,
            impactPercentage: float,
            riskLevel: str,
            affectedSinks: list[str],
        }
    """
    logger.info("Analyzing impact for target processor: %s", target)
    graph = build_lineage_graph(parse_result.processors, parse_result.connections)
    total = len(parse_result.processors)

    if target not in graph:
        return {
            "target": target,
            "affectedProcessors": [],
            "affectedCount": 0,
            "totalProcessors": total,
            "impactPercentage": 0.0,
            "riskLevel": "none",
            "affectedSinks": [],
        }

    affected = get_downstream(graph, target)
    affected_count = len(affected)
    impact_pct = round((affected_count / max(total, 1)) * 100, 1)

    # Determine risk level
    if impact_pct > 50:
        risk = "critical"
    elif impact_pct > 25:
        risk = "high"
    elif impact_pct > 10:
        risk = "medium"
    else:
        risk = "low"

    # Identify affected sinks (processors with no downstream)
    sinks = set(
        name for name, info in graph.items()
        if not info["downstream"]
    )
    affected_sinks = [p for p in affected if p in sinks]

    logger.info("Impact analysis: target=%s, affected=%d/%d (%.1f%%), risk=%s", target, affected_count, total, impact_pct, risk)
    return {
        "target": target,
        "affectedProcessors": affected,
        "affectedCount": affected_count,
        "totalProcessors": total,
        "impactPercentage": impact_pct,
        "riskLevel": risk,
        "affectedSinks": affected_sinks,
    }

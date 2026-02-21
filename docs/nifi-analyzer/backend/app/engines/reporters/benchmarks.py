"""Industry benchmarks — compare flow against migration benchmarks.

Provides hardcoded industry benchmarks for migration timelines based on
flow size and compares the current flow against these baselines.
"""

import logging

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Industry benchmark tiers — ESTIMATED GUIDELINES.
# These values are synthetic approximations derived from internal migration experience
# and general ETL migration literature. They are NOT validated against a formal
# industry survey or published benchmark dataset. Use them as rough planning aids
# only; actual timelines vary significantly by flow complexity, team expertise,
# and organizational factors.
_BENCHMARKS = [
    {
        "tier": "Small",
        "minProcessors": 0,
        "maxProcessors": 50,
        "avgMigrationWeeks": 2,
        "avgTeamSize": 2,
        "notes": "Single sprint; 1-2 engineers can handle end-to-end",
    },
    {
        "tier": "Medium",
        "minProcessors": 51,
        "maxProcessors": 200,
        "avgMigrationWeeks": 6,
        "avgTeamSize": 3,
        "notes": "Multi-sprint effort; requires planning and phased rollout",
    },
    {
        "tier": "Large",
        "minProcessors": 201,
        "maxProcessors": 500,
        "avgMigrationWeeks": 12,
        "avgTeamSize": 5,
        "notes": "Quarter-long project; dedicated team with architect oversight",
    },
    {
        "tier": "Enterprise",
        "minProcessors": 501,
        "maxProcessors": 999_999,
        "avgMigrationWeeks": 20,
        "avgTeamSize": 8,
        "notes": "Multi-quarter program; requires program management and governance",
    },
]


def get_benchmarks(parse_result: ParseResult, assessment: AssessmentResult) -> dict:
    """Compare current flow against industry migration benchmarks.

    Returns dict with flowSize, processorCount, percentile, benchmark data,
    and comparison notes.
    """
    processor_count = len(parse_result.processors)
    if processor_count == 0:
        return _empty_benchmarks()

    # Find matching tier
    matched_tier = _BENCHMARKS[-1]  # Default to Enterprise
    for tier in _BENCHMARKS:
        if tier["minProcessors"] <= processor_count <= tier["maxProcessors"]:
            matched_tier = tier
            break

    # Compute estimated weeks based on assessment quality
    mappings = assessment.mappings
    total = len(mappings)
    mapped = sum(1 for m in mappings if m.mapped)
    avg_confidence = sum(m.confidence for m in mappings) / max(total, 1)

    # Base estimate from benchmark tier
    base_weeks = matched_tier["avgMigrationWeeks"]

    # Adjust for automation rate and confidence
    automation_rate = mapped / max(total, 1)
    confidence_factor = max(0.5, avg_confidence)  # Higher confidence = less adjustment

    # Good automation + confidence reduces timeline
    adjustment = 1.0
    if automation_rate >= 0.9 and avg_confidence >= 0.85:
        adjustment = 0.7  # 30% faster
    elif automation_rate >= 0.7 and avg_confidence >= 0.7:
        adjustment = 0.85  # 15% faster
    elif automation_rate < 0.5:
        adjustment = 1.3  # 30% slower

    estimated_weeks = max(1, round(base_weeks * adjustment))

    # Percentile: where does this flow sit among migrations?
    # Based on processor count within the tier
    tier_range = matched_tier["maxProcessors"] - matched_tier["minProcessors"]
    if tier_range > 0:
        position_in_tier = (processor_count - matched_tier["minProcessors"]) / tier_range
    else:
        position_in_tier = 0.5

    # Overall percentile across all tiers
    tier_index = _BENCHMARKS.index(matched_tier)
    percentile = round((tier_index * 25) + (position_in_tier * 25))
    percentile = min(percentile, 99)

    logger.info("Benchmarks: %d processors → %s tier, estimated %d weeks (adjustment=%.2f)", processor_count, matched_tier["tier"], estimated_weeks, adjustment)

    # Comparison notes
    diff = estimated_weeks - base_weeks
    if diff < 0:
        comparison = (
            f"Estimated {abs(diff)} week(s) faster than industry average "
            f"due to {automation_rate:.0%} automation rate and {avg_confidence:.0%} avg confidence."
        )
    elif diff > 0:
        comparison = (
            f"Estimated {diff} week(s) slower than industry average. "
            f"Consider improving automation coverage (currently {automation_rate:.0%})."
        )
    else:
        comparison = (
            f"On par with industry average for {matched_tier['tier'].lower()} flows. "
            f"Automation rate: {automation_rate:.0%}, avg confidence: {avg_confidence:.0%}."
        )

    return {
        "flowSize": matched_tier["tier"],
        "processorCount": processor_count,
        "estimatedPercentile": percentile,
        "percentileNote": (
            "This percentile is a synthetic approximation based on the processor count's "
            "position within estimated tier ranges. It is not derived from real-world "
            "migration data and should be used for rough comparison only."
        ),
        "avgMigrationWeeks": base_weeks,
        "estimatedWeeks": estimated_weeks,
        "avgTeamSize": matched_tier["avgTeamSize"],
        "comparisonNotes": comparison,
        "automationRate": round(automation_rate * 100, 1),
        "avgConfidence": round(avg_confidence * 100, 1),
        "allTiers": _BENCHMARKS,
        "matchedTier": matched_tier,
    }


def _empty_benchmarks() -> dict:
    """Return empty benchmarks when no processors are present."""
    return {
        "flowSize": "Unknown",
        "processorCount": 0,
        "estimatedPercentile": 0,
        "percentileNote": "No data — flow contains no processors.",
        "avgMigrationWeeks": 0,
        "estimatedWeeks": 0,
        "avgTeamSize": 0,
        "comparisonNotes": "No processors found in flow.",
        "automationRate": 0,
        "avgConfidence": 0,
        "allTiers": _BENCHMARKS,
        "matchedTier": None,
    }

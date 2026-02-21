"""Score aggregation engine — weighted scoring across validation dimensions.

The simple average in the original validator treated all dimensions equally,
meaning a "no writes to validate" delta_format score of 1.0 had the same
impact as intent_coverage.  This module provides weighted aggregation with
configurable weights and the concept of "applicable" vs "not-applicable"
dimensions, so dimensions that have nothing to validate don't inflate the
overall score.
"""

from __future__ import annotations

import logging

from app.models.pipeline import ValidationScore

logger = logging.getLogger(__name__)

# Relative weights for each dimension.  Higher = more impactful.
DEFAULT_WEIGHTS: dict[str, float] = {
    "intent_coverage": 3.0,
    "code_quality": 2.5,
    "completeness": 2.0,
    "delta_format": 1.5,
    "checkpoint_coverage": 1.5,
    "credential_security": 2.0,
    "error_handling": 1.0,
    "schema_evolution": 1.0,
}

# Phrases indicating the dimension had nothing to validate (vacuously true)
_NOT_APPLICABLE_PHRASES = (
    "no write operations",
    "no streaming writes",
    "no jdbc connections",
    "no auto loader sources",
)


def is_vacuous(score: ValidationScore) -> bool:
    """Return True if the dimension scored 1.0 only because there was nothing to validate."""
    if score.score < 1.0:
        return False
    details_lower = score.details.lower()
    return any(phrase in details_lower for phrase in _NOT_APPLICABLE_PHRASES)


def compute_weighted_score(
    scores: list[ValidationScore],
    *,
    weights: dict[str, float] | None = None,
    skip_vacuous: bool = True,
) -> float:
    """Compute a weighted overall score from individual dimension scores.

    Args:
        scores: List of ValidationScore objects from all validators.
        weights: Optional override for per-dimension weights.
        skip_vacuous: If True, dimensions that are "not applicable" are excluded
                      from the weighted average rather than boosting the score.

    Returns:
        A float in [0.0, 1.0] representing the weighted overall score.
    """
    w = weights or DEFAULT_WEIGHTS
    total_weight = 0.0
    weighted_sum = 0.0

    for s in scores:
        if skip_vacuous and is_vacuous(s):
            continue

        dim_weight = w.get(s.dimension, 1.0)
        weighted_sum += s.score * dim_weight
        total_weight += dim_weight

    if total_weight == 0:
        return 1.0  # all dimensions vacuous — nothing to penalize

    result = round(weighted_sum / total_weight, 4)
    logger.debug("Weighted score: %.4f (from %d dimensions, total_weight=%.1f)", result, len(scores), total_weight)
    return result


__all__ = ["compute_weighted_score", "is_vacuous", "DEFAULT_WEIGHTS"]

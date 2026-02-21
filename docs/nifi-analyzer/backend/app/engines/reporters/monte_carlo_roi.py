"""Monte Carlo ROI simulation — probabilistic cost estimation.

Runs N simulations varying effort, rework probability, and hourly rate
to produce a cost distribution with percentile breakdowns.
"""

import logging
import math
import random

from app.models.pipeline import AssessmentResult

logger = logging.getLogger(__name__)

# Default effort hours by confidence band
_EFFORT_BY_CONFIDENCE = {
    "high": 1.0,     # confidence >= 0.9
    "medium": 4.0,   # 0.7 <= confidence < 0.9
    "low": 8.0,      # confidence < 0.7
    "unmapped": 16.0, # not mapped
}


def simulate_roi(
    assessment: AssessmentResult,
    n_simulations: int = 10_000,
    seed: int | None = None,
) -> dict:
    """Run Monte Carlo simulation for migration cost estimation.

    Varies effort per processor, rework probability, and hourly rate
    across n_simulations runs.

    Returns dict with percentiles, mean, std, and histogram data.
    """
    if not assessment:
        return _empty_simulation()
    if n_simulations <= 0:
        return _empty_simulation()

    mappings = assessment.mappings
    if not mappings:
        return _empty_simulation()

    if seed is not None:
        random.seed(seed)

    # Pre-compute base effort and rework probability per processor
    processor_params: list[tuple[float, float]] = []
    for m in mappings:
        if not m.mapped:
            base = _EFFORT_BY_CONFIDENCE["unmapped"]
            rework_prob = 0.5
        elif m.confidence >= 0.9:
            base = _EFFORT_BY_CONFIDENCE["high"]
            rework_prob = 1.0 - m.confidence  # ~0.0 to 0.1
        elif m.confidence >= 0.7:
            base = _EFFORT_BY_CONFIDENCE["medium"]
            rework_prob = 1.0 - m.confidence  # ~0.1 to 0.3
        else:
            base = _EFFORT_BY_CONFIDENCE["low"]
            rework_prob = 1.0 - m.confidence  # ~0.3+
        processor_params.append((base, rework_prob))

    # Run simulations
    results: list[float] = []
    for _ in range(n_simulations):
        total_cost = 0.0
        rate = random.uniform(100, 200)  # $/hr

        for base_hours, rework_prob in processor_params:
            # Effort varies: normal distribution around base
            effort = max(0.5, random.gauss(base_hours, base_hours * 0.3))

            # Rework: if triggered, add 50-100% extra effort
            if random.random() < rework_prob:
                effort *= random.uniform(1.5, 2.0)

            total_cost += effort * rate

        results.append(total_cost)

    # Sort for percentile computation
    results.sort()
    n = len(results)

    def _percentile(p: float) -> int:
        idx = int(p / 100 * n)
        idx = min(idx, n - 1)
        return round(results[idx])

    mean = sum(results) / n
    variance = sum((x - mean) ** 2 for x in results) / n
    std = math.sqrt(variance)

    # Build histogram (20 bins)
    min_val = results[0]
    max_val = results[-1]
    bin_count = 20
    bin_width = (max_val - min_val) / bin_count if max_val > min_val else 1

    histogram: list[dict] = []
    for i in range(bin_count):
        bin_start = min_val + i * bin_width
        bin_end = bin_start + bin_width
        count = sum(
            1 for x in results
            if bin_start <= x < bin_end or (i == bin_count - 1 and x == bin_end)
        )
        histogram.append({
            "binStart": round(bin_start),
            "binEnd": round(bin_end),
            "count": count,
        })

    logger.info("Monte Carlo: %d simulations, p50=$%d, p90=$%d, mean=$%d", n_simulations, _percentile(50), _percentile(90), round(mean))
    return {
        "p10": _percentile(10),
        "p25": _percentile(25),
        "p50": _percentile(50),
        "p75": _percentile(75),
        "p90": _percentile(90),
        "mean": round(mean),
        "std": round(std),
        "min": round(min_val),
        "max": round(max_val),
        "nSimulations": n_simulations,
        "processorCount": len(mappings),
        "histogram": histogram,
        "confidenceBand": {
            "lower": _percentile(10),
            "upper": _percentile(90),
            "label": "80% confidence interval",
        },
    }


def _empty_simulation() -> dict:
    """Return empty simulation result when no processors are present."""
    return {
        "p10": 0, "p25": 0, "p50": 0, "p75": 0, "p90": 0,
        "mean": 0, "std": 0, "min": 0, "max": 0,
        "nSimulations": 0, "processorCount": 0,
        "histogram": [],
        "confidenceBand": {"lower": 0, "upper": 0, "label": "No data"},
    }

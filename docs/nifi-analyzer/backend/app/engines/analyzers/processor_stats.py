"""Processor statistics aggregator — compute summary statistics from a parsed flow.

Aggregates per-group counts, connection density, type distributions,
confidence histograms, role breakdowns, and property coverage.
"""

from __future__ import annotations

from collections import Counter

from app.models.pipeline import AssessmentResult, ParseResult


def aggregate_stats(
    parse_result: ParseResult,
    assessment: AssessmentResult | None = None,
) -> dict:
    """Compute aggregate statistics for the parsed flow.

    Returns structured dict with processors_per_group, connections_per_processor,
    most_common_types, confidence_distribution, role_distribution,
    category_distribution, and property_coverage.
    """
    processors = parse_result.processors
    connections = parse_result.connections

    # ── Processors per group ──
    group_counter: Counter = Counter()
    for p in processors:
        group_counter[p.group] += 1
    processors_per_group = dict(group_counter.most_common())

    # ── Connections per processor ──
    conn_counter: Counter = Counter()
    for c in connections:
        conn_counter[c.source_name] += 1
        conn_counter[c.destination_name] += 1
    connections_per_processor = {
        "avg": round(sum(conn_counter.values()) / max(len(processors), 1), 2),
        "max": max(conn_counter.values(), default=0),
        "min": min(conn_counter.values(), default=0),
    }

    # ── Most common types (top 10) ──
    type_counter: Counter = Counter(p.type for p in processors)
    most_common_types = [
        {"type": t, "count": c} for t, c in type_counter.most_common(10)
    ]

    # ── Property coverage ──
    total_props = 0
    non_empty_props = 0
    for p in processors:
        for _k, v in p.properties.items():
            total_props += 1
            if v is not None and str(v).strip():
                non_empty_props += 1
    property_coverage = round(
        non_empty_props / max(total_props, 1) * 100, 1
    )

    result = {
        "processor_count": len(processors),
        "connection_count": len(connections),
        "group_count": len(group_counter),
        "processors_per_group": processors_per_group,
        "connections_per_processor": connections_per_processor,
        "most_common_types": most_common_types,
        "property_coverage_pct": property_coverage,
    }

    # ── Assessment-dependent stats ──
    if assessment and assessment.mappings:
        mappings = assessment.mappings

        # Confidence distribution
        conf_buckets = {"high": 0, "medium": 0, "low": 0}
        for m in mappings:
            if m.confidence >= 0.8:
                conf_buckets["high"] += 1
            elif m.confidence >= 0.5:
                conf_buckets["medium"] += 1
            else:
                conf_buckets["low"] += 1
        result["confidence_distribution"] = conf_buckets

        # Role distribution
        role_counter: Counter = Counter(m.role for m in mappings)
        result["role_distribution"] = dict(role_counter)

        # Category distribution
        cat_counter: Counter = Counter(m.category for m in mappings if m.category)
        result["category_distribution"] = dict(cat_counter)

    return result

"""Default statistical profiles for columns parsed from DDL/ETL/schema files.

When there is no live database to profile, we generate sensible default
statistics per data type so the DistributionSampler can produce realistic
synthetic values.
"""

from __future__ import annotations

from typing import Any, Optional

from dais_seg.source_loader.base import ParsedColumn


def generate_default_stats(
    col: ParsedColumn,
    row_count: int = 1000,
) -> dict[str, Any]:
    """Generate default column statistics based on data type and constraints.

    Returns a stats dict compatible with DistributionSampler.sample_column().
    """
    base_type = col.data_type.lower().split("(")[0]
    stats: dict[str, Any] = {}

    # Null ratio
    stats["null_ratio"] = 0.0 if not col.nullable else 0.05

    # CHECK constraint provides categorical values
    if col.check_constraints:
        n = len(col.check_constraints)
        stats["top_values"] = [
            {"value": v, "frequency": 1.0 / n}
            for v in col.check_constraints
        ]
        stats["distinct_count"] = n
        return stats

    # Type-specific defaults
    if base_type in ("int", "integer", "smallint", "tinyint"):
        if col.is_primary_key:
            stats["min"] = 1
            stats["max"] = row_count
            stats["mean"] = row_count / 2
            stats["stddev"] = row_count / 6
            stats["distinct_count"] = row_count
        else:
            stats["min"] = 1
            stats["max"] = 1000
            stats["mean"] = 500
            stats["stddev"] = 300
            stats["distinct_count"] = min(500, row_count)

    elif base_type in ("bigint", "long"):
        if col.is_primary_key:
            stats["min"] = 1
            stats["max"] = row_count
            stats["mean"] = row_count / 2
            stats["stddev"] = row_count / 6
            stats["distinct_count"] = row_count
        else:
            stats["min"] = 1
            stats["max"] = 100_000
            stats["mean"] = 50_000
            stats["stddev"] = 30_000
            stats["distinct_count"] = min(5000, row_count)

    elif base_type in ("float", "double"):
        stats["min"] = 0.0
        stats["max"] = 10_000.0
        stats["mean"] = 100.0
        stats["stddev"] = 50.0
        stats["distinct_count"] = min(800, row_count)

    elif base_type in ("decimal", "numeric"):
        p = col.precision or 10
        s = col.scale or 2
        max_val = min(10 ** (p - s) - 1, 100_000.0)
        stats["min"] = 0.0
        stats["max"] = max_val
        stats["mean"] = max_val / 10
        stats["stddev"] = max_val / 20
        stats["distinct_count"] = min(800, row_count)

    elif base_type in ("varchar", "char", "text", "string"):
        max_len = col.max_length or 50
        stats["min_length"] = min(3, max_len)
        stats["max_length"] = min(max_len, 100)
        stats["distinct_count"] = min(row_count, 500)

    elif base_type == "date":
        stats["min"] = "2020-01-01"
        stats["max"] = "2025-12-31"
        stats["distinct_count"] = min(row_count, 2000)

    elif base_type in ("timestamp", "datetime"):
        stats["min"] = "2020-01-01"
        stats["max"] = "2025-12-31"
        stats["distinct_count"] = row_count

    elif base_type in ("boolean", "bool"):
        true_ratio = 0.5
        if col.default_value:
            if col.default_value.lower() in ("true", "1", "yes"):
                true_ratio = 0.7
            elif col.default_value.lower() in ("false", "0", "no"):
                true_ratio = 0.3
        stats["top_values"] = [
            {"value": True, "frequency": true_ratio},
            {"value": False, "frequency": 1 - true_ratio},
        ]
        stats["distinct_count"] = 2

    elif base_type == "binary":
        stats["min_length"] = 10
        stats["max_length"] = 100
        stats["distinct_count"] = row_count

    else:
        # Fallback: treat as string
        stats["min_length"] = 5
        stats["max_length"] = 20
        stats["distinct_count"] = min(row_count, 500)

    return stats

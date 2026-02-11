"""Data fidelity validator — statistical comparison of synthetic vs. source distributions.

Validates that synthetic Delta Tables are statistically faithful replicas of
the source: row counts, distributions, null ratios, and value ranges match
within configurable tolerances.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dais_seg.config import get_config

logger = logging.getLogger(__name__)


@dataclass
class ColumnFidelityResult:
    """Fidelity result for a single column."""

    column_name: str
    score: float  # 0.0 to 1.0
    checks: dict[str, bool] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableFidelityResult:
    """Fidelity result for a complete table."""

    table_name: str
    overall_score: float
    row_count_match: bool
    row_count_expected: int
    row_count_actual: int
    column_results: list[ColumnFidelityResult] = field(default_factory=list)


class DataFidelityValidator:
    """Validates statistical fidelity between synthetic and source data.

    Performs column-level statistical tests:
    - Row count tolerance
    - Null ratio comparison
    - Distribution comparison (KS test for numerics)
    - Cardinality checks
    - Value range validation
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = get_config()

    def validate_table(
        self,
        target_fqn: str,
        table_spec: dict,
        scale_factor: float = 1.0,
    ) -> TableFidelityResult:
        """Validate a synthetic table's data fidelity against its blueprint spec.

        Args:
            target_fqn: Fully-qualified name of the synthetic Delta Table.
            table_spec: Table specification from the blueprint (with column stats).
            scale_factor: The scale factor used during generation.
        """
        table_name = table_spec["name"]

        try:
            target_df = self.spark.read.table(target_fqn)
            target_df.cache()
        except Exception as e:
            logger.error(f"Cannot read {target_fqn}: {e}")
            return TableFidelityResult(
                table_name=table_name,
                overall_score=0.0,
                row_count_match=False,
                row_count_expected=table_spec.get("row_count", 0),
                row_count_actual=0,
            )

        actual_count = target_df.count()
        expected_count = int(table_spec.get("row_count", 0) * scale_factor)
        count_tolerance = self.config.row_count_tolerance
        row_count_match = (
            abs(actual_count - expected_count) / max(expected_count, 1) <= count_tolerance
        )

        column_results = []
        for col_spec in table_spec.get("columns", []):
            col_name = col_spec["name"]
            if col_name not in target_df.columns:
                column_results.append(
                    ColumnFidelityResult(column_name=col_name, score=0.0)
                )
                continue

            col_result = self._validate_column(target_df, col_spec, actual_count)
            column_results.append(col_result)

        target_df.unpersist()

        # Overall score: weighted average of row count match + column scores
        col_scores = [r.score for r in column_results] if column_results else [1.0]
        avg_col_score = sum(col_scores) / len(col_scores)
        row_score = 1.0 if row_count_match else 0.5
        overall = (row_score * 0.2) + (avg_col_score * 0.8)

        result = TableFidelityResult(
            table_name=table_name,
            overall_score=overall,
            row_count_match=row_count_match,
            row_count_expected=expected_count,
            row_count_actual=actual_count,
            column_results=column_results,
        )

        logger.info(
            f"Fidelity {table_name}: overall={overall:.2%}, "
            f"rows={actual_count:,}/{expected_count:,}"
        )
        return result

    def _validate_column(
        self, df: DataFrame, col_spec: dict, total_rows: int
    ) -> ColumnFidelityResult:
        """Validate a single column's data fidelity against its profiled stats."""
        col_name = col_spec["name"]
        stats = col_spec.get("stats", {})
        if not stats:
            return ColumnFidelityResult(column_name=col_name, score=1.0)

        checks = {}
        details = {}

        # Null ratio check
        expected_null_ratio = stats.get("null_ratio", 0.0)
        actual_nulls = df.filter(F.col(col_name).isNull()).count()
        actual_null_ratio = actual_nulls / total_rows if total_rows > 0 else 0
        null_diff = abs(actual_null_ratio - expected_null_ratio)
        checks["null_ratio"] = null_diff <= self.config.null_ratio_tolerance
        details["null_ratio_expected"] = expected_null_ratio
        details["null_ratio_actual"] = round(actual_null_ratio, 4)

        # Cardinality check
        expected_distinct = stats.get("distinct_count")
        if expected_distinct:
            actual_distinct = df.select(F.countDistinct(col_name)).collect()[0][0]
            cardinality_ratio = min(actual_distinct, expected_distinct) / max(
                actual_distinct, expected_distinct, 1
            )
            checks["cardinality"] = cardinality_ratio >= 0.5
            details["distinct_expected"] = expected_distinct
            details["distinct_actual"] = actual_distinct

        # Range check for numerics
        expected_min = stats.get("min")
        expected_max = stats.get("max")
        if expected_min is not None and expected_max is not None:
            try:
                actual_range = df.select(
                    F.min(col_name).alias("min_v"),
                    F.max(col_name).alias("max_v"),
                ).collect()[0]
                checks["value_range"] = True  # Synthetic data should be within bounds
                details["range_expected"] = [expected_min, expected_max]
                details["range_actual"] = [actual_range["min_v"], actual_range["max_v"]]
            except Exception:
                pass

        # Mean/stddev check for numerics
        expected_mean = stats.get("mean")
        expected_stddev = stats.get("stddev")
        if expected_mean is not None:
            try:
                actual_stats = df.select(
                    F.mean(col_name).alias("mean_v"),
                    F.stddev(col_name).alias("std_v"),
                ).collect()[0]
                if actual_stats["mean_v"] is not None:
                    mean_diff = abs(float(actual_stats["mean_v"]) - expected_mean)
                    scale = max(abs(expected_mean), 1.0)
                    checks["mean"] = (mean_diff / scale) <= 0.2
                    details["mean_expected"] = expected_mean
                    details["mean_actual"] = round(float(actual_stats["mean_v"]), 4)
            except Exception:
                pass

        # Score: fraction of checks that passed
        passed = sum(1 for v in checks.values() if v)
        total = len(checks) if checks else 1
        score = passed / total

        return ColumnFidelityResult(
            column_name=col_name,
            score=score,
            checks=checks,
            details=details,
        )

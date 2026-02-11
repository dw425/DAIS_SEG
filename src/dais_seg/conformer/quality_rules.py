"""Quality rules — DLT-style data quality expectations for the medallion pipeline.

Defines and evaluates quality expectations at each medallion layer.
Mirrors Databricks DLT expectations syntax so rules can be directly
ported to DLT pipeline definitions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class ExpectationAction(str, Enum):
    """Action to take when an expectation is violated."""

    WARN = "warn"          # Log violation, keep rows
    DROP = "drop"          # Remove violating rows
    FAIL = "fail"          # Fail the pipeline


@dataclass
class QualityExpectation:
    """A single data quality expectation (mirrors DLT EXPECT syntax)."""

    name: str
    expression: str
    action: ExpectationAction = ExpectationAction.WARN
    layer: str = "silver"
    table: str = ""


@dataclass
class QualityViolation:
    """A recorded quality violation."""

    expectation_name: str
    table: str
    layer: str
    violating_rows: int
    total_rows: int
    violation_ratio: float
    action_taken: str


class QualityRules:
    """Manages data quality expectations across the medallion pipeline.

    Provides DLT-compatible quality checks:
    - EXPECT (warn): flag but keep violating rows
    - EXPECT OR DROP: remove violating rows
    - EXPECT OR FAIL: halt pipeline on violation
    """

    # Default quality rules applied to all Silver tables
    DEFAULT_EXPECTATIONS = [
        QualityExpectation(
            name="no_all_null_rows",
            expression="NOT ({{all_null_check}})",
            action=ExpectationAction.DROP,
            layer="silver",
        ),
    ]

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._custom_expectations: list[QualityExpectation] = []
        self._violations: list[QualityViolation] = []

    def add_expectation(self, expectation: QualityExpectation) -> None:
        """Register a custom quality expectation."""
        self._custom_expectations.append(expectation)

    def add_not_null_expectation(
        self, table: str, column: str, action: ExpectationAction = ExpectationAction.WARN
    ) -> None:
        """Add a NOT NULL expectation for a specific column."""
        self._custom_expectations.append(
            QualityExpectation(
                name=f"{table}_{column}_not_null",
                expression=f"`{column}` IS NOT NULL",
                action=action,
                layer="silver",
                table=table,
            )
        )

    def add_range_expectation(
        self,
        table: str,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        action: ExpectationAction = ExpectationAction.WARN,
    ) -> None:
        """Add a value range expectation for a numeric column."""
        conditions = []
        if min_val is not None:
            conditions.append(f"`{column}` >= {min_val}")
        if max_val is not None:
            conditions.append(f"`{column}` <= {max_val}")
        if not conditions:
            return

        self._custom_expectations.append(
            QualityExpectation(
                name=f"{table}_{column}_range",
                expression=" AND ".join(conditions),
                action=action,
                layer="silver",
                table=table,
            )
        )

    def add_uniqueness_expectation(
        self, table: str, column: str, action: ExpectationAction = ExpectationAction.WARN
    ) -> None:
        """Add a uniqueness expectation (checked post-aggregation)."""
        self._custom_expectations.append(
            QualityExpectation(
                name=f"{table}_{column}_unique",
                expression=f"__unique_check_{column}__",
                action=action,
                layer="silver",
                table=table,
            )
        )

    def apply_expectations(
        self, df: DataFrame, table_name: str, layer: str
    ) -> DataFrame:
        """Apply all matching quality expectations to a DataFrame.

        Returns the DataFrame with violations handled according to each
        expectation's action (warn/drop/fail).
        """
        applicable = [
            e for e in self._custom_expectations
            if (e.layer == layer) and (e.table == "" or e.table == table_name)
        ]

        total_rows = df.count()
        result_df = df

        for expectation in applicable:
            expr = expectation.expression

            # Skip template expressions that need runtime substitution
            if "{{" in expr or "__unique_check_" in expr:
                continue

            try:
                valid_df = result_df.filter(expr)
                invalid_count = total_rows - valid_df.count()

                if invalid_count > 0:
                    violation = QualityViolation(
                        expectation_name=expectation.name,
                        table=table_name,
                        layer=layer,
                        violating_rows=invalid_count,
                        total_rows=total_rows,
                        violation_ratio=invalid_count / total_rows if total_rows > 0 else 0,
                        action_taken=expectation.action.value,
                    )
                    self._violations.append(violation)

                    logger.warning(
                        f"Quality violation [{expectation.action.value}]: "
                        f"{expectation.name} on {table_name} — "
                        f"{invalid_count}/{total_rows} rows"
                    )

                    if expectation.action == ExpectationAction.DROP:
                        result_df = valid_df
                    elif expectation.action == ExpectationAction.FAIL:
                        raise ValueError(
                            f"Quality gate FAILED: {expectation.name} on {table_name} "
                            f"({invalid_count} violations)"
                        )
            except ValueError:
                raise
            except Exception as e:
                logger.error(f"Failed to evaluate expectation {expectation.name}: {e}")

        return result_df

    def get_violations(self) -> list[dict]:
        """Return all recorded quality violations as dictionaries."""
        return [
            {
                "expectation": v.expectation_name,
                "table": v.table,
                "layer": v.layer,
                "violating_rows": v.violating_rows,
                "total_rows": v.total_rows,
                "violation_ratio": round(v.violation_ratio, 4),
                "action": v.action_taken,
            }
            for v in self._violations
        ]

    def clear_violations(self) -> None:
        """Clear all recorded violations."""
        self._violations.clear()

    def generate_dlt_sql(self) -> str:
        """Generate DLT SQL expectation syntax from registered rules.

        Produces SQL that can be used directly in a DLT pipeline notebook.
        """
        lines = []
        for exp in self._custom_expectations:
            if "{{" in exp.expression or "__" in exp.expression:
                continue
            if exp.action == ExpectationAction.WARN:
                lines.append(f"  CONSTRAINT {exp.name} EXPECT ({exp.expression})")
            elif exp.action == ExpectationAction.DROP:
                lines.append(f"  CONSTRAINT {exp.name} EXPECT ({exp.expression}) ON VIOLATION DROP ROW")
            elif exp.action == ExpectationAction.FAIL:
                lines.append(f"  CONSTRAINT {exp.name} EXPECT ({exp.expression}) ON VIOLATION FAIL UPDATE")
        return "\n".join(lines)

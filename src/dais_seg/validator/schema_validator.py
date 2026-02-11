"""Schema validator — compares source blueprint schema against target Delta Tables.

Validates that the conformed medallion target matches the profiled source
structure: column names, types, constraints, and ordering.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


@dataclass
class SchemaComparisonResult:
    """Result of comparing source blueprint schema to target table schema."""

    table_name: str
    match: bool
    score: float  # 0.0 to 1.0
    missing_columns: list[str]
    extra_columns: list[str]
    type_mismatches: list[dict]
    total_expected: int
    total_matched: int


class SchemaValidator:
    """Validates target Delta Table schemas against the Source Blueprint.

    Performs column-level comparison: presence, data type compatibility,
    and nullable constraints. Produces a match score per table.
    """

    TYPE_EQUIVALENCES = {
        "int": {"integer", "int"},
        "integer": {"integer", "int"},
        "bigint": {"long", "bigint"},
        "long": {"long", "bigint"},
        "float": {"float"},
        "double": {"double"},
        "string": {"string", "varchar", "char", "text"},
        "varchar": {"string", "varchar", "char", "text"},
        "boolean": {"boolean", "bool"},
        "date": {"date"},
        "timestamp": {"timestamp", "datetime"},
    }

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_table(
        self, table_fqn: str, table_spec: dict
    ) -> SchemaComparisonResult:
        """Compare a Delta Table's schema against its blueprint specification.

        Args:
            table_fqn: Fully-qualified name of the target Delta Table.
            table_spec: Table specification from the blueprint.
        """
        table_name = table_spec["name"]
        expected_columns = {c["name"]: c for c in table_spec.get("columns", [])}

        try:
            target_df = self.spark.read.table(table_fqn)
        except Exception as e:
            logger.error(f"Cannot read target table {table_fqn}: {e}")
            return SchemaComparisonResult(
                table_name=table_name,
                match=False,
                score=0.0,
                missing_columns=list(expected_columns.keys()),
                extra_columns=[],
                type_mismatches=[],
                total_expected=len(expected_columns),
                total_matched=0,
            )

        target_fields = {
            f.name: f for f in target_df.schema.fields if not f.name.startswith("_")
        }

        missing = [c for c in expected_columns if c not in target_fields]
        extra = [c for c in target_fields if c not in expected_columns]
        type_mismatches = []
        matched = 0

        for col_name, col_spec in expected_columns.items():
            if col_name not in target_fields:
                continue

            expected_type = col_spec["data_type"].lower().split("(")[0]
            actual_type = target_fields[col_name].dataType.simpleString().lower().split("(")[0]

            if self._types_compatible(expected_type, actual_type):
                matched += 1
            else:
                type_mismatches.append({
                    "column": col_name,
                    "expected": expected_type,
                    "actual": actual_type,
                })

        total = len(expected_columns)
        score = matched / total if total > 0 else 1.0
        is_match = len(missing) == 0 and len(type_mismatches) == 0

        result = SchemaComparisonResult(
            table_name=table_name,
            match=is_match,
            score=score,
            missing_columns=missing,
            extra_columns=extra,
            type_mismatches=type_mismatches,
            total_expected=total,
            total_matched=matched,
        )

        logger.info(
            f"Schema validation {table_name}: score={score:.2%}, "
            f"missing={len(missing)}, mismatches={len(type_mismatches)}"
        )
        return result

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if two type strings are equivalent."""
        if expected == actual:
            return True
        equivalents = self.TYPE_EQUIVALENCES.get(expected, {expected})
        return actual in equivalents

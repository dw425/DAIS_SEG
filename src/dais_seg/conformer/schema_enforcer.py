"""Schema enforcer — validates and coerces DataFrame schemas to match blueprint specs.

Ensures that Silver-layer Delta Tables have the exact column types, constraints,
and structure defined in the Source Blueprint. Handles type casting, null enforcement,
and column ordering to guarantee schema parity.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

SPARK_TYPE_MAP = {
    "string": T.StringType(),
    "varchar": T.StringType(),
    "char": T.StringType(),
    "text": T.StringType(),
    "int": T.IntegerType(),
    "integer": T.IntegerType(),
    "smallint": T.ShortType(),
    "tinyint": T.ByteType(),
    "bigint": T.LongType(),
    "long": T.LongType(),
    "float": T.FloatType(),
    "double": T.DoubleType(),
    "decimal": T.DecimalType(18, 4),
    "numeric": T.DecimalType(18, 4),
    "boolean": T.BooleanType(),
    "bool": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
    "datetime": T.TimestampType(),
}


class SchemaEnforcer:
    """Enforces schema conformance on DataFrames based on blueprint table specs.

    Validates column presence, casts types to match the blueprint, enforces
    NOT NULL constraints, and reorders columns for consistent output.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def enforce(self, df: DataFrame, table_spec: dict) -> DataFrame:
        """Enforce the blueprint schema on a DataFrame.

        Args:
            df: Input DataFrame (e.g., from Bronze layer).
            table_spec: Table specification from the blueprint.

        Returns:
            DataFrame with enforced schema — correct types, nullability, column order.
        """
        columns = table_spec.get("columns", [])
        if not columns:
            return df

        result_df = df

        # Cast columns to target types
        for col_spec in columns:
            col_name = col_spec["name"]
            target_type = col_spec["data_type"].lower().split("(")[0]
            nullable = col_spec.get("nullable", True)

            if col_name not in result_df.columns:
                logger.warning(f"Column {col_name} not in DataFrame, adding as null")
                spark_type = SPARK_TYPE_MAP.get(target_type, T.StringType())
                result_df = result_df.withColumn(col_name, F.lit(None).cast(spark_type))
                continue

            # Cast to target type
            spark_type = SPARK_TYPE_MAP.get(target_type)
            if spark_type:
                result_df = result_df.withColumn(col_name, F.col(col_name).cast(spark_type))

            # Enforce NOT NULL — replace nulls with type-appropriate defaults
            if not nullable:
                result_df = self._enforce_not_null(result_df, col_name, target_type)

        # Reorder columns to match blueprint order, keep any extra columns at end
        blueprint_cols = [c["name"] for c in columns]
        extra_cols = [c for c in result_df.columns if c not in blueprint_cols]
        ordered_cols = [c for c in blueprint_cols if c in result_df.columns] + extra_cols
        result_df = result_df.select(*ordered_cols)

        return result_df

    def validate_schema(self, df: DataFrame, table_spec: dict) -> list[dict]:
        """Validate a DataFrame's schema against the blueprint without modifying it.

        Returns a list of schema discrepancies found.
        """
        columns = table_spec.get("columns", [])
        issues = []

        df_fields = {f.name: f for f in df.schema.fields}

        for col_spec in columns:
            col_name = col_spec["name"]
            if col_name not in df_fields:
                issues.append({
                    "column": col_name,
                    "issue": "missing",
                    "expected_type": col_spec["data_type"],
                })
                continue

            df_field = df_fields[col_name]
            expected_base = col_spec["data_type"].lower().split("(")[0]
            actual_base = df_field.dataType.simpleString().lower().split("(")[0]

            expected_spark = SPARK_TYPE_MAP.get(expected_base)
            if expected_spark and actual_base != expected_spark.simpleString().lower().split("(")[0]:
                issues.append({
                    "column": col_name,
                    "issue": "type_mismatch",
                    "expected": expected_base,
                    "actual": actual_base,
                })

        # Check for unexpected columns
        blueprint_cols = {c["name"] for c in columns}
        for col_name in df_fields:
            if col_name not in blueprint_cols and not col_name.startswith("_"):
                issues.append({
                    "column": col_name,
                    "issue": "unexpected_column",
                })

        return issues

    def _enforce_not_null(
        self, df: DataFrame, col_name: str, type_str: str
    ) -> DataFrame:
        """Replace null values with type-appropriate defaults for NOT NULL columns."""
        defaults = {
            "string": "",
            "varchar": "",
            "char": "",
            "int": 0,
            "integer": 0,
            "bigint": 0,
            "long": 0,
            "smallint": 0,
            "tinyint": 0,
            "float": 0.0,
            "double": 0.0,
            "decimal": 0.0,
            "boolean": False,
            "date": "1970-01-01",
            "timestamp": "1970-01-01T00:00:00",
        }
        default_val = defaults.get(type_str)
        if default_val is not None:
            return df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(default_val)).otherwise(F.col(col_name)),
            )
        return df

"""Synthetic Delta Table generation engine.

The core of Pillar 2: takes a Source Blueprint and generates statistically
faithful Delta Tables inside an isolated Databricks Workspace. Preserves
schema, distributions, referential integrity, and edge cases — without
copying any production data.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from dais_seg.config import get_config
from dais_seg.generator.distribution_sampler import DistributionSampler
from dais_seg.generator.relationship_preserver import RelationshipPreserver

logger = logging.getLogger(__name__)

# Map blueprint type strings to PySpark types
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
    "binary": T.BinaryType(),
}


class SyntheticDeltaTableGenerator:
    """Generates synthetic Delta Tables from a Source Blueprint.

    Orchestrates the full generation pipeline:
    1. Parse blueprint for table specs and relationships
    2. Determine generation order (parents before children)
    3. Generate synthetic data per column using DistributionSampler
    4. Apply FK constraints via RelationshipPreserver
    5. Write Delta Tables to the target catalog/schema
    """

    def __init__(
        self,
        spark: SparkSession,
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None,
        seed: int = 42,
    ):
        self.spark = spark
        self.config = get_config()
        self.target_catalog = target_catalog or self.config.catalog
        self.target_schema = target_schema or "synthetic"
        self.sampler = DistributionSampler(seed=seed)

    def generate_from_blueprint(
        self,
        blueprint: dict,
        scale_factor: float = 1.0,
        tables: Optional[list[str]] = None,
    ) -> dict[str, str]:
        """Generate all synthetic Delta Tables from a blueprint.

        Args:
            blueprint: Source Blueprint dictionary.
            scale_factor: Multiply row counts by this factor (e.g., 0.1 for 10%).
            tables: Specific tables to generate. If None, generates all.

        Returns:
            Mapping of table_name -> Delta Table fully-qualified name.
        """
        preserver = RelationshipPreserver(blueprint)
        generation_order = preserver.get_generation_order()

        # Filter to requested tables if specified
        if tables:
            generation_order = [t for t in generation_order if t in tables]

        self._ensure_target_schema()

        pk_pools: dict[str, dict[str, list[Any]]] = {}
        generated_tables: dict[str, str] = {}

        for table_name in generation_order:
            table_spec = next(
                (t for t in blueprint["tables"] if t["name"] == table_name), None
            )
            if not table_spec:
                logger.warning(f"Table {table_name} not found in blueprint, skipping")
                continue

            fqn = self._generate_table(
                table_spec, preserver, pk_pools, scale_factor
            )
            generated_tables[table_name] = fqn
            logger.info(f"Generated: {fqn}")

        logger.info(f"Generation complete: {len(generated_tables)} tables")
        return generated_tables

    def _generate_table(
        self,
        table_spec: dict,
        preserver: RelationshipPreserver,
        pk_pools: dict[str, dict[str, list[Any]]],
        scale_factor: float,
    ) -> str:
        """Generate a single synthetic Delta Table."""
        table_name = table_spec["name"]
        row_count = max(1, int(table_spec.get("row_count", 1000) * scale_factor))
        row_count = min(row_count, self.config.max_rows_per_table)

        logger.info(f"Generating {table_name}: {row_count:,} rows")

        # Generate synthetic data per column
        columns = table_spec.get("columns", [])
        generated_data: dict[str, list[Any]] = {}

        for col_spec in columns:
            col_name = col_spec["name"]
            values = self.sampler.sample_column(col_spec, row_count)
            generated_data[col_name] = values

        # Apply FK constraints — replace FK columns with valid parent references
        if self.config.preserve_referential_integrity:
            generated_data = preserver.apply_fk_values(
                table_name, generated_data, pk_pools
            )

        # Store PK pool for child tables
        pk_pool = preserver.build_pk_pool(table_name, generated_data)
        if pk_pool:
            pk_pools[table_name] = pk_pool

        # Build Spark DataFrame and write as Delta Table
        spark_schema = self._build_spark_schema(columns)
        rows = self._columns_to_rows(generated_data, columns, row_count)
        df = self.spark.createDataFrame(rows, spark_schema)

        fqn = f"`{self.target_catalog}`.`{self.target_schema}`.`{table_name}`"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(fqn)

        return fqn

    def _build_spark_schema(self, columns: list[dict]) -> T.StructType:
        """Build a PySpark StructType from blueprint column specs."""
        fields = []
        for col in columns:
            base_type = col["data_type"].lower().split("(")[0]
            spark_type = SPARK_TYPE_MAP.get(base_type, T.StringType())
            nullable = col.get("nullable", True)
            fields.append(T.StructField(col["name"], spark_type, nullable))
        return T.StructType(fields)

    def _columns_to_rows(
        self,
        generated_data: dict[str, list[Any]],
        columns: list[dict],
        row_count: int,
    ) -> list[tuple]:
        """Convert column-oriented data to row-oriented tuples for DataFrame creation."""
        col_names = [c["name"] for c in columns]
        rows = []
        for i in range(row_count):
            row = tuple(
                generated_data.get(name, [None] * row_count)[i]
                for name in col_names
            )
            rows.append(row)
        return rows

    def _ensure_target_schema(self) -> None:
        """Create the target catalog and schema if they don't exist."""
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{self.target_catalog}`.`{self.target_schema}`"
        )

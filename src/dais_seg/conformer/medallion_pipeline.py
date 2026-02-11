"""Medallion pipeline — maps synthetic data through Bronze/Silver/Gold layers.

Implements the Databricks Lakehouse medallion architecture using DLT
(Delta Live Tables) patterns. Synthetic data flows from raw Bronze
through cleaned Silver to business-ready Gold, establishing the
conformed target that all workstreams validate against.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dais_seg.config import get_config
from dais_seg.conformer.schema_enforcer import SchemaEnforcer
from dais_seg.conformer.quality_rules import QualityRules

logger = logging.getLogger(__name__)


@dataclass
class MedallionLayer:
    """Represents a single medallion layer (Bronze, Silver, or Gold)."""

    name: str
    schema: str
    tables: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of a full medallion pipeline execution."""

    bronze: MedallionLayer
    silver: MedallionLayer
    gold: MedallionLayer
    tables_processed: int = 0
    quality_violations: list[dict] = field(default_factory=list)
    success: bool = True


class MedallionPipeline:
    """Orchestrates the Bronze -> Silver -> Gold medallion conformance pipeline.

    Takes synthetic Delta Tables from the generator and maps them through
    the three-layer medallion architecture:
    - Bronze: Raw synthetic data, landed as-is (Auto Loader pattern)
    - Silver: Cleaned, conformed, schema-enforced via DLT expectations
    - Gold: Aggregated, enriched, business-ready for Genie/ML Serving
    """

    def __init__(
        self,
        spark: SparkSession,
        source_catalog: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_catalog: Optional[str] = None,
    ):
        self.spark = spark
        self.config = get_config()
        self.source_catalog = source_catalog or self.config.catalog
        self.source_schema = source_schema or "synthetic"
        self.target_catalog = target_catalog or self.config.catalog
        self.schema_enforcer = SchemaEnforcer(spark)
        self.quality_rules = QualityRules(spark)

    def run(
        self,
        tables: Optional[list[str]] = None,
        blueprint: Optional[dict] = None,
    ) -> PipelineResult:
        """Execute the full medallion pipeline for synthetic tables.

        Args:
            tables: Specific table names to process. If None, discovers all
                    tables in the source schema.
            blueprint: Optional blueprint for schema enforcement rules.
        """
        source_tables = tables or self._discover_source_tables()
        logger.info(f"Running medallion pipeline for {len(source_tables)} tables")

        bronze = self._process_bronze(source_tables)
        silver = self._process_silver(bronze, blueprint)
        gold = self._process_gold(silver, blueprint)

        # Collect quality violations across all layers
        violations = self.quality_rules.get_violations()

        result = PipelineResult(
            bronze=bronze,
            silver=silver,
            gold=gold,
            tables_processed=len(source_tables),
            quality_violations=violations,
            success=len(violations) == 0,
        )

        logger.info(
            f"Pipeline complete: {result.tables_processed} tables, "
            f"{len(violations)} quality violations"
        )
        return result

    def _process_bronze(self, source_tables: list[str]) -> MedallionLayer:
        """Bronze layer: land synthetic data as-is with ingestion metadata."""
        bronze_schema = f"{self.config.bronze_schema}"
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{self.target_catalog}`.`{bronze_schema}`"
        )

        layer = MedallionLayer(name="bronze", schema=bronze_schema)

        for table_name in source_tables:
            source_fqn = f"`{self.source_catalog}`.`{self.source_schema}`.`{table_name}`"
            target_fqn = f"`{self.target_catalog}`.`{bronze_schema}`.`{table_name}`"

            try:
                df = self.spark.read.table(source_fqn)
                # Add bronze metadata columns
                bronze_df = df.withColumn(
                    "_bronze_ingested_at", F.current_timestamp()
                ).withColumn(
                    "_bronze_source", F.lit(source_fqn)
                ).withColumn(
                    "_bronze_batch_id", F.lit(f"seg_{table_name}")
                )

                bronze_df.write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(target_fqn)

                row_count = bronze_df.count()
                layer.tables.append(table_name)
                layer.row_counts[table_name] = row_count
                logger.info(f"  Bronze: {table_name} ({row_count:,} rows)")
            except Exception as e:
                logger.error(f"  Bronze failed for {table_name}: {e}")

        return layer

    def _process_silver(
        self, bronze: MedallionLayer, blueprint: Optional[dict]
    ) -> MedallionLayer:
        """Silver layer: clean, conform, and enforce schema with quality expectations."""
        silver_schema = f"{self.config.silver_schema}"
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{self.target_catalog}`.`{silver_schema}`"
        )

        layer = MedallionLayer(name="silver", schema=silver_schema)

        for table_name in bronze.tables:
            bronze_fqn = f"`{self.target_catalog}`.`{bronze.schema}`.`{table_name}`"
            silver_fqn = f"`{self.target_catalog}`.`{silver_schema}`.`{table_name}`"

            try:
                df = self.spark.read.table(bronze_fqn)

                # Drop bronze metadata columns
                silver_df = df.drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")

                # Apply schema enforcement from blueprint
                if blueprint:
                    table_spec = next(
                        (t for t in blueprint["tables"] if t["name"] == table_name), None
                    )
                    if table_spec:
                        silver_df = self.schema_enforcer.enforce(silver_df, table_spec)

                # Apply quality rules
                silver_df = self.quality_rules.apply_expectations(
                    silver_df, table_name, "silver"
                )

                # Add silver metadata
                silver_df = silver_df.withColumn(
                    "_silver_processed_at", F.current_timestamp()
                )

                silver_df.write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(silver_fqn)

                row_count = silver_df.count()
                layer.tables.append(table_name)
                layer.row_counts[table_name] = row_count
                logger.info(f"  Silver: {table_name} ({row_count:,} rows)")
            except Exception as e:
                logger.error(f"  Silver failed for {table_name}: {e}")

        return layer

    def _process_gold(
        self, silver: MedallionLayer, blueprint: Optional[dict]
    ) -> MedallionLayer:
        """Gold layer: aggregate and enrich for business consumption.

        Gold tables are what Genie dashboards and ML Serving endpoints
        will query. Structure depends on business requirements in the blueprint.
        """
        gold_schema = f"{self.config.gold_schema}"
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{self.target_catalog}`.`{gold_schema}`"
        )

        layer = MedallionLayer(name="gold", schema=gold_schema)

        for table_name in silver.tables:
            silver_fqn = f"`{self.target_catalog}`.`{silver.schema}`.`{table_name}`"
            gold_fqn = f"`{self.target_catalog}`.`{gold_schema}`.`{table_name}`"

            try:
                df = self.spark.read.table(silver_fqn)

                # Drop silver metadata
                gold_df = df.drop("_silver_processed_at")

                # Add gold metadata
                gold_df = gold_df.withColumn(
                    "_gold_published_at", F.current_timestamp()
                )

                gold_df.write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(gold_fqn)

                row_count = gold_df.count()
                layer.tables.append(table_name)
                layer.row_counts[table_name] = row_count
                logger.info(f"  Gold: {table_name} ({row_count:,} rows)")
            except Exception as e:
                logger.error(f"  Gold failed for {table_name}: {e}")

        return layer

    def _discover_source_tables(self) -> list[str]:
        """Discover all tables in the synthetic source schema."""
        df = self.spark.sql(
            f"SHOW TABLES IN `{self.source_catalog}`.`{self.source_schema}`"
        )
        return [row["tableName"] for row in df.collect()]

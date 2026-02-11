"""Statistical distribution profiler for source system columns.

Samples data from foreign tables via Lakehouse Federation and computes
statistical fingerprints — distributions, cardinality, null ratios,
percentiles, and patterns — that drive synthetic data generation.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dais_seg.config import get_config
from dais_seg.profiler.catalog_crawler import CrawlResult, TableMetadata
from dais_seg.profiler.federation_connector import FederationConnector

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Statistical profile for a single column."""

    name: str
    data_type: str
    distinct_count: int = 0
    null_count: int = 0
    null_ratio: float = 0.0
    total_count: int = 0

    # Numeric stats
    min_value: Any = None
    max_value: Any = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    percentiles: dict[str, float] = field(default_factory=dict)

    # Categorical stats
    top_values: list[dict[str, Any]] = field(default_factory=list)

    # String stats
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for blueprint storage."""
        result = {
            "distinct_count": self.distinct_count,
            "null_ratio": self.null_ratio,
            "min": self.min_value,
            "max": self.max_value,
        }
        if self.mean is not None:
            result["mean"] = self.mean
            result["stddev"] = self.stddev
        if self.percentiles:
            result["percentiles"] = self.percentiles
        if self.top_values:
            result["top_values"] = self.top_values
        if self.pattern:
            result["pattern"] = self.pattern
        return result


@dataclass
class TableProfile:
    """Statistical profile for a table."""

    name: str
    schema: str
    row_count: int
    column_profiles: list[ColumnProfile] = field(default_factory=list)


class DistributionProfiler:
    """Profiles statistical distributions of source data via federation sampling.

    Computes per-column statistics including distributions, cardinality,
    null patterns, and value frequencies — all from sampled data to minimize
    source system load.
    """

    NUMERIC_TYPES = {"int", "bigint", "smallint", "tinyint", "float", "double", "decimal", "numeric"}
    STRING_TYPES = {"string", "varchar", "char", "text"}
    DATE_TYPES = {"date", "timestamp", "datetime"}

    def __init__(
        self,
        spark: SparkSession,
        connector: Optional[FederationConnector] = None,
        sample_size: int = 50_000,
    ):
        self.spark = spark
        self.config = get_config()
        self.connector = connector or FederationConnector(spark, self.config.catalog)
        self.sample_size = sample_size

    def profile_crawl_result(self, crawl: CrawlResult) -> list[TableProfile]:
        """Profile all tables discovered during a catalog crawl."""
        profiles = []
        for table_meta in crawl.tables:
            logger.info(f"Profiling {table_meta.schema}.{table_meta.name}")
            profile = self.profile_table(
                crawl.foreign_catalog, table_meta.schema, table_meta.name, table_meta
            )
            profiles.append(profile)
        return profiles

    def profile_table(
        self,
        foreign_catalog: str,
        schema: str,
        table: str,
        table_meta: Optional[TableMetadata] = None,
    ) -> TableProfile:
        """Profile a single table by sampling and computing column statistics."""
        sample_df = self.connector.sample_foreign_table(
            foreign_catalog, schema, table, self.sample_size
        )
        sample_df.cache()
        row_count = sample_df.count()

        column_profiles = []
        for col_field in sample_df.schema.fields:
            profile = self._profile_column(sample_df, col_field, row_count)
            column_profiles.append(profile)

        sample_df.unpersist()

        return TableProfile(
            name=table,
            schema=schema,
            row_count=table_meta.row_count if table_meta else row_count,
            column_profiles=column_profiles,
        )

    def _profile_column(
        self, df: DataFrame, col_field: T.StructField, total_rows: int
    ) -> ColumnProfile:
        """Compute statistical profile for a single column."""
        col_name = col_field.name
        base_type = self._normalize_type(col_field.dataType.simpleString())

        # Basic counts
        stats = df.select(
            F.count(F.col(col_name)).alias("non_null"),
            F.countDistinct(F.col(col_name)).alias("distinct"),
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias("nulls"),
        ).collect()[0]

        profile = ColumnProfile(
            name=col_name,
            data_type=col_field.dataType.simpleString(),
            distinct_count=stats["distinct"],
            null_count=stats["nulls"],
            null_ratio=stats["nulls"] / total_rows if total_rows > 0 else 0,
            total_count=total_rows,
        )

        # Type-specific profiling
        if base_type in self.NUMERIC_TYPES:
            self._profile_numeric(df, col_name, profile)
        elif base_type in self.STRING_TYPES:
            self._profile_string(df, col_name, profile)
        elif base_type in self.DATE_TYPES:
            self._profile_date(df, col_name, profile)

        # Top values for all types (useful for categorical detection)
        self._profile_top_values(df, col_name, profile, total_rows)

        return profile

    def _profile_numeric(
        self, df: DataFrame, col_name: str, profile: ColumnProfile
    ) -> None:
        """Compute numeric column statistics."""
        stats = df.select(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
            F.mean(col_name).alias("mean_val"),
            F.stddev(col_name).alias("stddev_val"),
        ).collect()[0]

        profile.min_value = stats["min_val"]
        profile.max_value = stats["max_val"]
        profile.mean = float(stats["mean_val"]) if stats["mean_val"] is not None else None
        profile.stddev = float(stats["stddev_val"]) if stats["stddev_val"] is not None else None

        # Percentiles
        try:
            pcts = df.stat.approxQuantile(
                col_name, [0.25, 0.50, 0.75, 0.95, 0.99], 0.01
            )
            if pcts and len(pcts) == 5:
                profile.percentiles = {
                    "p25": pcts[0], "p50": pcts[1], "p75": pcts[2],
                    "p95": pcts[3], "p99": pcts[4],
                }
        except Exception:
            pass

    def _profile_string(
        self, df: DataFrame, col_name: str, profile: ColumnProfile
    ) -> None:
        """Compute string column statistics."""
        stats = df.filter(F.col(col_name).isNotNull()).select(
            F.min(F.length(col_name)).alias("min_len"),
            F.max(F.length(col_name)).alias("max_len"),
        ).collect()[0]

        profile.min_length = stats["min_len"]
        profile.max_length = stats["max_len"]
        profile.min_value = None
        profile.max_value = None

        # Detect common patterns from sample
        profile.pattern = self._detect_pattern(df, col_name)

    def _profile_date(
        self, df: DataFrame, col_name: str, profile: ColumnProfile
    ) -> None:
        """Compute date/timestamp column statistics."""
        stats = df.select(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        profile.min_value = str(stats["min_val"]) if stats["min_val"] else None
        profile.max_value = str(stats["max_val"]) if stats["max_val"] else None

    def _profile_top_values(
        self, df: DataFrame, col_name: str, profile: ColumnProfile, total: int
    ) -> None:
        """Find the most frequent values in a column."""
        top_df = (
            df.filter(F.col(col_name).isNotNull())
            .groupBy(col_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(20)
        )
        profile.top_values = [
            {"value": row[col_name], "frequency": row["count"] / total}
            for row in top_df.collect()
        ]

    def _detect_pattern(self, df: DataFrame, col_name: str) -> Optional[str]:
        """Detect a regex pattern for string values by sampling."""
        sample = (
            df.filter(F.col(col_name).isNotNull())
            .select(col_name)
            .limit(100)
            .collect()
        )
        values = [str(row[col_name]) for row in sample]
        if not values:
            return None

        # Check for common patterns
        patterns = [
            (r"^\d{3}-\d{2}-\d{4}$", r"\d{3}-\d{2}-\d{4}"),  # SSN-like
            (r"^\d{5}(-\d{4})?$", r"\d{5}(-\d{4})?"),  # ZIP
            (r"^[A-Z]{2}\d+$", r"[A-Z]{2}\d+"),  # State + number
            (r"^\d{4}-\d{2}-\d{2}$", r"\d{4}-\d{2}-\d{2}"),  # Date
            (r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$", r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+"),
        ]
        for regex, pattern in patterns:
            if all(re.match(regex, v) for v in values[:20]):
                return pattern

        return None

    def _normalize_type(self, spark_type: str) -> str:
        """Normalize Spark type string to base type for comparison."""
        base = spark_type.lower().split("(")[0].strip()
        return base

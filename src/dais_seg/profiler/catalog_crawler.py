"""Unity Catalog crawler for discovering and inventorying source system metadata.

Crawls foreign catalogs registered via Lakehouse Federation to build a complete
inventory of schemas, tables, columns, constraints, and relationships — the raw
input for blueprint generation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession

from dais_seg.config import get_config
from dais_seg.profiler.federation_connector import FederationConnector

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Metadata for a single column discovered during crawl."""

    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    comment: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata for a single table discovered during crawl."""

    name: str
    schema: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    row_count: int = 0
    size_bytes: int = 0
    foreign_keys: list[dict] = field(default_factory=list)


@dataclass
class CrawlResult:
    """Complete crawl result for a source system."""

    foreign_catalog: str
    source_type: str
    schemas: list[str] = field(default_factory=list)
    tables: list[TableMetadata] = field(default_factory=list)
    total_columns: int = 0
    total_rows: int = 0


class CatalogCrawler:
    """Crawls Unity Catalog foreign catalogs to discover source system structure.

    Connects via Lakehouse Federation and systematically inventories every
    schema, table, and column — building the metadata foundation for profiling.
    """

    def __init__(
        self,
        spark: SparkSession,
        connector: Optional[FederationConnector] = None,
    ):
        self.spark = spark
        self.config = get_config()
        self.connector = connector or FederationConnector(spark, self.config.catalog)

    def crawl(
        self,
        foreign_catalog: str,
        schemas: Optional[list[str]] = None,
        exclude_schemas: Optional[list[str]] = None,
    ) -> CrawlResult:
        """Crawl a foreign catalog and return complete metadata.

        Args:
            foreign_catalog: Name of the foreign catalog in Unity Catalog.
            schemas: Specific schemas to crawl. If None, crawls all.
            exclude_schemas: Schemas to skip (e.g., system schemas).
        """
        exclude = set(exclude_schemas or ["information_schema", "sys", "pg_catalog"])

        # Discover schemas
        all_schemas = self.connector.list_foreign_schemas(foreign_catalog)
        target_schemas = schemas or [s for s in all_schemas if s.lower() not in exclude]

        logger.info(f"Crawling {len(target_schemas)} schemas in {foreign_catalog}")

        result = CrawlResult(
            foreign_catalog=foreign_catalog,
            source_type=self._detect_source_type(foreign_catalog),
            schemas=target_schemas,
        )

        for schema in target_schemas:
            tables = self._crawl_schema(foreign_catalog, schema)
            result.tables.extend(tables)

        result.total_columns = sum(len(t.columns) for t in result.tables)
        result.total_rows = sum(t.row_count for t in result.tables)

        logger.info(
            f"Crawl complete: {len(result.tables)} tables, "
            f"{result.total_columns} columns, {result.total_rows:,} rows"
        )
        return result

    def _crawl_schema(
        self, foreign_catalog: str, schema: str
    ) -> list[TableMetadata]:
        """Crawl all tables in a single schema."""
        table_names = self.connector.list_foreign_tables(foreign_catalog, schema)
        tables = []

        for table_name in table_names:
            try:
                table_meta = self._crawl_table(foreign_catalog, schema, table_name)
                tables.append(table_meta)
                logger.debug(f"  Crawled {schema}.{table_name}: {len(table_meta.columns)} cols")
            except Exception as e:
                logger.warning(f"  Failed to crawl {schema}.{table_name}: {e}")

        return tables

    def _crawl_table(
        self, foreign_catalog: str, schema: str, table: str
    ) -> TableMetadata:
        """Crawl a single table for column metadata, row count, and constraints."""
        # Get column descriptions
        desc_df = self.connector.describe_foreign_table(foreign_catalog, schema, table)
        columns = []
        for row in desc_df.collect():
            col_name = row["col_name"]
            if col_name.startswith("#") or col_name == "":
                continue  # Skip partition info / comment rows
            columns.append(
                ColumnMetadata(
                    name=col_name,
                    data_type=row["data_type"],
                    nullable=True,  # Refined during profiling
                )
            )

        # Get row count
        row_count = self.connector.get_row_count(foreign_catalog, schema, table)

        # Get foreign keys
        fks = self.connector.get_foreign_keys(foreign_catalog, schema, table)

        return TableMetadata(
            name=table,
            schema=schema,
            columns=columns,
            row_count=row_count,
            foreign_keys=fks,
        )

    def _detect_source_type(self, foreign_catalog: str) -> str:
        """Detect the source system type from the foreign catalog properties."""
        try:
            props = self.spark.sql(
                f"DESCRIBE CATALOG EXTENDED `{foreign_catalog}`"
            ).collect()
            for row in props:
                if "connection_type" in str(row).lower():
                    return str(row[1]).lower()
        except Exception:
            pass
        return "unknown"

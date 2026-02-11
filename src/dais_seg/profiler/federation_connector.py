"""Lakehouse Federation connector for querying source systems without moving data.

Uses Unity Catalog's Lakehouse Federation to connect to external source systems
(Oracle, SQL Server, Teradata, Snowflake, etc.) and query metadata + sample data
for profiling — all without extracting or copying production data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


@dataclass
class FederationConnection:
    """Represents a Lakehouse Federation connection in Unity Catalog."""

    connection_name: str
    source_type: str
    catalog_name: str
    schemas: list[str]


class FederationConnector:
    """Manages Lakehouse Federation connections for source profiling.

    Connects to external systems via Unity Catalog foreign connections,
    enabling read-only metadata and sample queries against source systems.
    """

    def __init__(self, spark: SparkSession, catalog: str = "dais_seg"):
        self.spark = spark
        self.catalog = catalog

    def list_connections(self) -> list[FederationConnection]:
        """List all Lakehouse Federation connections available in Unity Catalog."""
        connections_df = self.spark.sql("SHOW CONNECTIONS")
        connections = []
        for row in connections_df.collect():
            connections.append(
                FederationConnection(
                    connection_name=row["name"],
                    source_type=row["connection_type"],
                    catalog_name=row.get("catalog_name", ""),
                    schemas=[],
                )
            )
        return connections

    def create_foreign_catalog(
        self, connection_name: str, foreign_catalog_name: str
    ) -> str:
        """Create a foreign catalog in Unity Catalog for a federation connection.

        This registers the external system's catalog so tables can be queried
        directly via Spark SQL without data movement.
        """
        catalog_name = f"{self.catalog}_fed_{foreign_catalog_name}"
        self.spark.sql(f"""
            CREATE FOREIGN CATALOG IF NOT EXISTS `{catalog_name}`
            USING CONNECTION `{connection_name}`
            OPTIONS (database '{foreign_catalog_name}')
        """)
        return catalog_name

    def list_foreign_schemas(self, foreign_catalog: str) -> list[str]:
        """List schemas available in a foreign catalog."""
        df = self.spark.sql(f"SHOW SCHEMAS IN `{foreign_catalog}`")
        return [row["databaseName"] for row in df.collect()]

    def list_foreign_tables(self, foreign_catalog: str, schema: str) -> list[str]:
        """List tables in a foreign catalog schema."""
        df = self.spark.sql(f"SHOW TABLES IN `{foreign_catalog}`.`{schema}`")
        return [row["tableName"] for row in df.collect()]

    def describe_foreign_table(
        self, foreign_catalog: str, schema: str, table: str
    ) -> DataFrame:
        """Get column-level metadata for a foreign table."""
        return self.spark.sql(
            f"DESCRIBE TABLE `{foreign_catalog}`.`{schema}`.`{table}`"
        )

    def sample_foreign_table(
        self,
        foreign_catalog: str,
        schema: str,
        table: str,
        sample_size: int = 10000,
    ) -> DataFrame:
        """Sample rows from a foreign table for statistical profiling.

        Uses TABLESAMPLE or LIMIT to avoid full table scans on source systems.
        """
        fqn = f"`{foreign_catalog}`.`{schema}`.`{table}`"
        return self.spark.sql(f"SELECT * FROM {fqn} LIMIT {sample_size}")

    def get_row_count(
        self, foreign_catalog: str, schema: str, table: str
    ) -> int:
        """Get approximate row count for a foreign table."""
        fqn = f"`{foreign_catalog}`.`{schema}`.`{table}`"
        result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {fqn}").collect()
        return result[0]["cnt"]

    def get_foreign_keys(
        self, foreign_catalog: str, schema: str, table: str
    ) -> list[dict]:
        """Retrieve foreign key constraints from source system metadata.

        Uses INFORMATION_SCHEMA queries through federation to discover
        referential integrity relationships.
        """
        try:
            fk_df = self.spark.sql(f"""
                SELECT
                    kcu.COLUMN_NAME as fk_column,
                    ccu.TABLE_NAME as referenced_table,
                    ccu.COLUMN_NAME as referenced_column
                FROM `{foreign_catalog}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN `{foreign_catalog}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                    ON kcu.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                WHERE kcu.TABLE_SCHEMA = '{schema}'
                    AND kcu.TABLE_NAME = '{table}'
                    AND kcu.CONSTRAINT_NAME LIKE '%fk%'
            """)
            return [row.asDict() for row in fk_df.collect()]
        except Exception:
            # Not all source systems expose INFORMATION_SCHEMA uniformly
            return []

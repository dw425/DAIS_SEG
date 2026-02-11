"""Blueprint generator — synthesizes crawl + profile results into a Source Blueprint.

The Source Blueprint is the canonical representation of a profiled source system.
It is stored as a Delta Table in Unity Catalog with time travel for drift detection,
and serves as the input specification for synthetic environment generation.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from dais_seg.config import get_config
from dais_seg.profiler.catalog_crawler import CrawlResult
from dais_seg.profiler.distribution_profiler import DistributionProfiler, TableProfile

logger = logging.getLogger(__name__)


class BlueprintGenerator:
    """Generates Source Environment Blueprints from crawl and profile data.

    Combines metadata from CatalogCrawler and statistical profiles from
    DistributionProfiler into a unified blueprint document, then persists
    it as a versioned Delta Table in Unity Catalog.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = get_config()
        self.fqn = (
            f"`{self.config.catalog}`.`{self.config.schema}`"
            f".`{self.config.blueprint_table}`"
        )

    def generate(
        self,
        crawl: CrawlResult,
        profiles: list[TableProfile],
    ) -> dict:
        """Generate a Source Blueprint from crawl and profile results.

        Returns the blueprint as a dictionary matching the source_blueprint.json schema.
        """
        blueprint_id = str(uuid4())
        profile_map = {(p.schema, p.name): p for p in profiles}

        tables = []
        for table_meta in crawl.tables:
            key = (table_meta.schema, table_meta.name)
            profile = profile_map.get(key)

            columns = []
            if profile:
                for cp in profile.column_profiles:
                    columns.append({
                        "name": cp.name,
                        "data_type": cp.data_type,
                        "nullable": cp.null_ratio > 0,
                        "is_primary_key": False,  # Refined by FK analysis
                        "stats": cp.to_dict(),
                    })
            else:
                for col in table_meta.columns:
                    columns.append({
                        "name": col.name,
                        "data_type": col.data_type,
                        "nullable": col.nullable,
                        "is_primary_key": col.is_primary_key,
                    })

            tables.append({
                "name": table_meta.name,
                "schema": table_meta.schema,
                "row_count": table_meta.row_count,
                "size_bytes": table_meta.size_bytes,
                "columns": columns,
                "foreign_keys": table_meta.foreign_keys,
                "business_rules": [],
            })

        # Build relationship graph from foreign keys
        relationships = self._extract_relationships(crawl)

        blueprint = {
            "blueprint_id": blueprint_id,
            "source_system": {
                "name": crawl.foreign_catalog,
                "type": crawl.source_type,
            },
            "profiled_at": datetime.now(timezone.utc).isoformat(),
            "profiled_by": self.config.model_serving_endpoint,
            "tables": tables,
            "relationships": relationships,
            "security_model": {
                "rbac_roles": [],
                "row_level_security": False,
                "column_masking": [],
            },
        }

        logger.info(
            f"Generated blueprint {blueprint_id}: "
            f"{len(tables)} tables, {len(relationships)} relationships"
        )
        return blueprint

    def save_blueprint(self, blueprint: dict) -> str:
        """Persist a blueprint as a Delta Table row in Unity Catalog.

        Uses Delta time travel for versioning — every save is a new version,
        enabling drift detection between profiling runs.
        """
        self._ensure_blueprint_table()

        blueprint_json = json.dumps(blueprint, default=str)
        row_data = [(
            blueprint["blueprint_id"],
            blueprint["source_system"]["name"],
            blueprint["source_system"]["type"],
            blueprint["profiled_at"],
            len(blueprint["tables"]),
            sum(len(t["columns"]) for t in blueprint["tables"]),
            blueprint_json,
        )]

        schema = StructType([
            StructField("blueprint_id", StringType(), False),
            StructField("source_name", StringType(), False),
            StructField("source_type", StringType(), False),
            StructField("profiled_at", StringType(), False),
            StructField("table_count", StringType(), False),
            StructField("column_count", StringType(), False),
            StructField("blueprint_json", StringType(), False),
        ])

        df = self.spark.createDataFrame(row_data, schema)
        df.write.format("delta").mode("append").saveAsTable(self.fqn)

        logger.info(f"Saved blueprint {blueprint['blueprint_id']} to {self.fqn}")
        return blueprint["blueprint_id"]

    def load_blueprint(self, blueprint_id: str) -> dict:
        """Load a blueprint by ID from the Delta Table."""
        row = (
            self.spark.read.table(self.fqn)
            .filter(F.col("blueprint_id") == blueprint_id)
            .orderBy(F.desc("profiled_at"))
            .limit(1)
            .collect()
        )
        if not row:
            raise ValueError(f"Blueprint not found: {blueprint_id}")
        return json.loads(row[0]["blueprint_json"])

    def list_blueprints(self) -> list[dict]:
        """List all saved blueprints with summary metadata."""
        try:
            df = self.spark.read.table(self.fqn).select(
                "blueprint_id", "source_name", "source_type",
                "profiled_at", "table_count", "column_count",
            )
            return [row.asDict() for row in df.orderBy(F.desc("profiled_at")).collect()]
        except Exception:
            return []

    def detect_drift(
        self, blueprint_id: str, previous_version: Optional[int] = None
    ) -> dict:
        """Compare current blueprint against a previous Delta version.

        Uses Delta time travel to detect schema drift between profiling runs.
        """
        current = self.load_blueprint(blueprint_id)

        if previous_version is not None:
            prev_df = (
                self.spark.read.format("delta")
                .option("versionAsOf", previous_version)
                .table(self.fqn)
                .filter(F.col("source_name") == current["source_system"]["name"])
                .orderBy(F.desc("profiled_at"))
                .limit(1)
            )
            prev_rows = prev_df.collect()
            if not prev_rows:
                return {"drift_detected": False, "message": "No previous version found"}
            previous = json.loads(prev_rows[0]["blueprint_json"])
        else:
            return {"drift_detected": False, "message": "No previous version specified"}

        # Compare table lists
        current_tables = {t["name"] for t in current["tables"]}
        previous_tables = {t["name"] for t in previous["tables"]}

        added = current_tables - previous_tables
        removed = previous_tables - current_tables

        # Compare columns for shared tables
        column_changes = []
        for table_name in current_tables & previous_tables:
            curr_cols = {
                c["name"]: c for t in current["tables"]
                if t["name"] == table_name for c in t["columns"]
            }
            prev_cols = {
                c["name"]: c for t in previous["tables"]
                if t["name"] == table_name for c in t["columns"]
            }
            new_cols = set(curr_cols) - set(prev_cols)
            dropped_cols = set(prev_cols) - set(curr_cols)
            if new_cols or dropped_cols:
                column_changes.append({
                    "table": table_name,
                    "added_columns": list(new_cols),
                    "removed_columns": list(dropped_cols),
                })

        drift_detected = bool(added or removed or column_changes)
        return {
            "drift_detected": drift_detected,
            "tables_added": list(added),
            "tables_removed": list(removed),
            "column_changes": column_changes,
        }

    def _extract_relationships(self, crawl: CrawlResult) -> list[dict]:
        """Build relationship graph from foreign key metadata."""
        relationships = []
        for table in crawl.tables:
            for fk in table.foreign_keys:
                relationships.append({
                    "from_table": f"{table.schema}.{table.name}",
                    "to_table": f"{table.schema}.{fk.get('referenced_table', fk.get('references_table', ''))}",
                    "relationship_type": "one_to_many",
                    "join_columns": [{
                        "from_column": fk.get("fk_column", fk.get("column", "")),
                        "to_column": fk.get("referenced_column", fk.get("references_column", "")),
                    }],
                })
        return relationships

    def _ensure_blueprint_table(self) -> None:
        """Create the blueprint Delta Table if it doesn't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.config.catalog}`.`{self.config.schema}`")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.fqn} (
                blueprint_id STRING NOT NULL,
                source_name STRING NOT NULL,
                source_type STRING NOT NULL,
                profiled_at STRING NOT NULL,
                table_count STRING,
                column_count STRING,
                blueprint_json STRING
            )
            USING DELTA
            COMMENT 'Source Environment Blueprints for Synthetic Generation'
        """)

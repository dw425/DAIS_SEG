"""Workspace validator — orchestrates end-to-end validation of a synthetic workspace.

The top-level validation entry point: takes a synthetic workspace and its
blueprint, runs schema validation, data fidelity checks, and quality
assessment, then produces a unified confidence report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession

from dais_seg.config import get_config
from dais_seg.validator.schema_validator import SchemaValidator
from dais_seg.validator.data_fidelity import DataFidelityValidator
from dais_seg.validator.confidence_scorer import (
    ConfidenceScorer,
    TableConfidence,
    WorkspaceConfidence,
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Complete validation report for a synthetic workspace."""

    workspace_id: str
    blueprint_id: str
    confidence: WorkspaceConfidence
    table_details: list[TableConfidence] = field(default_factory=list)
    quality_violations: list[dict] = field(default_factory=list)
    validated_tables: int = 0
    total_tables: int = 0

    def to_dict(self) -> dict:
        """Convert report to a dictionary for storage/display."""
        return {
            "workspace_id": self.workspace_id,
            "blueprint_id": self.blueprint_id,
            "overall_score": self.confidence.overall_score,
            "overall_level": self.confidence.level.value,
            "summary": self.confidence.summary,
            "validated_tables": self.validated_tables,
            "total_tables": self.total_tables,
            "quality_violations": len(self.quality_violations),
            "tables": [
                {
                    "name": t.table_name,
                    "score": t.overall_score,
                    "level": t.level.value,
                    "recommendations": t.recommendations,
                }
                for t in self.table_details
            ],
        }


class WorkspaceValidator:
    """Orchestrates complete validation of a synthetic workspace.

    Runs all validation pillars against each table in the workspace:
    1. Schema validation — structure matches blueprint
    2. Data fidelity — distributions match profiled statistics
    3. Quality compliance — DLT expectations pass
    4. Confidence scoring — aggregate into green/amber/red

    Results are stored as a Delta Table for audit and Genie dashboard queries.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = get_config()
        self.schema_validator = SchemaValidator(spark)
        self.fidelity_validator = DataFidelityValidator(spark)
        self.confidence_scorer = ConfidenceScorer()

    def validate(
        self,
        workspace_id: str,
        blueprint: dict,
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None,
        quality_violations: Optional[list[dict]] = None,
        pipeline_results: Optional[dict] = None,
        scale_factor: float = 1.0,
    ) -> ValidationReport:
        """Run full validation on a synthetic workspace.

        Args:
            workspace_id: Identifier for the synthetic workspace.
            blueprint: Source Blueprint dictionary.
            target_catalog: Catalog containing the synthetic tables.
            target_schema: Schema containing the Gold-layer tables.
            quality_violations: Violations from the conformer quality rules.
            pipeline_results: Results from medallion pipeline execution.
            scale_factor: Scale factor used during generation.
        """
        catalog = target_catalog or self.config.catalog
        schema = target_schema or self.config.gold_schema
        blueprint_id = blueprint.get("blueprint_id", "unknown")
        tables = blueprint.get("tables", [])

        logger.info(
            f"Validating workspace {workspace_id}: "
            f"{len(tables)} tables in {catalog}.{schema}"
        )

        table_scores = []
        for table_spec in tables:
            table_name = table_spec["name"]
            table_fqn = f"`{catalog}`.`{schema}`.`{table_name}`"

            # Schema validation
            schema_result = self.schema_validator.validate_table(table_fqn, table_spec)

            # Data fidelity validation
            fidelity_result = self.fidelity_validator.validate_table(
                table_fqn, table_spec, scale_factor
            )

            # Pipeline success for this table
            pipeline_ok = True
            if pipeline_results:
                pipeline_ok = table_name in pipeline_results.get("tables_processed", [])

            # Score this table
            table_confidence = self.confidence_scorer.score_table(
                table_name=table_name,
                schema_result=schema_result,
                fidelity_result=fidelity_result,
                quality_violations=quality_violations,
                pipeline_success=pipeline_ok,
            )
            table_scores.append(table_confidence)

            logger.info(
                f"  {table_name}: {table_confidence.overall_score:.1%} "
                f"({table_confidence.level.value})"
            )

        # Workspace-level confidence
        workspace_confidence = self.confidence_scorer.score_workspace(
            workspace_id, table_scores
        )

        report = ValidationReport(
            workspace_id=workspace_id,
            blueprint_id=blueprint_id,
            confidence=workspace_confidence,
            table_details=table_scores,
            quality_violations=quality_violations or [],
            validated_tables=len(table_scores),
            total_tables=len(tables),
        )

        # Save report to Delta
        self._save_report(report)

        logger.info(
            f"Validation complete: {workspace_confidence.summary}"
        )
        return report

    def _save_report(self, report: ValidationReport) -> None:
        """Persist the validation report as a Delta Table row."""
        import json

        report_fqn = (
            f"`{self.config.catalog}`.`{self.config.schema}`.`validation_reports`"
        )

        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {report_fqn} (
                    workspace_id STRING,
                    blueprint_id STRING,
                    overall_score DOUBLE,
                    overall_level STRING,
                    summary STRING,
                    report_json STRING,
                    validated_at TIMESTAMP
                ) USING DELTA
            """)

            report_dict = report.to_dict()
            from pyspark.sql import functions as F

            row_df = self.spark.createDataFrame(
                [(
                    report.workspace_id,
                    report.blueprint_id,
                    report.confidence.overall_score,
                    report.confidence.level.value,
                    report.confidence.summary,
                    json.dumps(report_dict, default=str),
                )],
                ["workspace_id", "blueprint_id", "overall_score",
                 "overall_level", "summary", "report_json"],
            ).withColumn("validated_at", F.current_timestamp())

            row_df.write.format("delta").mode("append").saveAsTable(report_fqn)
            logger.info(f"Report saved to {report_fqn}")
        except Exception as e:
            logger.warning(f"Failed to save report to Delta: {e}")

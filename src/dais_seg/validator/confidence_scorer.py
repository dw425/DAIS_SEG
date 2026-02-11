"""Confidence scorer — produces green/amber/red migration readiness scores.

Aggregates results from schema validation, data fidelity, quality rules,
and performance benchmarks into a unified confidence score per table and
per workspace — the decision gate for production cutover.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from dais_seg.config import get_config
from dais_seg.validator.schema_validator import SchemaComparisonResult
from dais_seg.validator.data_fidelity import TableFidelityResult

logger = logging.getLogger(__name__)


class ConfidenceLevel(str, Enum):
    """Traffic-light confidence level."""

    GREEN = "green"    # >= 90% — ready for production
    AMBER = "amber"    # >= 70% — needs attention
    RED = "red"        # < 70% — not ready


@dataclass
class DimensionScore:
    """Score for a single validation dimension."""

    dimension: str
    score: float
    weight: float
    level: ConfidenceLevel
    details: dict = field(default_factory=dict)


@dataclass
class TableConfidence:
    """Confidence assessment for a single table."""

    table_name: str
    overall_score: float
    level: ConfidenceLevel
    dimensions: list[DimensionScore] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)


@dataclass
class WorkspaceConfidence:
    """Confidence assessment for an entire synthetic workspace."""

    workspace_id: str
    overall_score: float
    level: ConfidenceLevel
    table_scores: list[TableConfidence] = field(default_factory=list)
    summary: str = ""


class ConfidenceScorer:
    """Produces confidence scores from validation results.

    Weighs four dimensions:
    - Schema parity (25%): columns, types, constraints match
    - Data fidelity (35%): distributions, null ratios, cardinality match
    - Quality compliance (20%): DLT expectations pass rate
    - Pipeline integrity (20%): successful medallion conformance

    Outputs green/amber/red per table and workspace.
    """

    DIMENSION_WEIGHTS = {
        "schema": 0.25,
        "data_fidelity": 0.35,
        "quality": 0.20,
        "pipeline": 0.20,
    }

    def __init__(self):
        self.config = get_config()

    def score_table(
        self,
        table_name: str,
        schema_result: Optional[SchemaComparisonResult] = None,
        fidelity_result: Optional[TableFidelityResult] = None,
        quality_violations: Optional[list[dict]] = None,
        pipeline_success: bool = True,
    ) -> TableConfidence:
        """Score a single table across all validation dimensions."""
        dimensions = []
        recommendations = []

        # Schema dimension
        if schema_result:
            schema_score = schema_result.score
            schema_dim = DimensionScore(
                dimension="schema",
                score=schema_score,
                weight=self.DIMENSION_WEIGHTS["schema"],
                level=self._to_level(schema_score),
                details={
                    "missing_columns": schema_result.missing_columns,
                    "type_mismatches": schema_result.type_mismatches,
                },
            )
            dimensions.append(schema_dim)
            if schema_result.missing_columns:
                recommendations.append(
                    f"Missing columns: {', '.join(schema_result.missing_columns)}"
                )
            if schema_result.type_mismatches:
                recommendations.append(
                    f"Type mismatches in {len(schema_result.type_mismatches)} columns"
                )
        else:
            dimensions.append(
                DimensionScore("schema", 1.0, self.DIMENSION_WEIGHTS["schema"], ConfidenceLevel.GREEN)
            )

        # Data fidelity dimension
        if fidelity_result:
            fidelity_score = fidelity_result.overall_score
            fidelity_dim = DimensionScore(
                dimension="data_fidelity",
                score=fidelity_score,
                weight=self.DIMENSION_WEIGHTS["data_fidelity"],
                level=self._to_level(fidelity_score),
                details={
                    "row_count_match": fidelity_result.row_count_match,
                    "rows_expected": fidelity_result.row_count_expected,
                    "rows_actual": fidelity_result.row_count_actual,
                },
            )
            dimensions.append(fidelity_dim)
            if not fidelity_result.row_count_match:
                recommendations.append(
                    f"Row count deviation: expected {fidelity_result.row_count_expected:,}, "
                    f"got {fidelity_result.row_count_actual:,}"
                )
        else:
            dimensions.append(
                DimensionScore("data_fidelity", 1.0, self.DIMENSION_WEIGHTS["data_fidelity"], ConfidenceLevel.GREEN)
            )

        # Quality dimension
        table_violations = [
            v for v in (quality_violations or []) if v.get("table") == table_name
        ]
        if quality_violations is not None:
            quality_score = 1.0 if not table_violations else max(
                0.0, 1.0 - (len(table_violations) * 0.1)
            )
            quality_dim = DimensionScore(
                dimension="quality",
                score=quality_score,
                weight=self.DIMENSION_WEIGHTS["quality"],
                level=self._to_level(quality_score),
                details={"violations": len(table_violations)},
            )
            dimensions.append(quality_dim)
            if table_violations:
                recommendations.append(
                    f"{len(table_violations)} quality violations detected"
                )
        else:
            dimensions.append(
                DimensionScore("quality", 1.0, self.DIMENSION_WEIGHTS["quality"], ConfidenceLevel.GREEN)
            )

        # Pipeline dimension
        pipeline_score = 1.0 if pipeline_success else 0.0
        dimensions.append(
            DimensionScore(
                dimension="pipeline",
                score=pipeline_score,
                weight=self.DIMENSION_WEIGHTS["pipeline"],
                level=self._to_level(pipeline_score),
            )
        )
        if not pipeline_success:
            recommendations.append("Medallion pipeline failed for this table")

        # Weighted overall score
        overall = sum(d.score * d.weight for d in dimensions)

        return TableConfidence(
            table_name=table_name,
            overall_score=round(overall, 4),
            level=self._to_level(overall),
            dimensions=dimensions,
            recommendations=recommendations,
        )

    def score_workspace(
        self,
        workspace_id: str,
        table_scores: list[TableConfidence],
    ) -> WorkspaceConfidence:
        """Aggregate table scores into a workspace-level confidence score."""
        if not table_scores:
            return WorkspaceConfidence(
                workspace_id=workspace_id,
                overall_score=0.0,
                level=ConfidenceLevel.RED,
                summary="No tables validated",
            )

        overall = sum(t.overall_score for t in table_scores) / len(table_scores)
        level = self._to_level(overall)

        green_count = sum(1 for t in table_scores if t.level == ConfidenceLevel.GREEN)
        amber_count = sum(1 for t in table_scores if t.level == ConfidenceLevel.AMBER)
        red_count = sum(1 for t in table_scores if t.level == ConfidenceLevel.RED)

        summary = (
            f"{len(table_scores)} tables validated: "
            f"{green_count} green, {amber_count} amber, {red_count} red. "
            f"Overall confidence: {overall:.1%} ({level.value})"
        )

        return WorkspaceConfidence(
            workspace_id=workspace_id,
            overall_score=round(overall, 4),
            level=level,
            table_scores=table_scores,
            summary=summary,
        )

    def _to_level(self, score: float) -> ConfidenceLevel:
        """Convert a numeric score to a confidence level."""
        if score >= self.config.confidence_green:
            return ConfidenceLevel.GREEN
        elif score >= self.config.confidence_amber:
            return ConfidenceLevel.AMBER
        return ConfidenceLevel.RED

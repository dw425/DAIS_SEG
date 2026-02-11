"""Tests for the Validator pillar."""

import pytest
from dais_seg.validator.confidence_scorer import (
    ConfidenceScorer,
    ConfidenceLevel,
    TableConfidence,
)
from dais_seg.validator.schema_validator import SchemaComparisonResult
from dais_seg.validator.data_fidelity import TableFidelityResult


class TestConfidenceScorer:
    """Tests for ConfidenceScorer."""

    def setup_method(self):
        self.scorer = ConfidenceScorer()

    def test_perfect_score_is_green(self):
        schema = SchemaComparisonResult(
            table_name="test", match=True, score=1.0,
            missing_columns=[], extra_columns=[], type_mismatches=[],
            total_expected=10, total_matched=10,
        )
        fidelity = TableFidelityResult(
            table_name="test", overall_score=1.0,
            row_count_match=True, row_count_expected=1000, row_count_actual=1000,
        )
        result = self.scorer.score_table(
            "test", schema_result=schema, fidelity_result=fidelity,
            quality_violations=[], pipeline_success=True,
        )
        assert result.level == ConfidenceLevel.GREEN
        assert result.overall_score >= 0.9

    def test_missing_columns_reduces_score(self):
        schema = SchemaComparisonResult(
            table_name="test", match=False, score=0.5,
            missing_columns=["col_a", "col_b"], extra_columns=[],
            type_mismatches=[], total_expected=4, total_matched=2,
        )
        result = self.scorer.score_table("test", schema_result=schema)
        assert result.overall_score < 1.0
        assert len(result.recommendations) > 0

    def test_pipeline_failure_is_red(self):
        result = self.scorer.score_table("test", pipeline_success=False)
        assert result.overall_score < 1.0

    def test_workspace_scoring(self):
        tables = [
            TableConfidence("t1", 0.95, ConfidenceLevel.GREEN),
            TableConfidence("t2", 0.85, ConfidenceLevel.AMBER),
            TableConfidence("t3", 0.60, ConfidenceLevel.RED),
        ]
        ws = self.scorer.score_workspace("ws1", tables)
        assert ws.overall_score == pytest.approx(0.8, abs=0.01)
        assert "3 tables validated" in ws.summary

    def test_confidence_levels(self):
        assert self.scorer._to_level(0.95) == ConfidenceLevel.GREEN
        assert self.scorer._to_level(0.80) == ConfidenceLevel.AMBER
        assert self.scorer._to_level(0.50) == ConfidenceLevel.RED

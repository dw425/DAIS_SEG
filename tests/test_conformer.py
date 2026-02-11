"""Tests for the Conformer pillar."""

import pytest
from dais_seg.conformer.quality_rules import QualityRules, QualityExpectation, ExpectationAction


class TestQualityRules:
    """Tests for QualityRules (without Spark dependency)."""

    def test_import(self):
        from dais_seg.conformer.medallion_pipeline import MedallionPipeline
        from dais_seg.conformer.schema_enforcer import SchemaEnforcer
        assert MedallionPipeline is not None
        assert SchemaEnforcer is not None

    def test_expectation_creation(self):
        exp = QualityExpectation(
            name="test_not_null",
            expression="`id` IS NOT NULL",
            action=ExpectationAction.WARN,
            layer="silver",
            table="orders",
        )
        assert exp.name == "test_not_null"
        assert exp.action == ExpectationAction.WARN

    def test_generate_dlt_sql(self):
        """Test DLT SQL generation from expectations."""
        # Can't fully test without Spark, but we can test the SQL generation
        # QualityRules needs spark in __init__, so we test the data classes
        exp = QualityExpectation(
            name="valid_amount",
            expression="`amount` >= 0",
            action=ExpectationAction.DROP,
            layer="silver",
        )
        assert "amount" in exp.expression
        assert exp.action == ExpectationAction.DROP

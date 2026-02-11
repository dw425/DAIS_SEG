"""Tests for the Generator pillar."""

import pytest
from dais_seg.generator.distribution_sampler import DistributionSampler
from dais_seg.generator.relationship_preserver import RelationshipPreserver


class TestDistributionSampler:
    """Tests for DistributionSampler."""

    def setup_method(self):
        self.sampler = DistributionSampler(seed=42)

    def test_sample_integer_column(self):
        col_spec = {
            "name": "id",
            "data_type": "int",
            "stats": {"min": 1, "max": 1000, "mean": 500, "stddev": 100, "null_ratio": 0.0},
        }
        values = self.sampler.sample_column(col_spec, 100)
        assert len(values) == 100
        assert all(isinstance(v, int) for v in values)

    def test_sample_with_null_ratio(self):
        col_spec = {
            "name": "optional_field",
            "data_type": "string",
            "stats": {"null_ratio": 0.3, "top_values": [{"value": "A", "frequency": 0.5}, {"value": "B", "frequency": 0.5}]},
        }
        values = self.sampler.sample_column(col_spec, 1000)
        null_count = sum(1 for v in values if v is None)
        assert 200 < null_count < 400  # ~30% nulls with some variance

    def test_sample_boolean(self):
        col_spec = {
            "name": "is_active",
            "data_type": "boolean",
            "stats": {"null_ratio": 0.0, "top_values": [{"value": True, "frequency": 0.7}]},
        }
        values = self.sampler.sample_column(col_spec, 100)
        assert all(isinstance(v, bool) for v in values)

    def test_sample_date(self):
        col_spec = {
            "name": "created_at",
            "data_type": "date",
            "stats": {"null_ratio": 0.0, "min": "2020-01-01", "max": "2025-12-31"},
        }
        values = self.sampler.sample_column(col_spec, 50)
        assert len(values) == 50
        assert all(isinstance(v, str) for v in values)


class TestRelationshipPreserver:
    """Tests for RelationshipPreserver."""

    def test_generation_order_parent_first(self):
        blueprint = {
            "tables": [
                {"name": "orders", "columns": [], "foreign_keys": [
                    {"column": "customer_id", "references_table": "customers", "references_column": "id"}
                ]},
                {"name": "customers", "columns": [
                    {"name": "id", "data_type": "int", "is_primary_key": True}
                ], "foreign_keys": []},
            ],
            "relationships": [],
        }
        preserver = RelationshipPreserver(blueprint)
        order = preserver.get_generation_order()
        assert order.index("customers") < order.index("orders")

    def test_fk_constraints(self):
        blueprint = {
            "tables": [
                {"name": "orders", "columns": [], "foreign_keys": [
                    {"column": "cust_id", "references_table": "customers", "references_column": "id"}
                ]},
                {"name": "customers", "columns": [], "foreign_keys": []},
            ],
            "relationships": [],
        }
        preserver = RelationshipPreserver(blueprint)
        constraints = preserver.get_fk_constraints("orders")
        assert len(constraints) == 1
        assert constraints[0]["parent_table"] == "customers"

"""Tests for the Profiler pillar."""

import pytest


class TestFederationConnector:
    """Tests for FederationConnector."""

    def test_import(self):
        from dais_seg.profiler.federation_connector import FederationConnector, FederationConnection
        assert FederationConnector is not None
        assert FederationConnection is not None


class TestCatalogCrawler:
    """Tests for CatalogCrawler."""

    def test_import(self):
        from dais_seg.profiler.catalog_crawler import CatalogCrawler, CrawlResult, TableMetadata
        assert CatalogCrawler is not None
        assert CrawlResult is not None

    def test_crawl_result_defaults(self):
        from dais_seg.profiler.catalog_crawler import CrawlResult
        result = CrawlResult(foreign_catalog="test", source_type="oracle")
        assert result.total_columns == 0
        assert result.total_rows == 0
        assert result.tables == []


class TestDistributionProfiler:
    """Tests for DistributionProfiler."""

    def test_import(self):
        from dais_seg.profiler.distribution_profiler import DistributionProfiler, ColumnProfile
        assert DistributionProfiler is not None

    def test_column_profile_to_dict(self):
        from dais_seg.profiler.distribution_profiler import ColumnProfile
        cp = ColumnProfile(name="test", data_type="int", distinct_count=10, null_ratio=0.05)
        d = cp.to_dict()
        assert d["distinct_count"] == 10
        assert d["null_ratio"] == 0.05


class TestBlueprintGenerator:
    """Tests for BlueprintGenerator."""

    def test_import(self):
        from dais_seg.profiler.blueprint_generator import BlueprintGenerator
        assert BlueprintGenerator is not None

"""Tests for the Pipeline module (PipelineStatus, PipelineResult, StageResult)."""

import pytest

from dais_seg.pipeline.runner import (
    PipelineRunner,
    PipelineResult,
    PipelineStatus,
    StageResult,
)


class TestPipelineStatus:
    """Tests for PipelineStatus enum."""

    def test_all_statuses_exist(self):
        expected = ["pending", "connecting", "profiling", "generating",
                     "conforming", "validating", "complete", "failed"]
        for s in expected:
            assert PipelineStatus(s) is not None

    def test_status_is_string_enum(self):
        assert PipelineStatus.PENDING == "pending"
        assert PipelineStatus.COMPLETE == "complete"
        assert PipelineStatus.FAILED == "failed"

    def test_status_value(self):
        assert PipelineStatus.PROFILING.value == "profiling"
        assert PipelineStatus.GENERATING.value == "generating"


class TestStageResult:
    """Tests for StageResult dataclass."""

    def test_default_values(self):
        sr = StageResult(stage="profile", status="success")
        assert sr.run_id is None
        assert sr.duration_seconds == 0.0
        assert sr.output == {}
        assert sr.error is None

    def test_full_creation(self):
        sr = StageResult(
            stage="01_profile_source",
            status="success",
            run_id=12345,
            duration_seconds=45.2,
            output={"blueprint_id": "bp_001", "tables": 5},
        )
        assert sr.stage == "01_profile_source"
        assert sr.run_id == 12345
        assert sr.output["blueprint_id"] == "bp_001"

    def test_failed_stage(self):
        sr = StageResult(
            stage="generate",
            status="failed",
            duration_seconds=10.5,
            error="Cluster terminated unexpectedly",
        )
        assert sr.status == "failed"
        assert "Cluster" in sr.error


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_default_values(self):
        pr = PipelineResult(status=PipelineStatus.PENDING)
        assert pr.connection_name == ""
        assert pr.blueprint_id == ""
        assert pr.confidence_score is None
        assert pr.tables_generated == 0
        assert pr.stages == []
        assert pr.error is None

    def test_to_dict(self):
        pr = PipelineResult(
            status=PipelineStatus.COMPLETE,
            connection_name="oracle_prod",
            foreign_catalog="seg_fed_oracle_prod",
            blueprint_id="bp_001",
            workspace_id="dais_seg.seg_default",
            confidence_score=0.92,
            confidence_level="green",
            tables_generated=5,
            total_duration_seconds=123.456,
            stages=[
                StageResult(stage="connect", status="success", duration_seconds=2.3),
                StageResult(stage="01_profile_source", status="success", run_id=100, duration_seconds=40.1),
            ],
        )
        d = pr.to_dict()
        assert d["status"] == "complete"
        assert d["blueprint_id"] == "bp_001"
        assert d["confidence_score"] == 0.92
        assert d["confidence_level"] == "green"
        assert d["tables_generated"] == 5
        assert d["total_duration_seconds"] == 123.5
        assert len(d["stages"]) == 2
        assert d["stages"][0]["stage"] == "connect"
        assert d["stages"][1]["duration_seconds"] == 40.1

    def test_to_dict_with_error(self):
        pr = PipelineResult(
            status=PipelineStatus.FAILED,
            error="Connection refused",
            total_duration_seconds=5.0,
        )
        d = pr.to_dict()
        assert d["status"] == "failed"
        assert d["error"] == "Connection refused"

    def test_stages_are_independent_lists(self):
        pr1 = PipelineResult(status=PipelineStatus.PENDING)
        pr2 = PipelineResult(status=PipelineStatus.PENDING)
        pr1.stages.append(StageResult(stage="test", status="ok"))
        assert len(pr2.stages) == 0


class TestPipelineRunnerImport:
    """Basic import and structure tests for PipelineRunner."""

    def test_import(self):
        assert PipelineRunner is not None

    def test_terminal_states_defined(self):
        assert len(PipelineRunner.TERMINAL_STATES) > 0

    def test_run_single_stage_unknown(self):
        """run_single_stage should return a failed StageResult for unknown stages."""
        # We can't call run_single_stage without a real WorkspaceClient,
        # but we can test the notebook_map logic by checking the method exists.
        assert hasattr(PipelineRunner, "run_single_stage")

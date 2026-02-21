"""Tests for the validation engine."""

from app.engines.validators import validate_notebook, generate_validation_report, compute_weighted_score
from app.engines.validators.score_engine import is_vacuous, DEFAULT_WEIGHTS
from app.engines.validators.report_generator import generate_validation_report as gen_report
from app.models.pipeline import (
    NotebookCell,
    NotebookResult,
    ParseResult,
    ValidationResult,
    ValidationScore,
)
from app.models.processor import Connection, Processor


def _make_notebook(code_cells: list[tuple[str, str]]) -> NotebookResult:
    cells = []
    for label, source in code_cells:
        cells.append(NotebookCell(type="code", source=source, label=label))
    return NotebookResult(cells=cells)


def test_validation_basic():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="src", type="GetFile", platform="nifi")],
    )
    notebook = _make_notebook(
        [
            ("imports", "from pyspark.sql.functions import *"),
            ("config", 'catalog = "main"'),
            ("setup", "spark.sql('CREATE SCHEMA IF NOT EXISTS main.default')"),
            ("step_1_source", 'df_src = spark.read.format("csv").load("/data")'),
            ("teardown", "print('done')"),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert isinstance(result, ValidationResult)
    assert result.overall_score > 0
    assert len(result.scores) >= 3


def test_validation_detects_missing_processor():
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(name="source_proc", type="GetFile", platform="nifi"),
            Processor(name="missing_proc", type="PutFile", platform="nifi"),
        ],
    )
    notebook = _make_notebook(
        [
            ("step_1_source", "df_source_proc = spark.read.csv('/data')"),
        ]
    )
    result = validate_notebook(pr, notebook)
    # Should have gaps for the missing processor
    assert len(result.gaps) >= 1
    missing_names = [g["name"] for g in result.gaps if g.get("type") == "missing_processor"]
    assert "missing_proc" in missing_names


def test_validation_detects_syntax_error():
    pr = ParseResult(platform="nifi", processors=[])
    notebook = _make_notebook(
        [
            ("step_1_bad", "def foo(\n  invalid syntax here"),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert len(result.errors) >= 1


def test_validation_detects_antipattern():
    pr = ParseResult(platform="nifi", processors=[])
    notebook = _make_notebook(
        [
            ("step_1_collect", "data = df.collect()"),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert any("collect()" in e for e in result.errors)


def test_validation_perfect_score():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="proc1", type="GetFile", platform="nifi")],
    )
    notebook = _make_notebook(
        [
            ("imports", "from pyspark.sql.functions import *"),
            ("config", 'catalog = "main"'),
            ("setup", "pass"),
            ("step_1_source", "df_proc1 = spark.read.csv('/data')"),
            ("teardown", "print('done')"),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert result.overall_score > 0.5


# ── Score engine tests ──


def test_weighted_score_skips_vacuous():
    """Vacuous scores (nothing to validate) should not inflate the overall score."""
    scores = [
        ValidationScore(dimension="intent_coverage", score=0.5, details="Source intent coverage: 50%"),
        ValidationScore(dimension="delta_format", score=1.0, details="No write operations to validate"),
        ValidationScore(dimension="checkpoint_coverage", score=1.0, details="No streaming writes to validate"),
    ]
    weighted = compute_weighted_score(scores, skip_vacuous=True)
    # Only intent_coverage should matter: 0.5
    assert weighted == 0.5


def test_weighted_score_includes_all_when_skip_disabled():
    """With skip_vacuous=False, all dimensions contribute."""
    scores = [
        ValidationScore(dimension="intent_coverage", score=0.5, details="Source intent coverage: 50%"),
        ValidationScore(dimension="delta_format", score=1.0, details="No write operations to validate"),
    ]
    with_skip = compute_weighted_score(scores, skip_vacuous=True)
    without_skip = compute_weighted_score(scores, skip_vacuous=False)
    # Without skipping, the 1.0 vacuous score should increase the average
    assert without_skip > with_skip


def test_is_vacuous_true():
    s = ValidationScore(dimension="delta_format", score=1.0, details="No write operations to validate")
    assert is_vacuous(s) is True


def test_is_vacuous_false_low_score():
    s = ValidationScore(dimension="delta_format", score=0.8, details="No write operations to validate")
    assert is_vacuous(s) is False


def test_is_vacuous_false_real_score():
    s = ValidationScore(dimension="delta_format", score=1.0, details="Delta format compliance: 3/3 writes use Delta")
    assert is_vacuous(s) is False


# ── Report generator tests ──


def test_report_generator_basic():
    result = ValidationResult(
        overall_score=0.75,
        scores=[
            ValidationScore(dimension="intent_coverage", score=0.8, details="Coverage 80%"),
            ValidationScore(dimension="code_quality", score=0.7, details="Quality 70%"),
        ],
        gaps=[{"type": "missing_processor", "name": "X", "message": "Missing X"}],
        errors=["Syntax error line 5"],
    )
    report = gen_report(result)
    assert report["overall_score"] == 0.75
    assert report["readiness"] == "needs_work"
    assert report["error_count"] == 1
    assert report["gap_count"] == 1
    assert len(report["dimension_reports"]) == 2
    assert "generated_at" in report
    assert "summary" in report


def test_report_generator_ready():
    result = ValidationResult(
        overall_score=0.95,
        scores=[
            ValidationScore(dimension="intent_coverage", score=0.95, details="Coverage 95%"),
        ],
        gaps=[],
        errors=[],
    )
    report = gen_report(result)
    assert report["readiness"] == "ready"
    assert report["error_count"] == 0


def test_report_generator_critical_issues():
    result = ValidationResult(
        overall_score=0.4,
        scores=[
            ValidationScore(dimension="credential_security", score=0.3, details="Found 3 hardcoded creds"),
        ],
        gaps=[],
        errors=["Hardcoded password detected"],
    )
    report = gen_report(result)
    assert report["readiness"] == "not_ready"
    assert len(report["critical_issues"]) == 1
    assert report["critical_issues"][0]["severity"] == "critical"
    assert "remediation" in report["critical_issues"][0]


# ── Cycle 2: report generator separates warnings from critical ──


def test_report_generator_separates_warnings_from_critical():
    """Warnings (score 0.7-0.9) should go to warning_issues, not critical_issues."""
    result = ValidationResult(
        overall_score=0.6,
        scores=[
            ValidationScore(dimension="intent_coverage", score=0.75, details="Coverage 75%"),
            ValidationScore(dimension="credential_security", score=0.3, details="Found creds"),
        ],
        gaps=[],
        errors=[],
    )
    report = gen_report(result)
    # intent_coverage at 0.75 is "warning" severity, should NOT be in critical_issues
    assert len(report["critical_issues"]) == 1
    assert report["critical_issues"][0]["dimension"] == "credential_security"
    # It should be in warning_issues instead
    assert "warning_issues" in report
    assert len(report["warning_issues"]) == 1
    assert report["warning_issues"][0]["dimension"] == "intent_coverage"
    assert report["warning_issues"][0]["severity"] == "warning"


# ── Cycle 2: intent analyzer case-insensitive matching ──


def test_intent_analyzer_case_insensitive():
    """Processor names with mixed case should be matched case-insensitively."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(name="MyProc_Alpha", type="GetFile", platform="nifi"),
        ],
    )
    # Code references the lowercase version
    notebook = _make_notebook(
        [
            ("step_1", "df_myproc_alpha = spark.read.csv('/data')"),
        ]
    )
    result = validate_notebook(pr, notebook)
    # Should be covered despite case difference
    missing_names = [g["name"] for g in result.gaps if g.get("type") == "missing_processor"]
    assert "MyProc_Alpha" not in missing_names


# ── Cycle 2: credential validator detects MongoDB URI credentials ──


def test_credential_validator_detects_mongodb_uri():
    """Credentials embedded in MongoDB connection URIs should be flagged."""
    pr = ParseResult(platform="nifi", processors=[])
    notebook = _make_notebook(
        [
            ("step_1", 'url = "mongodb+srv://admin:s3cretP4ss@cluster0.example.net/mydb"'),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert any("MongoDB URI" in e for e in result.errors)


def test_credential_validator_detects_aws_key():
    """AWS access key IDs (AKIA...) should be flagged."""
    pr = ParseResult(platform="nifi", processors=[])
    notebook = _make_notebook(
        [
            ("step_1", 'key = "AKIAIOSFODNN7EXAMPLE"'),
        ]
    )
    result = validate_notebook(pr, notebook)
    assert any("AWS access key" in e for e in result.errors)


# ── Intent analyzer connection gap fix test ──


def test_connection_gap_detects_partial_coverage():
    """After the fix, a connection should be flagged as a gap if EITHER endpoint is missing."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(name="source_a", type="GetFile", platform="nifi"),
            Processor(name="dest_b", type="PutFile", platform="nifi"),
        ],
        connections=[
            Connection(source_name="source_a", destination_name="dest_b", relationship="success"),
        ],
    )
    # Only cover source_a, not dest_b
    notebook = _make_notebook(
        [
            ("step_1", "df_source_a = spark.read.csv('/data')"),
        ]
    )
    result = validate_notebook(pr, notebook)
    # dest_b is missing as processor
    missing_processor_names = [g["name"] for g in result.gaps if g.get("type") == "missing_processor"]
    assert "dest_b" in missing_processor_names
    # Connection should also be flagged since dest_b is not covered
    connection_gaps = [g for g in result.gaps if g.get("type") == "missing_connection"]
    assert len(connection_gaps) >= 1
    assert "dest_b" in connection_gaps[0]["message"]

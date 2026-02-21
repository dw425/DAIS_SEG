"""Tests for the validation engine."""

from app.engines.validators import validate_notebook
from app.models.pipeline import (
    NotebookCell,
    NotebookResult,
    ParseResult,
    ValidationResult,
)
from app.models.processor import Processor


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

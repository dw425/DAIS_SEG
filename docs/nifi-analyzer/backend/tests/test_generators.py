"""Tests for the notebook generator."""

from app.engines.generators.code_validator import validate_python_syntax
from app.engines.generators.notebook_generator import generate_notebook
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, MappingEntry, NotebookResult, ParseResult
from app.models.processor import Processor


def test_generate_basic_notebook():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="src", type="GetFile", platform="nifi")],
        metadata={"source_file": "test.xml"},
    )
    assessment = AssessmentResult(
        mappings=[
            MappingEntry(
                name="src",
                type="GetFile",
                role="source",
                mapped=True,
                confidence=0.9,
                code='df_src = spark.read.format("csv").load("/data")',
            ),
        ],
    )
    config = DatabricksConfig()

    result = generate_notebook(pr, assessment, config)
    assert isinstance(result, NotebookResult)
    assert len(result.cells) >= 4  # title, imports, config, at least one step
    assert any(c.label == "title" for c in result.cells)
    assert any(c.label == "config" for c in result.cells)
    assert result.workflow is not None


def test_notebook_cells_have_types():
    pr = ParseResult(platform="nifi", processors=[], metadata={"source_file": "test.xml"})
    assessment = AssessmentResult(mappings=[])
    result = generate_notebook(pr, assessment, DatabricksConfig())
    for cell in result.cells:
        assert cell.type in ("code", "markdown")


def test_workflow_generation():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="p1", type="GetFile", platform="nifi")],
        metadata={"source_file": "flow.xml"},
    )
    assessment = AssessmentResult(
        mappings=[MappingEntry(name="p1", type="GetFile", role="source", mapped=True, confidence=0.9, code="x = 1")],
    )
    result = generate_notebook(pr, assessment, DatabricksConfig())
    assert "name" in result.workflow
    assert "tasks" in result.workflow


def test_validate_python_syntax_valid():
    errors = validate_python_syntax("x = 1\nprint(x)")
    assert len(errors) == 0


def test_validate_python_syntax_invalid():
    errors = validate_python_syntax("def foo(\n  x = ")
    assert len(errors) >= 1

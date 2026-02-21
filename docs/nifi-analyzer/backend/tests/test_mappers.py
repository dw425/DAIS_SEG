"""Tests for the NiFi mapper."""

from app.engines.mappers.nifi_mapper import map_nifi
from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import Processor


def _make_parse_result(*names_types):
    procs = [Processor(name=n, type=t, platform="nifi") for n, t in names_types]
    return ParseResult(platform="nifi", processors=procs)


def _empty_analysis():
    return AnalysisResult()


def test_nifi_mapper_basic():
    pr = _make_parse_result(("Get Data", "GetFile"), ("Write Data", "PutFile"))
    result = map_nifi(pr, _empty_analysis())
    assert len(result.mappings) == 2
    # Both should be mapped (present in YAML)
    assert result.mappings[0].mapped is True
    assert result.mappings[0].confidence > 0


def test_nifi_mapper_unmapped():
    pr = _make_parse_result(("Custom", "MyCustomProcessor"))
    result = map_nifi(pr, _empty_analysis())
    assert result.unmapped_count == 1
    assert result.mappings[0].mapped is False
    assert "UNMAPPED" in result.mappings[0].code


def test_nifi_mapper_roles():
    pr = _make_parse_result(
        ("src", "GetFile"),
        ("xform", "ReplaceText"),
        ("sink", "PutFile"),
        ("route", "RouteOnAttribute"),
    )
    result = map_nifi(pr, _empty_analysis())
    roles = {m.name: m.role for m in result.mappings}
    assert roles["src"] == "source"
    assert roles["xform"] == "transform"
    assert roles["sink"] == "sink"
    assert roles["route"] == "route"


def test_nifi_mapper_code_substitution():
    pr = _make_parse_result(("My Processor", "GetFile"))
    result = map_nifi(pr, _empty_analysis())
    # The code should contain the safe variable name
    assert "my_processor" in result.mappings[0].code.lower() or "{v}" not in result.mappings[0].code

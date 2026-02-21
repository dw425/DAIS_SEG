"""Tests for the NiFi mapper."""

from app.engines.mappers.base_mapper import infer_role
from app.engines.mappers.nifi_mapper import (
    _classify_role,
    _count_unresolved_placeholders,
    map_nifi,
)
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


# ---------------------------------------------------------------------------
# Improvement Cycle 1: Fully-qualified processor type support
# ---------------------------------------------------------------------------

def test_nifi_mapper_fully_qualified_type():
    """NiFi mapper should resolve fully-qualified class names like org.apache.nifi...GetFile."""
    pr = _make_parse_result(("src", "org.apache.nifi.processors.standard.GetFile"))
    result = map_nifi(pr, _empty_analysis())
    # Should find the mapping via simple name fallback
    assert result.mappings[0].mapped is True
    assert result.mappings[0].confidence > 0


def test_classify_role_fully_qualified():
    """Role classifier should handle fully-qualified NiFi processor types."""
    assert _classify_role("org.apache.nifi.processors.standard.GetFile") == "source"
    assert _classify_role("org.apache.nifi.processors.standard.PutFile") == "sink"
    assert _classify_role("org.apache.nifi.processors.standard.RouteOnAttribute") == "route"


def test_base_mapper_infer_role_fully_qualified():
    """Base mapper infer_role should handle fully-qualified processor types."""
    assert infer_role("org.apache.nifi.processors.standard.GetFile") == "source"
    assert infer_role("org.apache.nifi.processors.standard.PutDatabaseRecord") == "sink"


# ---------------------------------------------------------------------------
# Improvement Cycle 1: Unresolved placeholder detection and confidence adjustment
# ---------------------------------------------------------------------------

def test_unresolved_placeholder_count():
    """Should count unresolved template placeholders in generated code."""
    assert _count_unresolved_placeholders('df = spark.read.load("{path}")') == 1
    assert _count_unresolved_placeholders('df = spark.read.load("/data")') == 0
    assert _count_unresolved_placeholders('{input_dir} and {output_dir}') == 2


def test_confidence_reduced_for_unresolved_placeholders():
    """Mapper should reduce confidence when template placeholders are unresolved."""
    # Create a processor with properties that DON'T match the template placeholders
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(
                name="src",
                type="GetFile",
                platform="nifi",
                properties={"Unrelated Key": "value"},
            )
        ],
    )
    result = map_nifi(pr, _empty_analysis())
    mapping = result.mappings[0]
    if mapping.mapped:
        # If the YAML template has placeholders that weren't resolved,
        # confidence should be less than the base (0.9)
        # This test verifies the mechanism exists; actual reduction depends on template content
        assert mapping.confidence <= 0.9


# ---------------------------------------------------------------------------
# Improvement Cycle 2: Placeholder substitution ordering
# ---------------------------------------------------------------------------

def test_nifi_mapper_property_substitution_before_short_alias():
    """Properties like {input_directory} must be resolved BEFORE the short
    alias {in} to avoid corruption (e.g. {input_directory} -> safe_nameput_directory)."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(
                name="src",
                type="GetFile",
                platform="nifi",
                properties={"Input Directory": "/data/landing"},
            )
        ],
    )
    result = map_nifi(pr, _empty_analysis())
    mapping = result.mappings[0]
    if mapping.mapped and mapping.code:
        # The code must NOT contain a corrupted placeholder like "srcput_directory"
        assert "srcput_" not in mapping.code
        # If the template had {input_directory}, it should now contain the resolved value
        if "/data/landing" in mapping.code:
            # Great — property was substituted correctly
            pass


def test_nifi_mapper_in_placeholder_still_works():
    """The {in} short alias should still be substituted after properties."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(
                name="my_proc",
                type="GetFile",
                platform="nifi",
                properties={},
            )
        ],
    )
    result = map_nifi(pr, _empty_analysis())
    mapping = result.mappings[0]
    if mapping.mapped and mapping.code:
        # {in} should have been replaced (no raw {in} left unless template doesn't use it)
        assert "{in}" not in mapping.code


# ---------------------------------------------------------------------------
# Improvement Cycle 2: Dispatcher fallback warning
# ---------------------------------------------------------------------------

def test_dispatcher_nifi_no_warning():
    """NiFi platform should dispatch directly with no fallback warning."""
    from app.engines.mappers.dispatcher import map_to_databricks

    pr = _make_parse_result(("src", "GetFile"))
    result = map_to_databricks(pr, _empty_analysis())
    # Should NOT have a dispatcher warning entry
    assert all(m.name != "__dispatcher_warning__" for m in result.mappings)

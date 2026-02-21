"""Classify exceptions into user-friendly error responses with remediation hints."""

from app.utils.errors import (
    AnalysisError,
    GenerationError,
    MappingError,
    ParseError,
    UnsupportedFormatError,
    ValidationError,
)

# Category constants
PARSE_ERROR = "parse_error"
MAPPING_ERROR = "mapping_error"
GENERATION_ERROR = "generation_error"
VALIDATION_ERROR = "validation_error"
INTERNAL_ERROR = "internal_error"

_CATEGORY_MAP: dict[type, str] = {
    ParseError: PARSE_ERROR,
    UnsupportedFormatError: PARSE_ERROR,
    AnalysisError: MAPPING_ERROR,
    MappingError: MAPPING_ERROR,
    GenerationError: GENERATION_ERROR,
    ValidationError: VALIDATION_ERROR,
}

_SUGGESTIONS: dict[str, list[str]] = {
    PARSE_ERROR: [
        "Verify the file is a valid ETL definition (JSON, XML, Python, SQL, etc.).",
        "Check that the file is not truncated or corrupted.",
        "Ensure the file extension matches the actual content format.",
    ],
    MAPPING_ERROR: [
        "Check that all processor types are supported for the detected platform.",
        "Try uploading a simpler flow to isolate the problematic component.",
        "Review the mapping YAML for your platform under constants/processor_maps/.",
    ],
    GENERATION_ERROR: [
        "Ensure the assessment step completed successfully before generation.",
        "Check for unmapped processors that may block notebook generation.",
        "Try regenerating with default Databricks configuration.",
    ],
    VALIDATION_ERROR: [
        "Re-run the generation step and then validate again.",
        "Check that the generated notebook is not empty.",
        "Review validation dimension scores for specific issues.",
    ],
    INTERNAL_ERROR: [
        "This is an unexpected server error. Please try again.",
        "If the problem persists, report it with the error code.",
        "Check server logs for additional details.",
    ],
}

_FRIENDLY_MESSAGES: dict[str, str] = {
    PARSE_ERROR: "The uploaded file could not be parsed.",
    MAPPING_ERROR: "An error occurred while mapping processors to Databricks equivalents.",
    GENERATION_ERROR: "Notebook generation encountered an error.",
    VALIDATION_ERROR: "Validation of the generated output failed.",
    INTERNAL_ERROR: "An unexpected internal error occurred.",
}


def classify_error(exc: Exception) -> dict:
    """Classify *exc* and return a structured error response.

    Returns
    -------
    dict
        Keys: ``code``, ``category``, ``message``, ``detail``, ``suggestions``.
    """
    category = _resolve_category(exc)
    detail = str(exc) if str(exc) else type(exc).__name__

    return {
        "code": f"ERR_{category.upper()}",
        "category": category,
        "message": _FRIENDLY_MESSAGES.get(category, _FRIENDLY_MESSAGES[INTERNAL_ERROR]),
        "detail": detail,
        "suggestions": _SUGGESTIONS.get(category, _SUGGESTIONS[INTERNAL_ERROR]),
    }


def _resolve_category(exc: Exception) -> str:
    """Walk the MRO to find the most specific matching category."""
    for cls in type(exc).__mro__:
        if cls in _CATEGORY_MAP:
            return _CATEGORY_MAP[cls]
    # Heuristic: check exception message for keywords
    msg = str(exc).lower()
    if "parse" in msg or "syntax" in msg or "unexpected token" in msg:
        return PARSE_ERROR
    if "map" in msg or "mapping" in msg:
        return MAPPING_ERROR
    if "generat" in msg:
        return GENERATION_ERROR
    if "validat" in msg:
        return VALIDATION_ERROR
    return INTERNAL_ERROR

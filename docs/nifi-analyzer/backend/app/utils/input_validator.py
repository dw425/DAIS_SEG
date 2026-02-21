"""Input validation utilities for file uploads and user-supplied data."""

import re

# Allowed upload extensions (lowercase, with dot)
_ALLOWED_EXTENSIONS: set[str] = {
    ".xml", ".json", ".dtsx", ".zip", ".item", ".py", ".yml", ".yaml",
    ".sql", ".ktr", ".kjb", ".dsx",
}

_MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB

# Characters permitted in processor names used in generated code
_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_ \-./]")

# Matches likely XML or JSON content (quick sniff, not full validation)
_XML_MAGIC = (b"<?xml", b"<flow", b"<template", b"<pipeline", b"<root")
_JSON_MAGIC = (b"{", b"[")
_PY_MAGIC = (b"import ", b"from ", b"def ", b"#")


class FileValidationError(ValueError):
    """Raised when an uploaded file fails validation."""


def validate_file_upload(filename: str, content: bytes, size: int | None = None) -> None:
    """Validate an uploaded file's name, size, and content header.

    Raises :class:`FileValidationError` on any violation.
    """
    if not filename:
        raise FileValidationError("Filename is required")

    # Extension check
    ext = _extension(filename)
    if ext not in _ALLOWED_EXTENSIONS:
        raise FileValidationError(
            f"Unsupported file extension '{ext}'. "
            f"Allowed: {', '.join(sorted(_ALLOWED_EXTENSIONS))}"
        )

    # Size check
    actual_size = size if size is not None else len(content)
    if actual_size > _MAX_FILE_SIZE:
        raise FileValidationError(
            f"File size ({actual_size:,} bytes) exceeds the {_MAX_FILE_SIZE // (1024 * 1024)} MB limit"
        )
    if actual_size == 0:
        raise FileValidationError("File is empty")

    # Content sniffing — ensure content roughly matches extension
    _sniff_content(ext, content)


def sanitize_processor_name(name: str) -> str:
    """Sanitize a processor name so it is safe to embed in generated code.

    Strips characters that could cause injection (quotes, semicolons, etc.)
    and truncates to a reasonable length.
    """
    clean = _SAFE_NAME_RE.sub("_", name)
    # Collapse runs of underscores
    clean = re.sub(r"_+", "_", clean).strip("_")
    return clean[:200] if clean else "unnamed_processor"


def validate_json_depth(data: object, max_depth: int = 20, _current: int = 0) -> None:
    """Raise ``ValueError`` if *data* is nested deeper than *max_depth*.

    Prevents denial-of-service via excessively nested JSON payloads.
    """
    if _current > max_depth:
        raise ValueError(f"JSON nesting depth exceeds maximum of {max_depth}")

    if isinstance(data, dict):
        for v in data.values():
            validate_json_depth(v, max_depth, _current + 1)
    elif isinstance(data, list):
        for item in data:
            validate_json_depth(item, max_depth, _current + 1)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _extension(filename: str) -> str:
    """Return the lowercase file extension including the dot."""
    idx = filename.rfind(".")
    if idx == -1:
        return ""
    return filename[idx:].lower()


def _sniff_content(ext: str, content: bytes) -> None:
    """Basic content sniffing to catch obvious mismatches."""
    head = content[:512].lstrip()

    if ext in (".xml", ".dtsx", ".dsx"):
        if not any(head.startswith(m) for m in _XML_MAGIC):
            raise FileValidationError(
                f"File has extension '{ext}' but content does not appear to be XML"
            )
    elif ext == ".json":
        if not any(head.startswith(m) for m in _JSON_MAGIC):
            raise FileValidationError(
                "File has extension '.json' but content does not appear to be JSON"
            )
    elif ext == ".py":
        if not any(head.startswith(m) for m in _PY_MAGIC) and b"\n" not in head[:256]:
            raise FileValidationError(
                "File has extension '.py' but does not appear to be a Python script"
            )
    # .zip, .yml, .yaml, .sql, .ktr, .kjb, .item — no content sniffing required

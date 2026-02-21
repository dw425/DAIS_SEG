"""Input sanitization utilities."""

import re

_SENSITIVE_RE = re.compile(
    r"(password|secret|token|api[_.]?key|private[_.]?key|passphrase|credential)",
    re.IGNORECASE,
)

_HTML_RE = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return _HTML_RE.sub("", text)


def mask_sensitive_values(props: dict) -> dict:
    """Return a copy of props with sensitive values masked."""
    masked = {}
    for k, v in props.items():
        if _SENSITIVE_RE.search(k) and v:
            masked[k] = "****"
        else:
            masked[k] = v
    return masked


def sanitize_filename(name: str) -> str:
    """Sanitize a filename for safe filesystem use."""
    return re.sub(r"[^\w.\-]", "_", name)

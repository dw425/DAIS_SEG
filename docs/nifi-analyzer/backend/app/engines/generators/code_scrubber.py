"""Code scrubber — sanitize generated code for safety."""

import re

_SENSITIVE_RE = re.compile(
    r'(password|secret|api_key|token|private_key)\s*=\s*["\'][^"\']+["\']',
    re.IGNORECASE,
)


def scrub_code(code: str) -> str:
    """Remove hardcoded secrets and sanitize generated code."""
    # Replace hardcoded secrets with dbutils.secrets references
    code = _SENSITIVE_RE.sub(
        lambda m: f'{m.group(1)} = dbutils.secrets.get(scope="app", key="{m.group(1)}")',
        code,
    )
    return code

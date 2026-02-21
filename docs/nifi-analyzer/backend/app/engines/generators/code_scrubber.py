"""Code scrubber — sanitize generated code for safety."""

import logging
import re

logger = logging.getLogger(__name__)

# Pattern 1: key = "value" style assignments
_SENSITIVE_RE = re.compile(
    r'(password|secret|api_key|token|private_key|passphrase|credential)\s*=\s*["\'][^"\']+["\']',
    re.IGNORECASE,
)

# Pattern 2: Embedded credentials in JDBC/connection URLs
# e.g., jdbc:mysql://user:password@host:3306/db  or  https://user:token@api.example.com
_CONN_STRING_RE = re.compile(
    r'(jdbc:\w+://|https?://)([^:"\s]+):([^@"\s]{3,})@',
    re.IGNORECASE,
)

# Pattern 3: .option("password", "hardcoded_value") style (common in Spark code)
_OPTION_SENSITIVE_RE = re.compile(
    r'\.option\(\s*["\'](?:password|secret|token|api[_.]?key|credential)["\']'
    r'\s*,\s*["\']([^"\']+)["\']\s*\)',
    re.IGNORECASE,
)


def scrub_code(code: str) -> str:
    """Remove hardcoded secrets and sanitize generated code.

    Detects and replaces:
    - Direct assignment of sensitive values (password = "secret")
    - Embedded credentials in JDBC/HTTP connection strings
    - Spark .option() calls with sensitive values
    """
    scrub_count = sum(1 for p in [_SENSITIVE_RE, _CONN_STRING_RE, _OPTION_SENSITIVE_RE] if p.search(code))
    if scrub_count:
        logger.info("Code scrubber: %d sensitive pattern(s) detected and replaced", scrub_count)
    # Pattern 1: Replace hardcoded secret assignments
    code = _SENSITIVE_RE.sub(
        lambda m: f'{m.group(1)} = dbutils.secrets.get(scope="app", key="{m.group(1)}")',
        code,
    )

    # Pattern 2: Replace embedded credentials in connection URLs
    code = _CONN_STRING_RE.sub(
        lambda m: (
            f'{m.group(1)}'
            f'dbutils.secrets.get(scope="app", key="conn_user"):'
            f'dbutils.secrets.get(scope="app", key="conn_password")@'
        ),
        code,
    )

    # Pattern 3: Replace .option("password", "literal") with secrets reference
    code = _OPTION_SENSITIVE_RE.sub(
        lambda m: (
            '.option("{key}", dbutils.secrets.get(scope="app", key="{key}"))'.format(
                key=re.search(r'["\'](\w+)["\']', m.group(0)).group(1)
            )
        ),
        code,
    )

    return code

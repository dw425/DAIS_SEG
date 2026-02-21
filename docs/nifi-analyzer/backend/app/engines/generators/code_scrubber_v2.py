"""Enhanced code scrubber v2 — sanitize generated code for safety.

Extends code_scrubber.py with additional patterns for cloud credentials,
hardcoded IPs/ports, filesystem paths, and review markers.
"""

import logging
import re

logger = logging.getLogger(__name__)

# ── Original pattern from code_scrubber.py ──────────────────────────────
_SENSITIVE_RE = re.compile(
    r'(password|secret|api_key|token|private_key|passphrase|credential)\s*=\s*["\'][^"\']+["\']',
    re.IGNORECASE,
)

# ── AWS access keys (AKIA...) ───────────────────────────────────────────
_AWS_ACCESS_KEY_RE = re.compile(
    r'(?:access_key|aws_access_key_id)\s*=\s*["\']?(AKIA[A-Z0-9]{16})["\']?',
    re.IGNORECASE,
)
_AWS_SECRET_KEY_RE = re.compile(
    r'(?:secret_key|aws_secret_access_key)\s*=\s*["\'][^"\']{20,}["\']',
    re.IGNORECASE,
)

# ── Azure connection strings ───────────────────────────────────────────
_AZURE_CONN_STR_RE = re.compile(
    r'((?:DefaultEndpointsProtocol|AccountName|AccountKey|EndpointSuffix)'
    r'=[^"\';\n]+(?:;[^"\';\n]+)*)',
    re.IGNORECASE,
)

# ── GCP service account JSON paths ─────────────────────────────────────
_GCP_SA_RE = re.compile(
    r'(?:service_account|credentials|GOOGLE_APPLICATION_CREDENTIALS)\s*=\s*["\'][^"\']+\.json["\']',
    re.IGNORECASE,
)

# ── Hardcoded IP addresses ─────────────────────────────────────────────
_IP_RE = re.compile(
    r'(?<![0-9.])(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?![0-9.])',
)

# ── Hardcoded ports (not 80, 443, 8080) ────────────────────────────────
_PORT_RE = re.compile(
    r':(\d{4,5})(?=[/"\'\s,;)\]])',
)
_STANDARD_PORTS = {"80", "443", "8080", "8443", "22", "21"}

# ── Filesystem paths ───────────────────────────────────────────────────
_FS_PATH_RE = re.compile(
    r'(?:/opt/nifi/|/tmp/nifi|/var/lib/nifi|C:\\\\(?:nifi|NiFi|Program Files))[^\s"\'<>,]*',
    re.IGNORECASE,
)
_TMP_PATH_RE = re.compile(
    r'(?<=["\'])/tmp/[^\s"\'<>,]+',
)


def scrub_code(code: str) -> str:
    """Remove hardcoded secrets and sanitize generated code.

    Enhanced version with additional cloud-credential and path patterns.
    """
    # Original sensitive pattern
    code = _SENSITIVE_RE.sub(
        lambda m: f'{m.group(1)} = dbutils.secrets.get(scope="app", key="{m.group(1)}")',
        code,
    )

    # AWS access keys
    code = _AWS_ACCESS_KEY_RE.sub(
        'access_key = dbutils.secrets.get(scope="app", key="aws_access_key")',
        code,
    )
    code = _AWS_SECRET_KEY_RE.sub(
        'secret_key = dbutils.secrets.get(scope="app", key="aws_secret_key")',
        code,
    )

    # Azure connection strings
    code = _AZURE_CONN_STR_RE.sub(
        'dbutils.secrets.get(scope="app", key="azure_connection_string")',
        code,
    )

    # GCP service account paths
    code = _GCP_SA_RE.sub(
        'credentials = dbutils.secrets.get(scope="app", key="gcp_service_account")',
        code,
    )

    # Filesystem paths -> Volumes paths
    code = _FS_PATH_RE.sub('/Volumes/main/default/data', code)
    code = _TMP_PATH_RE.sub('/Volumes/main/default/tmp', code)

    return code


def scrub_all_cells(cells: list[dict]) -> tuple[list[dict], list[str]]:
    """Scrub all code cells in a notebook and track changes.

    Args:
        cells: list of cell dicts with at least {type, source}

    Returns:
        Tuple of (scrubbed_cells, changes_made)
        where changes_made is a list of human-readable change descriptions.
    """
    logger.info("Scrubbing %d cells", len(cells))
    scrubbed: list[dict] = []
    changes: list[str] = []

    for cell in cells:
        if cell.get("type") != "code":
            scrubbed.append(cell)
            continue

        original = cell["source"]
        cleaned = original

        # Apply each pattern and track what changed
        cleaned, cell_changes = _scrub_with_tracking(cleaned, cell.get("label", ""))
        changes.extend(cell_changes)

        # Add review markers for remaining suspicious patterns
        cleaned, marker_changes = _add_review_markers(cleaned, cell.get("label", ""))
        changes.extend(marker_changes)

        new_cell = dict(cell)
        new_cell["source"] = cleaned
        scrubbed.append(new_cell)

    return scrubbed, changes


def _scrub_with_tracking(code: str, label: str) -> tuple[str, list[str]]:
    """Apply scrubbing patterns and return (cleaned_code, list_of_changes)."""
    changes: list[str] = []
    prefix = f"[{label}] " if label else ""

    # Sensitive patterns
    if _SENSITIVE_RE.search(code):
        code = _SENSITIVE_RE.sub(
            lambda m: f'{m.group(1)} = dbutils.secrets.get(scope="app", key="{m.group(1)}")',
            code,
        )
        changes.append(f"{prefix}Replaced hardcoded credential with dbutils.secrets.get()")

    # AWS keys
    if _AWS_ACCESS_KEY_RE.search(code):
        code = _AWS_ACCESS_KEY_RE.sub(
            'access_key = dbutils.secrets.get(scope="app", key="aws_access_key")',
            code,
        )
        changes.append(f"{prefix}Replaced AWS access key (AKIA...) with dbutils.secrets.get()")

    if _AWS_SECRET_KEY_RE.search(code):
        code = _AWS_SECRET_KEY_RE.sub(
            'secret_key = dbutils.secrets.get(scope="app", key="aws_secret_key")',
            code,
        )
        changes.append(f"{prefix}Replaced AWS secret key with dbutils.secrets.get()")

    # Azure connection strings
    if _AZURE_CONN_STR_RE.search(code):
        code = _AZURE_CONN_STR_RE.sub(
            'dbutils.secrets.get(scope="app", key="azure_connection_string")',
            code,
        )
        changes.append(f"{prefix}Replaced Azure connection string with dbutils.secrets.get()")

    # GCP service account
    if _GCP_SA_RE.search(code):
        code = _GCP_SA_RE.sub(
            'credentials = dbutils.secrets.get(scope="app", key="gcp_service_account")',
            code,
        )
        changes.append(f"{prefix}Replaced GCP service account path with dbutils.secrets.get()")

    # Hardcoded IPs
    ip_matches = _IP_RE.findall(code)
    for ip in ip_matches:
        if ip.startswith("127.") or ip.startswith("0.") or ip == "255.255.255.255":
            continue
        code = code.replace(ip, '${host_address}')
        changes.append(f"{prefix}Replaced hardcoded IP {ip} with config variable")

    # Hardcoded ports
    for m in _PORT_RE.finditer(code):
        port = m.group(1)
        if port not in _STANDARD_PORTS:
            code = code.replace(f":{port}", ':${port}')
            changes.append(f"{prefix}Replaced hardcoded port {port} with config variable")

    # Filesystem paths
    if _FS_PATH_RE.search(code):
        code = _FS_PATH_RE.sub('/Volumes/main/default/data', code)
        changes.append(f"{prefix}Replaced NiFi filesystem path with Volumes path")

    if _TMP_PATH_RE.search(code):
        code = _TMP_PATH_RE.sub('/Volumes/main/default/tmp', code)
        changes.append(f"{prefix}Replaced /tmp/ path with Volumes path")

    return code, changes


def _add_review_markers(code: str, label: str) -> tuple[str, list[str]]:
    """Add comment markers for items that need manual review."""
    changes: list[str] = []
    prefix = f"[{label}] " if label else ""

    # Mark TODO comments for manual review
    review_patterns = [
        (re.compile(r'# UNMAPPED:'), "Unmapped processor requires manual migration"),
        (re.compile(r'jdbc_url|jdbc:'), "JDBC connection — verify URL and credentials"),
        (re.compile(r'\.option\("subscribe"'), "Kafka topic — verify topic name for Databricks"),
        (re.compile(r'http://|https://(?!docs\.|spark\.)'), "External URL — verify accessibility"),
    ]

    for pattern, reason in review_patterns:
        if pattern.search(code):
            if f"# REVIEW:" not in code:
                code = f"# REVIEW: {reason}\n{code}"
                changes.append(f"{prefix}Added review marker: {reason}")

    return code, changes

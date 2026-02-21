"""Security scanner — scan processor properties for vulnerability patterns.

Ported from security-scanner.js.
"""

import re

from app.models.processor import Processor

_SQL_INJECTION_RE = re.compile(
    r"(\bDROP\s+TABLE\b|\bUNION\s+SELECT\b|\bxp_cmdshell\b"
    r"|\bEXEC\s+sp_|\bDELETE\s+FROM\b.*;\s*--|;\s*DROP\b)",
    re.I,
)
_CMD_INJECTION_RE = re.compile(
    r"(rm\s+-rf\s+\/|curl\s+evil|wget\s+.*\|\s*bash"
    r"|\/bin\/sh\s+-[ic]|exec\s*\(|system\s*\(|subprocess)",
    re.I,
)
_SSRF_RE = re.compile(
    r"(169\.254\.169\.254|metadata\.google\.internal"
    r"|localhost:\d{4}\/admin)",
    re.I,
)
_SECRET_RE = re.compile(
    r"(password\s*=\s*['\"][^'\"]{3,}|secret\s*=\s*['\"][^'\"]{3,}"
    r"|api_key\s*=\s*['\"][^'\"]{3,})",
    re.I,
)
_REVERSE_SHELL_RE = re.compile(
    r"(\/dev\/tcp\/|nc\s+-[el]|ncat\s"
    r"|socket\.SOCK_STREAM.*connect|bash\s+-i\s*>&)",
    re.I,
)
_PATH_TRAVERSAL_RE = re.compile(
    r"(\.\.\/\.\.\/|\/etc\/passwd|\/etc\/shadow|\/root\/)",
    re.I,
)
_DANGEROUS_FILE_RE = re.compile(
    r"(\.pem|\.key|\.crt|credentials\.json|\.env\b)",
    re.I,
)
_JAVA_EXPLOIT_RE = re.compile(
    r"(Runtime\.getRuntime\(\)|ProcessBuilder"
    r"|jndi:ldap|Class\.forName)",
    re.I,
)
_DATA_EXFIL_RE = re.compile(
    r"(LOAD_FILE|INTO\s+OUTFILE"
    r"|COPY\s+.*\s+TO\s+'\/|UTL_HTTP\.REQUEST)",
    re.I,
)

_SECURITY_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    ("SQL Injection", _SQL_INJECTION_RE, "CRITICAL"),
    ("Command Injection", _CMD_INJECTION_RE, "CRITICAL"),
    ("SSRF", _SSRF_RE, "HIGH"),
    ("Hardcoded Secret", _SECRET_RE, "HIGH"),
    ("Reverse Shell", _REVERSE_SHELL_RE, "CRITICAL"),
    ("Path Traversal", _PATH_TRAVERSAL_RE, "MEDIUM"),
    ("Dangerous File Access", _DANGEROUS_FILE_RE, "MEDIUM"),
    ("Java Exploitation", _JAVA_EXPLOIT_RE, "CRITICAL"),
    ("Data Exfiltration", _DATA_EXFIL_RE, "HIGH"),
]


def scan_security(processors: list[Processor]) -> list[dict]:
    """Scan processor properties for known security vulnerability patterns."""
    findings: list[dict] = []

    for p in processors:
        all_values = " ".join(str(v) for v in p.properties.values() if v)
        if not all_values:
            continue

        for pat_name, regex, severity in _SECURITY_PATTERNS:
            match = regex.search(all_values)
            if match:
                findings.append(
                    {
                        "processor": p.name,
                        "type": p.type,
                        "finding": pat_name,
                        "severity": severity,
                        "snippet": match.group(0)[:100],
                    }
                )

    return findings

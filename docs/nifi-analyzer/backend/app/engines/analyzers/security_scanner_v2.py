"""Enhanced security scanner v2 — extends base scanner with CWE-mapped patterns.

Preserves all 9 original patterns and adds 10 new categories with
CVSS scores, CWE IDs, and remediation guidance.
"""

from __future__ import annotations

import re

from app.models.processor import Processor

# ── Original 9 patterns (preserved from security_scanner.py) ──

_ORIGINAL_PATTERNS: list[tuple[str, re.Pattern, str, str, float, str]] = [
    (
        "SQL Injection", re.compile(
            r"(\bDROP\s+TABLE\b|\bUNION\s+SELECT\b|\bxp_cmdshell\b"
            r"|\bEXEC\s+sp_|\bDELETE\s+FROM\b.*;\s*--|;\s*DROP\b)", re.I
        ), "CRITICAL", "CWE-89", 9.8,
        "Use parameterized queries or prepared statements instead of string concatenation."
    ),
    (
        "Command Injection", re.compile(
            r"(rm\s+-rf\s+\/|curl\s+evil|wget\s+.*\|\s*bash"
            r"|\/bin\/sh\s+-[ic]|exec\s*\(|system\s*\(|subprocess)", re.I
        ), "CRITICAL", "CWE-78", 9.8,
        "Avoid shell execution; use subprocess with shell=False and explicit argument lists."
    ),
    (
        "SSRF", re.compile(
            r"(169\.254\.169\.254|metadata\.google\.internal"
            r"|localhost:\d{4}\/admin)", re.I
        ), "HIGH", "CWE-918", 7.5,
        "Validate and whitelist destination URLs; block internal IP ranges."
    ),
    (
        "Hardcoded Secret", re.compile(
            r"(password\s*=\s*['\"][^'\"]{3,}|secret\s*=\s*['\"][^'\"]{3,}"
            r"|api_key\s*=\s*['\"][^'\"]{3,})", re.I
        ), "HIGH", "CWE-798", 7.5,
        "Use dbutils.secrets.get() or environment variables for credentials."
    ),
    (
        "Reverse Shell", re.compile(
            r"(\/dev\/tcp\/|nc\s+-[el]|ncat\s"
            r"|socket\.SOCK_STREAM.*connect|bash\s+-i\s*>&)", re.I
        ), "CRITICAL", "CWE-78", 10.0,
        "Remove shell access patterns; implement network segmentation."
    ),
    (
        "Path Traversal", re.compile(
            r"(\.\.\/\.\.\/|\/etc\/passwd|\/etc\/shadow|\/root\/)", re.I
        ), "MEDIUM", "CWE-22", 5.3,
        "Validate and canonicalize file paths; use chroot or sandboxing."
    ),
    (
        "Dangerous File Access", re.compile(
            r"(\.pem|\.key|\.crt|credentials\.json|\.env\b)", re.I
        ), "MEDIUM", "CWE-552", 5.3,
        "Avoid referencing credential files directly; use secret management services."
    ),
    (
        "Java Exploitation", re.compile(
            r"(Runtime\.getRuntime\(\)|ProcessBuilder"
            r"|jndi:ldap|Class\.forName)", re.I
        ), "CRITICAL", "CWE-502", 9.8,
        "Disable JNDI lookups; use allowlists for class loading."
    ),
    (
        "Data Exfiltration", re.compile(
            r"(LOAD_FILE|INTO\s+OUTFILE"
            r"|COPY\s+.*\s+TO\s+'\/|UTL_HTTP\.REQUEST)", re.I
        ), "HIGH", "CWE-200", 7.5,
        "Restrict file system and network access from SQL contexts."
    ),
]

# ── New enhanced patterns ──

_ENHANCED_PATTERNS: list[tuple[str, re.Pattern, str, str, float, str]] = [
    # CWE-89: Dynamic SQL from user input
    (
        "Dynamic SQL Construction", re.compile(
            r"(f['\"].*SELECT.*\{|\.format\(.*SELECT"
            r"|%s.*SELECT|\+\s*['\"].*SELECT|concat.*SELECT)", re.I
        ), "CRITICAL", "CWE-89", 9.8,
        "Use parameterized queries; never concatenate user input into SQL strings."
    ),
    # CWE-78: OS Command Injection
    (
        "OS Command Injection", re.compile(
            r"(shell\s*=\s*True|os\.system\s*\(|os\.popen\s*\("
            r"|subprocess\.call\s*\(.*shell|commands\.getoutput)", re.I
        ), "CRITICAL", "CWE-78", 9.8,
        "Use subprocess with shell=False; validate and sanitize all command arguments."
    ),
    # CWE-611: XXE
    (
        "XML External Entity (XXE)", re.compile(
            r"(xml\.etree\.ElementTree\.parse|lxml\.etree\.parse"
            r"|xml\.dom\.minidom\.parse|<!DOCTYPE.*ENTITY"
            r"|xml\.sax\.parseString)", re.I
        ), "HIGH", "CWE-611", 7.5,
        "Disable DTD processing; use defusedxml library for XML parsing."
    ),
    # CWE-918: SSRF (enhanced)
    (
        "Server-Side Request Forgery", re.compile(
            r"(requests\.get\s*\(\s*[^'\"]+\)|urllib\.request\.urlopen\s*\(\s*[^'\"]"
            r"|http\.client\.HTTPConnection\s*\(\s*[^'\"]"
            r"|urlopen\s*\(\s*[^'\"])", re.I
        ), "HIGH", "CWE-918", 7.5,
        "Validate and whitelist destination URLs; implement URL parsing and filtering."
    ),
    # CWE-502: Unsafe deserialization
    (
        "Unsafe Deserialization", re.compile(
            r"(pickle\.loads?\s*\(|yaml\.load\s*\([^)]*(?!Loader\s*=\s*yaml\.SafeLoader)"
            r"|marshal\.loads?\s*\(|shelve\.open"
            r"|jsonpickle\.decode)", re.I
        ), "HIGH", "CWE-502", 8.1,
        "Use yaml.safe_load(); avoid pickle for untrusted data; use JSON instead."
    ),
    # CWE-327: Weak cryptography
    (
        "Weak Cryptography", re.compile(
            r"(hashlib\.md5\s*\(|hashlib\.sha1\s*\("
            r"|DES\.new|RC4|Blowfish|ARC4"
            r"|md5\s*\(|sha1\s*\()", re.I
        ), "MEDIUM", "CWE-327", 5.9,
        "Use SHA-256 or stronger hashing; use AES-256 for encryption."
    ),
    # CWE-798: Enhanced hardcoded credentials
    (
        "Hardcoded AWS/Cloud Credentials", re.compile(
            r"(AKIA[0-9A-Z]{16}|AccountKey\s*="
            r"|\"type\"\s*:\s*\"service_account\""
            r"|PRIVATE\s+KEY-----"
            r"|aws_secret_access_key\s*=\s*['\"][^'\"]{10,})", re.I
        ), "CRITICAL", "CWE-798", 9.1,
        "Use IAM roles, managed identities, or secret managers for cloud credentials."
    ),
    # SSL/TLS weaknesses
    (
        "SSL/TLS Misconfiguration", re.compile(
            r"(ssl\.PROTOCOL_TLSv1\b|ssl\.PROTOCOL_SSLv"
            r"|verify\s*=\s*False|CERT_NONE"
            r"|check_hostname\s*=\s*False)", re.I
        ), "HIGH", "CWE-295", 7.4,
        "Use TLS 1.2+; enable certificate verification; never set verify=False in production."
    ),
    # Privilege escalation
    (
        "Privilege Escalation", re.compile(
            r"(chmod\s+777|chmod\s+666|setuid|setgid"
            r"|sudo\s|os\.setuid|os\.setgid"
            r"|chown\s+root)", re.I
        ), "HIGH", "CWE-269", 7.8,
        "Apply least-privilege principle; avoid broad file permissions."
    ),
    # Data leakage
    (
        "Sensitive Data Leakage", re.compile(
            r"((?:print|log(?:ger)?\.(?:info|debug|warning|error))\s*\(.*"
            r"(?:ssn|social_security|credit_card|card_number|cvv"
            r"|password|secret|token|api_key))", re.I
        ), "HIGH", "CWE-532", 7.5,
        "Never log sensitive data; mask or redact PII before logging."
    ),
]


def scan_v2(processors: list[Processor]) -> list[dict]:
    """Scan processor properties with enhanced CWE-mapped patterns.

    Returns list of findings with category, CWE, severity, CVSS,
    description, processor info, evidence, and remediation.
    """
    all_patterns = _ORIGINAL_PATTERNS + _ENHANCED_PATTERNS
    findings: list[dict] = []

    for p in processors:
        all_values = " ".join(str(v) for v in p.properties.values() if v)
        if not all_values:
            continue

        for pat_name, regex, severity, cwe_id, cvss, remediation in all_patterns:
            match = regex.search(all_values)
            if match:
                findings.append({
                    "category": pat_name,
                    "cwe_id": cwe_id,
                    "severity": severity,
                    "cvss_score": cvss,
                    "description": f"{pat_name} pattern detected in processor '{p.name}'",
                    "processor": p.name,
                    "processor_type": p.type,
                    "evidence": match.group(0)[:120],
                    "remediation": remediation,
                })

    # Sort by CVSS score descending
    findings.sort(key=lambda f: f["cvss_score"], reverse=True)
    return findings


# Backward-compatible alias
def scan_security(processors: list[Processor]) -> list[dict]:
    """Backward-compatible wrapper returning simplified findings."""
    enhanced = scan_v2(processors)
    return [
        {
            "processor": f["processor"],
            "type": f["processor_type"],
            "finding": f["category"],
            "severity": f["severity"],
            "snippet": f["evidence"],
        }
        for f in enhanced
    ]

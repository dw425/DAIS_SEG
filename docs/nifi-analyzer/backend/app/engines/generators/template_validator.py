"""Template validator — validate generated Databricks notebook code quality.

Checks syntax, unresolved placeholders, imports, bare prints, and
credential handling in generated code templates.
"""

from __future__ import annotations

import ast
import re

_PLACEHOLDER_RE = re.compile(r"(?<![\w\"{])\{(\w+)\}(?![}\"])")
_BARE_PRINT_RE = re.compile(r"^\s*print\s*\(", re.MULTILINE)
_LOGGING_CONTEXT_RE = re.compile(r"(logger\.|logging\.)", re.MULTILINE)
_HARDCODED_CRED_RE = re.compile(
    r"""(?:password|secret|token|api_key|credential)\s*=\s*['"][^'"]{3,}['"]""",
    re.IGNORECASE,
)
_DBUTILS_SECRETS_RE = re.compile(r"dbutils\.secrets\.get")

_KNOWN_PACKAGES = frozenset({
    "pyspark", "delta", "databricks", "dbutils", "os", "sys", "re", "json",
    "datetime", "time", "math", "collections", "itertools", "functools",
    "typing", "pathlib", "hashlib", "base64", "uuid", "logging", "io",
    "csv", "xml", "html", "urllib", "http", "requests", "pandas", "numpy",
    "yaml", "toml", "configparser", "ast", "inspect", "textwrap",
    "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "delta.tables", "spark",
})


def validate_template(code: str, context: dict | None = None) -> list[dict]:
    """Validate a generated code template.

    Returns list of {severity, message, line} findings.
    """
    findings: list[dict] = []
    context = context or {}

    # Check 1: Python AST parse
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        findings.append({
            "severity": "error",
            "message": f"Syntax error: {exc.msg}",
            "line": exc.lineno or 0,
        })
        return findings  # Can't continue without valid AST

    # Check 2: Unresolved placeholders (not in f-strings)
    lines = code.split("\n")
    for i, line in enumerate(lines, 1):
        # Skip f-strings
        stripped = line.strip()
        if stripped.startswith('f"') or stripped.startswith("f'"):
            continue
        for m in _PLACEHOLDER_RE.finditer(line):
            placeholder = m.group(1)
            # Skip known Python format spec and common false positives
            if placeholder in ("self", "cls", "args", "kwargs", "key", "value"):
                continue
            findings.append({
                "severity": "warning",
                "message": f"Unresolved placeholder: {{{placeholder}}}",
                "line": i,
            })

    # Check 3: Import statements reference known packages
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_pkg = alias.name.split(".")[0]
                if root_pkg not in _KNOWN_PACKAGES:
                    findings.append({
                        "severity": "info",
                        "message": f"Unknown import package: {alias.name}",
                        "line": node.lineno,
                    })
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                root_pkg = node.module.split(".")[0]
                if root_pkg not in _KNOWN_PACKAGES:
                    findings.append({
                        "severity": "info",
                        "message": f"Unknown import package: {node.module}",
                        "line": node.lineno,
                    })

    # Check 4: Bare print() without logging context
    has_logging = bool(_LOGGING_CONTEXT_RE.search(code))
    for i, line in enumerate(lines, 1):
        if _BARE_PRINT_RE.match(line):
            if not has_logging:
                findings.append({
                    "severity": "warning",
                    "message": "Bare print() without logging context — prefer logger",
                    "line": i,
                })

    # Check 5: Credential handling
    for i, line in enumerate(lines, 1):
        if _HARDCODED_CRED_RE.search(line):
            findings.append({
                "severity": "error",
                "message": "Hardcoded credential detected — use dbutils.secrets.get()",
                "line": i,
            })

    # Bonus: verify dbutils.secrets.get is used if credentials referenced
    has_cred_ref = any(
        kw in code.lower()
        for kw in ("password", "secret", "token", "api_key", "credential")
    )
    if has_cred_ref and not _DBUTILS_SECRETS_RE.search(code):
        findings.append({
            "severity": "info",
            "message": "Code references credentials but does not use dbutils.secrets.get()",
            "line": 0,
        })

    return findings


def validate_notebook(cells: list[dict]) -> dict:
    """Validate all cells in a notebook, return summary.

    Each cell should have 'type' and 'source' keys.
    """
    total = 0
    passed = 0
    failed = 0
    all_findings: list[dict] = []

    for idx, cell in enumerate(cells):
        if cell.get("type") != "code":
            continue
        total += 1
        source = cell.get("source", "")
        findings = validate_template(source)
        errors = [f for f in findings if f["severity"] == "error"]
        if errors:
            failed += 1
        else:
            passed += 1
        for f in findings:
            f["cell_index"] = idx
            f["cell_label"] = cell.get("label", f"cell_{idx}")
        all_findings.extend(findings)

    return {
        "total_cells": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": round(passed / max(total, 1), 2),
        "findings": all_findings,
    }

"""Runnable Checker -- 12-point quality gate for generated notebooks.

Validates that a generated notebook will actually execute in Databricks
without manual edits.  Each check returns a CheckResult; the overall
report marks the notebook ``is_runnable`` only when ALL critical checks pass.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    check_id: int
    name: str
    passed: bool
    severity: str  # critical, high, medium, low
    message: str
    details: list[str] = field(default_factory=list)


@dataclass
class RunnableReport:
    checks: list[CheckResult]
    passed_count: int
    failed_count: int
    is_runnable: bool  # True if ALL critical checks pass
    overall_score: float  # 0.0-1.0
    summary: str


# ---------------------------------------------------------------------------
# Known-good packages available on DBR 15+
# ---------------------------------------------------------------------------

_DBR_PACKAGES: set[str] = {
    # PySpark & Databricks
    "pyspark", "delta", "databricks", "mlflow", "koalas",
    # Python stdlib (subset most commonly imported in notebooks)
    "os", "sys", "re", "json", "csv", "io", "math", "time", "datetime",
    "collections", "itertools", "functools", "operator", "typing",
    "pathlib", "tempfile", "hashlib", "base64", "uuid", "copy",
    "logging", "warnings", "traceback", "contextlib", "dataclasses",
    "textwrap", "string", "struct", "decimal", "fractions",
    "statistics", "random", "secrets", "urllib", "http", "email",
    "html", "xml", "sqlite3", "zipfile", "gzip", "bz2", "lzma",
    "shutil", "glob", "fnmatch", "pickle", "shelve",
    "threading", "multiprocessing", "subprocess", "concurrent",
    "asyncio", "socket", "ssl", "select", "signal",
    "abc", "enum", "inspect",
    # Common third-party on DBR
    "pandas", "numpy", "scipy", "sklearn", "scikit_learn",
    "requests", "urllib3", "certifi", "chardet", "idna",
    "boto3", "botocore", "s3transfer",
    "azure", "google", "grpc",
    "pyarrow", "fastparquet",
    "matplotlib", "seaborn", "plotly", "PIL", "pillow",
    "yaml", "pyyaml", "toml",
    "cryptography", "jwt", "jose",
    "jinja2", "markupsafe",
    "sqlalchemy", "alembic",
    "py4j",
    "IPython", "ipywidgets",
    "tqdm", "rich", "tabulate",
    "jsonschema", "attrs", "pydantic",
    "tenacity", "retry", "backoff",
    "paramiko", "fabric",
    "beautifulsoup4", "bs4", "lxml", "html5lib",
    "openpyxl", "xlrd",
    "psutil", "humanize",
    "networkx", "graphviz",
    "shapely", "geopandas", "geopy",
    "transformers", "torch", "tensorflow",
    "langchain", "openai",
    "smtplib",
}

# ---------------------------------------------------------------------------
# Secret / credential patterns
# ---------------------------------------------------------------------------

_SECRET_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("hardcoded password", re.compile(
        r'''(?:password|passwd|pwd)\s*=\s*(['"])[^'"]{3,}\1''', re.I)),
    ("hardcoded API key", re.compile(
        r'''(?:api[_\-]?key|apikey)\s*=\s*(['"])[^'"]{8,}\1''', re.I)),
    ("AWS access key", re.compile(
        r'''(?:^|['"])AKIA[0-9A-Z]{16}''', re.M)),
    ("AWS secret key", re.compile(
        r'''(?:aws_secret_access_key|secret_key)\s*=\s*(['"])[^'"]{20,}\1''', re.I)),
    ("hardcoded token", re.compile(
        r'''(?:token|bearer)\s*=\s*(['"])[^'"]{10,}\1''', re.I)),
    ("password in JDBC URL", re.compile(
        r'''jdbc:.*password=[^&\s]{3,}''', re.I)),
    ("credentials in connection URI", re.compile(
        r'''(?:mongodb|redis|amqp|postgres|mysql)\+?(?:srv)?://[^:]+:[^@]{3,}@''', re.I)),
    ("hardcoded secret", re.compile(
        r'''secret\s*=\s*(['"])[^'"]{3,}\1''', re.I)),
]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def check_runnable(cells: list, parsed=None) -> RunnableReport:
    """Run all 12 checks on generated notebook cells.

    Args:
        cells: list of objects with ``.type`` and ``.source`` attributes,
               OR list of dicts with ``type`` and ``source`` keys.
        parsed: Optional parsed flow (ParseResult or dict-like) for
                cross-referencing connections and processors.

    Returns:
        RunnableReport with per-check results and overall scoring.
    """
    # Normalize cells to dicts for uniform access
    norm_cells = _normalize_cells(cells)
    code_cells = [c for c in norm_cells if c["type"] == "code"]
    all_code = "\n".join(c["source"] for c in code_cells)

    checks: list[CheckResult] = [
        _check_syntax_valid(code_cells),
        _check_imports_resolve(all_code),
        _check_data_flows_end_to_end(code_cells),
        _check_no_dangling_variables(code_cells),
        _check_all_streams_have_checkpoints(all_code),
        _check_all_streams_have_triggers(all_code),
        _check_no_plaintext_secrets(all_code),
        _check_terminal_writes_exist(all_code),
        _check_config_cell_present(code_cells),
        _check_dag_order_correct(code_cells),
        _check_relationship_routing_honored(all_code, parsed),
        _check_processor_coverage_complete(code_cells, parsed),
    ]

    passed_count = sum(1 for c in checks if c.passed)
    failed_count = len(checks) - passed_count
    critical_all_pass = all(c.passed for c in checks if c.severity == "critical")
    overall_score = passed_count / len(checks) if checks else 0.0

    # Build summary
    failed_names = [c.name for c in checks if not c.passed]
    if critical_all_pass and failed_count == 0:
        summary = "All 12 checks passed -- notebook is runnable."
    elif critical_all_pass:
        summary = (
            f"Notebook is runnable ({passed_count}/12 checks passed). "
            f"Non-critical issues: {', '.join(failed_names)}"
        )
    else:
        critical_fails = [c.name for c in checks if not c.passed and c.severity == "critical"]
        summary = (
            f"NOTEBOOK NOT RUNNABLE -- {len(critical_fails)} critical check(s) failed: "
            f"{', '.join(critical_fails)}"
        )

    return RunnableReport(
        checks=checks,
        passed_count=passed_count,
        failed_count=failed_count,
        is_runnable=critical_all_pass,
        overall_score=round(overall_score, 4),
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Cell normalization helper
# ---------------------------------------------------------------------------

def _normalize_cells(cells: list) -> list[dict]:
    """Convert cells to uniform dicts with 'type', 'source', 'label' keys."""
    result: list[dict] = []
    for c in cells:
        if isinstance(c, dict):
            result.append({
                "type": c.get("type", "code"),
                "source": c.get("source", ""),
                "label": c.get("label", ""),
            })
        else:
            result.append({
                "type": getattr(c, "type", "code"),
                "source": getattr(c, "source", ""),
                "label": getattr(c, "label", ""),
            })
    return result


# ---------------------------------------------------------------------------
# Check 1: syntax_valid  (critical)
# ---------------------------------------------------------------------------

def _check_syntax_valid(code_cells: list[dict]) -> CheckResult:
    """Run ast.parse() on all code cells. Report any syntax errors with line numbers."""
    errors: list[str] = []

    for i, cell in enumerate(code_cells):
        source = cell["source"]
        label = cell.get("label", f"cell_{i}")
        try:
            ast.parse(source)
        except SyntaxError as exc:
            errors.append(
                f"Cell '{label}' line {exc.lineno}: {exc.msg}"
            )

    return CheckResult(
        check_id=1,
        name="syntax_valid",
        passed=len(errors) == 0,
        severity="critical",
        message="All code cells are syntactically valid" if not errors
                else f"{len(errors)} syntax error(s) found",
        details=errors,
    )


# ---------------------------------------------------------------------------
# Check 2: imports_resolve  (critical)
# ---------------------------------------------------------------------------

def _check_imports_resolve(all_code: str) -> CheckResult:
    """Check all import statements against known DBR 15+ packages."""
    unresolved: list[str] = []

    # Match: import X, from X import Y, from X.Y import Z
    import_re = re.compile(
        r'^\s*(?:from\s+([\w.]+)|import\s+([\w.]+(?:\s*,\s*[\w.]+)*))',
        re.MULTILINE,
    )

    for m in import_re.finditer(all_code):
        from_pkg = m.group(1)
        import_pkgs = m.group(2)

        if from_pkg:
            root = from_pkg.split(".")[0]
            if root.lower() not in _DBR_PACKAGES and root not in ("__future__",):
                unresolved.append(f"from {from_pkg}")
        elif import_pkgs:
            for raw_pkg in import_pkgs.split(","):
                pkg = raw_pkg.strip().split(" ")[0]  # handle "import X as Y"
                root = pkg.split(".")[0]
                if root.lower() not in _DBR_PACKAGES and root not in ("__future__",):
                    unresolved.append(f"import {pkg}")

    return CheckResult(
        check_id=2,
        name="imports_resolve",
        passed=len(unresolved) == 0,
        severity="critical",
        message="All imports resolve against DBR 15+ packages" if not unresolved
                else f"{len(unresolved)} unresolved import(s)",
        details=unresolved,
    )


# ---------------------------------------------------------------------------
# Check 3: data_flows_end_to_end  (critical)
# ---------------------------------------------------------------------------

def _strip_comments(source: str) -> str:
    """Remove comment lines and inline comments from Python source.

    Preserves string literals by only stripping ``#`` outside of quotes.
    For simplicity, strips full-line comments and trailing ``# ...`` segments.
    """
    lines: list[str] = []
    for line in source.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue  # skip full-line comments
        # Strip trailing inline comments (simple heuristic: outside of strings)
        in_str: str | None = None
        clean = []
        for ch in line:
            if in_str:
                clean.append(ch)
                if ch == in_str:
                    in_str = None
            elif ch in ('"', "'"):
                clean.append(ch)
                in_str = ch
            elif ch == "#":
                break  # rest is comment
            else:
                clean.append(ch)
        lines.append("".join(clean))
    return "\n".join(lines)


def _check_data_flows_end_to_end(code_cells: list[dict]) -> CheckResult:
    """Every df_X variable referenced in a cell must be defined in a prior cell."""
    df_assign_re = re.compile(r'^(df_\w+)\s*=', re.MULTILINE)
    df_ref_re = re.compile(r'\b(df_\w+)\b')

    defined: set[str] = set()
    undefined_refs: list[str] = []

    for i, cell in enumerate(code_cells):
        source = _strip_comments(cell["source"])
        label = cell.get("label", f"cell_{i}")

        # Collect references that are NOT on the LHS of an assignment in this cell
        local_assigns = set(df_assign_re.findall(source))
        all_refs = set(df_ref_re.findall(source))

        # References that aren't locally assigned and weren't previously defined
        for ref in sorted(all_refs - local_assigns):
            if ref not in defined:
                undefined_refs.append(
                    f"'{ref}' referenced in cell '{label}' but not defined in any prior cell"
                )

        # After processing references, add this cell's assignments to defined set
        defined.update(local_assigns)

    return CheckResult(
        check_id=3,
        name="data_flows_end_to_end",
        passed=len(undefined_refs) == 0,
        severity="critical",
        message="All DataFrame references resolve to prior definitions" if not undefined_refs
                else f"{len(undefined_refs)} undefined DataFrame reference(s)",
        details=undefined_refs,
    )


# ---------------------------------------------------------------------------
# Check 4: no_dangling_variables  (critical)
# ---------------------------------------------------------------------------

def _check_no_dangling_variables(code_cells: list[dict]) -> CheckResult:
    """No bare 'df' references that aren't bound to a specific processor.

    Must be df_{name}, not bare 'df'.  We allow 'df' only when:
    * It appears in a comment (after #)
    * It appears inside a string literal
    * It is the word 'dataframe' or similar (not standalone token)
    """
    dangling: list[str] = []
    # Match standalone 'df' as a variable (not df_xxx, not inside a word)
    bare_df_re = re.compile(r'(?<![.\w])df(?!\w)')

    for i, cell in enumerate(code_cells):
        source = cell["source"]
        label = cell.get("label", f"cell_{i}")

        for line_no, line in enumerate(source.splitlines(), 1):
            # Strip comments
            code_part = line.split("#")[0]
            # Strip string literals (simple heuristic)
            code_part = re.sub(r'''(['"])(?:(?!\1).)*\1''', '""', code_part)

            if bare_df_re.search(code_part):
                dangling.append(
                    f"Cell '{label}' line {line_no}: bare 'df' reference "
                    f"(should be df_{{processor_name}})"
                )

    return CheckResult(
        check_id=4,
        name="no_dangling_variables",
        passed=len(dangling) == 0,
        severity="critical",
        message="No dangling 'df' variables found" if not dangling
                else f"{len(dangling)} dangling 'df' reference(s)",
        details=dangling,
    )


# ---------------------------------------------------------------------------
# Check 5: all_streams_have_checkpoints  (critical)
# ---------------------------------------------------------------------------

def _check_all_streams_have_checkpoints(all_code: str) -> CheckResult:
    """Every .writeStream must have .option('checkpointLocation', ...)."""
    # Find writeStream blocks (from .writeStream up to .start() or .toTable())
    write_stream_blocks = re.findall(
        r'\.writeStream\b.*?(?:\.start\(\)|\.toTable\([^)]*\))',
        all_code,
        re.DOTALL,
    )
    missing: list[str] = []

    for i, block in enumerate(write_stream_blocks):
        if "checkpointLocation" not in block:
            missing.append(f"writeStream block #{i + 1} missing checkpointLocation")

    if not write_stream_blocks:
        return CheckResult(
            check_id=5,
            name="all_streams_have_checkpoints",
            passed=True,
            severity="critical",
            message="No streaming writes found (check not applicable)",
        )

    return CheckResult(
        check_id=5,
        name="all_streams_have_checkpoints",
        passed=len(missing) == 0,
        severity="critical",
        message=f"All {len(write_stream_blocks)} streaming write(s) have checkpoints" if not missing
                else f"{len(missing)}/{len(write_stream_blocks)} streaming write(s) missing checkpoints",
        details=missing,
    )


# ---------------------------------------------------------------------------
# Check 6: all_streams_have_triggers  (high)
# ---------------------------------------------------------------------------

def _check_all_streams_have_triggers(all_code: str) -> CheckResult:
    """Every .writeStream must have .trigger(...)."""
    write_stream_blocks = re.findall(
        r'\.writeStream\b.*?(?:\.start\(\)|\.toTable\([^)]*\))',
        all_code,
        re.DOTALL,
    )
    missing: list[str] = []

    for i, block in enumerate(write_stream_blocks):
        if ".trigger(" not in block:
            missing.append(f"writeStream block #{i + 1} missing .trigger()")

    if not write_stream_blocks:
        return CheckResult(
            check_id=6,
            name="all_streams_have_triggers",
            passed=True,
            severity="high",
            message="No streaming writes found (check not applicable)",
        )

    return CheckResult(
        check_id=6,
        name="all_streams_have_triggers",
        passed=len(missing) == 0,
        severity="high",
        message=f"All {len(write_stream_blocks)} streaming write(s) have triggers" if not missing
                else f"{len(missing)}/{len(write_stream_blocks)} streaming write(s) missing triggers",
        details=missing,
    )


# ---------------------------------------------------------------------------
# Check 7: no_plaintext_secrets  (high)
# ---------------------------------------------------------------------------

def _check_no_plaintext_secrets(all_code: str) -> CheckResult:
    """Zero hardcoded passwords, API keys, AWS keys."""
    findings: list[str] = []

    for label, pattern in _SECRET_PATTERNS:
        matches = pattern.findall(all_code)
        if matches:
            findings.append(f"{label}: {len(matches)} occurrence(s)")

    return CheckResult(
        check_id=7,
        name="no_plaintext_secrets",
        passed=len(findings) == 0,
        severity="high",
        message="No plaintext secrets detected" if not findings
                else f"{len(findings)} secret pattern(s) detected",
        details=findings,
    )


# ---------------------------------------------------------------------------
# Check 8: terminal_writes_exist  (critical)
# ---------------------------------------------------------------------------

def _check_terminal_writes_exist(all_code: str) -> CheckResult:
    """At least one .write or .writeStream call in the notebook."""
    write_count = len(re.findall(r'\.\s*write\b', all_code))
    write_stream_count = len(re.findall(r'\.\s*writeStream\b', all_code))
    save_as_table = len(re.findall(r'\.saveAsTable\(', all_code))
    to_table = len(re.findall(r'\.toTable\(', all_code))

    total = write_count + write_stream_count + save_as_table + to_table

    return CheckResult(
        check_id=8,
        name="terminal_writes_exist",
        passed=total > 0,
        severity="critical",
        message=f"{total} write operation(s) found" if total > 0
                else "No write operations found -- pipeline produces no output",
        details=[f"write: {write_count}, writeStream: {write_stream_count}, "
                 f"saveAsTable: {save_as_table}, toTable: {to_table}"],
    )


# ---------------------------------------------------------------------------
# Check 9: config_cell_present  (high)
# ---------------------------------------------------------------------------

def _check_config_cell_present(code_cells: list[dict]) -> CheckResult:
    """CATALOG, SCHEMA, SECRETS_SCOPE must be defined."""
    all_code = "\n".join(c["source"] for c in code_cells)

    # Accept various naming conventions
    catalog_re = re.compile(r'\b(?:catalog|CATALOG)\s*=', re.IGNORECASE)
    schema_re = re.compile(r'\b(?:schema|SCHEMA|schema_name)\s*=', re.IGNORECASE)
    scope_re = re.compile(r'\b(?:secret_scope|SECRETS_SCOPE|scope)\s*=', re.IGNORECASE)

    missing: list[str] = []
    if not catalog_re.search(all_code):
        missing.append("CATALOG not defined")
    if not schema_re.search(all_code):
        missing.append("SCHEMA not defined")
    if not scope_re.search(all_code):
        missing.append("SECRETS_SCOPE not defined")

    return CheckResult(
        check_id=9,
        name="config_cell_present",
        passed=len(missing) == 0,
        severity="high",
        message="Config cell contains CATALOG, SCHEMA, SECRETS_SCOPE" if not missing
                else f"Missing config variable(s): {', '.join(missing)}",
        details=missing,
    )


# ---------------------------------------------------------------------------
# Check 10: dag_order_correct  (medium)
# ---------------------------------------------------------------------------

def _check_dag_order_correct(code_cells: list[dict]) -> CheckResult:
    """Cell execution order matches definition order (every read happens after write).

    Specifically: when a df_X is written to (via .write/.saveAsTable/.toTable),
    any cell that reads df_X must appear BEFORE the write cell in the notebook
    (reads happen first, then the terminal write).
    """
    df_assign_re = re.compile(r'^(df_\w+)\s*=', re.MULTILINE)
    df_ref_re = re.compile(r'\b(df_\w+)\b')

    # Track first-definition cell index and first-reference cell index
    first_defined: dict[str, int] = {}
    first_referenced: dict[str, int] = {}

    for i, cell in enumerate(code_cells):
        source = cell["source"]
        for var in df_assign_re.findall(source):
            if var not in first_defined:
                first_defined[var] = i

        for var in df_ref_re.findall(source):
            if var not in first_referenced:
                first_referenced[var] = i

    order_violations: list[str] = []
    for var, ref_idx in first_referenced.items():
        def_idx = first_defined.get(var)
        if def_idx is not None and ref_idx < def_idx:
            order_violations.append(
                f"'{var}' referenced in cell {ref_idx} but first defined in cell {def_idx}"
            )

    return CheckResult(
        check_id=10,
        name="dag_order_correct",
        passed=len(order_violations) == 0,
        severity="medium",
        message="Cell order is correct (all reads after definitions)" if not order_violations
                else f"{len(order_violations)} order violation(s)",
        details=order_violations,
    )


# ---------------------------------------------------------------------------
# Check 11: relationship_routing_honored  (medium)
# ---------------------------------------------------------------------------

def _check_relationship_routing_honored(all_code: str, parsed=None) -> CheckResult:
    """If parsed flow has failure/matched/unmatched connections,
    corresponding filter/try-except code exists.
    """
    if parsed is None:
        return CheckResult(
            check_id=11,
            name="relationship_routing_honored",
            passed=True,
            severity="medium",
            message="No parsed flow provided -- skipping relationship check",
        )

    # Extract connections (handle both object and dict)
    connections = []
    if hasattr(parsed, "connections"):
        connections = parsed.connections
    elif isinstance(parsed, dict):
        connections = parsed.get("connections", [])

    # Identify non-success relationships
    special_rels: list[dict] = []
    for conn in connections:
        rel = getattr(conn, "relationship", None) or (conn.get("relationship", "") if isinstance(conn, dict) else "")
        if rel and rel.lower() not in ("success", ""):
            src = getattr(conn, "source_name", None) or (conn.get("source_name", "") if isinstance(conn, dict) else "")
            special_rels.append({"relationship": rel, "source": src})

    if not special_rels:
        return CheckResult(
            check_id=11,
            name="relationship_routing_honored",
            passed=True,
            severity="medium",
            message="No special relationship routing in parsed flow",
        )

    # Check that code handles each special relationship
    missing: list[str] = []
    for rel_info in special_rels:
        rel = rel_info["relationship"].lower()
        src = rel_info["source"]

        if rel == "failure":
            # Should have try/except or error-handling code
            if "try:" not in all_code and "except" not in all_code and "_errors" not in all_code:
                missing.append(f"failure relationship from '{src}' -- no try/except or error handling found")
        elif rel in ("matched", "unmatched"):
            # Should have .filter() calls
            if ".filter(" not in all_code and "F.when(" not in all_code:
                missing.append(f"'{rel}' relationship from '{src}' -- no .filter() or F.when() found")
        elif rel in ("valid", "invalid"):
            if ".filter(" not in all_code:
                missing.append(f"'{rel}' relationship from '{src}' -- no validation filter found")

    return CheckResult(
        check_id=11,
        name="relationship_routing_honored",
        passed=len(missing) == 0,
        severity="medium",
        message="All relationship routing is honored in generated code" if not missing
                else f"{len(missing)} relationship routing gap(s)",
        details=missing,
    )


# ---------------------------------------------------------------------------
# Check 12: processor_coverage_complete  (medium)
# ---------------------------------------------------------------------------

def _check_processor_coverage_complete(code_cells: list[dict], parsed=None) -> CheckResult:
    """Every processor in parsed flow has at least one notebook cell."""
    if parsed is None:
        return CheckResult(
            check_id=12,
            name="processor_coverage_complete",
            passed=True,
            severity="medium",
            message="No parsed flow provided -- skipping processor coverage check",
        )

    # Extract processor names (handle both object and dict)
    processors = []
    if hasattr(parsed, "processors"):
        processors = parsed.processors
    elif isinstance(parsed, dict):
        processors = parsed.get("processors", [])

    if not processors:
        return CheckResult(
            check_id=12,
            name="processor_coverage_complete",
            passed=True,
            severity="medium",
            message="No processors in parsed flow",
        )

    proc_names: list[str] = []
    for p in processors:
        name = getattr(p, "name", None) or (p.get("name", "") if isinstance(p, dict) else "")
        if name:
            proc_names.append(name)

    # Scan all code + markdown cells for processor name mentions
    all_text = "\n".join(c["source"] for c in _normalize_cells_full(code_cells))
    all_text_lower = all_text.lower()

    # Also check cell labels
    all_labels = " ".join(c.get("label", "") for c in code_cells).lower()

    uncovered: list[str] = []
    for name in proc_names:
        name_lower = name.lower()
        # Create safe variable form for matching
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()
        if (name_lower not in all_text_lower
                and safe not in all_text_lower
                and safe not in all_labels):
            uncovered.append(name)

    total = len(proc_names)
    covered = total - len(uncovered)

    return CheckResult(
        check_id=12,
        name="processor_coverage_complete",
        passed=len(uncovered) == 0,
        severity="medium",
        message=f"All {total} processor(s) covered" if not uncovered
                else f"{covered}/{total} processors covered -- {len(uncovered)} missing",
        details=[f"Missing: {name}" for name in uncovered],
    )


def _normalize_cells_full(cells: list) -> list[dict]:
    """Normalize cells accepting the already-normalized code_cells list."""
    result: list[dict] = []
    for c in cells:
        if isinstance(c, dict):
            result.append(c)
        else:
            result.append({
                "type": getattr(c, "type", "code"),
                "source": getattr(c, "source", ""),
                "label": getattr(c, "label", ""),
            })
    return result


__all__ = [
    "CheckResult",
    "RunnableReport",
    "check_runnable",
]

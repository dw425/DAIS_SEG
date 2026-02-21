"""
YAML Mapping File Validator.

Validates NiFi → Databricks processor mapping YAML files for:
- Required fields on every entry
- Template Python syntax (via ast.parse)
- Confidence range [0.0, 1.0]
- No duplicate processor types
- Valid role values
"""

import ast
import os
import re
from pathlib import Path
from typing import Dict, List, Optional


# ── Constants ─────────────────────────────────────────────────────
REQUIRED_FIELDS = {"type", "category", "template", "description", "imports", "confidence", "role"}
VALID_ROLES = {"source", "transform", "route", "process", "sink", "utility"}
PROCESSOR_MAPS_DIR = Path(__file__).resolve().parent.parent.parent / "constants" / "processor_maps"


class ValidationIssue:
    """A single validation issue found in a mapping file."""

    SEVERITY_ERROR = "ERROR"
    SEVERITY_WARNING = "WARNING"

    def __init__(self, file: str, index: int, proc_type: str, field: str, severity: str, message: str):
        self.file = file
        self.index = index
        self.proc_type = proc_type
        self.field = field
        self.severity = severity
        self.message = message

    def __repr__(self):
        return f"[{self.severity}] {self.file} #{self.index} ({self.proc_type}).{self.field}: {self.message}"

    def to_dict(self) -> Dict:
        return {
            "file": self.file,
            "index": self.index,
            "proc_type": self.proc_type,
            "field": self.field,
            "severity": self.severity,
            "message": self.message,
        }


def _strip_placeholders(template: str) -> str:
    """
    Replace ``{placeholder}`` tokens with dummy Python-safe values so
    that ``ast.parse`` can check syntax without false positives.
    """
    # Replace {name}-style placeholders with safe identifiers
    result = re.sub(r"\{(\w+)\}", r"__PH_\1__", template)
    # Fix f-string-like patterns that break syntax
    result = result.replace("__PH_", "PH_").replace("__", "")
    return result


def _check_template_syntax(template: str) -> Optional[str]:
    """
    Try to parse the template as Python. Returns None on success or
    an error message string on failure.

    Templates contain placeholders like ``{name}`` which are not valid
    Python, so we substitute them before parsing.
    """
    cleaned = _strip_placeholders(template)
    try:
        ast.parse(cleaned, mode="exec")
        return None
    except SyntaxError as e:
        return f"Line {e.lineno}: {e.msg}"


def validate_mapping_file(yaml_path: str) -> List[ValidationIssue]:
    """
    Validate a single YAML mapping file.

    Parameters
    ----------
    yaml_path : str
        Absolute or relative path to the YAML file.

    Returns
    -------
    list[ValidationIssue]
        All issues found. Empty list means the file is valid.
    """
    import yaml

    issues: List[ValidationIssue] = []
    filename = os.path.basename(yaml_path)

    # ── Load file ─────────────────────────────────────────────────
    try:
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        issues.append(ValidationIssue(filename, -1, "", "file", "ERROR", f"YAML parse error: {e}"))
        return issues
    except FileNotFoundError:
        issues.append(ValidationIssue(filename, -1, "", "file", "ERROR", "File not found"))
        return issues

    if not isinstance(data, dict) or "mappings" not in data:
        issues.append(ValidationIssue(filename, -1, "", "file", "ERROR", "Missing top-level 'mappings' key"))
        return issues

    mappings = data["mappings"]
    if not isinstance(mappings, list):
        issues.append(ValidationIssue(filename, -1, "", "file", "ERROR", "'mappings' must be a list"))
        return issues

    # ── Duplicate check ───────────────────────────────────────────
    seen_types: Dict[str, int] = {}

    for idx, entry in enumerate(mappings):
        proc_type = entry.get("type", f"<missing-type-at-{idx}>")

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in entry:
                issues.append(ValidationIssue(
                    filename, idx, proc_type, field,
                    "ERROR", f"Missing required field: {field}",
                ))

        # Check for duplicate types
        if proc_type in seen_types:
            issues.append(ValidationIssue(
                filename, idx, proc_type, "type",
                "ERROR", f"Duplicate type (first at index {seen_types[proc_type]})",
            ))
        else:
            seen_types[proc_type] = idx

        # Validate confidence range
        confidence = entry.get("confidence")
        if confidence is not None:
            if not isinstance(confidence, (int, float)):
                issues.append(ValidationIssue(
                    filename, idx, proc_type, "confidence",
                    "ERROR", f"Confidence must be a number, got {type(confidence).__name__}",
                ))
            elif not (0.0 <= confidence <= 1.0):
                issues.append(ValidationIssue(
                    filename, idx, proc_type, "confidence",
                    "ERROR", f"Confidence {confidence} out of range [0.0, 1.0]",
                ))

        # Validate role
        role = entry.get("role")
        if role is not None and role not in VALID_ROLES:
            issues.append(ValidationIssue(
                filename, idx, proc_type, "role",
                "ERROR", f"Invalid role '{role}'. Must be one of: {VALID_ROLES}",
            ))

        # Validate imports is a list
        imports = entry.get("imports")
        if imports is not None and not isinstance(imports, list):
            issues.append(ValidationIssue(
                filename, idx, proc_type, "imports",
                "ERROR", f"'imports' must be a list, got {type(imports).__name__}",
            ))

        # Validate template syntax
        template = entry.get("template")
        if template is not None and isinstance(template, str):
            syntax_err = _check_template_syntax(template)
            if syntax_err:
                issues.append(ValidationIssue(
                    filename, idx, proc_type, "template",
                    "WARNING", f"Template syntax issue: {syntax_err}",
                ))

            # Check template is not empty
            if not template.strip():
                issues.append(ValidationIssue(
                    filename, idx, proc_type, "template",
                    "ERROR", "Template is empty",
                ))

        # Validate description is non-empty
        desc = entry.get("description")
        if desc is not None and (not isinstance(desc, str) or not desc.strip()):
            issues.append(ValidationIssue(
                filename, idx, proc_type, "description",
                "WARNING", "Description is empty",
            ))

        # Validate category is non-empty
        category = entry.get("category")
        if category is not None and (not isinstance(category, str) or not category.strip()):
            issues.append(ValidationIssue(
                filename, idx, proc_type, "category",
                "WARNING", "Category is empty",
            ))

    return issues


def validate_all(maps_dir: Optional[str] = None) -> Dict[str, List[ValidationIssue]]:
    """
    Validate all YAML files in the processor_maps directory.

    Parameters
    ----------
    maps_dir : str, optional
        Override directory path. Defaults to the standard processor_maps location.

    Returns
    -------
    dict[str, list[ValidationIssue]]
        Mapping of filename → list of issues.
    """
    directory = Path(maps_dir) if maps_dir else PROCESSOR_MAPS_DIR

    results: Dict[str, List[ValidationIssue]] = {}
    for yaml_file in sorted(directory.glob("*.yaml")):
        issues = validate_mapping_file(str(yaml_file))
        results[yaml_file.name] = issues

    return results


def print_report(results: Dict[str, List[ValidationIssue]]) -> int:
    """
    Print a human-readable validation report.

    Returns the total number of errors (not warnings).
    """
    total_errors = 0
    total_warnings = 0

    for filename, issues in results.items():
        errors = [i for i in issues if i.severity == "ERROR"]
        warnings = [i for i in issues if i.severity == "WARNING"]
        total_errors += len(errors)
        total_warnings += len(warnings)

        if not issues:
            print(f"  [OK] {filename}")
        else:
            print(f"  [{len(errors)}E/{len(warnings)}W] {filename}")
            for issue in issues:
                print(f"    {issue}")

    print(f"\nTotal: {total_errors} errors, {total_warnings} warnings")
    return total_errors


# ── CLI entrypoint ────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Validate a specific file
        issues = validate_mapping_file(sys.argv[1])
        results = {os.path.basename(sys.argv[1]): issues}
    else:
        results = validate_all()

    error_count = print_report(results)
    sys.exit(1 if error_count > 0 else 0)

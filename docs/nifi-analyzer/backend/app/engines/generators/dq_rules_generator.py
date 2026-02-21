"""Data-quality rules generator — extracts DQ expectations from NiFi processors.

Scans ValidateRecord, RouteOnAttribute, EvaluateJsonPath, and Filter processors
to produce DLT expectations (@dlt.expect / @dlt.expect_or_drop / @dlt.expect_or_fail).
"""

import logging
import re

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)


def generate_dq_rules(
    parse_result: ParseResult,
    assessment: AssessmentResult,
) -> list[dict]:
    """Extract data-quality rules from processor definitions.

    Returns list of:
        {name, expression, action, source_processor}
    """
    rules: list[dict] = []

    for proc in parse_result.processors:
        proc_type = proc.type
        props = proc.properties

        if "ValidateRecord" in proc_type:
            rules.extend(_rules_from_validate_record(proc.name, props))
        elif "RouteOnAttribute" in proc_type:
            rules.extend(_rules_from_route_on_attribute(proc.name, props))
        elif "EvaluateJsonPath" in proc_type:
            rules.extend(_rules_from_evaluate_json_path(proc.name, props))
        elif proc_type in ("RouteOnContent", "RouteText"):
            rules.extend(_rules_from_route_content(proc.name, props))
        elif "Filter" in proc_type or "Validate" in proc_type:
            rules.extend(_rules_from_filter(proc.name, props))

    logger.info("DQ rules: %d rule(s) extracted from flow", len(rules))
    return rules


def generate_dq_notebook_cells(rules: list[dict]) -> list[dict]:
    """Generate DLT notebook cells that apply data-quality expectations.

    Returns list of notebook cell dicts with {type, source, label}.
    """
    if not rules:
        return [{
            "type": "markdown",
            "source": "## Data Quality\n\nNo data quality rules detected in the source flow.",
            "label": "dq_header",
        }]

    cells: list[dict] = []

    cells.append({
        "type": "markdown",
        "source": f"## Data Quality Rules\n\n{len(rules)} rule(s) extracted from the source flow.",
        "label": "dq_header",
    })

    # Group rules by action
    by_action: dict[str, list[dict]] = {"warn": [], "drop": [], "fail": []}
    for r in rules:
        by_action.setdefault(r["action"], []).append(r)

    # Build a single DLT table cell with all expectations
    expect_lines: list[str] = []
    for r in rules:
        decorator = _action_to_decorator(r["action"])
        expect_lines.append(
            f'@{decorator}("{r["name"]}", "{r["expression"]}")'
        )

    code = "\n".join(expect_lines) + "\n"
    code += '@dlt.table(\n    name="validated_output",\n    comment="Table with DQ expectations applied"\n)\n'
    code += 'def validated_output():\n'
    code += '    return dlt.read_stream("UNSET_upstream_table")  # USER ACTION: set actual upstream table\n'

    cells.append({
        "type": "code",
        "source": code,
        "label": "dq_expectations",
    })

    # Summary cell
    summary_lines = [f"- **{action.upper()}**: {len(rlist)} rule(s)" for action, rlist in by_action.items() if rlist]
    cells.append({
        "type": "markdown",
        "source": "### DQ Summary\n\n" + "\n".join(summary_lines),
        "label": "dq_summary",
    })

    return cells


# ---------------------------------------------------------------------------
# Rule extractors
# ---------------------------------------------------------------------------

def _rules_from_validate_record(proc_name: str, props: dict) -> list[dict]:
    """Extract rules from ValidateRecord processor properties."""
    rules: list[dict] = []

    # Check for schema-based validation
    schema_text = props.get("Schema Text", props.get("schema-text", ""))
    if schema_text:
        # Try to parse required fields from JSON/Avro schema
        for field_match in re.finditer(r'"name"\s*:\s*"(\w+)"', schema_text):
            field = field_match.group(1)
            rules.append({
                "name": f"not_null_{field}",
                "expression": f"{field} IS NOT NULL",
                "action": "drop",
                "source_processor": proc_name,
            })

    # Check validation strategy for action mapping
    strategy = props.get("Invalid Record Strategy", props.get("invalid-record-strategy", ""))
    action = "drop" if "route" in strategy.lower() else "warn"

    # If no concrete rules could be extracted, return empty list
    # rather than generating a tautology rule (1 = 1).
    return rules


def _rules_from_route_on_attribute(proc_name: str, props: dict) -> list[dict]:
    """Extract rules from RouteOnAttribute conditions."""
    rules: list[dict] = []

    for key, val in props.items():
        if key.lower() in ("routing strategy", "routing-strategy"):
            continue
        if not isinstance(val, str):
            continue

        expr = _nel_to_sql(val)
        rules.append({
            "name": f"route_{_safe_name(key)}",
            "expression": expr,
            "action": "warn",
            "source_processor": proc_name,
        })

    return rules


def _rules_from_evaluate_json_path(proc_name: str, props: dict) -> list[dict]:
    """Extract null-check rules from EvaluateJsonPath fields."""
    rules: list[dict] = []

    for key, val in props.items():
        if key.lower() in ("destination", "return type", "null value representation"):
            continue
        if not isinstance(val, str) or not val.startswith("$"):
            continue

        # JSON path field -> null check
        field_name = key.replace(" ", "_").lower()
        rules.append({
            "name": f"not_null_{field_name}",
            "expression": f"{field_name} IS NOT NULL",
            "action": "warn",
            "source_processor": proc_name,
        })

    return rules


def _rules_from_route_content(proc_name: str, props: dict) -> list[dict]:
    """Extract rules from RouteOnContent / RouteText processors."""
    rules: list[dict] = []
    match_req = props.get("Match Requirement", props.get("match-requirement", "content must match exactly"))

    for key, val in props.items():
        if key.lower() in ("match requirement", "match-requirement", "character set"):
            continue
        if isinstance(val, str) and val.strip():
            rules.append({
                "name": f"content_{_safe_name(key)}",
                "expression": f"value RLIKE '{_escape_sql(val)}'",
                "action": "warn",
                "source_processor": proc_name,
            })

    return rules


def _rules_from_filter(proc_name: str, props: dict) -> list[dict]:
    """Extract NOT NULL / range checks from filter-like processors."""
    rules: list[dict] = []

    filter_expr = props.get("Filter Expression", props.get("filter-expression", ""))
    if filter_expr:
        expr = _nel_to_sql(filter_expr)
        rules.append({
            "name": f"filter_{_safe_name(proc_name)}",
            "expression": expr,
            "action": "drop",
            "source_processor": proc_name,
        })
    # If no filter expression found, return empty list rather than
    # generating a tautology rule (1 = 1).
    return rules


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()


def _escape_sql(s: str) -> str:
    return s.replace("'", "''").replace("\\", "\\\\")


def _action_to_decorator(action: str) -> str:
    return {
        "warn": "dlt.expect",
        "drop": "dlt.expect_or_drop",
        "fail": "dlt.expect_or_fail",
    }.get(action, "dlt.expect")


def _nel_to_sql(nel_expr: str) -> str:
    """Best-effort NiFi Expression Language to SQL conversion."""
    expr = nel_expr
    expr = re.sub(r'\$\{([^}:]+):isEmpty\(\)\}', r'\1 IS NULL', expr)
    expr = re.sub(r'\$\{([^}:]+):isNull\(\)\}', r'\1 IS NULL', expr)
    expr = re.sub(r'\$\{([^}:]+):equals\(["\']?([^"\')}]+)["\']?\)\}',
                  r"\1 = '\2'", expr)
    expr = re.sub(r'\$\{([^}:]+):contains\(["\']?([^"\')}]+)["\']?\)\}',
                  r"\1 LIKE '%\2%'", expr)
    expr = re.sub(r'\$\{([^}:]+):gt\((\d+)\)\}', r'\1 > \2', expr)
    expr = re.sub(r'\$\{([^}:]+):lt\((\d+)\)\}', r'\1 < \2', expr)
    expr = re.sub(r'\$\{([^}]+)\}', r'\1', expr)
    return expr

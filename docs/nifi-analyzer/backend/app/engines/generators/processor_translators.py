"""Processor-specific translators — specialized code generation for complex NiFi processors.

Handles:
- RouteOnAttribute -> condition_task or df.filter() / withColumn(when/otherwise)
- JoltTransformJSON -> PySpark select/alias, na.fill, drop
- ExecuteScript/ExecuteGroovyScript -> PySpark UDF wrapping
"""

import json
import re
from typing import Any

from app.models.pipeline import MappingEntry, ParseResult


# ---------------------------------------------------------------------------
# RouteOnAttribute translation
# ---------------------------------------------------------------------------

def translate_route_on_attribute(
    mapping: MappingEntry,
    parse_result: ParseResult,
) -> dict[str, Any]:
    """Translate RouteOnAttribute to either condition_task or df.filter().

    Returns:
        dict with keys:
          - strategy: "workflow_condition" | "dataframe_filter"
          - code: generated PySpark code
          - conditions: parsed route conditions
          - workflow_tasks: (only if strategy == "workflow_condition") list of task dicts
    """
    conditions = _extract_route_conditions(mapping, parse_result)
    strategy = _determine_route_strategy(mapping, parse_result, conditions)

    if strategy == "workflow_condition":
        return {
            "strategy": "workflow_condition",
            "code": _generate_condition_task_comment(mapping, conditions),
            "conditions": conditions,
            "workflow_tasks": _generate_condition_tasks(mapping, conditions),
        }
    else:
        return {
            "strategy": "dataframe_filter",
            "code": _generate_filter_code(mapping, conditions),
            "conditions": conditions,
            "workflow_tasks": [],
        }


def _determine_route_strategy(
    mapping: MappingEntry,
    parse_result: ParseResult,
    conditions: dict[str, str],
) -> str:
    """Determine whether routing should use workflow condition_task or DataFrame filter.

    Use workflow condition_task when routes lead to entirely different execution paths
    (different sinks/outputs). Use DataFrame filter when routes tag/filter rows.
    """
    # Check how many distinct downstream paths exist
    route_destinations: dict[str, list[str]] = {}
    for conn in parse_result.connections:
        if conn.source_name == mapping.name:
            rel = conn.relationship
            route_destinations.setdefault(rel, []).append(conn.destination_name)

    # If routes go to completely different processor types -> workflow condition
    if len(route_destinations) >= 2:
        dest_types = set()
        for dests in route_destinations.values():
            for dest_name in dests:
                for proc in parse_result.processors:
                    if proc.name == dest_name:
                        dest_types.add(proc.type)
        # If destinations are all different types (e.g., PutS3 vs PutDatabase), use workflow
        sink_count = sum(1 for t in dest_types if any(
            t.startswith(p) for p in ("Put", "Publish", "Send", "Write", "Insert")
        ))
        if sink_count >= 2:
            return "workflow_condition"

    return "dataframe_filter"


def _extract_route_conditions(
    mapping: MappingEntry,
    parse_result: ParseResult,
) -> dict[str, str]:
    """Extract routing conditions from processor properties."""
    conditions: dict[str, str] = {}
    props = _get_processor_properties(mapping.name, parse_result)

    for key, val in props.items():
        # Skip non-condition properties
        if key.lower() in ("routing strategy", "name", "id", "comments",
                           "scheduling strategy", "scheduling period"):
            continue
        # NiFi stores route conditions as property name -> NEL expression
        if "${" in str(val) or "==" in str(val) or "!=" in str(val):
            conditions[key] = str(val)
        elif key not in ("Routing Strategy",) and val:
            conditions[key] = str(val)

    return conditions


def _nel_condition_to_spark(nel_expr: str) -> str:
    """Convert a NiFi Expression Language condition to PySpark filter expression."""
    expr = nel_expr.strip()

    # ${attr:equals('value')}
    expr = re.sub(
        r"\$\{([^}:]+):equals\(['\"]?([^'\")}]+)['\"]?\)\}",
        r'(F.col("\1") == "\2")',
        expr,
    )

    # ${attr:isEmpty()}
    expr = re.sub(
        r"\$\{([^}:]+):isEmpty\(\)\}",
        r'F.col("\1").isNull() | (F.col("\1") == "")',
        expr,
    )

    # ${attr:contains('value')}
    expr = re.sub(
        r"\$\{([^}:]+):contains\(['\"]?([^'\")}]+)['\"]?\)\}",
        r'F.col("\1").contains("\2")',
        expr,
    )

    # ${attr:matches('regex')}
    expr = re.sub(
        r"\$\{([^}:]+):matches\(['\"]?([^'\")}]+)['\"]?\)\}",
        r'F.col("\1").rlike("\2")',
        expr,
    )

    # Plain ${attr} reference
    expr = re.sub(r"\$\{([^}]+)\}", r'F.col("\1")', expr)

    # If nothing was converted, wrap as literal
    if not expr.startswith("(") and not expr.startswith("F.col"):
        expr = f'F.lit("{expr}")'

    return expr


def _generate_filter_code(
    mapping: MappingEntry,
    conditions: dict[str, str],
) -> str:
    """Generate df.filter() or withColumn(when/otherwise) code for route conditions."""
    safe_name = _safe_var(mapping.name)
    lines = [f"# RouteOnAttribute: {mapping.name}"]

    if not conditions:
        lines.append(f"# No conditions extracted; passthrough")
        lines.append(f"df_{safe_name} = df")
        return "\n".join(lines)

    if len(conditions) == 1:
        # Single condition -> simple filter
        cond_name, cond_expr = next(iter(conditions.items()))
        spark_expr = _nel_condition_to_spark(cond_expr)
        lines.append(f"# Route: {cond_name}")
        lines.append(f"df_{safe_name} = df.filter({spark_expr})")
    else:
        # Multiple conditions -> withColumn tagging + individual filtered DataFrames
        lines.append(f"# Tag rows with route labels using when/otherwise")
        when_chain = ""
        for i, (cond_name, cond_expr) in enumerate(conditions.items()):
            spark_expr = _nel_condition_to_spark(cond_expr)
            safe_cond = _safe_var(cond_name)
            if i == 0:
                when_chain = f'F.when({spark_expr}, F.lit("{safe_cond}"))'
            else:
                when_chain += f'\n    .when({spark_expr}, F.lit("{safe_cond}"))'

        when_chain += '\n    .otherwise(F.lit("unmatched"))'
        lines.append(f'df_{safe_name} = df.withColumn("_route", {when_chain})')
        lines.append("")

        # Generate individual filtered DataFrames per route
        for cond_name in conditions:
            safe_cond = _safe_var(cond_name)
            lines.append(f'df_{safe_name}_{safe_cond} = df_{safe_name}.filter(F.col("_route") == "{safe_cond}")')

    return "\n".join(lines)


def _generate_condition_task_comment(
    mapping: MappingEntry,
    conditions: dict[str, str],
) -> str:
    """Generate a comment block showing the workflow condition_task configuration."""
    lines = [
        f"# RouteOnAttribute: {mapping.name}",
        f"# This route splits into distinct execution paths.",
        f"# Implemented as Databricks Workflow condition_task (see workflow JSON).",
        f"#",
        f"# Route conditions:",
    ]
    for cond_name, cond_expr in conditions.items():
        lines.append(f"#   {cond_name}: {cond_expr}")

    lines.append("")
    lines.append("# Inter-task metadata passing:")
    lines.append('dbutils.jobs.taskValues.set(key="route_result", value="<computed_route>")')

    return "\n".join(lines)


def _generate_condition_tasks(
    mapping: MappingEntry,
    conditions: dict[str, str],
) -> list[dict]:
    """Generate Databricks Workflow condition_task entries."""
    safe_name = _safe_var(mapping.name)
    tasks = []

    # Condition evaluation task
    tasks.append({
        "task_key": f"route_{safe_name}",
        "condition_task": {
            "op": "EQUAL_TO",
            "left": f'{{{{tasks.route_{safe_name}_eval.values.route_result}}}}',
            "right": "true",
        },
    })

    # One task per route branch
    for cond_name in conditions:
        safe_cond = _safe_var(cond_name)
        tasks.append({
            "task_key": f"route_{safe_name}_{safe_cond}",
            "depends_on": [{"task_key": f"route_{safe_name}", "outcome": "true"}],
            "notebook_task": {
                "notebook_path": f"./notebooks/route_{safe_name}_{safe_cond}",
            },
        })

    return tasks


# ---------------------------------------------------------------------------
# JoltTransformJSON translation
# ---------------------------------------------------------------------------

def translate_jolt_transform(
    mapping: MappingEntry,
    parse_result: ParseResult,
) -> str:
    """Translate JoltTransformJSON specification to PySpark structural manipulation.

    Jolt operation types:
    - shift -> PySpark select() with alias()
    - default -> na.fill() or coalesce()
    - remove -> drop()
    - sort -> orderBy() (sort keys in JSON)
    - cardinality -> select with appropriate column types
    """
    props = _get_processor_properties(mapping.name, parse_result)
    safe_name = _safe_var(mapping.name)

    jolt_spec_raw = props.get("Jolt Specification", props.get("jolt-spec", ""))
    jolt_transform = props.get("Jolt Transform", props.get("jolt-transform", ""))

    lines = [f"# JoltTransformJSON: {mapping.name}"]

    if not jolt_spec_raw:
        lines.append(f"# No Jolt specification found in processor properties")
        lines.append(f"# USER ACTION: Add Jolt transformation logic")
        lines.append(f"# (passthrough — manual review needed)")
        lines.append(f"df_{safe_name} = df.select('*')")
        return "\n".join(lines)

    # Try to parse the Jolt spec as JSON
    try:
        jolt_spec = json.loads(jolt_spec_raw) if isinstance(jolt_spec_raw, str) else jolt_spec_raw
    except (json.JSONDecodeError, TypeError):
        lines.append(f"# Could not parse Jolt spec: {jolt_spec_raw[:100]}...")
        lines.append(f"# USER ACTION: Add Jolt transformation logic")
        lines.append(f"# (passthrough — manual review needed)")
        lines.append(f"df_{safe_name} = df.select('*')")
        return "\n".join(lines)

    # Handle both single spec and chain of specs
    specs = jolt_spec if isinstance(jolt_spec, list) else [jolt_spec]

    lines.append(f"df_{safe_name} = df")

    for spec in specs:
        if not isinstance(spec, dict):
            continue

        operation = spec.get("operation", jolt_transform).lower()

        if operation == "shift":
            shift_spec = spec.get("spec", {})
            lines.extend(_translate_jolt_shift(safe_name, shift_spec))

        elif operation == "default":
            default_spec = spec.get("spec", {})
            lines.extend(_translate_jolt_default(safe_name, default_spec))

        elif operation == "remove":
            remove_spec = spec.get("spec", {})
            lines.extend(_translate_jolt_remove(safe_name, remove_spec))

        elif operation == "sort":
            lines.append(f"# Jolt sort: key ordering (no-op in Spark, columns already named)")

        elif operation == "cardinality":
            cardinality_spec = spec.get("spec", {})
            lines.extend(_translate_jolt_cardinality(safe_name, cardinality_spec))

        else:
            lines.append(f"# Unhandled Jolt operation: {operation}")
            lines.append(f"# TODO: translate Jolt {operation} to PySpark")

    return "\n".join(lines)


def _translate_jolt_shift(safe_name: str, shift_spec: dict, prefix: str = "") -> list[str]:
    """Translate Jolt shift spec to PySpark select with alias.

    Jolt shift maps source paths to destination paths.
    e.g., {"name": "customer.fullName"} -> select(col("name").alias("customer_fullName"))
    """
    lines = []
    select_exprs = []

    for src_key, dest_value in shift_spec.items():
        if src_key in ("*", "@"):
            # Wildcard / self-reference
            if src_key == "*":
                lines.append(f"# Jolt wildcard shift: all fields mapped")
                continue
            else:
                # @ = current value
                continue

        src_path = f"{prefix}.{src_key}" if prefix else src_key
        spark_src = src_path.replace(".", "_")

        if isinstance(dest_value, str):
            dest_path = dest_value.replace(".", "_")
            select_exprs.append(f'F.col("{spark_src}").alias("{dest_path}")')
        elif isinstance(dest_value, dict):
            # Nested shift -> recurse
            nested = _translate_jolt_shift(safe_name, dest_value, src_path)
            lines.extend(nested)
        elif isinstance(dest_value, list):
            # Array of destinations -> duplicate column
            for dest in dest_value:
                dest_path = str(dest).replace(".", "_")
                select_exprs.append(f'F.col("{spark_src}").alias("{dest_path}")')

    if select_exprs:
        expr_str = ",\n    ".join(select_exprs)
        lines.append(f"df_{safe_name} = df_{safe_name}.select(\n    {expr_str}\n)")

    return lines


def _translate_jolt_default(safe_name: str, default_spec: dict) -> list[str]:
    """Translate Jolt default spec to PySpark na.fill() or coalesce()."""
    lines = ["# Jolt default: fill null values"]
    fill_map: dict[str, Any] = {}

    def _flatten_defaults(spec: dict, prefix: str = "") -> None:
        for key, val in spec.items():
            col_name = f"{prefix}.{key}" if prefix else key
            spark_col = col_name.replace(".", "_")
            if isinstance(val, dict):
                _flatten_defaults(val, col_name)
            else:
                fill_map[spark_col] = val

    _flatten_defaults(default_spec)

    if fill_map:
        # Group by type for na.fill
        # IMPORTANT: check bool BEFORE int/float because bool is a subclass of int in Python
        bool_fills = {k: v for k, v in fill_map.items() if isinstance(v, bool)}
        str_fills = {k: v for k, v in fill_map.items() if isinstance(v, str)}
        num_fills = {k: v for k, v in fill_map.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}

        if str_fills:
            lines.append(f"df_{safe_name} = df_{safe_name}.na.fill({str_fills})")
        if num_fills:
            lines.append(f"df_{safe_name} = df_{safe_name}.na.fill({num_fills})")
        if bool_fills:
            for col_name, val in bool_fills.items():
                lines.append(
                    f'df_{safe_name} = df_{safe_name}.withColumn("{col_name}", '
                    f'F.coalesce(F.col("{col_name}"), F.lit({val})))'
                )
    else:
        lines.append(f"# No default values specified")

    return lines


def _translate_jolt_remove(safe_name: str, remove_spec: dict) -> list[str]:
    """Translate Jolt remove spec to PySpark drop()."""
    lines = ["# Jolt remove: drop columns"]
    cols_to_drop = []

    def _collect_removes(spec: dict, prefix: str = "") -> None:
        for key, val in spec.items():
            col_name = f"{prefix}.{key}" if prefix else key
            spark_col = col_name.replace(".", "_")
            if isinstance(val, str) and val == "":
                cols_to_drop.append(spark_col)
            elif isinstance(val, dict):
                _collect_removes(val, col_name)
            else:
                cols_to_drop.append(spark_col)

    _collect_removes(remove_spec)

    if cols_to_drop:
        cols_str = ", ".join(f'"{c}"' for c in cols_to_drop)
        lines.append(f"df_{safe_name} = df_{safe_name}.drop({cols_str})")
    else:
        lines.append(f"# No columns to remove")

    return lines


def _translate_jolt_cardinality(safe_name: str, cardinality_spec: dict) -> list[str]:
    """Translate Jolt cardinality spec to PySpark first()/collect_list().

    ONE  -> first() aggregate (collapse array to scalar)
    MANY -> collect_list() (ensure value is an array)
    """
    lines = ["# Jolt cardinality: coerce array/scalar types"]

    def _collect_rules(spec: dict, prefix: str = "") -> list[tuple[str, str]]:
        rules = []
        for key, val in spec.items():
            col_name = f"{prefix}.{key}" if prefix else key
            spark_col = col_name.replace(".", "_")
            if isinstance(val, dict):
                rules.extend(_collect_rules(val, col_name))
            elif isinstance(val, str):
                rules.append((spark_col, val.upper()))
        return rules

    rules = _collect_rules(cardinality_spec)

    for col_name, card in rules:
        if card == "ONE":
            lines.append(
                f'df_{safe_name} = df_{safe_name}.withColumn("{col_name}", '
                f'F.element_at(F.col("{col_name}"), 1))'
            )
        elif card == "MANY":
            lines.append(
                f'df_{safe_name} = df_{safe_name}.withColumn("{col_name}", '
                f'F.array(F.col("{col_name}")))'
            )
        else:
            lines.append(f"# Unknown cardinality '{card}' for column '{col_name}'")

    if not rules:
        lines.append(f"# No cardinality rules specified")

    return lines


# ---------------------------------------------------------------------------
# ExecuteScript / ExecuteGroovyScript -> PySpark UDF
# ---------------------------------------------------------------------------

def translate_execute_script(
    mapping: MappingEntry,
    parse_result: ParseResult,
) -> str:
    """Translate ExecuteScript/ExecuteGroovyScript to a PySpark UDF.

    Extracts the embedded script payload and wraps it in a @udf decorator.
    """
    props = _get_processor_properties(mapping.name, parse_result)
    safe_name = _safe_var(mapping.name)

    # NiFi stores script body in different properties depending on version
    script_body = props.get("Script Body", props.get("script-body", ""))
    script_engine = props.get("Script Engine", props.get("script-engine", "python"))
    script_file = props.get("Script File", props.get("script-file", ""))

    lines = [f"# ExecuteScript: {mapping.name}"]

    if not script_body and script_file:
        lines.append(f"# Original script file: {script_file}")
        lines.append(f"# USER ACTION: Port external script file to this UDF")
        lines.append(f"@F.udf(returnType=StringType())")
        lines.append(f"def udf_{safe_name}(value):")
        lines.append(f'    """Ported from {script_file}."""')
        lines.append(f"    # USER ACTION: Port external script file to this UDF")
        lines.append(f"    return value")
        lines.append(f"")
        lines.append(f'df_{safe_name} = df.withColumn("result", udf_{safe_name}(F.col("value")))')
        return "\n".join(lines)

    if not script_body:
        lines.append(f"# No script body found")
        lines.append(f"df_{safe_name} = df.select('*')  # TODO: add UDF logic")
        return "\n".join(lines)

    # Determine script language and wrap appropriately
    language = script_engine.lower() if script_engine else "python"

    if "groovy" in language or "ExecuteGroovy" in mapping.type:
        lines.append(f"# Original Groovy script (ported to Python UDF):")
        lines.extend(f"# {line}" for line in script_body.strip().split("\n")[:20])
        if len(script_body.strip().split("\n")) > 20:
            lines.append(f"# ... ({len(script_body.strip().split(chr(10)))} lines total)")
        lines.append("")
        lines.append(f"@F.udf(returnType=StringType())")
        lines.append(f"def udf_{safe_name}(value):")
        lines.append(f'    """Ported from Groovy ExecuteScript: {mapping.name}."""')
        lines.append(f"    # USER ACTION: Port Groovy script logic to Python")
        lines.append(f"    return value")
    elif "python" in language or "jython" in language:
        # Python/Jython script -> extract and wrap
        lines.append(f"@F.udf(returnType=StringType())")
        lines.append(f"def udf_{safe_name}(value):")
        lines.append(f'    """Ported from NiFi ExecuteScript: {mapping.name}."""')

        # Try to extract the core logic from NiFi Python script
        cleaned = _extract_python_logic(script_body)
        if cleaned:
            for cl in cleaned.split("\n"):
                lines.append(f"    {cl}")
        else:
            lines.append(f"    # Original script:")
            for sl in script_body.strip().split("\n")[:30]:
                lines.append(f"    # {sl}")
            lines.append(f"    return value")
    else:
        lines.append(f"# Script engine: {language}")
        lines.append(f"@F.udf(returnType=StringType())")
        lines.append(f"def udf_{safe_name}(value):")
        lines.append(f'    """Ported from {language} script: {mapping.name}."""')
        lines.append(f"    # TODO: translate {language} logic to Python")
        lines.append(f"    return value")

    lines.append("")
    lines.append(f"# Register UDF and apply to DataFrame")
    lines.append(f'spark.udf.register("{safe_name}", udf_{safe_name})')
    lines.append(f'df_{safe_name} = df.withColumn("result", udf_{safe_name}(F.col("value")))')

    return "\n".join(lines)


def _extract_python_logic(script_body: str) -> str:
    """Extract the core transformation logic from a NiFi Python script.

    NiFi Python scripts typically access FlowFile via session.get(), read content,
    process it, and write back. We extract the processing part.
    """
    lines = script_body.strip().split("\n")
    core_lines = []
    in_process = False

    for line in lines:
        stripped = line.strip()
        # Skip NiFi session boilerplate
        if any(kw in stripped for kw in [
            "session.get", "session.transfer", "session.commit",
            "session.rollback", "flowFile", "FlowFile",
            "import org.", "import java.",
        ]):
            continue
        # Skip empty lines at the start
        if not core_lines and not stripped:
            continue
        # Start collecting after we see actual logic
        if stripped and not stripped.startswith("#"):
            in_process = True
        if in_process:
            core_lines.append(line)

    if core_lines:
        return "\n".join(core_lines)
    return ""


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_processor_properties(proc_name: str, parse_result: ParseResult) -> dict:
    """Look up properties from the original processor."""
    for proc in parse_result.processors:
        if proc.name == proc_name:
            return proc.properties
    return {}


def _safe_var(name: str) -> str:
    """Convert name to safe Python variable."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()

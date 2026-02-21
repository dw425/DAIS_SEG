"""Airflow DAG Python file parser using ast module.

Extracts DAG id, operator tasks, and task dependencies.
"""

import ast
import logging
import re

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_airflow(content: bytes, filename: str) -> ParseResult:
    """Parse an Airflow DAG Python file."""
    text = content.decode("utf-8", errors="replace")
    warnings: list[Warning] = []
    processors: list[Processor] = []
    connections: list[Connection] = []

    try:
        tree = ast.parse(text, filename=filename)
    except SyntaxError as exc:
        raise ValueError(f"Python syntax error: {exc}") from exc

    dag_id = ""
    task_vars: dict[str, dict] = {}  # var_name -> {name, type, properties}

    for node in ast.walk(tree):
        # Find DAG() instantiation
        if isinstance(node, ast.Call):
            func_name = _get_call_name(node)
            if func_name == "DAG" or func_name.endswith(".DAG"):
                if node.args:
                    dag_id = _get_const_value(node.args[0])
                for kw in node.keywords:
                    if kw.arg == "dag_id":
                        dag_id = _get_const_value(kw.value)

        # Find task assignments: task = Operator(...)
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
                call = node.value
                func_name = _get_call_name(call)
                if func_name and ("Operator" in func_name or "Sensor" in func_name or "Task" in func_name):
                    props: dict[str, str] = {}
                    task_id = target.id
                    for kw in call.keywords:
                        if kw.arg:
                            props[kw.arg] = _get_const_value(kw.value)
                        if kw.arg == "task_id":
                            task_id = _get_const_value(kw.value) or target.id
                    task_vars[target.id] = {"name": task_id, "type": func_name, "properties": props}

    # Build processors from discovered tasks
    for var_name, info in task_vars.items():
        processors.append(
            Processor(
                name=info["name"],
                type=info["type"],
                platform="airflow",
                properties=info["properties"],
            )
        )

    # Extract dependencies from >> chains (e.g. extract >> transform >> load)
    chain_dep_re = re.compile(r"(\w+(?:\s*>>\s*\w+)+)")
    for match in chain_dep_re.finditer(text):
        parts = [p.strip() for p in match.group(0).split(">>")]
        for i in range(len(parts) - 1):
            src_var = parts[i].strip()
            dst_var = parts[i + 1].strip()
            src_name = task_vars.get(src_var, {}).get("name", src_var)
            dst_name = task_vars.get(dst_var, {}).get("name", dst_var)
            connections.append(Connection(source_name=src_name, destination_name=dst_name))

    # Also handle chain() calls
    chain_re = re.compile(r"chain\(([^)]+)\)")
    for match in chain_re.finditer(text):
        items = [s.strip() for s in match.group(1).split(",")]
        for i in range(len(items) - 1):
            src_name = task_vars.get(items[i], {}).get("name", items[i])
            dst_name = task_vars.get(items[i + 1], {}).get("name", items[i + 1])
            connections.append(Connection(source_name=src_name, destination_name=dst_name))

    if not processors:
        warnings.append(Warning(severity="warning", message="No tasks found in Airflow DAG", source=filename))

    return ParseResult(
        platform="airflow",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename, "dag_id": dag_id},
        warnings=warnings,
    )


def _get_call_name(node: ast.Call) -> str:
    """Extract the function name from a Call node."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _get_const_value(node: ast.expr) -> str:
    """Extract a constant string value from an AST node."""
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        return "<f-string>"
    return ""

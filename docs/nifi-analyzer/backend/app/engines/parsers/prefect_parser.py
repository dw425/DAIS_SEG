"""Prefect flow Python file parser using ast module.

Extracts flow name, task decorators, and dependencies.
"""

import ast
import logging
import re

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_prefect(content: bytes, filename: str) -> ParseResult:
    """Parse a Prefect flow Python file."""
    text = content.decode("utf-8", errors="replace")
    warnings: list[Warning] = []
    processors: list[Processor] = []
    connections: list[Connection] = []

    try:
        tree = ast.parse(text, filename=filename)
    except SyntaxError as exc:
        raise ValueError(f"Python syntax error: {exc}") from exc

    flow_name = ""
    tasks: dict[str, str] = {}  # func_name -> display_name

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                dec_name = ""
                if isinstance(decorator, ast.Name):
                    dec_name = decorator.id
                elif isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name):
                        dec_name = decorator.func.id
                    elif isinstance(decorator.func, ast.Attribute):
                        dec_name = decorator.func.attr

                if dec_name == "flow":
                    flow_name = node.name
                    # Extract name kwarg
                    if isinstance(decorator, ast.Call):
                        for kw in decorator.keywords:
                            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                flow_name = str(kw.value.value)
                elif dec_name == "task":
                    task_name = node.name
                    if isinstance(decorator, ast.Call):
                        for kw in decorator.keywords:
                            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                task_name = str(kw.value.value)
                    tasks[node.name] = task_name

    # Build processors from tasks
    for func_name, display_name in tasks.items():
        processors.append(
            Processor(
                name=display_name,
                type="PrefectTask",
                platform="prefect",
                properties={"function": func_name},
            )
        )

    # Extract call order from flow function body
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == (flow_name or ""):
            prev_task = ""
            for stmt in node.body:
                if isinstance(stmt, (ast.Expr, ast.Assign)):
                    call = stmt.value if isinstance(stmt, ast.Expr) else stmt.value
                    if isinstance(call, ast.Call):
                        called = ""
                        if isinstance(call.func, ast.Name):
                            called = call.func.id
                        elif isinstance(call.func, ast.Attribute):
                            called = call.func.attr
                        if called in tasks:
                            if prev_task:
                                connections.append(
                                    Connection(
                                        source_name=tasks[prev_task],
                                        destination_name=tasks[called],
                                    )
                                )
                            prev_task = called

    # Also try regex-based dependency detection for .submit() patterns
    submit_re = re.compile(r"(\w+)\.submit\(.*?wait_for\s*=\s*\[([^\]]+)\]", re.DOTALL)
    for match in submit_re.finditer(text):
        task_name = match.group(1)
        deps = [d.strip() for d in match.group(2).split(",")]
        for dep in deps:
            if dep in tasks and task_name in tasks:
                connections.append(
                    Connection(
                        source_name=tasks[dep],
                        destination_name=tasks[task_name],
                    )
                )

    if not processors:
        warnings.append(Warning(severity="warning", message="No tasks found in Prefect flow", source=filename))

    return ParseResult(
        platform="prefect",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename, "flow_name": flow_name},
        warnings=warnings,
    )

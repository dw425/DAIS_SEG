"""Dagster pipeline Python file parser using ast module.

Extracts ops, jobs, assets, and resources.
"""

import ast
import logging

from app.models.pipeline import ParseResult, Warning
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)


def parse_dagster(content: bytes, filename: str) -> ParseResult:
    """Parse a Dagster pipeline Python file."""
    text = content.decode("utf-8", errors="replace")
    warnings: list[Warning] = []
    processors: list[Processor] = []
    connections: list[Connection] = []

    try:
        tree = ast.parse(text, filename=filename)
    except SyntaxError as exc:
        raise ValueError(f"Python syntax error: {exc}") from exc

    ops: dict[str, str] = {}  # func_name -> display_name
    assets: dict[str, str] = {}
    jobs: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                dec_name = _get_decorator_name(decorator)

                if dec_name == "op":
                    op_name = node.name
                    if isinstance(decorator, ast.Call):
                        for kw in decorator.keywords:
                            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                op_name = str(kw.value.value)
                    ops[node.name] = op_name

                elif dec_name == "asset":
                    asset_name = node.name
                    if isinstance(decorator, ast.Call):
                        for kw in decorator.keywords:
                            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                asset_name = str(kw.value.value)
                    assets[node.name] = asset_name

                elif dec_name == "job":
                    jobs.append(node.name)

                elif dec_name == "graph":
                    jobs.append(node.name)

    # Build processors
    for func_name, display_name in ops.items():
        processors.append(
            Processor(
                name=display_name,
                type="DagsterOp",
                platform="dagster",
                properties={"function": func_name},
            )
        )

    for func_name, display_name in assets.items():
        processors.append(
            Processor(
                name=display_name,
                type="DagsterAsset",
                platform="dagster",
                properties={"function": func_name},
            )
        )

    # Extract dependencies from asset function parameters (ins)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in assets:
            # Asset deps come from parameter names that match other assets
            for arg in node.args.args:
                arg_name = arg.arg
                if arg_name in assets:
                    connections.append(
                        Connection(
                            source_name=assets[arg_name],
                            destination_name=assets[node.name],
                        )
                    )

    # Extract op chaining from graph/job function bodies
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in jobs:
            _extract_op_chain(node.body, ops, connections)

    if not processors:
        warnings.append(Warning(severity="warning", message="No ops/assets found in Dagster file", source=filename))

    return ParseResult(
        platform="dagster",
        processors=processors,
        connections=connections,
        metadata={"source_file": filename, "jobs": jobs},
        warnings=warnings,
    )


def _get_decorator_name(decorator: ast.expr) -> str:
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Call):
        if isinstance(decorator.func, ast.Name):
            return decorator.func.id
        if isinstance(decorator.func, ast.Attribute):
            return decorator.func.attr
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    return ""


def _extract_op_chain(body: list[ast.stmt], ops: dict[str, str], connections: list[Connection]) -> None:
    """Extract op call chain from job/graph body."""
    prev_op = ""
    for stmt in body:
        if isinstance(stmt, (ast.Expr, ast.Assign)):
            call = stmt.value if isinstance(stmt, ast.Expr) else stmt.value
            if isinstance(call, ast.Call):
                called = ""
                if isinstance(call.func, ast.Name):
                    called = call.func.id
                elif isinstance(call.func, ast.Attribute):
                    called = call.func.attr
                if called in ops:
                    if prev_op:
                        connections.append(
                            Connection(
                                source_name=ops[prev_op],
                                destination_name=ops[called],
                            )
                        )
                    prev_op = called
                    # Check if any arg is a call to another op
                    for arg in call.args:
                        if isinstance(arg, ast.Call):
                            inner = ""
                            if isinstance(arg.func, ast.Name):
                                inner = arg.func.id
                            elif isinstance(arg.func, ast.Attribute):
                                inner = arg.func.attr
                            if inner in ops:
                                connections.append(
                                    Connection(
                                        source_name=ops[inner],
                                        destination_name=ops[called],
                                    )
                                )

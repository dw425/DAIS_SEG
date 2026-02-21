"""Code validator — basic syntax checks on generated Python code."""

import ast


def validate_python_syntax(code: str) -> list[str]:
    """Check if generated Python code is syntactically valid.

    Returns list of error messages (empty if valid).
    """
    errors: list[str] = []
    try:
        ast.parse(code)
    except SyntaxError as exc:
        errors.append(f"Syntax error at line {exc.lineno}: {exc.msg}")
    return errors

"""Line-level validator — checks generated code for common issues.

Ported from validators/line-validator.js.
"""

import ast
import re

from app.models.pipeline import NotebookResult

_ANTIPATTERNS = [
    (re.compile(r"while\s+True.*sleep", re.DOTALL), "Polling loop detected -- use trigger() instead"),
    (re.compile(r"\.collect\(\)"), "collect() pulls all data to driver -- may cause OOM"),
    (re.compile(r"\.toPandas\(\)"), "toPandas() pulls all data to driver -- limit rows first"),
    (re.compile(r"import\s+flask", re.I), "Flask should not run in notebook -- use Model Serving"),
    (re.compile(r"password\s*=\s*['\"][^'\"]+['\"]"), "Hardcoded password detected -- use Secret Scopes"),
]


def validate_lines(notebook: NotebookResult) -> tuple[float, list[str]]:
    """Validate code cells for syntax and anti-patterns.

    Returns:
        (score, errors) where score is 0-1 quality ratio.
    """
    errors: list[str] = []
    total_cells = 0
    clean_cells = 0

    for cell in notebook.cells:
        if cell.type != "code":
            continue
        total_cells += 1
        cell_clean = True

        # Syntax check
        try:
            ast.parse(cell.source)
        except SyntaxError as exc:
            errors.append(f"[{cell.label}] Syntax error: {exc.msg} (line {exc.lineno})")
            cell_clean = False

        # Anti-pattern check
        for pattern, message in _ANTIPATTERNS:
            if pattern.search(cell.source):
                errors.append(f"[{cell.label}] {message}")
                cell_clean = False

        if cell_clean:
            clean_cells += 1

    score = clean_cells / max(total_cells, 1)
    return score, errors

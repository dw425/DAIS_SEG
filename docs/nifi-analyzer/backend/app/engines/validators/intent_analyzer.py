"""Intent analyzer — checks that generated notebook covers source flow intent.

Ported from validators/intent-analyzer.js.
"""

from app.models.pipeline import NotebookResult, ParseResult


def analyze_intent(parse_result: ParseResult, notebook: NotebookResult) -> tuple[float, list[dict]]:
    """Analyze whether the generated notebook covers all source processors.

    Returns:
        (score, gaps) where score is 0-1 coverage ratio and gaps is list of missing items.
    """
    source_names = {p.name for p in parse_result.processors}
    code_text = "\n".join(c.source for c in notebook.cells if c.type == "code")

    covered = set()
    gaps: list[dict] = []

    for name in source_names:
        # Check if processor is referenced in generated code
        if name in code_text or _safe_var(name) in code_text:
            covered.add(name)
        else:
            gaps.append(
                {
                    "type": "missing_processor",
                    "name": name,
                    "message": f"Processor '{name}' not found in generated notebook",
                }
            )

    # Check connections are represented
    source_connections = {(c.source_name, c.destination_name) for c in parse_result.connections}
    for src, dst in source_connections:
        if src not in covered and dst not in covered:
            gaps.append(
                {
                    "type": "missing_connection",
                    "source": src,
                    "destination": dst,
                    "message": f"Connection {src} -> {dst} not represented",
                }
            )

    score = len(covered) / max(len(source_names), 1)
    return score, gaps


def _safe_var(name: str) -> str:
    import re

    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()

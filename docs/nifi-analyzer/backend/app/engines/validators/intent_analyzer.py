"""Intent analyzer — checks that generated notebook covers source flow intent.

Ported from validators/intent-analyzer.js.
"""

import logging

from app.models.pipeline import NotebookResult, ParseResult

logger = logging.getLogger(__name__)


def analyze_intent(parse_result: ParseResult, notebook: NotebookResult) -> tuple[float, list[dict]]:
    """Analyze whether the generated notebook covers all source processors.

    Returns:
        (score, gaps) where score is 0-1 coverage ratio and gaps is list of missing items.
    """
    if not parse_result or not parse_result.processors:
        return 0.0, [{"type": "error", "name": "N/A", "message": "No parse result or processors"}]
    if not notebook or not notebook.cells:
        return 0.0, [{"type": "error", "name": "N/A", "message": "No notebook or cells"}]

    source_names = {p.name for p in parse_result.processors}
    code_text = "\n".join(c.source for c in notebook.cells if c.type == "code")
    code_text_lower = code_text.lower()

    covered = set()
    gaps: list[dict] = []

    for name in source_names:
        # Check if processor is referenced in generated code (case-insensitive
        # for raw name, exact for safe_var which is already lowercased)
        if name.lower() in code_text_lower or _safe_var(name) in code_text_lower:
            covered.add(name)
        else:
            gaps.append(
                {
                    "type": "missing_processor",
                    "name": name,
                    "message": f"Processor '{name}' not found in generated notebook",
                }
            )

    # Check connections are represented — flag when EITHER endpoint is uncovered
    source_connections = {(c.source_name, c.destination_name) for c in parse_result.connections}
    for src, dst in source_connections:
        if src not in covered or dst not in covered:
            missing_side = []
            if src not in covered:
                missing_side.append(f"source '{src}'")
            if dst not in covered:
                missing_side.append(f"destination '{dst}'")
            gaps.append(
                {
                    "type": "missing_connection",
                    "source": src,
                    "destination": dst,
                    "message": f"Connection {src} -> {dst} not fully represented (missing {', '.join(missing_side)})",
                }
            )

    score = len(covered) / max(len(source_names), 1)
    logger.info("Intent coverage: %d/%d processors covered (%.0f%%), %d gaps", len(covered), len(source_names), score * 100, len(gaps))
    return score, gaps


def _safe_var(name: str) -> str:
    import re

    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()

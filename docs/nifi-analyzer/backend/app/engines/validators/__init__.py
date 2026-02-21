"""Validation engine — validates generated notebook against source flow intent."""

from app.engines.validators.feedback import compute_feedback
from app.engines.validators.intent_analyzer import analyze_intent
from app.engines.validators.line_validator import validate_lines
from app.models.pipeline import NotebookResult, ParseResult, ValidationResult, ValidationScore


def validate_notebook(parse_result: ParseResult, notebook: NotebookResult) -> ValidationResult:
    """Run all validators and return combined ValidationResult."""
    scores: list[ValidationScore] = []
    gaps: list[dict] = []
    errors: list[str] = []

    # Intent coverage
    intent_score, intent_gaps = analyze_intent(parse_result, notebook)
    scores.append(
        ValidationScore(
            dimension="intent_coverage",
            score=intent_score,
            details=f"Source intent coverage: {intent_score:.0%}",
        )
    )
    gaps.extend(intent_gaps)

    # Line-level validation
    line_score, line_errors = validate_lines(notebook)
    scores.append(
        ValidationScore(
            dimension="code_quality",
            score=line_score,
            details=f"Code quality score: {line_score:.0%}",
        )
    )
    errors.extend(line_errors)

    # Feedback analysis
    fb_score, fb_details = compute_feedback(parse_result, notebook)
    scores.append(
        ValidationScore(
            dimension="completeness",
            score=fb_score,
            details=fb_details,
        )
    )

    overall = sum(s.score for s in scores) / max(len(scores), 1)

    return ValidationResult(
        overall_score=round(overall, 3),
        scores=scores,
        gaps=gaps,
        errors=errors,
    )


__all__ = ["validate_notebook"]

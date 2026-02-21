"""Validation engine — validates generated notebook against source flow intent.

Enhanced with advanced validation dimensions: Delta format enforcement, checkpoint
verification, credential security, error handling, and schema evolution checks.
"""

import logging
import re

from app.engines.validators.feedback import compute_feedback
from app.engines.validators.intent_analyzer import analyze_intent
from app.engines.validators.line_validator import validate_lines
from app.models.pipeline import NotebookResult, ParseResult, ValidationResult, ValidationScore

logger = logging.getLogger(__name__)


def validate_notebook(parse_result: ParseResult, notebook: NotebookResult) -> ValidationResult:
    """Run all validators and return combined ValidationResult."""
    if not parse_result or not parse_result.processors:
        return ValidationResult(overall_score=0, scores=[], gaps=["No parse result"], errors=["No processors found"])
    if not notebook or not notebook.cells:
        return ValidationResult(overall_score=0, scores=[], gaps=["No notebook"], errors=["No notebook cells"])

    logger.info("Validating notebook: %d cells against %d processors", len(notebook.cells), len(parse_result.processors))
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

    # Advanced validation dimensions
    advanced_scores, advanced_errors = _run_advanced_validations(parse_result, notebook)
    scores.extend(advanced_scores)
    errors.extend(advanced_errors)

    # Use weighted score engine instead of naive average
    from app.engines.validators.score_engine import compute_weighted_score

    overall = compute_weighted_score(scores)

    logger.info("Validation complete: overall_score=%.4f, %d errors, %d gaps", overall, len(errors), len(gaps))
    return ValidationResult(
        overall_score=round(overall, 4),
        scores=scores,
        gaps=gaps,
        errors=errors,
    )


def _run_advanced_validations(
    parse_result: ParseResult,
    notebook: NotebookResult,
) -> tuple[list[ValidationScore], list[str]]:
    """Run advanced validation dimensions added in Phase 3."""
    scores: list[ValidationScore] = []
    errors: list[str] = []

    code_text = "\n".join(c.source for c in notebook.cells if c.type == "code")

    # 1. Delta format enforcement
    delta_score, delta_errors = _validate_delta_format(code_text)
    scores.append(delta_score)
    errors.extend(delta_errors)

    # 2. Checkpoint verification for streaming
    ckpt_score, ckpt_errors = _validate_checkpoints(code_text, parse_result)
    scores.append(ckpt_score)
    errors.extend(ckpt_errors)

    # 3. Credential security
    cred_score, cred_errors = _validate_credentials(code_text)
    scores.append(cred_score)
    errors.extend(cred_errors)

    # 4. Error handling for JDBC
    eh_score, eh_errors = _validate_error_handling(code_text)
    scores.append(eh_score)
    errors.extend(eh_errors)

    # 5. Schema evolution options
    se_score, se_errors = _validate_schema_evolution(code_text, parse_result)
    scores.append(se_score)
    errors.extend(se_errors)

    return scores, errors


def _validate_delta_format(code_text: str) -> tuple[ValidationScore, list[str]]:
    """Validate that all intermediate writes use Delta format."""
    errors: list[str] = []

    # Find all .write or .writeStream calls
    write_calls = re.findall(
        r'\.(?:write|writeStream)\s*(?:\n\s*)?\.format\(\s*["\'](\w+)["\']\s*\)',
        code_text,
    )
    save_as_table = len(re.findall(r'\.saveAsTable\(', code_text))
    to_table = len(re.findall(r'\.toTable\(', code_text))

    total_writes = len(write_calls) + save_as_table + to_table
    non_delta = [fmt for fmt in write_calls if fmt.lower() not in ("delta", "deltasharing")]

    if non_delta:
        for fmt in non_delta:
            errors.append(f"Non-Delta write format detected: '{fmt}' -- use Delta for ACID guarantees")

    if total_writes == 0:
        score = 1.0
        details = "No write operations to validate"
    else:
        delta_writes = total_writes - len(non_delta)
        score = delta_writes / total_writes
        details = f"Delta format compliance: {delta_writes}/{total_writes} writes use Delta"

    return ValidationScore(dimension="delta_format", score=score, details=details), errors


def _validate_checkpoints(
    code_text: str,
    parse_result: ParseResult,
) -> tuple[ValidationScore, list[str]]:
    """Validate checkpoint locations are specified for streaming processors."""
    errors: list[str] = []

    # Count streaming writes
    streaming_writes = len(re.findall(r'\.writeStream', code_text))
    checkpoint_opts = len(re.findall(r'checkpointLocation', code_text))

    if streaming_writes == 0:
        return (
            ValidationScore(
                dimension="checkpoint_coverage",
                score=1.0,
                details="No streaming writes to validate",
            ),
            errors,
        )

    missing = streaming_writes - checkpoint_opts
    if missing > 0:
        errors.append(
            f"{missing} streaming write(s) missing checkpointLocation option"
        )

    score = checkpoint_opts / max(streaming_writes, 1)
    details = f"Checkpoint coverage: {checkpoint_opts}/{streaming_writes} streaming writes have checkpoints"

    return ValidationScore(dimension="checkpoint_coverage", score=min(score, 1.0), details=details), errors


def _validate_credentials(code_text: str) -> tuple[ValidationScore, list[str]]:
    """Validate no hardcoded credentials exist in generated code."""
    errors: list[str] = []

    # Patterns for hardcoded credentials
    hardcoded_patterns = [
        (re.compile(r'password\s*=\s*["\'][^"\']{3,}["\']', re.I), "hardcoded password"),
        (re.compile(r'secret\s*=\s*["\'][^"\']{3,}["\']', re.I), "hardcoded secret"),
        (re.compile(r'api[_\-]?key\s*=\s*["\'][^"\']{8,}["\']', re.I), "hardcoded API key"),
        (re.compile(r'token\s*=\s*["\'][^"\']{8,}["\']', re.I), "hardcoded token"),
        (re.compile(r'jdbc:.*password=[^&\s]{3,}', re.I), "password in JDBC URL"),
        (re.compile(r'mongodb(?:\+srv)?://[^:]+:[^@]{3,}@', re.I), "credentials in MongoDB URI"),
        (re.compile(r'(?:^|["\'])AKIA[0-9A-Z]{16}', re.M), "AWS access key ID"),
        (re.compile(r'(?:aws_secret_access_key|secret_key)\s*=\s*["\'][^"\']{20,}["\']', re.I), "AWS secret access key"),
    ]

    # Count secrets usage (good practice)
    secrets_usage = len(re.findall(r'dbutils\.secrets\.get', code_text))

    violations = 0
    for pattern, label in hardcoded_patterns:
        matches = pattern.findall(code_text)
        if matches:
            violations += len(matches)
            errors.append(f"Hardcoded {label} detected ({len(matches)} occurrence(s)) -- use dbutils.secrets.get()")

    if violations == 0:
        score = 1.0
        details = f"No hardcoded credentials detected. Secrets API used {secrets_usage} time(s)."
    else:
        score = max(0.0, 1.0 - (violations * 0.25))
        details = f"Found {violations} hardcoded credential(s). Migrate to Databricks Secret Scopes."

    return ValidationScore(dimension="credential_security", score=score, details=details), errors


def _validate_error_handling(code_text: str) -> tuple[ValidationScore, list[str]]:
    """Validate proper error handling around JDBC connections."""
    errors: list[str] = []

    # Find JDBC-related code blocks
    jdbc_refs = re.findall(r'format\(\s*["\']jdbc["\']\s*\)', code_text)

    if not jdbc_refs:
        return (
            ValidationScore(
                dimension="error_handling",
                score=1.0,
                details="No JDBC connections to validate",
            ),
            errors,
        )

    # Check for try/except around JDBC operations
    try_blocks = len(re.findall(r'\btry\s*:', code_text))

    if try_blocks == 0:
        errors.append("JDBC connections found without try/except error handling")
        score = 0.3
        details = f"{len(jdbc_refs)} JDBC connection(s) without error handling"
    elif try_blocks < len(jdbc_refs):
        score = try_blocks / len(jdbc_refs)
        details = f"Partial error handling: {try_blocks}/{len(jdbc_refs)} JDBC blocks wrapped"
    else:
        score = 1.0
        details = f"All {len(jdbc_refs)} JDBC connection(s) have error handling"

    return ValidationScore(dimension="error_handling", score=score, details=details), errors


def _validate_schema_evolution(
    code_text: str,
    parse_result: ParseResult,
) -> tuple[ValidationScore, list[str]]:
    """Validate schema evolution options for schema-less sources."""
    errors: list[str] = []

    # Check for Auto Loader usage
    autoloader_refs = len(re.findall(r'cloudFiles', code_text))

    if autoloader_refs == 0:
        return (
            ValidationScore(
                dimension="schema_evolution",
                score=1.0,
                details="No Auto Loader sources to validate",
            ),
            errors,
        )

    # Check for schema evolution configuration
    schema_location = len(re.findall(r'cloudFiles\.schemaLocation', code_text))
    infer_types = len(re.findall(r'cloudFiles\.inferColumnTypes', code_text))
    evolution_mode = len(re.findall(r'cloudFiles\.schemaEvolutionMode', code_text))

    configured = min(schema_location, autoloader_refs)
    if schema_location < autoloader_refs:
        errors.append(
            f"{autoloader_refs - schema_location} Auto Loader source(s) missing "
            f"cloudFiles.schemaLocation option"
        )

    score = configured / max(autoloader_refs, 1)
    details = (
        f"Schema evolution: {configured}/{autoloader_refs} Auto Loader sources configured "
        f"(schemaLocation: {schema_location}, inferTypes: {infer_types}, evolutionMode: {evolution_mode})"
    )

    return ValidationScore(dimension="schema_evolution", score=min(score, 1.0), details=details), errors


from app.engines.validators.report_generator import generate_validation_report  # noqa: E402
from app.engines.validators.score_engine import compute_weighted_score  # noqa: E402

__all__ = ["validate_notebook", "generate_validation_report", "compute_weighted_score"]

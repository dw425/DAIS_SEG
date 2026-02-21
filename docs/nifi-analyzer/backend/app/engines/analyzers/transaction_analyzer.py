"""Transaction boundary & Delta Lake enforcement analyzer.

Detects processors that require transactional boundaries (write/merge operations),
ensures all generated intermediate outputs use Delta format, and flags non-Delta writes.
"""

import re

from app.models.pipeline import ParseResult
from app.models.processor import Processor

# Processors that perform write operations requiring ACID guarantees
_WRITE_PROCESSORS = re.compile(
    r"^(Put|Publish|Send|Post|Insert|Write|Delete|Merge|Update)",
    re.I,
)

# Merge-specific patterns in processor types or properties
_MERGE_RE = re.compile(r"(Merge|Upsert|CDC|ChangeData)", re.I)

# Non-Delta output formats that should be flagged
_NON_DELTA_FORMATS = {"csv", "json", "parquet", "avro", "orc", "text", "xml"}

# Property keys that specify output format
_FORMAT_PROPS = re.compile(
    r"(output[\.\-_]?format|file[\.\-_]?format|write[\.\-_]?format|format|compression[\.\-_]?format)",
    re.I,
)

# Property keys that indicate a write path
_PATH_PROPS = re.compile(
    r"(directory|output[\.\-_]?directory|path|output[\.\-_]?path|location)",
    re.I,
)


def analyze_transactions(parse_result: ParseResult) -> dict:
    """Analyze transactional boundaries and Delta Lake enforcement.

    Returns:
        {
            "transaction_boundaries": [...],
            "delta_enforcement": {
                "compliant_writes": [...],
                "non_delta_warnings": [...],
            },
            "acid_annotations": [...],
            "summary": {...},
        }
    """
    processors = parse_result.processors
    connections = parse_result.connections

    boundaries: list[dict] = []
    compliant_writes: list[dict] = []
    non_delta_warnings: list[dict] = []
    acid_annotations: list[dict] = []

    # Build downstream map for boundary detection
    downstream: dict[str, list[str]] = {}
    for c in connections:
        downstream.setdefault(c.source_name, []).append(c.destination_name)

    for p in processors:
        is_write = bool(_WRITE_PROCESSORS.match(p.type))
        is_merge = bool(_MERGE_RE.search(p.type))

        if not is_write:
            continue

        # Detect output format from properties
        output_format = _detect_output_format(p)
        output_path = _detect_output_path(p)

        boundary = {
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "operation": "merge" if is_merge else "write",
            "requires_acid": True,
            "output_format": output_format,
            "output_path": output_path,
        }
        boundaries.append(boundary)

        # Check Delta compliance
        if output_format and output_format.lower() in _NON_DELTA_FORMATS:
            non_delta_warnings.append({
                "processor": p.name,
                "type": p.type,
                "current_format": output_format,
                "recommended_format": "delta",
                "severity": "warning",
                "message": (
                    f"Processor '{p.name}' writes in '{output_format}' format. "
                    f"Migrate to Delta format for ACID guarantees and time travel."
                ),
            })
        else:
            compliant_writes.append({
                "processor": p.name,
                "type": p.type,
                "format": output_format or "delta (default)",
            })

        # Generate ACID annotation
        annotation = _build_acid_annotation(p, is_merge, output_path)
        acid_annotations.append(annotation)

    # Check for intermediate writes that should use Delta
    _check_intermediate_writes(processors, connections, non_delta_warnings)

    return {
        "transaction_boundaries": boundaries,
        "delta_enforcement": {
            "compliant_writes": compliant_writes,
            "non_delta_warnings": non_delta_warnings,
        },
        "acid_annotations": acid_annotations,
        "summary": {
            "total_write_processors": len(boundaries),
            "merge_operations": sum(1 for b in boundaries if b["operation"] == "merge"),
            "delta_compliant": len(compliant_writes),
            "non_delta_count": len(non_delta_warnings),
        },
    }


def _detect_output_format(p: Processor) -> str:
    """Detect the output format from processor properties."""
    for key, val in p.properties.items():
        if _FORMAT_PROPS.search(key) and isinstance(val, str) and val.strip():
            return val.strip().lower()
    # Infer from processor type
    t = p.type.lower()
    if "parquet" in t:
        return "parquet"
    if "csv" in t:
        return "csv"
    if "json" in t:
        return "json"
    if "avro" in t:
        return "avro"
    return ""


def _detect_output_path(p: Processor) -> str:
    """Detect output path from processor properties."""
    for key, val in p.properties.items():
        if _PATH_PROPS.search(key) and isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _build_acid_annotation(p: Processor, is_merge: bool, output_path: str) -> dict:
    """Build ACID transaction annotation for notebook code generation."""
    if is_merge:
        code = (
            f"# ACID Transaction: MERGE operation from '{p.name}'\n"
            f"# Delta Lake provides automatic ACID guarantees for MERGE INTO\n"
            f"deltaTable = DeltaTable.forPath(spark, \"{output_path or f'/Volumes/{{{{catalog}}}}/{{{{schema}}}}/delta/{_safe_name(p.name)}'}\")\n"
            f"deltaTable.alias(\"target\").merge(\n"
            f"    source_df.alias(\"source\"),\n"
            f"    \"target.id = source.id\"\n"
            f").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()"
        )
    else:
        code = (
            f"# ACID Transaction: Write from '{p.name}'\n"
            f"# Delta Lake ensures atomic writes — partial writes are rolled back\n"
            f"(\n"
            f"    df.write\n"
            f"    .format(\"delta\")\n"
            f"    .mode(\"append\")\n"
            f"    .option(\"mergeSchema\", \"true\")\n"
            f"    .saveAsTable(\"{{catalog}}.{{schema}}.{_safe_name(p.name)}\")\n"
            f")"
        )

    return {
        "processor": p.name,
        "type": p.type,
        "operation": "merge" if is_merge else "write",
        "code_snippet": code,
    }


def _check_intermediate_writes(
    processors: list[Processor],
    connections: list,
    warnings: list[dict],
) -> None:
    """Flag intermediate processors that write non-Delta and feed downstream."""
    dest_names = {c.destination_name for c in connections}
    source_names = {c.source_name for c in connections}

    for p in processors:
        if not _WRITE_PROCESSORS.match(p.type):
            continue
        # If this write processor also feeds downstream, it is intermediate
        if p.name in source_names and p.name in dest_names:
            fmt = _detect_output_format(p)
            if fmt and fmt.lower() in _NON_DELTA_FORMATS:
                warnings.append({
                    "processor": p.name,
                    "type": p.type,
                    "current_format": fmt,
                    "recommended_format": "delta",
                    "severity": "critical",
                    "message": (
                        f"Intermediate processor '{p.name}' writes '{fmt}' format. "
                        f"Intermediate outputs MUST use Delta for pipeline reliability."
                    ),
                })


def _safe_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()

"""FlowFile attribute to DataFrame column translator.

Extracts FlowFile attributes from UpdateAttribute processors, generates
withColumn() calls to append attributes as physical DataFrame columns,
and handles NiFi Expression Language in attribute values via the NEL transpiler.
"""

import logging
import re

from app.engines.parsers.nel.parser import parse_nel_expression, translate_nel_string
from app.models.pipeline import ParseResult
from app.models.processor import Processor

logger = logging.getLogger(__name__)

# Processors that create/modify FlowFile attributes
_ATTRIBUTE_CREATORS = {
    "UpdateAttribute", "PutAttribute",
}

# Internal NiFi properties to skip
_INTERNAL_PROPS = {
    "Destination", "Return Type", "Path Not Found Behavior",
    "Null Value Representation", "Character Set", "Maximum Buffer Size",
    "Maximum Capture Group Length", "Enable Canonical Equivalence",
    "Enable Case-insensitive Matching", "Permit Whitespace and Comments",
    "Include Zero Capture Groups", "Delete Attributes Expression",
    "Store State", "Stateful Variables Initial Value",
    "Scheduling Strategy", "Scheduling Period", "Penalty Duration",
    "Yield Duration", "Bulletin Level", "Run Duration",
}

# NiFi Expression Language pattern
_NEL_RE = re.compile(r"\$\{[^}]+\}")


def translate_attributes(parse_result: ParseResult) -> dict:
    """Translate FlowFile attributes to DataFrame withColumn() calls.

    Returns:
        {
            "attribute_sources": [...],
            "column_additions": [...],
            "ingestion_metadata": {...},
            "summary": {...},
        }
    """
    processors = parse_result.processors

    attribute_sources: list[dict] = []
    column_additions: list[dict] = []

    for p in processors:
        if p.type not in _ATTRIBUTE_CREATORS:
            continue

        attrs = _extract_attributes(p)
        if not attrs:
            continue

        source = {
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "attributes": attrs,
        }
        attribute_sources.append(source)

        # Generate withColumn() calls
        columns = _generate_column_additions(p, attrs)
        column_additions.extend(columns)

    # Generate ingestion-phase metadata injection
    ingestion_meta = _generate_ingestion_metadata(attribute_sources, parse_result)

    logger.info("Attribute translation: %d source processors, %d columns generated", len(attribute_sources), len(column_additions))
    return {
        "attribute_sources": attribute_sources,
        "column_additions": column_additions,
        "ingestion_metadata": ingestion_meta,
        "summary": {
            "attribute_processors": len(attribute_sources),
            "total_attributes": sum(len(s["attributes"]) for s in attribute_sources),
            "columns_generated": len(column_additions),
        },
    }


def _extract_attributes(p: Processor) -> list[dict]:
    """Extract user-defined attributes from a processor's properties."""
    attrs = []
    for key, val in p.properties.items():
        if key in _INTERNAL_PROPS:
            continue
        if key.startswith("nifi-") or key.startswith("Record "):
            continue
        if not isinstance(val, str):
            continue

        has_nel = bool(_NEL_RE.search(val))
        attrs.append({
            "name": key,
            "value": val,
            "has_expression": has_nel,
        })

    return attrs


def _generate_column_additions(p: Processor, attrs: list[dict]) -> list[dict]:
    """Generate withColumn() calls for each attribute."""
    columns = []
    safe_proc = re.sub(r"[^a-zA-Z0-9_]", "_", p.name).strip("_").lower()

    for attr in attrs:
        col_name = _safe_column_name(attr["name"])

        if attr["has_expression"]:
            # Translate NEL expression to PySpark
            try:
                pyspark_expr = translate_nel_string(attr["value"], mode="col")
            except Exception:
                logger.warning("NEL translation failed for attribute %r on processor %s", attr["name"], p.name)
                pyspark_expr = f'lit("{_escape(attr["value"])}")  # NEL translation failed'

            code = f'df = df.withColumn("{col_name}", {pyspark_expr})'
        else:
            # Static value
            escaped = _escape(attr["value"])
            code = f'df = df.withColumn("{col_name}", lit("{escaped}"))'

        columns.append({
            "processor": p.name,
            "attribute_name": attr["name"],
            "column_name": col_name,
            "original_value": attr["value"],
            "has_expression": attr["has_expression"],
            "code": code,
        })

    return columns


def _generate_ingestion_metadata(
    attribute_sources: list[dict],
    parse_result: ParseResult,
) -> dict:
    """Generate an attribute map at ingestion phase so metadata flows through the pipeline."""
    if not attribute_sources:
        return {"code_snippet": "# No FlowFile attributes to inject at ingestion", "attributes": []}

    all_attrs: list[dict] = []
    lines = [
        "# FlowFile Attribute -> DataFrame Column injection",
        "# Apply at ingestion phase so metadata flows through the entire pipeline",
        "",
    ]

    for source in attribute_sources:
        lines.append(f"# Attributes from '{source['processor']}' ({source['type']})")
        for attr in source["attributes"]:
            col_name = _safe_column_name(attr["name"])
            all_attrs.append({"name": attr["name"], "column": col_name})

            if attr["has_expression"]:
                try:
                    pyspark_expr = translate_nel_string(attr["value"], mode="col")
                except Exception:
                    pyspark_expr = f'lit("{_escape(attr["value"])}")'
                lines.append(f'df = df.withColumn("{col_name}", {pyspark_expr})')
            else:
                escaped = _escape(attr["value"])
                lines.append(f'df = df.withColumn("{col_name}", lit("{escaped}"))')

        lines.append("")

    return {
        "code_snippet": "\n".join(lines),
        "attributes": all_attrs,
    }


def _safe_column_name(name: str) -> str:
    """Convert an attribute name to a safe DataFrame column name."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "attr_" + s
    return s.lower()


def _escape(s: str) -> str:
    """Escape a string for embedding in Python code."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

"""Recommender — suggests Databricks alternatives for unmapped processors.

For processors with no direct mapping (or low confidence), searches YAML
mapping tables for similar types and recommends the top-3 alternatives.
"""

import logging
import re
from pathlib import Path

import yaml

from app.engines.mappers.nifi_mapper import _load_mapping as _load_nifi_mapping

logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).parent.parent.parent / "constants" / "processor_maps"


def recommend_alternatives(
    proc_type: str,
    properties: dict | None = None,
    platform: str = "nifi",
) -> list[dict]:
    """Suggest alternative Databricks mappings for an unmapped processor.

    Returns list of up to 3 recommendations:
        {suggestedType, confidence, reason, template}
    """
    if properties is None:
        properties = {}

    candidates: list[dict] = []

    # 1. Search YAML mappings for similar processor types
    mapping_table = _load_platform_mappings(platform)
    type_tokens = _tokenize(proc_type)

    for mapped_type, entry in mapping_table.items():
        if not isinstance(entry, dict):
            continue

        mapped_tokens = _tokenize(mapped_type)
        similarity = _jaccard(type_tokens, mapped_tokens)

        if similarity > 0.1:
            candidates.append({
                "suggestedType": entry.get("databricks_type", mapped_type),
                "confidence": round(min(similarity * 1.5, 0.95), 2),
                "reason": f"Type similarity ({similarity:.0%}) with mapped {mapped_type}",
                "template": entry.get("template", ""),
                "_score": similarity,
            })

    # 2. Check if properties suggest a Databricks feature
    prop_suggestions = _suggest_from_properties(properties)
    candidates.extend(prop_suggestions)

    # 3. Prefix matching (e.g., "PutS3" matches "PutS3Object")
    prefix = proc_type[:4] if len(proc_type) >= 4 else proc_type
    for mapped_type, entry in mapping_table.items():
        if not isinstance(entry, dict):
            continue
        if mapped_type.startswith(prefix) and mapped_type != proc_type:
            existing = {c["suggestedType"] for c in candidates}
            stype = entry.get("databricks_type", mapped_type)
            if stype not in existing:
                candidates.append({
                    "suggestedType": stype,
                    "confidence": 0.6,
                    "reason": f"Prefix match: {mapped_type} shares prefix '{prefix}'",
                    "template": entry.get("template", ""),
                    "_score": 0.6,
                })

    # Deduplicate and sort by score
    seen: set[str] = set()
    unique: list[dict] = []
    for c in sorted(candidates, key=lambda x: x.get("_score", 0), reverse=True):
        key = c["suggestedType"]
        if key not in seen:
            seen.add(key)
            unique.append(c)

    # Return top 3, removing internal _score key
    results: list[dict] = []
    for c in unique[:3]:
        results.append({
            "suggestedType": c["suggestedType"],
            "confidence": c["confidence"],
            "reason": c["reason"],
            "template": c["template"],
        })

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_platform_mappings(platform: str) -> dict:
    """Load YAML mappings for the given platform."""
    yaml_file = _YAML_DIR / f"{platform}_databricks.yaml"
    if not yaml_file.exists():
        # Try nifi as fallback
        return _load_nifi_mapping()

    try:
        with open(yaml_file) as f:
            raw = yaml.safe_load(f) or {}
        if "mappings" in raw and isinstance(raw["mappings"], list):
            result = {}
            for entry in raw["mappings"]:
                if isinstance(entry, dict) and "type" in entry:
                    result[entry["type"]] = entry
            return result
        return raw if isinstance(raw, dict) else {}
    except Exception:
        logger.warning("Failed to load mappings for %s", platform)
        return {}


def _tokenize(name: str) -> set[str]:
    """Split a CamelCase or snake_case name into lowercase tokens."""
    # Split on camel case boundaries and underscores
    parts = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    return {t.lower() for t in re.split(r'[_\-\s]+', parts) if len(t) > 1}


def _jaccard(a: set[str], b: set[str]) -> float:
    """Compute Jaccard similarity between two token sets."""
    if not a or not b:
        return 0.0
    intersection = a & b
    union = a | b
    return len(intersection) / len(union)


def _suggest_from_properties(properties: dict) -> list[dict]:
    """Suggest Databricks features based on property values."""
    suggestions: list[dict] = []
    prop_text = " ".join(str(v) for v in properties.values()).lower()

    if "select " in prop_text or "insert " in prop_text or "sql" in prop_text:
        suggestions.append({
            "suggestedType": "Spark SQL",
            "confidence": 0.75,
            "reason": "SQL detected in properties — use spark.sql()",
            "template": 'df = spark.sql("{sql}")',
            "_score": 0.75,
        })

    if "kafka" in prop_text:
        suggestions.append({
            "suggestedType": "Structured Streaming (Kafka)",
            "confidence": 0.8,
            "reason": "Kafka reference found in properties",
            "template": 'df = spark.readStream.format("kafka").option("subscribe", "topic").load()',
            "_score": 0.8,
        })

    if "s3://" in prop_text or "s3a://" in prop_text:
        suggestions.append({
            "suggestedType": "Auto Loader (S3)",
            "confidence": 0.8,
            "reason": "S3 path found in properties — use Auto Loader",
            "template": 'df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://...")',
            "_score": 0.8,
        })

    if "abfss://" in prop_text or "wasbs://" in prop_text:
        suggestions.append({
            "suggestedType": "Auto Loader (ADLS)",
            "confidence": 0.8,
            "reason": "ADLS path found in properties — use Auto Loader",
            "template": 'df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("abfss://...")',
            "_score": 0.8,
        })

    if "http" in prop_text or "rest" in prop_text or "api" in prop_text:
        suggestions.append({
            "suggestedType": "Requests / REST API",
            "confidence": 0.6,
            "reason": "HTTP/REST reference — use requests library or spark UDF",
            "template": 'import requests\nresponse = requests.get(url, headers=headers)',
            "_score": 0.6,
        })

    return suggestions

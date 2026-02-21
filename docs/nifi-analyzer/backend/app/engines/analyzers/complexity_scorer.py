"""Complexity scorer — per-processor complexity scoring.

Computes a 0-100 complexity score for each processor based on connection
count, property count, NEL expression usage, external dependencies,
cycle membership, and custom code patterns.
"""

import logging
import re

from app.models.pipeline import AnalysisResult, ParseResult

logger = logging.getLogger(__name__)

# NEL expression pattern: ${...}
_NEL_RE = re.compile(r"\$\{[^}]+\}")

# External system indicators in processor types
_EXTERNAL_RE = re.compile(
    r"(Kafka|Oracle|MySQL|Postgres|Mongo|Elastic|Cassandra|HBase|"
    r"HDFS|S3|Azure|GCS|BigQuery|Snowflake|Redis|SFTP|FTP|HTTP|REST|"
    r"JMS|AMQP|MQTT|SMTP|Email|Syslog|Slack|Splunk|JDBC|Database)",
    re.I,
)

# Custom code processor types
_CUSTOM_CODE_TYPES = {
    "executescript", "executestreamcommand", "executeprocess",
    "executesql", "executesqlrecord", "invokehttp",
    "executegroovyscript", "executepython",
}

# Complexity thresholds
_THRESHOLD_LOW = 25
_THRESHOLD_MEDIUM = 50
_THRESHOLD_HIGH = 75


def score_complexity(parse_result: ParseResult, analysis: AnalysisResult) -> dict:
    """Score complexity for each processor in the flow.

    Returns dict with overall score, distribution, and per-processor details.
    """
    processors = parse_result.processors
    connections = parse_result.connections
    if not processors:
        return _empty_complexity()

    # Pre-compute connection counts per processor
    conn_in: dict[str, int] = {}
    conn_out: dict[str, int] = {}
    for c in connections:
        conn_out[c.source_name] = conn_out.get(c.source_name, 0) + 1
        conn_in[c.destination_name] = conn_in.get(c.destination_name, 0) + 1

    # Pre-compute cycle membership
    cycle_members: set[str] = set()
    for cycle in analysis.cycles:
        cycle_members.update(cycle)

    # Score each processor
    scored: list[dict] = []
    for p in processors:
        score = 0
        factors: list[str] = []

        # Connection count: +5 per connection (in + out)
        in_count = conn_in.get(p.name, 0)
        out_count = conn_out.get(p.name, 0)
        conn_score = (in_count + out_count) * 5
        if conn_score > 0:
            score += conn_score
            factors.append(f"connections: {in_count} in + {out_count} out (+{conn_score})")

        # Property count: +2 per property
        prop_count = len(p.properties)
        prop_score = prop_count * 2
        if prop_score > 0:
            score += prop_score
            factors.append(f"properties: {prop_count} (+{prop_score})")

        # NEL expression usage: +10 per expression
        nel_count = 0
        for _k, v in p.properties.items():
            if isinstance(v, str):
                nel_count += len(_NEL_RE.findall(v))
        nel_score = nel_count * 10
        if nel_score > 0:
            score += nel_score
            factors.append(f"NEL expressions: {nel_count} (+{nel_score})")

        # External system dependency: +15
        if _EXTERNAL_RE.search(p.type):
            score += 15
            factors.append("external system dependency (+15)")

        # Cycle membership: +20
        if p.name in cycle_members:
            score += 20
            factors.append("cycle membership (+20)")

        # Custom code: +25
        if p.type.lower() in _CUSTOM_CODE_TYPES:
            score += 25
            factors.append(f"custom code ({p.type}) (+25)")

        # Cap at 100
        score = min(score, 100)

        scored.append({
            "name": p.name,
            "type": p.type,
            "score": score,
            "factors": factors,
            "group": p.group,
        })

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Distribution
    low = sum(1 for s in scored if s["score"] < _THRESHOLD_LOW)
    medium = sum(1 for s in scored if _THRESHOLD_LOW <= s["score"] < _THRESHOLD_MEDIUM)
    high = sum(1 for s in scored if _THRESHOLD_MEDIUM <= s["score"] < _THRESHOLD_HIGH)
    critical = sum(1 for s in scored if s["score"] >= _THRESHOLD_HIGH)

    overall = sum(s["score"] for s in scored) / len(scored)

    logger.info("Complexity score: %.1f (low=%d, medium=%d, high=%d, critical=%d)", overall, low, medium, high, critical)
    return {
        "overall": round(overall, 1),
        "distribution": {
            "low": low,
            "medium": medium,
            "high": high,
            "critical": critical,
        },
        "thresholds": {
            "low": _THRESHOLD_LOW,
            "medium": _THRESHOLD_MEDIUM,
            "high": _THRESHOLD_HIGH,
        },
        "processors": scored[:10],  # Top 10 most complex
        "allProcessors": scored,
        "totalProcessors": len(scored),
    }


def _empty_complexity() -> dict:
    """Return empty complexity result when no processors are present."""
    return {
        "overall": 0,
        "distribution": {"low": 0, "medium": 0, "high": 0, "critical": 0},
        "thresholds": {"low": _THRESHOLD_LOW, "medium": _THRESHOLD_MEDIUM, "high": _THRESHOLD_HIGH},
        "processors": [],
        "allProcessors": [],
        "totalProcessors": 0,
    }

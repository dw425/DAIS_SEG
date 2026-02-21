"""Streaming vs batch execution mode analyzer.

Analyzes NiFi scheduling configurations to determine optimal Databricks execution
mode. Generates Trigger.AvailableNow() for batch or Trigger.ProcessingTime() for
streaming, with cost estimation.
"""

import re

from app.models.pipeline import ParseResult
from app.models.processor import Processor

# Cron-driven scheduling pattern
_CRON_RE = re.compile(r"^\d+\s+\d+\s+\d+\s+[\d\*]+\s+[\d\*]+\s*[\d\*\?]*$")

# Timer-driven patterns with duration parsing
_DURATION_RE = re.compile(r"(\d+)\s*(sec|second|min|minute|hour|hr|ms|millisecond)s?", re.I)

# Processors that inherently require streaming
_STREAMING_PROCESSORS = {
    "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
    "ListenHTTP", "ListenTCP", "ListenUDP", "ListenSyslog",
    "ListenRELP", "ConsumeMQTT", "ConsumeJMS", "ConsumeAMQP",
    "TailFile",
}

# Processors that are typically batch-oriented
_BATCH_PROCESSORS = {
    "ExecuteSQL", "ExecuteSQLRecord", "QueryDatabaseTable",
    "QueryDatabaseTableRecord", "GenerateFlowFile",
    "GetFile", "FetchFile", "ListFile",
}

# Cost estimation constants (relative DBU/hour)
_STREAMING_DBU_PER_HOUR = 4.0  # all-purpose compute, always-on
_BATCH_DBU_PER_TRIGGER = 0.5   # jobs compute, spin-up/process/terminate
_BATCH_TRIGGERS_PER_HOUR = 1   # default: once per hour


def analyze_execution_mode(parse_result: ParseResult) -> dict:
    """Analyze optimal execution mode (streaming vs batch) for each pipeline segment.

    Returns:
        {
            "processor_modes": [...],
            "pipeline_recommendation": str,
            "trigger_configs": [...],
            "cost_analysis": {...},
            "summary": {...},
        }
    """
    processors = parse_result.processors
    connections = parse_result.connections

    processor_modes: list[dict] = []
    trigger_configs: list[dict] = []

    streaming_count = 0
    batch_count = 0

    for p in processors:
        mode = _classify_mode(p)
        interval = _extract_interval(p)

        entry = {
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "recommended_mode": mode,
            "scheduling": p.scheduling or {},
            "interval": interval,
        }
        processor_modes.append(entry)

        if mode == "streaming":
            streaming_count += 1
        else:
            batch_count += 1

        # Generate trigger config
        trigger = _build_trigger_config(p, mode, interval)
        trigger_configs.append(trigger)

    # Overall pipeline recommendation
    recommendation = _determine_pipeline_recommendation(
        streaming_count, batch_count, processors
    )

    # Cost analysis
    cost = _estimate_costs(recommendation, streaming_count, batch_count)

    return {
        "processor_modes": processor_modes,
        "pipeline_recommendation": recommendation,
        "trigger_configs": trigger_configs,
        "cost_analysis": cost,
        "summary": {
            "streaming_processors": streaming_count,
            "batch_processors": batch_count,
            "recommendation": recommendation,
            "estimated_monthly_savings": cost.get("monthly_savings_pct", 0),
        },
    }


def _classify_mode(p: Processor) -> str:
    """Classify a processor as streaming or batch based on its type and scheduling."""
    # Check processor type first
    if p.type in _STREAMING_PROCESSORS:
        return "streaming"
    if p.type in _BATCH_PROCESSORS:
        return "batch"

    # Check scheduling configuration
    # Support both key conventions: JSON parser uses "strategy"/"period",
    # XML parser uses "schedulingStrategy"/"schedulingPeriod"
    sched = p.scheduling or {}
    strategy = (sched.get("strategy", "") or sched.get("schedulingStrategy", "")).lower()
    period = sched.get("period", "") or sched.get("schedulingPeriod", "")

    if strategy == "event_driven":
        return "streaming"

    if strategy == "cron_driven" or _CRON_RE.match(period):
        return "batch"

    # Timer-driven: check interval
    if period:
        seconds = _parse_duration_seconds(period)
        if seconds is not None:
            # Sub-30-second intervals suggest streaming
            if seconds < 30:
                return "streaming"
            return "batch"

    # Default to batch (cheaper, simpler)
    return "batch"


def _extract_interval(p: Processor) -> str:
    """Extract human-readable scheduling interval."""
    sched = p.scheduling or {}
    period = sched.get("period", "") or sched.get("schedulingPeriod", "")
    strategy = sched.get("strategy", "") or sched.get("schedulingStrategy", "")

    if strategy.lower() == "event_driven":
        return "event-driven (continuous)"
    if _CRON_RE.match(period):
        return f"cron: {period}"
    if period:
        return period
    return "not specified"


def _parse_duration_seconds(duration: str) -> float | None:
    """Parse a NiFi duration string into seconds."""
    m = _DURATION_RE.search(duration)
    if not m:
        return None

    value = float(m.group(1))
    unit = m.group(2).lower()

    if unit.startswith("ms") or unit.startswith("millisecond"):
        return value / 1000
    if unit.startswith("sec") or unit.startswith("second"):
        return value
    if unit.startswith("min") or unit.startswith("minute"):
        return value * 60
    if unit.startswith("hour") or unit.startswith("hr"):
        return value * 3600
    return value


def _build_trigger_config(p: Processor, mode: str, interval: str) -> dict:
    """Build Databricks trigger configuration for a processor."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name).strip("_").lower()

    if mode == "streaming":
        seconds = _parse_duration_seconds(interval) if interval != "event-driven (continuous)" else None
        processing_time = f"{int(seconds)} seconds" if seconds and seconds >= 1 else "10 seconds"

        code = (
            f"# Streaming trigger for '{p.name}'\n"
            f"(\n"
            f"    df.writeStream\n"
            f"    .trigger(processingTime=\"{processing_time}\")\n"
            f"    .option(\"checkpointLocation\", "
            f"\"/Volumes/{{catalog}}/{{schema}}/checkpoints/{safe_name}\")\n"
            f"    .toTable(f\"{{catalog}}.{{schema}}.{safe_name}\")\n"
            f")"
        )
    else:
        code = (
            f"# Batch trigger for '{p.name}' — cost-optimized\n"
            f"# Trigger.AvailableNow: spin up, process all queued data, terminate\n"
            f"(\n"
            f"    df.writeStream\n"
            f"    .trigger(availableNow=True)\n"
            f"    .option(\"checkpointLocation\", "
            f"\"/Volumes/{{catalog}}/{{schema}}/checkpoints/{safe_name}\")\n"
            f"    .toTable(f\"{{catalog}}.{{schema}}.{safe_name}\")\n"
            f")"
        )

    return {
        "processor": p.name,
        "type": p.type,
        "mode": mode,
        "trigger_code": code,
        "cost_annotation": (
            "Cost-optimized: Jobs compute with AvailableNow trigger"
            if mode == "batch"
            else "Always-on streaming: higher cost, lower latency"
        ),
    }


def _determine_pipeline_recommendation(
    streaming_count: int,
    batch_count: int,
    processors: list[Processor],
) -> str:
    """Determine overall pipeline execution recommendation."""
    total = streaming_count + batch_count
    if total == 0:
        return "batch"

    streaming_ratio = streaming_count / total

    # If any processors are inherently streaming, recommend streaming
    has_inherent_streaming = any(p.type in _STREAMING_PROCESSORS for p in processors)
    if has_inherent_streaming:
        return "streaming"

    if streaming_ratio > 0.5:
        return "streaming"
    if streaming_ratio > 0.2:
        return "mixed"  # hybrid approach
    return "batch"


def _estimate_costs(recommendation: str, streaming_count: int, batch_count: int) -> dict:
    """Estimate relative cost comparison between streaming and batch modes."""
    hours_per_month = 730  # average month

    # Streaming cost: always-on compute
    streaming_cost = _STREAMING_DBU_PER_HOUR * hours_per_month

    # Batch cost: triggered execution
    batch_cost = _BATCH_DBU_PER_TRIGGER * _BATCH_TRIGGERS_PER_HOUR * hours_per_month

    if recommendation == "streaming":
        actual_cost = streaming_cost
        alternative_cost = batch_cost
    elif recommendation == "batch":
        actual_cost = batch_cost
        alternative_cost = streaming_cost
    else:
        # Mixed: weighted average
        total = streaming_count + batch_count
        s_ratio = streaming_count / max(total, 1)
        actual_cost = (streaming_cost * s_ratio) + (batch_cost * (1 - s_ratio))
        alternative_cost = streaming_cost  # if they went all-streaming

    savings_pct = round(
        (1 - actual_cost / max(alternative_cost, 1)) * 100, 1
    ) if alternative_cost > actual_cost else 0

    return {
        "recommended_mode": recommendation,
        "estimated_streaming_dbu_month": round(streaming_cost, 1),
        "estimated_batch_dbu_month": round(batch_cost, 1),
        "estimated_actual_dbu_month": round(actual_cost, 1),
        "monthly_savings_pct": max(savings_pct, 0),
        "cost_note": (
            f"Batch mode with Trigger.AvailableNow() saves ~{max(savings_pct, 0):.0f}% "
            f"vs always-on streaming."
            if recommendation == "batch"
            else "Streaming mode required for real-time data. Consider mixed-mode for cost savings."
        ),
    }

"""State management & checkpointing analyzer.

Identifies stateful NiFi processors (ListHDFS, ListS3, DistributedMapCache users)
and injects Structured Streaming checkpoint configuration and transformWithState
boilerplate for advanced state tracking.
"""

import re

from app.models.pipeline import ParseResult
from app.models.processor import ControllerService, Processor

# Processors that maintain internal state
_STATEFUL_PROCESSORS = {
    "ListHDFS", "ListS3", "ListSFTP", "ListFTP", "ListFile",
    "ListGCSBucket", "ListAzureBlobStorage", "ListAzureDataLakeStorage",
    "ListDatabaseTables", "ListenHTTP", "ListenTCP", "ListenUDP",
    "ListenSyslog", "ListenRELP",
    "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
    "ConsumeJMS", "ConsumeAMQP", "ConsumeMQTT",
    "TailFile", "GetFile", "GetSFTP", "GetFTP", "GetHDFS",
    "GetS3Object", "GetAzureBlobContent",
    "DetectDuplicate", "DistributedMapCacheClient",
    "Wait", "Notify",
}

# Controller service types that indicate stateful operation
_STATEFUL_SERVICES = re.compile(
    r"(DistributedMapCache|DistributedSetCache|AtomicDistributedMapCache|"
    r"RedisDistributedMapCacheClient|HBase.*MapCache)",
    re.I,
)

# Processor types needing advanced state (windowing, dedup, session)
_ADVANCED_STATE_PROCESSORS = {
    "DetectDuplicate", "Wait", "Notify",
    "MergeContent", "MergeRecord",
}

# Scheduling types that imply streaming
_STREAMING_SCHEDULE_RE = re.compile(r"(0\s+sec|event[\-_]?driven|continuous)", re.I)


def analyze_state(parse_result: ParseResult) -> dict:
    """Analyze state management needs for NiFi-to-Databricks migration.

    Returns:
        {
            "stateful_processors": [...],
            "checkpoint_configs": [...],
            "advanced_state": [...],
            "cache_service_mappings": [...],
            "summary": {...},
        }
    """
    processors = parse_result.processors
    controller_services = parse_result.controller_services

    # Map controller service names to their types for lookup
    cs_type_map: dict[str, ControllerService] = {
        cs.name: cs for cs in controller_services
    }

    stateful: list[dict] = []
    checkpoint_configs: list[dict] = []
    advanced_state: list[dict] = []
    cache_mappings: list[dict] = []

    for p in processors:
        is_stateful = _is_stateful(p, cs_type_map)
        if not is_stateful:
            continue

        state_type = _classify_state_type(p, cs_type_map)
        is_streaming = _is_streaming_processor(p)

        entry = {
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "state_type": state_type,
            "is_streaming": is_streaming,
            "resolved_services": _get_cache_services(p, cs_type_map),
        }
        stateful.append(entry)

        # Generate checkpoint config
        checkpoint = _build_checkpoint_config(p, parse_result)
        checkpoint_configs.append(checkpoint)

        # Advanced state for specific processor patterns
        if p.type in _ADVANCED_STATE_PROCESSORS or state_type == "distributed_cache":
            advanced = _build_advanced_state(p, state_type)
            advanced_state.append(advanced)

    # Map cache controller services to Databricks equivalents
    for cs in controller_services:
        if _STATEFUL_SERVICES.search(cs.type):
            cache_mappings.append(_map_cache_service(cs))

    return {
        "stateful_processors": stateful,
        "checkpoint_configs": checkpoint_configs,
        "advanced_state": advanced_state,
        "cache_service_mappings": cache_mappings,
        "summary": {
            "total_stateful": len(stateful),
            "streaming_processors": sum(1 for s in stateful if s["is_streaming"]),
            "advanced_state_count": len(advanced_state),
            "cache_services": len(cache_mappings),
        },
    }


def _is_stateful(p: Processor, cs_map: dict[str, ControllerService]) -> bool:
    """Determine if a processor requires state management."""
    if p.type in _STATEFUL_PROCESSORS:
        return True
    # Check if any property references a stateful controller service
    for val in p.properties.values():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            if _STATEFUL_SERVICES.search(cs.type):
                return True
    return False


def _classify_state_type(p: Processor, cs_map: dict[str, ControllerService]) -> str:
    """Classify the type of state management needed."""
    if p.type.startswith(("List", "Tail")):
        return "file_listing"
    if p.type.startswith(("Consume", "Listen")):
        return "offset_tracking"
    if p.type in ("DetectDuplicate",):
        return "deduplication"
    if p.type in ("Wait", "Notify"):
        return "coordination"
    if p.type in ("MergeContent", "MergeRecord"):
        return "aggregation"
    # Check for distributed cache usage
    for val in p.properties.values():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            if _STATEFUL_SERVICES.search(cs.type):
                return "distributed_cache"
    return "general"


def _is_streaming_processor(p: Processor) -> bool:
    """Check if processor scheduling implies streaming."""
    sched = p.scheduling or {}
    # Support both key conventions: JSON parser uses "strategy"/"period",
    # XML parser uses "schedulingStrategy"/"schedulingPeriod"
    strategy = sched.get("strategy", "") or sched.get("schedulingStrategy", "")
    period = sched.get("period", "") or sched.get("schedulingPeriod", "")
    if strategy.lower() == "event_driven":
        return True
    if _STREAMING_SCHEDULE_RE.search(period):
        return True
    if p.type.startswith(("Consume", "Listen", "Tail")):
        return True
    return False


def _get_cache_services(p: Processor, cs_map: dict[str, ControllerService]) -> list[str]:
    """Get cache controller services referenced by this processor."""
    services = []
    for val in p.properties.values():
        if isinstance(val, str) and val in cs_map:
            cs = cs_map[val]
            if _STATEFUL_SERVICES.search(cs.type):
                services.append(cs.name)
    return services


def _build_checkpoint_config(p: Processor, parse_result: ParseResult) -> dict:
    """Build checkpoint configuration for a stateful processor."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name).strip("_").lower()

    code = (
        f"# Checkpoint configuration for '{p.name}' ({p.type})\n"
        f"checkpoint_path = f\"/Volumes/{{catalog}}/{{schema}}/checkpoints/{safe_name}\"\n"
        f"\n"
        f"(\n"
        f"    df.writeStream\n"
        f"    .option(\"checkpointLocation\", checkpoint_path)\n"
        f"    .trigger(availableNow=True)  # or processingTime='10 seconds' for streaming\n"
        f"    .toTable(f\"{{catalog}}.{{schema}}.{safe_name}\")\n"
        f")"
    )

    return {
        "processor": p.name,
        "type": p.type,
        "checkpoint_path": f"/Volumes/{{catalog}}/{{schema}}/checkpoints/{safe_name}",
        "code_snippet": code,
    }


def _build_advanced_state(p: Processor, state_type: str) -> dict:
    """Build transformWithState boilerplate for advanced state tracking."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name).strip("_").lower()

    if state_type == "deduplication":
        code = (
            f"# Advanced state: Deduplication for '{p.name}'\n"
            f"# Uses RocksDB state store for exactly-once semantics\n"
            f"from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle\n"
            f"\n"
            f"class {_to_class_name(p.name)}Dedup(StatefulProcessor):\n"
            f"    def init(self, handle: StatefulProcessorHandle):\n"
            f"        self.seen = handle.getValueState(\"seen_keys\", \"STRING\")\n"
            f"\n"
            f"    def handleInputRows(self, key, rows, timer_values):\n"
            f"        if not self.seen.exists():\n"
            f"            self.seen.update(\"seen\")\n"
            f"            yield from rows  # emit only first occurrence\n"
            f"\n"
            f"    def close(self):\n"
            f"        pass\n"
            f"\n"
            f"df = df.groupBy(\"key\").transformWithStateInPandas(\n"
            f"    statefulProcessor={_to_class_name(p.name)}Dedup(),\n"
            f"    outputStructType=df.schema,\n"
            f"    outputMode=\"append\",\n"
            f"    timeMode=\"none\",\n"
            f")"
        )
    elif state_type == "aggregation":
        code = (
            f"# Advanced state: Aggregation for '{p.name}'\n"
            f"# Windowed aggregation with watermark for late data\n"
            f"df = (\n"
            f"    df.withWatermark(\"event_time\", \"10 minutes\")\n"
            f"    .groupBy(window(col(\"event_time\"), \"5 minutes\"))\n"
            f"    .agg(count(\"*\").alias(\"record_count\"))\n"
            f")"
        )
    elif state_type == "coordination":
        if "Notify" in p.type:
            code = (
                f"# Advanced state: Coordination for '{p.name}' (Signal Producer)\n"
                f"# NiFi Notify maps to Databricks task value signaling\n"
                f"signal_key = \"{safe_name}_signal\"\n"
                f"signal_value = \"READY\"  # or derive from flow attributes\n"
                f"dbutils.jobs.taskValues.set(key=signal_key, value=signal_value)\n"
                f"print(f\"Signal sent: {{signal_key}} = {{signal_value}}\")"
            )
        else:
            code = (
                f"# Advanced state: Coordination for '{p.name}' (Waiter)\n"
                f"# NiFi Wait maps to Databricks task value polling\n"
                f"import time\n"
                f"\n"
                f"signal_key = \"{safe_name}_signal\"\n"
                f"timeout_seconds = 300\n"
                f"poll_interval = 10\n"
                f"elapsed = 0\n"
                f"\n"
                f"while elapsed < timeout_seconds:\n"
                f"    try:\n"
                f"        value = dbutils.jobs.taskValues.get(\n"
                f"            taskKey=\"<upstream_task>\", key=signal_key\n"
                f"        )\n"
                f"        if value:\n"
                f"            print(f\"Signal received: {{signal_key}} = {{value}}\")\n"
                f"            break\n"
                f"    except Exception as e:\n"
                f"        print(f\"Signal poll error: {{e}}\")\n"
                f"    time.sleep(poll_interval)\n"
                f"    elapsed += poll_interval\n"
                f"else:\n"
                f"    raise TimeoutError(f\"Timed out waiting for signal '{{signal_key}}'\")"
            )
    else:
        code = (
            f"# Advanced state: {state_type} for '{p.name}'\n"
            f"# Configure RocksDB state store for large state\n"
            f"spark.conf.set(\"spark.sql.streaming.stateStore.providerClass\",\n"
            f"    \"com.databricks.sql.streaming.state.RocksDBStateStoreProvider\")\n"
            f"\n"
            f"# State checkpoint: /Volumes/{{catalog}}/{{schema}}/checkpoints/{safe_name}\n"
        )

    return {
        "processor": p.name,
        "type": p.type,
        "state_type": state_type,
        "code_snippet": code,
    }


def _map_cache_service(cs: ControllerService) -> dict:
    """Map a NiFi cache controller service to Databricks equivalent."""
    return {
        "service_name": cs.name,
        "service_type": cs.type,
        "databricks_equivalent": "Delta Table (state store)",
        "migration_notes": (
            f"Replace NiFi '{cs.type}' with a Delta table for state persistence. "
            f"Use RocksDB state store for Structured Streaming state management."
        ),
        "properties": cs.properties,
    }


def _to_class_name(name: str) -> str:
    """Convert processor name to PascalCase class name."""
    parts = re.sub(r"[^a-zA-Z0-9]", " ", name).split()
    return "".join(w.capitalize() for w in parts) if parts else "Processor"

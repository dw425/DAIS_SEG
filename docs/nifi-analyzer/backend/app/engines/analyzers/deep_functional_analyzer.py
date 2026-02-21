"""Pass 1: Functional Analysis -- What does the flow DO?

Classifies each processor's role, identifies functional zones via
connected-component analysis, detects data domains, infers pipeline
patterns from DAG shape, and determines SLA profile from scheduling.
"""

from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import dataclass, field

from app.models.pipeline import AnalysisResult, ParseResult
from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FunctionalZone:
    zone: str
    processors: list[str]
    description: str


@dataclass
class FunctionalReport:
    flow_purpose: str
    functional_zones: list[FunctionalZone]
    data_domains: list[str]
    pipeline_pattern: str   # linear, fan_out, fan_in, diamond, complex_dag, streaming_fan_out
    estimated_complexity: str  # simple, medium, complex, enterprise
    sla_profile: str        # batch, near_real_time, real_time, event_driven


# ---------------------------------------------------------------------------
# Processor role classification
# ---------------------------------------------------------------------------

_SOURCE_RE = re.compile(
    r"^(Get|List|Consume|Listen|Fetch|Query|Scan|Select|Generate|Tail)", re.I
)
_SINK_RE = re.compile(
    r"^(Put|Publish|Send|Post|Insert|Write|Delete)", re.I
)
_ROUTE_RE = re.compile(r"^(Route|Distribute|Detect)", re.I)
_TRANSFORM_RE = re.compile(
    r"(Convert|Replace|Update|Jolt|Extract|Split|Merge|Compress|"
    r"Encrypt|Hash|Transform|Attribute|Validate|Fork|Partition|Lookup)",
    re.I,
)
_UTILITY_RE = re.compile(
    r"(Log|Debug|Count|Control|Wait|Notify|ControlRate|Monitor|Sample|Limit)",
    re.I,
)
_ENRICH_RE = re.compile(r"(InvokeHTTP|LookupRecord|HandleHttp|Enrich)", re.I)
_EXECUTE_RE = re.compile(r"(Execute|Invoke)", re.I)


def _classify_role(proc_type: str) -> str:
    """Classify a processor type into a functional role."""
    short = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type
    if _SOURCE_RE.match(short):
        return "source"
    if _SINK_RE.match(short):
        return "sink"
    if _ROUTE_RE.match(short):
        return "route"
    if _ENRICH_RE.search(short):
        return "enrich"
    if _UTILITY_RE.search(short):
        return "utility"
    if _TRANSFORM_RE.search(short):
        return "transform"
    if _EXECUTE_RE.search(short):
        return "process"
    return "transform"


# ---------------------------------------------------------------------------
# Data-domain keyword detection
# ---------------------------------------------------------------------------

_DOMAIN_KEYWORDS: dict[str, list[str]] = {
    "customer": ["customer", "client", "user", "account", "profile", "member"],
    "order": ["order", "purchase", "cart", "checkout", "invoice", "billing"],
    "transaction": ["transaction", "payment", "transfer", "txn", "ledger"],
    "sensor": ["sensor", "iot", "telemetry", "device", "metric", "reading"],
    "log": ["log", "audit", "event_log", "access_log", "syslog", "trace"],
    "event": ["event", "notification", "alert", "signal", "message"],
    "product": ["product", "catalog", "item", "sku", "inventory"],
    "employee": ["employee", "staff", "hr", "payroll", "personnel"],
    "healthcare": ["patient", "clinical", "hl7", "fhir", "diagnosis", "medical"],
    "financial": ["finance", "stock", "market", "trade", "portfolio", "risk"],
    "geospatial": ["geo", "location", "coordinate", "gps", "map", "address"],
    "media": ["image", "video", "audio", "media", "content", "document"],
}

_DOMAIN_RE: dict[str, re.Pattern] = {
    domain: re.compile("|".join(kws), re.I)
    for domain, kws in _DOMAIN_KEYWORDS.items()
}


def _detect_data_domains(processors: list[Processor]) -> list[str]:
    """Detect data domains from processor names and property values."""
    domain_hits: dict[str, int] = {}
    for p in processors:
        text_pool = p.name + " " + p.type
        # Sample a few key property values for domain clues
        for key, val in p.properties.items():
            if isinstance(val, str) and len(val) < 300:
                text_pool += " " + key + " " + val

        for domain, pattern in _DOMAIN_RE.items():
            if pattern.search(text_pool):
                domain_hits[domain] = domain_hits.get(domain, 0) + 1

    # Return domains sorted by hit count, threshold >= 1
    return [d for d, _ in sorted(domain_hits.items(), key=lambda x: -x[1]) if _ >= 1]


# ---------------------------------------------------------------------------
# Functional zone detection (connected components of same-role processors)
# ---------------------------------------------------------------------------

_ZONE_LABELS = {
    "source": "Data Ingestion",
    "sink": "Data Output",
    "transform": "Data Transformation",
    "route": "Data Routing",
    "enrich": "Data Enrichment",
    "utility": "Monitoring / Utility",
    "process": "Custom Processing",
}


def _detect_functional_zones(
    processors: list[Processor],
    connections: list[Connection],
) -> list[FunctionalZone]:
    """Identify functional zones using connected-component analysis on
    processors of the same role, connected via any path."""
    # Build adjacency (undirected) between processor names
    adj: dict[str, set[str]] = {p.name: set() for p in processors}
    for c in connections:
        if c.source_name in adj and c.destination_name in adj:
            adj[c.source_name].add(c.destination_name)
            adj[c.destination_name].add(c.source_name)

    role_map: dict[str, str] = {p.name: _classify_role(p.type) for p in processors}

    # BFS connected components restricted to same role
    visited: set[str] = set()
    zones: list[FunctionalZone] = []
    zone_counter: dict[str, int] = {}

    for p in processors:
        if p.name in visited:
            continue
        role = role_map[p.name]
        component: list[str] = []
        queue: deque[str] = deque([p.name])
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            if role_map.get(node) != role:
                continue
            visited.add(node)
            component.append(node)
            for neighbor in adj.get(node, set()):
                if neighbor not in visited and role_map.get(neighbor) == role:
                    queue.append(neighbor)

        if component:
            zone_counter[role] = zone_counter.get(role, 0) + 1
            idx = zone_counter[role]
            label = _ZONE_LABELS.get(role, role.title())
            zone_name = f"{label} #{idx}" if idx > 1 else label
            desc = (
                f"{len(component)} processor(s) performing "
                f"{label.lower()} operations"
            )
            zones.append(FunctionalZone(
                zone=zone_name,
                processors=component,
                description=desc,
            ))

    return zones


# ---------------------------------------------------------------------------
# Pipeline pattern detection from DAG shape
# ---------------------------------------------------------------------------

def _infer_pipeline_pattern(
    processors: list[Processor],
    connections: list[Connection],
) -> str:
    """Infer the pipeline pattern from the DAG structure."""
    if not processors or not connections:
        return "linear"

    downstream: dict[str, list[str]] = {p.name: [] for p in processors}
    upstream: dict[str, list[str]] = {p.name: [] for p in processors}
    proc_names = {p.name for p in processors}

    for c in connections:
        if c.source_name in proc_names and c.destination_name in proc_names:
            downstream[c.source_name].append(c.destination_name)
            upstream[c.destination_name].append(c.source_name)

    sources = [name for name, ups in upstream.items() if not ups]
    sinks = [name for name, dns in downstream.items() if not dns]

    # Detect streaming fan-out: streaming sources with fan-out > 1
    streaming_types = {
        "ConsumeKafka", "ConsumeKafka_2_6", "ConsumeKafkaRecord_2_6",
        "ListenHTTP", "ListenTCP", "ListenUDP", "ListenSyslog",
        "ConsumeMQTT", "ConsumeJMS", "ConsumeAMQP", "TailFile",
    }
    type_map = {p.name: p.type for p in processors}
    streaming_sources = [
        s for s in sources
        if type_map.get(s, "").rsplit(".", 1)[-1] in streaming_types
    ]
    if streaming_sources:
        max_fan = max(len(downstream.get(s, [])) for s in streaming_sources)
        if max_fan > 1:
            return "streaming_fan_out"

    # Count nodes with fan-out > 1 and fan-in > 1
    fan_out_nodes = [n for n, ds in downstream.items() if len(ds) > 1]
    fan_in_nodes = [n for n, us in upstream.items() if len(us) > 1]

    has_fan_out = len(fan_out_nodes) > 0
    has_fan_in = len(fan_in_nodes) > 0

    if has_fan_out and has_fan_in:
        # Check for diamond: a fan-out node whose branches reconverge at a fan-in node
        for fo_node in fan_out_nodes:
            branches = set(downstream[fo_node])
            for fi_node in fan_in_nodes:
                fi_parents = set(upstream[fi_node])
                # If at least 2 branches of the fan-out converge at this fan-in
                if len(branches & fi_parents) >= 2:
                    return "diamond"
        return "complex_dag"

    if has_fan_out and not has_fan_in:
        return "fan_out"

    if has_fan_in and not has_fan_out:
        return "fan_in"

    # Check if truly linear (single chain)
    if len(sources) <= 1 and len(sinks) <= 1:
        return "linear"

    # Multiple disconnected chains or other patterns
    if len(sources) > 1 or len(sinks) > 1:
        return "complex_dag"

    return "linear"


# ---------------------------------------------------------------------------
# Complexity estimation
# ---------------------------------------------------------------------------

def _estimate_complexity(
    processors: list[Processor],
    connections: list[Connection],
    zones: list[FunctionalZone],
) -> str:
    """Estimate overall flow complexity."""
    n_procs = len(processors)
    n_conns = len(connections)
    n_zones = len(zones)

    # Count NEL expressions
    nel_count = 0
    script_count = 0
    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if short.lower() in ("executescript", "executegroovyscript", "executepython"):
            script_count += 1
        for val in p.properties.values():
            if isinstance(val, str) and "${" in val:
                nel_count += 1

    score = 0
    score += min(n_procs, 100)          # up to 100 for processor count
    score += min(n_conns, 80)           # up to 80 for connections
    score += n_zones * 5                # 5 per zone
    score += nel_count * 2              # 2 per NEL expression
    score += script_count * 20          # 20 per script processor

    if score < 30:
        return "simple"
    if score < 80:
        return "medium"
    if score < 200:
        return "complex"
    return "enterprise"


# ---------------------------------------------------------------------------
# SLA profile from scheduling
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"(\d+)\s*(sec|second|min|minute|hour|hr|ms|millisecond)s?", re.I)


def _parse_seconds(duration: str) -> float | None:
    """Parse a NiFi duration string to seconds."""
    m = _DURATION_RE.search(duration)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("ms") or unit.startswith("millisecond"):
        return val / 1000
    if unit.startswith("sec") or unit.startswith("second"):
        return val
    if unit.startswith("min") or unit.startswith("minute"):
        return val * 60
    if unit.startswith("hour") or unit.startswith("hr"):
        return val * 3600
    return val


def _determine_sla_profile(processors: list[Processor]) -> str:
    """Determine SLA profile from scheduling periods across all processors."""
    min_seconds: float | None = None
    has_event_driven = False
    has_cron = False
    cron_re = re.compile(r"^\d+\s+\d+\s+\d+\s+[\d*]+\s+[\d*]+")

    for p in processors:
        sched = p.scheduling or {}
        strategy = (sched.get("strategy", "") or sched.get("schedulingStrategy", "")).lower()
        period = sched.get("period", "") or sched.get("schedulingPeriod", "")

        if strategy == "event_driven":
            has_event_driven = True
            continue

        if cron_re.match(period):
            has_cron = True
            continue

        if period:
            secs = _parse_seconds(period)
            if secs is not None:
                if min_seconds is None or secs < min_seconds:
                    min_seconds = secs

    if has_event_driven:
        return "event_driven"
    if min_seconds is not None:
        if min_seconds < 1:
            return "real_time"
        if min_seconds < 60:
            return "near_real_time"
    if has_cron:
        return "batch"
    if min_seconds is not None and min_seconds >= 3600:
        return "batch"
    if min_seconds is not None:
        return "near_real_time"
    return "batch"


# ---------------------------------------------------------------------------
# Flow purpose inference
# ---------------------------------------------------------------------------

def _infer_flow_purpose(
    processors: list[Processor],
    domains: list[str],
    pattern: str,
    sla: str,
) -> str:
    """Generate a human-readable flow purpose description."""
    role_counts: dict[str, int] = {}
    for p in processors:
        role = _classify_role(p.type)
        role_counts[role] = role_counts.get(role, 0) + 1

    source_count = role_counts.get("source", 0)
    sink_count = role_counts.get("sink", 0)
    transform_count = role_counts.get("transform", 0)

    # Determine primary action
    if transform_count > source_count + sink_count:
        action = "Data transformation pipeline"
    elif source_count > sink_count:
        action = "Data ingestion pipeline"
    elif sink_count > source_count:
        action = "Data distribution pipeline"
    else:
        action = "Data processing pipeline"

    # Determine source/target systems
    source_types: set[str] = set()
    sink_types: set[str] = set()
    for p in processors:
        short = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        if _SOURCE_RE.match(short):
            # Extract system hint
            for sys_name in ("Kafka", "S3", "SFTP", "HDFS", "File", "HTTP",
                             "Database", "SQL", "MongoDB", "Elasticsearch",
                             "Azure", "GCS", "JMS", "AMQP", "MQTT", "Syslog"):
                if sys_name.lower() in short.lower():
                    source_types.add(sys_name)
                    break
            else:
                source_types.add(short)
        elif _SINK_RE.match(short):
            for sys_name in ("Kafka", "S3", "SFTP", "HDFS", "File", "HTTP",
                             "Database", "SQL", "MongoDB", "Elasticsearch",
                             "Azure", "GCS", "JMS", "AMQP", "Email", "Slack"):
                if sys_name.lower() in short.lower():
                    sink_types.add(sys_name)
                    break
            else:
                sink_types.add(short)

    parts = [action]
    if source_types:
        parts.append(f"ingesting from {', '.join(sorted(source_types))}")
    if sink_types:
        parts.append(f"outputting to {', '.join(sorted(sink_types))}")
    if domains:
        parts.append(f"in the {', '.join(domains[:3])} domain(s)")

    sla_labels = {
        "batch": "on a batch schedule",
        "near_real_time": "in near-real-time",
        "real_time": "in real-time",
        "event_driven": "as event-driven processing",
    }
    parts.append(sla_labels.get(sla, ""))

    return " ".join(parts).rstrip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_functional(
    parsed: ParseResult,
    analysis_result: AnalysisResult | None = None,
) -> FunctionalReport:
    """Analyze what the flow does at a business level.

    Args:
        parsed: Normalized parse result from the parser engine.
        analysis_result: Optional existing analysis result for reuse.

    Returns:
        FunctionalReport with purpose, zones, domains, pattern, complexity, SLA.
    """
    processors = parsed.processors
    connections = parsed.connections

    logger.info(
        "Pass 1 (Functional): analyzing %d processors, %d connections",
        len(processors), len(connections),
    )

    # 1. Detect functional zones via connected-component analysis
    zones = _detect_functional_zones(processors, connections)

    # 2. Detect data domains from names and properties
    domains = _detect_data_domains(processors)

    # 3. Infer pipeline pattern from DAG shape
    pattern = _infer_pipeline_pattern(processors, connections)

    # 4. Estimate complexity
    complexity = _estimate_complexity(processors, connections, zones)

    # 5. Determine SLA profile from scheduling
    sla = _determine_sla_profile(processors)

    # 6. Synthesize flow purpose
    purpose = _infer_flow_purpose(processors, domains, pattern, sla)

    report = FunctionalReport(
        flow_purpose=purpose,
        functional_zones=zones,
        data_domains=domains,
        pipeline_pattern=pattern,
        estimated_complexity=complexity,
        sla_profile=sla,
    )

    logger.info(
        "Pass 1 complete: pattern=%s, complexity=%s, sla=%s, zones=%d, domains=%s",
        pattern, complexity, sla, len(zones), domains,
    )
    return report

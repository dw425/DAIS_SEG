"""Flow linter — configurable rule engine for NiFi flow checkstyle.

Loads lint rules from YAML, runs each check against the parsed flow,
and returns a structured LintReport with findings by severity.

Inspired by Snowflake-Labs/snowflake-flow-diff checkstyle approach.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)

# ── Constants ──

_DEPRECATED_PROCESSORS = {
    "GetHTTP": "InvokeHTTP",
    "PostHTTP": "InvokeHTTP",
    "GetFileTransfer": "GetSFTP/GetFTP",
    "PutFileTransfer": "PutSFTP/PutFTP",
    "Base64EncodeContent": "EncodeContent",
    "Base64DecodeContent": "EncodeContent",
    "CompressContent": "ModifyCompression",
    "DecompressContent": "ModifyCompression",
    "ConvertCharacterSet": "ConvertRecord",
    "HashAttribute": "CryptographicHashAttribute",
    "GetJMSTopic": "ConsumeJMS",
    "GetJMSQueue": "ConsumeJMS",
    "PutJMS": "PublishJMS",
}

_SENSITIVE_PROPERTY_KEYS = {
    "password", "secret", "token", "api_key", "apikey", "api-key",
    "credential", "private_key", "private-key", "passphrase",
    "auth_token", "access_key", "secret_key",
}

_NIFI_SPECIFIC_TYPES = {
    "HandleHttpRequest", "HandleHttpResponse", "ListenHTTP",
    "ListenSyslog", "ListenTCP", "ListenUDP", "ListenRELP",
    "GetHTTPClient", "InvokeHTTP",
    "DistributeLoad", "ControlRate", "MonitorActivity",
}

_SITE_TO_SITE_TYPES = {
    "RemoteProcessGroup", "Remote Input Port", "Remote Output Port",
}

_SCHEMA_VALIDATION_TYPES = {
    "ValidateRecord", "ValidateXml", "ValidateCsv", "SchemaValidation",
}

_RECORD_TYPES = {
    "ConvertRecord", "QueryRecord", "PublishKafkaRecord_2_6",
    "ConsumeKafkaRecord_2_6", "PutDatabaseRecord", "LookupRecord",
}


# ── Data classes ──

@dataclass
class LintFinding:
    """A single lint finding."""
    rule_id: str
    rule_name: str
    severity: str  # critical, high, medium, low, info
    category: str
    message: str
    processor: str = ""  # affected processor name (if applicable)
    suggestion: str = ""  # fix suggestion


@dataclass
class LintReport:
    """Complete lint report for a flow."""
    findings: list[LintFinding] = field(default_factory=list)
    rules_checked: int = 0
    rules_triggered: int = 0
    summary: str = ""

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "critical")

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "high")

    @property
    def medium_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "medium")

    @property
    def low_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "low")

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "info")

    def to_dict(self) -> dict:
        return {
            "findings": [
                {
                    "ruleId": f.rule_id,
                    "ruleName": f.rule_name,
                    "severity": f.severity,
                    "category": f.category,
                    "message": f.message,
                    "processor": f.processor,
                    "suggestion": f.suggestion,
                }
                for f in self.findings
            ],
            "rulesChecked": self.rules_checked,
            "rulesTriggered": self.rules_triggered,
            "summary": self.summary,
            "counts": {
                "critical": self.critical_count,
                "high": self.high_count,
                "medium": self.medium_count,
                "low": self.low_count,
                "info": self.info_count,
                "total": len(self.findings),
            },
        }


# ── Rule loading ──

def _load_rules() -> list[dict]:
    """Load lint rules from the YAML definition file."""
    rules_path = Path(__file__).resolve().parent.parent.parent / "constants" / "lint_rules.yaml"
    if not rules_path.exists():
        logger.warning("Lint rules file not found: %s", rules_path)
        return []
    with open(rules_path) as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])


# ── Check implementations ──

def _check_default_name(parsed: ParseResult) -> list[LintFinding]:
    """LINT-001: Processors with default names (name == type)."""
    findings = []
    for p in parsed.processors:
        if p.name == p.type or p.name == f"{p.type} 1" or p.name.startswith(f"{p.type} "):
            findings.append(LintFinding(
                rule_id="LINT-001", rule_name="processor_naming_convention",
                severity="low", category="naming",
                message=f"Processor '{p.name}' has a default name (same as type '{p.type}')",
                processor=p.name,
                suggestion=f"Rename to describe its purpose, e.g., 'Read Customer Orders' instead of '{p.type}'",
            ))
    return findings


def _check_generic_group_name(parsed: ParseResult) -> list[LintFinding]:
    """LINT-002: Process groups with generic names."""
    findings = []
    generic_names = {"root", "nifi flow", "process group", "pg", "main", "default"}
    for pg in parsed.process_groups:
        if pg.name.lower().strip() in generic_names:
            findings.append(LintFinding(
                rule_id="LINT-002", rule_name="process_group_naming",
                severity="low", category="naming",
                message=f"Process group '{pg.name}' has a generic name",
                suggestion="Use a descriptive name like 'Customer Data Ingestion' or 'Error Handling'",
            ))
    return findings


def _check_duplicate_names(parsed: ParseResult) -> list[LintFinding]:
    """LINT-003: Multiple processors with identical names."""
    findings = []
    name_counts: dict[str, int] = {}
    for p in parsed.processors:
        name_counts[p.name] = name_counts.get(p.name, 0) + 1
    for name, count in name_counts.items():
        if count > 1:
            findings.append(LintFinding(
                rule_id="LINT-003", rule_name="duplicate_processor_names",
                severity="medium", category="naming",
                message=f"'{name}' is used by {count} processors — causes confusion in lineage",
                processor=name,
                suggestion="Give each processor a unique, descriptive name",
            ))
    return findings


def _check_hardcoded_secrets(parsed: ParseResult) -> list[LintFinding]:
    """LINT-004: Hardcoded credentials in processor properties."""
    findings = []
    secret_re = re.compile(
        r"(password|secret|token|api[_-]?key|credential|private[_-]?key|passphrase)",
        re.I,
    )
    for p in parsed.processors:
        for key, val in p.properties.items():
            if secret_re.search(key) and val and not val.startswith("${") and not val.startswith("#{"):
                # Avoid false positives on empty or placeholder values
                if len(val) > 2 and val not in ("None", "null", ""):
                    findings.append(LintFinding(
                        rule_id="LINT-004", rule_name="hardcoded_credentials",
                        severity="critical", category="security",
                        message=f"Potential hardcoded credential in '{p.name}' property '{key}'",
                        processor=p.name,
                        suggestion="Use NiFi Parameter Contexts or Databricks Secret Scopes",
                    ))
    return findings


def _check_sensitive_not_parameterized(parsed: ParseResult) -> list[LintFinding]:
    """LINT-005: Sensitive properties not using parameter references."""
    findings = []
    for p in parsed.processors:
        for key, val in p.properties.items():
            if key.lower() in _SENSITIVE_PROPERTY_KEYS:
                if val and not val.startswith("${") and not val.startswith("#{"):
                    findings.append(LintFinding(
                        rule_id="LINT-005", rule_name="sensitive_property_exposure",
                        severity="high", category="security",
                        message=f"Sensitive property '{key}' in '{p.name}' not using parameter context",
                        processor=p.name,
                        suggestion="Reference via #{param_name} or ${variable_name}",
                    ))
    return findings


def _check_insecure_protocol(parsed: ParseResult) -> list[LintFinding]:
    """LINT-006: Using insecure protocols."""
    findings = []
    for p in parsed.processors:
        for key, val in p.properties.items():
            if isinstance(val, str) and val.startswith("http://"):
                findings.append(LintFinding(
                    rule_id="LINT-006", rule_name="insecure_protocol",
                    severity="high", category="security",
                    message=f"Insecure HTTP URL in '{p.name}' property '{key}'",
                    processor=p.name,
                    suggestion="Use HTTPS for secure communication",
                ))
    return findings


def _check_unbounded_backpressure(parsed: ParseResult) -> list[LintFinding]:
    """LINT-007: Connections with no backpressure limits."""
    findings = []
    for c in parsed.connections:
        if c.back_pressure_object_threshold == 0 and not c.back_pressure_data_size_threshold:
            findings.append(LintFinding(
                rule_id="LINT-007", rule_name="unbounded_backpressure",
                severity="medium", category="performance",
                message=f"Connection {c.source_name} -> {c.destination_name} has no backpressure limits",
                suggestion="Set backpressure object threshold (e.g., 10000) and data size threshold (e.g., 1 GB)",
            ))
    return findings


def _check_high_timer_frequency(parsed: ParseResult) -> list[LintFinding]:
    """LINT-008: Processors scheduled at very high frequency."""
    findings = []
    for p in parsed.processors:
        if p.scheduling:
            period = p.scheduling.get("period", "")
            if isinstance(period, str):
                # Check for sub-100ms scheduling
                ms_match = re.match(r"^(\d+)\s*ms$", period.strip(), re.I)
                if ms_match and int(ms_match.group(1)) < 100:
                    findings.append(LintFinding(
                        rule_id="LINT-008", rule_name="high_timer_frequency",
                        severity="low", category="performance",
                        message=f"'{p.name}' scheduled every {period} — very high frequency",
                        processor=p.name,
                        suggestion="Consider increasing scheduling period to reduce resource usage",
                    ))
    return findings


def _check_large_batch_size(parsed: ParseResult) -> list[LintFinding]:
    """LINT-009: Extremely large batch sizes."""
    findings = []
    batch_keys = {"batch size", "batch.size", "max batch size", "max-batch-size"}
    for p in parsed.processors:
        for key, val in p.properties.items():
            if key.lower().replace(" ", "").replace("-", "").replace("_", "") in {
                k.replace(" ", "").replace("-", "") for k in batch_keys
            }:
                try:
                    if int(val) > 100000:
                        findings.append(LintFinding(
                            rule_id="LINT-009", rule_name="large_batch_size",
                            severity="medium", category="performance",
                            message=f"'{p.name}' has batch size {val} — may cause memory issues",
                            processor=p.name,
                            suggestion="Consider reducing batch size to 10000 or less",
                        ))
                except (ValueError, TypeError):
                    pass
    return findings


def _check_missing_failure_connection(parsed: ParseResult) -> list[LintFinding]:
    """LINT-010: Processors with unconnected failure relationship."""
    findings = []
    # Build set of (source, relationship) pairs from connections
    connected_rels: set[tuple[str, str]] = set()
    for c in parsed.connections:
        for rel in c.relationship.split(","):
            connected_rels.add((c.source_name, rel.strip().lower()))

    # Check which processors have failure connections
    # Only check processors that typically have failure relationships
    failure_types = {
        "ExecuteSQL", "PutSQL", "PutDatabaseRecord", "QueryDatabaseTable",
        "InvokeHTTP", "PostHTTP", "GetHTTP", "PutFile", "GetFile",
        "PutS3Object", "FetchS3Object", "PutHDFS", "GetHDFS",
        "PublishKafka", "PublishKafka_2_6", "ConsumeKafka", "ConsumeKafka_2_6",
        "PutSFTP", "GetSFTP", "ConvertRecord", "ExecuteScript",
    }
    for p in parsed.processors:
        if p.type in failure_types:
            if (p.name, "failure") not in connected_rels:
                findings.append(LintFinding(
                    rule_id="LINT-010", rule_name="missing_failure_relationship",
                    severity="high", category="error-handling",
                    message=f"'{p.name}' ({p.type}) has no 'failure' relationship connected — errors will be lost",
                    processor=p.name,
                    suggestion="Connect the failure relationship to a LogMessage or error-handling processor",
                ))
    return findings


def _check_missing_description(parsed: ParseResult) -> list[LintFinding]:
    """LINT-011: Processors without comments/descriptions."""
    findings = []
    for p in parsed.processors:
        comments = p.properties.get("Comments", p.properties.get("comments", ""))
        if not comments:
            findings.append(LintFinding(
                rule_id="LINT-011", rule_name="missing_description",
                severity="info", category="best-practice",
                message=f"'{p.name}' has no description/comments",
                processor=p.name,
                suggestion="Add a brief description of what this processor does",
            ))
    return findings


def _check_disconnected_processor(parsed: ParseResult) -> list[LintFinding]:
    """LINT-012: Processor with no connections (dead code)."""
    findings = []
    connected_names: set[str] = set()
    for c in parsed.connections:
        connected_names.add(c.source_name)
        connected_names.add(c.destination_name)

    for p in parsed.processors:
        if p.name not in connected_names and len(parsed.processors) > 1:
            findings.append(LintFinding(
                rule_id="LINT-012", rule_name="disconnected_processor",
                severity="high", category="connectivity",
                message=f"'{p.name}' ({p.type}) has no connections — appears to be dead code",
                processor=p.name,
                suggestion="Connect to the flow or remove if unused",
            ))
    return findings


def _check_orphan_process_group(parsed: ParseResult) -> list[LintFinding]:
    """LINT-013: Process groups with no inter-group connections."""
    findings = []
    if len(parsed.process_groups) <= 1:
        return findings

    # Build map of which groups have cross-group connections
    proc_to_group: dict[str, str] = {}
    for p in parsed.processors:
        proc_to_group[p.name] = p.group

    groups_with_connections: set[str] = set()
    for c in parsed.connections:
        src_group = proc_to_group.get(c.source_name, "")
        dst_group = proc_to_group.get(c.destination_name, "")
        if src_group != dst_group:
            groups_with_connections.add(src_group)
            groups_with_connections.add(dst_group)

    for pg in parsed.process_groups:
        if pg.name not in groups_with_connections and pg.processors:
            findings.append(LintFinding(
                rule_id="LINT-013", rule_name="orphan_process_group",
                severity="medium", category="connectivity",
                message=f"Process group '{pg.name}' has no connections to other groups",
                suggestion="Connect to the main flow or consider if this group is needed",
            ))
    return findings


def _check_single_thread_bottleneck(parsed: ParseResult) -> list[LintFinding]:
    """LINT-014: Processors with max concurrent tasks = 1."""
    findings = []
    for p in parsed.processors:
        concurrent = p.properties.get("Max Concurrent Tasks",
                     p.properties.get("max-concurrent-tasks", ""))
        if concurrent == "1":
            # Only flag for processors that typically benefit from parallelism
            parallel_types = {"InvokeHTTP", "PutSQL", "ExecuteSQL", "PutS3Object",
                            "PublishKafka", "PublishKafka_2_6", "PutFile"}
            if p.type in parallel_types:
                findings.append(LintFinding(
                    rule_id="LINT-014", rule_name="single_thread_bottleneck",
                    severity="medium", category="performance",
                    message=f"'{p.name}' limited to 1 concurrent task — potential bottleneck",
                    processor=p.name,
                    suggestion="Increase Max Concurrent Tasks for I/O-bound processors",
                ))
    return findings


def _check_hardcoded_paths(parsed: ParseResult) -> list[LintFinding]:
    """LINT-015: Absolute file system paths that should be variables."""
    findings = []
    path_re = re.compile(r"^(/[a-zA-Z][a-zA-Z0-9_/.-]+|[A-Z]:\\[a-zA-Z0-9_\\.-]+)")
    for p in parsed.processors:
        for key, val in p.properties.items():
            if isinstance(val, str) and path_re.match(val) and not val.startswith("${"):
                findings.append(LintFinding(
                    rule_id="LINT-015", rule_name="hardcoded_paths",
                    severity="medium", category="best-practice",
                    message=f"Hardcoded path '{val[:60]}' in '{p.name}' property '{key}'",
                    processor=p.name,
                    suggestion="Use parameter contexts or variables for file paths",
                ))
    return findings


def _check_deprecated_processor(parsed: ParseResult) -> list[LintFinding]:
    """LINT-016: Using deprecated NiFi processors."""
    findings = []
    for p in parsed.processors:
        if p.type in _DEPRECATED_PROCESSORS:
            replacement = _DEPRECATED_PROCESSORS[p.type]
            findings.append(LintFinding(
                rule_id="LINT-016", rule_name="deprecated_processor",
                severity="high", category="deprecated",
                message=f"'{p.name}' uses deprecated processor '{p.type}'",
                processor=p.name,
                suggestion=f"Replace with '{replacement}'",
            ))
    return findings


def _check_legacy_nel(parsed: ParseResult) -> list[LintFinding]:
    """LINT-017: Legacy NiFi Expression Language syntax."""
    findings = []
    legacy_patterns = [
        (r"\$\{literal\(", "Use string literals directly"),
        (r"\$\{hostname\(\)\}", "Use #{hostname} in parameter contexts"),
    ]
    for p in parsed.processors:
        for key, val in p.properties.items():
            if not isinstance(val, str):
                continue
            for pattern, suggestion in legacy_patterns:
                if re.search(pattern, val):
                    findings.append(LintFinding(
                        rule_id="LINT-017", rule_name="legacy_expression_language",
                        severity="low", category="deprecated",
                        message=f"Legacy NEL syntax in '{p.name}' property '{key}'",
                        processor=p.name,
                        suggestion=suggestion,
                    ))
    return findings


def _check_no_error_handling(parsed: ParseResult) -> list[LintFinding]:
    """LINT-018: Flow has no global error handling."""
    findings = []
    has_failure = False
    for c in parsed.connections:
        if "failure" in c.relationship.lower():
            has_failure = True
            break

    if not has_failure and len(parsed.processors) > 3:
        findings.append(LintFinding(
            rule_id="LINT-018", rule_name="no_error_handling_path",
            severity="high", category="error-handling",
            message="Flow has no failure relationship connections — no error handling detected",
            suggestion="Add failure connections to LogMessage or PutFile for error capture",
        ))
    return findings


def _check_retry_without_limit(parsed: ParseResult) -> list[LintFinding]:
    """LINT-019: Retry loops without maximum count."""
    findings = []
    # Detect cycles where a failure goes back to the same processor
    for c in parsed.connections:
        if "retry" in c.relationship.lower() or (
            "failure" in c.relationship.lower() and c.source_name == c.destination_name
        ):
            findings.append(LintFinding(
                rule_id="LINT-019", rule_name="retry_without_limit",
                severity="medium", category="error-handling",
                message=f"Retry loop detected: {c.source_name} -> {c.destination_name}",
                suggestion="Add a retry count attribute and RouteOnAttribute to limit retries",
            ))
    return findings


def _check_fan_out_too_wide(parsed: ParseResult) -> list[LintFinding]:
    """LINT-020: Processor with >10 outgoing connections."""
    findings = []
    out_count: dict[str, int] = {}
    for c in parsed.connections:
        out_count[c.source_name] = out_count.get(c.source_name, 0) + 1
    for name, count in out_count.items():
        if count > 10:
            findings.append(LintFinding(
                rule_id="LINT-020", rule_name="fan_out_too_wide",
                severity="medium", category="performance",
                message=f"'{name}' has {count} outgoing connections",
                processor=name,
                suggestion="Consider grouping destinations or using a DistributeLoad processor",
            ))
    return findings


def _check_fan_in_too_deep(parsed: ParseResult) -> list[LintFinding]:
    """LINT-021: Processor with >10 incoming connections."""
    findings = []
    in_count: dict[str, int] = {}
    for c in parsed.connections:
        in_count[c.destination_name] = in_count.get(c.destination_name, 0) + 1
    for name, count in in_count.items():
        if count > 10:
            findings.append(LintFinding(
                rule_id="LINT-021", rule_name="fan_in_too_deep",
                severity="low", category="performance",
                message=f"'{name}' has {count} incoming connections — potential bottleneck",
                processor=name,
                suggestion="Consider adding a MergeContent or funnel before this processor",
            ))
    return findings


def _check_self_loop(parsed: ParseResult) -> list[LintFinding]:
    """LINT-022: Processor connected to itself."""
    findings = []
    for c in parsed.connections:
        if c.source_name == c.destination_name:
            findings.append(LintFinding(
                rule_id="LINT-022", rule_name="self_loop",
                severity="high", category="connectivity",
                message=f"'{c.source_name}' has a self-loop on relationship '{c.relationship}'",
                processor=c.source_name,
                suggestion="Remove the self-loop or use a retry mechanism with a counter",
            ))
    return findings


def _check_no_schema_validation(parsed: ParseResult) -> list[LintFinding]:
    """LINT-023: Flow processes structured data but has no schema validation."""
    findings = []
    has_structured = any(p.type in _RECORD_TYPES for p in parsed.processors)
    has_validation = any(p.type in _SCHEMA_VALIDATION_TYPES for p in parsed.processors)

    if has_structured and not has_validation and len(parsed.processors) > 5:
        findings.append(LintFinding(
            rule_id="LINT-023", rule_name="no_schema_validation",
            severity="medium", category="best-practice",
            message="Flow processes structured data but has no schema validation step",
            suggestion="Add a ValidateRecord processor to catch schema violations early",
        ))
    return findings


def _check_missing_content_type(parsed: ParseResult) -> list[LintFinding]:
    """LINT-024: ConvertRecord without explicit reader/writer."""
    findings = []
    for p in parsed.processors:
        if p.type in _RECORD_TYPES:
            reader = p.properties.get("Record Reader", p.properties.get("record-reader", ""))
            writer = p.properties.get("Record Writer", p.properties.get("record-writer", ""))
            if not reader or not writer:
                findings.append(LintFinding(
                    rule_id="LINT-024", rule_name="missing_content_type",
                    severity="low", category="best-practice",
                    message=f"'{p.name}' ({p.type}) missing explicit Record Reader/Writer",
                    processor=p.name,
                    suggestion="Set Record Reader and Record Writer explicitly for clarity",
                ))
    return findings


def _check_nifi_specific_dependency(parsed: ParseResult) -> list[LintFinding]:
    """LINT-025: Processor depends on NiFi-specific features."""
    findings = []
    for p in parsed.processors:
        if p.type in _NIFI_SPECIFIC_TYPES:
            findings.append(LintFinding(
                rule_id="LINT-025", rule_name="nifi_specific_dependency",
                severity="medium", category="best-practice",
                message=f"'{p.name}' ({p.type}) uses NiFi-specific features not available in Databricks",
                processor=p.name,
                suggestion="Plan manual migration or find a Databricks equivalent",
            ))
    return findings


def _check_custom_processor(parsed: ParseResult) -> list[LintFinding]:
    """LINT-026: Custom/third-party processors."""
    findings = []
    known_prefixes = {
        "org.apache.nifi", "GetFile", "PutFile", "ConsumeKafka", "PublishKafka",
        "ExecuteSQL", "PutSQL", "InvokeHTTP", "RouteOnAttribute", "UpdateAttribute",
    }
    for p in parsed.processors:
        props_str = str(p.properties)
        # If the full type contains a custom package (not org.apache.nifi)
        if "." in p.type and not p.type.startswith("org.apache.nifi"):
            findings.append(LintFinding(
                rule_id="LINT-026", rule_name="custom_processor",
                severity="high", category="best-practice",
                message=f"'{p.name}' uses custom processor '{p.type}' — requires manual migration",
                processor=p.name,
                suggestion="Review the custom processor's logic and implement equivalent PySpark code",
            ))
    return findings


def _check_site_to_site_usage(parsed: ParseResult) -> list[LintFinding]:
    """LINT-027: Site-to-Site transport usage."""
    findings = []
    for p in parsed.processors:
        if p.type in _SITE_TO_SITE_TYPES:
            findings.append(LintFinding(
                rule_id="LINT-027", rule_name="site_to_site_usage",
                severity="high", category="connectivity",
                message=f"'{p.name}' ({p.type}) uses NiFi Site-to-Site — no direct Databricks equivalent",
                processor=p.name,
                suggestion="Replace with Kafka, Delta Sharing, or REST API for inter-system data transfer",
            ))
    return findings


def _check_provenance_dependency(parsed: ParseResult) -> list[LintFinding]:
    """LINT-028: Flow relies on provenance events."""
    findings = []
    provenance_types = {"QueryNiFiReportingTask", "SiteToSiteProvenanceReportingTask"}
    for p in parsed.processors:
        if p.type in provenance_types:
            findings.append(LintFinding(
                rule_id="LINT-028", rule_name="provenance_dependency",
                severity="medium", category="best-practice",
                message=f"'{p.name}' depends on NiFi provenance events",
                processor=p.name,
                suggestion="Use Databricks audit logs or Delta Lake change data capture instead",
            ))
    return findings


def _check_attribute_overuse(parsed: ParseResult) -> list[LintFinding]:
    """LINT-029: Excessive FlowFile attribute usage."""
    findings = []
    for p in parsed.processors:
        if p.type == "UpdateAttribute":
            attr_count = len([k for k in p.properties.keys()
                            if k not in ("Delete Attributes Expression", "Store State")])
            if attr_count > 20:
                findings.append(LintFinding(
                    rule_id="LINT-029", rule_name="flowfile_attribute_overuse",
                    severity="low", category="performance",
                    message=f"'{p.name}' sets {attr_count} attributes — excessive attribute usage",
                    processor=p.name,
                    suggestion="Consider consolidating attributes or using FlowFile content instead",
                ))
    return findings


def _check_empty_properties(parsed: ParseResult) -> list[LintFinding]:
    """LINT-030: Processor with no configured properties."""
    findings = []
    for p in parsed.processors:
        # Skip processors that typically don't need properties
        no_prop_types = {"GenerateFlowFile", "LogMessage", "LogAttribute", "Funnel", "DebugFlow"}
        if not p.properties and p.type not in no_prop_types:
            findings.append(LintFinding(
                rule_id="LINT-030", rule_name="empty_processor_properties",
                severity="info", category="best-practice",
                message=f"'{p.name}' ({p.type}) has no configured properties (all defaults)",
                processor=p.name,
                suggestion="Verify that default settings are appropriate for your use case",
            ))
    return findings


# ── Check dispatcher ──

_CHECK_FUNCTIONS: dict[str, callable] = {
    "default_name": _check_default_name,
    "generic_group_name": _check_generic_group_name,
    "duplicate_names": _check_duplicate_names,
    "hardcoded_secrets": _check_hardcoded_secrets,
    "sensitive_not_parameterized": _check_sensitive_not_parameterized,
    "insecure_protocol": _check_insecure_protocol,
    "unbounded_backpressure": _check_unbounded_backpressure,
    "high_timer_frequency": _check_high_timer_frequency,
    "large_batch_size": _check_large_batch_size,
    "missing_failure_connection": _check_missing_failure_connection,
    "missing_description": _check_missing_description,
    "disconnected_processor": _check_disconnected_processor,
    "orphan_process_group": _check_orphan_process_group,
    "single_thread_bottleneck": _check_single_thread_bottleneck,
    "hardcoded_paths": _check_hardcoded_paths,
    "deprecated_processor": _check_deprecated_processor,
    "legacy_nel": _check_legacy_nel,
    "no_error_handling": _check_no_error_handling,
    "retry_without_limit": _check_retry_without_limit,
    "fan_out_too_wide": _check_fan_out_too_wide,
    "fan_in_too_deep": _check_fan_in_too_deep,
    "self_loop": _check_self_loop,
    "no_schema_validation": _check_no_schema_validation,
    "missing_content_type": _check_missing_content_type,
    "nifi_specific_dependency": _check_nifi_specific_dependency,
    "custom_processor": _check_custom_processor,
    "site_to_site_usage": _check_site_to_site_usage,
    "provenance_dependency": _check_provenance_dependency,
    "attribute_overuse": _check_attribute_overuse,
    "empty_properties": _check_empty_properties,
}


# ── Public API ──

def lint_flow(parsed: ParseResult) -> LintReport:
    """Run all lint rules against a parsed flow and return a LintReport."""
    rules = _load_rules()
    report = LintReport()
    report.rules_checked = len(rules)
    triggered_rules: set[str] = set()

    for rule in rules:
        check_key = rule.get("check", "")
        check_fn = _CHECK_FUNCTIONS.get(check_key)
        if check_fn is None:
            logger.debug("No check function for rule '%s' (check='%s')", rule.get("id"), check_key)
            continue

        try:
            findings = check_fn(parsed)
            if findings:
                triggered_rules.add(rule["id"])
                report.findings.extend(findings)
        except Exception as exc:
            logger.warning("Lint check '%s' failed: %s", rule.get("id"), exc)

    report.rules_triggered = len(triggered_rules)

    # Sort findings by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    report.findings.sort(key=lambda f: severity_order.get(f.severity, 5))

    # Generate summary
    report.summary = (
        f"Checked {report.rules_checked} rules, {report.rules_triggered} triggered. "
        f"Findings: {report.critical_count} critical, {report.high_count} high, "
        f"{report.medium_count} medium, {report.low_count} low, {report.info_count} info."
    )

    return report


__all__ = ["LintFinding", "LintReport", "lint_flow"]

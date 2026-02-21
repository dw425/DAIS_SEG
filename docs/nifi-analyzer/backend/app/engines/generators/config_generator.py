"""Config Generator -- Generates configuration, secrets, and imports cells.

Produces:
1. Imports cell (deduplicated, organized by category)
2. Config cell (widgets, secrets scope, catalog/schema)
3. Secrets documentation cell (markdown with CLI commands)
4. Required secrets inventory (from controller services + processor properties)
5. Required packages list (for cluster library installation)
"""

import logging
import re
from dataclasses import dataclass, field

from app.models.config import DatabricksConfig
from app.models.pipeline import (
    AnalysisResult,
    AssessmentResult,
    MappingEntry,
    ParseResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ConfigCells:
    """All configuration-related notebook cells."""

    imports_cell: str
    config_cell: str
    secrets_doc_cell: str  # markdown
    required_secrets: list[dict]  # [{scope, key, description, source_processor}]
    required_packages: list[str]


# ---------------------------------------------------------------------------
# Credential/secret detection patterns
# ---------------------------------------------------------------------------

_CREDENTIAL_KEYS = re.compile(
    r"(password|secret|api[._-]?key|token|private[._-]?key|passphrase"
    r"|credential|access[._-]?key|client[._-]?secret|auth)",
    re.IGNORECASE,
)

_URL_KEYS = re.compile(
    r"(jdbc[._-]?url|connection[._-]?string|connection[._-]?url"
    r"|broker|bootstrap[._-]?server|endpoint|host)",
    re.IGNORECASE,
)

_SENSITIVE_VALUE_PATTERNS = re.compile(
    r"(^[A-Za-z0-9+/=]{20,}$"  # base64-ish
    r"|^AKIA[A-Z0-9]{16}$"      # AWS access key
    r"|^sk-[A-Za-z0-9]{32,}$"   # OpenAI-style key
    r"|^xox[bpras]-[A-Za-z0-9-]+$"  # Slack token
    r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_config_cells(
    parsed: ParseResult,
    assessment: AssessmentResult,
    analysis_result: AnalysisResult,
    deep_analysis: dict | None = None,
) -> ConfigCells:
    """Generate all configuration-related notebook cells.

    Orchestrates secret detection, import scanning, widget generation,
    and secrets documentation.
    """
    # 1. Detect all required secrets
    required_secrets = _detect_secrets(parsed, assessment)

    # 2. Scan imports from all mapping entries
    required_packages = _scan_packages(assessment, analysis_result)
    imports_cell = _generate_imports_cell(assessment, analysis_result, required_packages)

    # 3. Build config cell (widgets, catalog/schema, secrets scope)
    config_cell = _generate_config_cell(parsed, assessment, required_secrets)

    # 4. Build secrets documentation markdown
    secrets_doc_cell = _generate_secrets_doc_cell(required_secrets)

    return ConfigCells(
        imports_cell=imports_cell,
        config_cell=config_cell,
        secrets_doc_cell=secrets_doc_cell,
        required_secrets=required_secrets,
        required_packages=required_packages,
    )


# ---------------------------------------------------------------------------
# 1. Imports cell generation
# ---------------------------------------------------------------------------

_STDLIB_MODULES = frozenset({
    "os", "sys", "json", "re", "time", "datetime", "hashlib", "base64",
    "csv", "io", "pathlib", "collections", "functools", "itertools",
    "logging", "typing", "dataclasses", "uuid", "smtplib", "email",
    "urllib", "xml", "html",
})

_PYSPARK_PREFIXES = ("pyspark", "delta", "databricks")

# Core imports always included in generated notebooks
_CORE_IMPORTS = [
    "from pyspark.sql import SparkSession",
    "from pyspark.sql import functions as F",
    "from pyspark.sql.functions import col, lit, when, coalesce, regexp_replace, from_json, sha2, current_timestamp",
    "from pyspark.sql.types import *",
]


def _generate_imports_cell(
    assessment: AssessmentResult,
    analysis_result: AnalysisResult,
    required_packages: list[str],
) -> str:
    """Build a deduplicated, organized imports cell.

    Sort order: core PySpark -> stdlib -> PySpark extended -> third-party -> conditional
    """
    # Collect all imports from mapping entries
    raw_imports: set[str] = set()
    for mapping in assessment.mappings:
        if mapping.code:
            raw_imports.update(_extract_imports_from_code(mapping.code))

    # Add package imports from assessment
    for pkg in assessment.packages:
        if pkg.startswith("from ") or pkg.startswith("import "):
            raw_imports.add(pkg)
        else:
            raw_imports.add(f"import {pkg}")

    # Categorize
    stdlib_imports: list[str] = []
    pyspark_imports: list[str] = []
    third_party_imports: list[str] = []
    conditional_imports: list[str] = []

    for imp in sorted(raw_imports):
        module_name = _extract_module_name(imp)
        if module_name in _STDLIB_MODULES:
            stdlib_imports.append(imp)
        elif any(module_name.startswith(p) for p in _PYSPARK_PREFIXES):
            # Skip if already covered by core imports
            if not any(imp.strip() == core.strip() for core in _CORE_IMPORTS):
                pyspark_imports.append(imp)
        elif module_name in ("boto3", "requests", "google", "azure"):
            # Optional third-party -- wrap in try/except
            conditional_imports.append(imp)
        else:
            third_party_imports.append(imp)

    # Assemble
    lines: list[str] = []
    lines.append("# -- Imports --")

    # Core PySpark (always present)
    lines.append("# Core PySpark")
    for core in _CORE_IMPORTS:
        lines.append(core)

    # Extended PySpark
    if pyspark_imports:
        lines.append("")
        lines.append("# Extended PySpark / Delta")
        for imp in sorted(set(pyspark_imports)):
            lines.append(imp)

    # Stdlib
    if stdlib_imports:
        lines.append("")
        lines.append("# Standard library")
        for imp in sorted(set(stdlib_imports)):
            lines.append(imp)

    # Third-party (unconditional)
    if third_party_imports:
        lines.append("")
        lines.append("# Third-party")
        for imp in sorted(set(third_party_imports)):
            lines.append(imp)

    # Conditional / optional
    if conditional_imports:
        lines.append("")
        lines.append("# Optional packages (install via cluster library if needed)")
        for imp in sorted(set(conditional_imports)):
            module_name = _extract_module_name(imp)
            lines.append(f"try:")
            lines.append(f"    {imp}")
            lines.append(f"except ImportError:")
            lines.append(f'    print("[WARN] Optional package not available: {module_name}")')

    return "\n".join(lines)


def _extract_imports_from_code(code: str) -> list[str]:
    """Extract import statements from generated code snippets."""
    imports: list[str] = []
    for line in code.split("\n"):
        stripped = line.strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            # Skip inline comments
            clean = stripped.split("#")[0].strip()
            if clean:
                imports.append(clean)
    return imports


def _extract_module_name(import_line: str) -> str:
    """Extract the top-level module name from an import statement."""
    line = import_line.strip()
    if line.startswith("from "):
        # ``from foo.bar import baz`` -> ``foo``
        parts = line.split()
        if len(parts) >= 2:
            return parts[1].split(".")[0]
    elif line.startswith("import "):
        parts = line.split()
        if len(parts) >= 2:
            return parts[1].split(".")[0].rstrip(",")
    return ""


# ---------------------------------------------------------------------------
# 2. Config cell generation
# ---------------------------------------------------------------------------

def _generate_config_cell(
    parsed: ParseResult,
    assessment: AssessmentResult,
    required_secrets: list[dict],
) -> str:
    """Generate the notebook configuration cell.

    Includes:
    - Widget declarations with fallbacks
    - Catalog/schema context
    - Secrets scope constant
    - Checkpoint base path
    - Parameter Context entries as widgets
    """
    lines: list[str] = [
        "# -- Configuration --",
        "",
        "# Catalog and schema (set via job parameters or widgets)",
        "try:",
        '    CATALOG = dbutils.widgets.get("catalog")',
        "except Exception:",
        '    CATALOG = "main"',
        "try:",
        '    SCHEMA = dbutils.widgets.get("schema")',
        "except Exception:",
        '    SCHEMA = "default"',
        "",
        '# Secrets scope for credentials',
        'SECRETS_SCOPE = "nifi_migration"',
        "",
        "# Checkpoint and volume paths",
        'CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"',
        'VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"',
        "",
        "# Set catalog context",
        'spark.sql(f"USE CATALOG {CATALOG}")',
        'spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")',
        'spark.sql(f"USE SCHEMA {SCHEMA}")',
    ]

    # Add widget declarations from Parameter Contexts
    param_widgets = _generate_parameter_widgets(parsed)
    if param_widgets:
        lines.append("")
        lines.append("# -- Parameter Context Widgets --")
        lines.append("# Migrated from NiFi Parameter Contexts")
        lines.extend(param_widgets)

    # Add secret lookups for detected credentials
    secret_lookups = _generate_secret_lookups(required_secrets)
    if secret_lookups:
        lines.append("")
        lines.append("# -- Secrets --")
        lines.append("# Credentials loaded from Databricks Secrets")
        lines.extend(secret_lookups)

    lines.append("")
    lines.append('print(f"[CONFIG] catalog={CATALOG}, schema={SCHEMA}, scope={SECRETS_SCOPE}")')

    return "\n".join(lines)


def _generate_parameter_widgets(parsed: ParseResult) -> list[str]:
    """Generate dbutils.widgets declarations from NiFi Parameter Contexts."""
    lines: list[str] = []
    seen_keys: set[str] = set()

    for ctx in parsed.parameter_contexts:
        lines.append(f"# Parameter Context: {ctx.name}")
        for param in ctx.parameters:
            if param.key in seen_keys:
                continue
            seen_keys.add(param.key)

            safe_key = re.sub(r"[^a-zA-Z0-9_.]", "_", param.key).strip("_")
            default_value = param.value if not param.sensitive else ""

            if param.sensitive:
                # Sensitive params use secrets, not widgets
                lines.append(
                    f'# {param.key} (sensitive) -> use dbutils.secrets.get(scope=SECRETS_SCOPE, key="{safe_key}")'
                )
            elif param.inferred_type == "numeric":
                lines.append(f'dbutils.widgets.text("{safe_key}", "{default_value}", "{param.key}")')
                lines.append(f'{safe_key} = int(dbutils.widgets.get("{safe_key}"))')
            else:
                lines.append(f'dbutils.widgets.text("{safe_key}", "{default_value}", "{param.key}")')
                lines.append(f'{safe_key} = dbutils.widgets.get("{safe_key}")')

    return lines


def _generate_secret_lookups(required_secrets: list[dict]) -> list[str]:
    """Generate dbutils.secrets.get() calls for each required secret."""
    lines: list[str] = []
    seen: set[tuple[str, str]] = set()

    for secret in required_secrets:
        scope = secret["scope"]
        key = secret["key"]
        if (scope, key) in seen:
            continue
        seen.add((scope, key))

        var_name = re.sub(r"[^a-zA-Z0-9_]", "_", key).strip("_").upper()
        lines.append(
            f'{var_name} = dbutils.secrets.get(scope="{scope}", key="{key}")  # {secret.get("description", "")}'
        )

    return lines


# ---------------------------------------------------------------------------
# 3. Secrets documentation cell (markdown)
# ---------------------------------------------------------------------------

def _generate_secrets_doc_cell(required_secrets: list[dict]) -> str:
    """Generate markdown documentation for all required Databricks Secrets."""
    lines: list[str] = [
        "## Required Databricks Secrets",
        "",
        "Before running this notebook, configure the following secrets using the Databricks CLI.",
        "",
    ]

    if not required_secrets:
        lines.append("_No secrets detected. This pipeline does not require external credentials._")
        return "\n".join(lines)

    # Group by scope
    scopes: dict[str, list[dict]] = {}
    for secret in required_secrets:
        scope = secret["scope"]
        scopes.setdefault(scope, []).append(secret)

    lines.append("```bash")
    lines.append("# -- Setup Databricks Secrets --")
    lines.append("")

    for scope, secrets in sorted(scopes.items()):
        lines.append(f"# Create scope: {scope}")
        lines.append(f"databricks secrets create-scope {scope}")
        lines.append("")

        for secret in secrets:
            desc = secret.get("description", "")
            source = secret.get("source_processor", "")
            comment_parts = [desc, f"(from: {source})" if source else ""]
            comment = " ".join(p for p in comment_parts if p)
            lines.append(f"# {comment}")
            lines.append(
                f"databricks secrets put-secret --scope {scope} --key {secret['key']}"
            )

        lines.append("")

    lines.append("```")

    # Add a table summary
    lines.append("")
    lines.append("| Scope | Key | Description | Source Processor |")
    lines.append("|-------|-----|-------------|------------------|")
    for secret in required_secrets:
        lines.append(
            f"| `{secret['scope']}` | `{secret['key']}` "
            f"| {secret.get('description', '')} "
            f"| {secret.get('source_processor', '')} |"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 4. Secret detection
# ---------------------------------------------------------------------------

def _detect_secrets(
    parsed: ParseResult,
    assessment: AssessmentResult,
) -> list[dict]:
    """Scan controller services and processor properties for credentials.

    Returns a deduplicated list of required secrets.
    """
    secrets: list[dict] = []
    seen: set[tuple[str, str]] = set()

    # Scan controller services (highest priority -- these hold connection details)
    for cs in parsed.controller_services or []:
        scope = "nifi_migration"
        safe_svc = re.sub(r"[^a-zA-Z0-9_]", "_", cs.name).strip("_").lower()

        for prop_key, prop_val in cs.properties.items():
            val_str = str(prop_val) if prop_val else ""

            # JDBC URLs
            if _URL_KEYS.search(prop_key):
                key = f"{safe_svc}_url"
                if (scope, key) not in seen:
                    seen.add((scope, key))
                    secrets.append({
                        "scope": scope,
                        "key": key,
                        "description": f"Connection URL for {cs.name}",
                        "source_processor": cs.name,
                        "original_value": val_str[:50] if not _CREDENTIAL_KEYS.search(prop_key) else "[REDACTED]",
                    })

            # Passwords, API keys, tokens
            if _CREDENTIAL_KEYS.search(prop_key):
                # Derive a meaningful key name
                safe_prop = re.sub(r"[^a-zA-Z0-9_]", "_", prop_key).strip("_").lower()
                key = f"{safe_svc}_{safe_prop}"
                if (scope, key) not in seen:
                    seen.add((scope, key))
                    secrets.append({
                        "scope": scope,
                        "key": key,
                        "description": f"{prop_key} for {cs.name}",
                        "source_processor": cs.name,
                        "original_value": "[REDACTED]",
                    })

            # Detect hardcoded sensitive values by pattern
            if val_str and not val_str.startswith("${") and _SENSITIVE_VALUE_PATTERNS.search(val_str):
                safe_prop = re.sub(r"[^a-zA-Z0-9_]", "_", prop_key).strip("_").lower()
                key = f"{safe_svc}_{safe_prop}"
                if (scope, key) not in seen:
                    seen.add((scope, key))
                    secrets.append({
                        "scope": scope,
                        "key": key,
                        "description": f"Detected credential: {prop_key} in {cs.name}",
                        "source_processor": cs.name,
                        "original_value": "[REDACTED]",
                    })

    # Scan processor properties
    for proc in parsed.processors:
        scope = "nifi_migration"
        safe_proc = re.sub(r"[^a-zA-Z0-9_]", "_", proc.name).strip("_").lower()

        for prop_key, prop_val in proc.properties.items():
            val_str = str(prop_val) if prop_val else ""

            if _CREDENTIAL_KEYS.search(prop_key):
                safe_prop = re.sub(r"[^a-zA-Z0-9_]", "_", prop_key).strip("_").lower()
                key = f"{safe_proc}_{safe_prop}"
                if (scope, key) not in seen:
                    seen.add((scope, key))
                    secrets.append({
                        "scope": scope,
                        "key": key,
                        "description": f"{prop_key} for processor {proc.name}",
                        "source_processor": proc.name,
                        "original_value": "[REDACTED]",
                    })

            # Check for NiFi Expression Language references to parameter contexts
            # These may contain sensitive values: #{secretParam}
            if "#{" in val_str:
                param_name = re.findall(r"#\{([^}]+)\}", val_str)
                for pname in param_name:
                    key = re.sub(r"[^a-zA-Z0-9_]", "_", pname).strip("_").lower()
                    if (scope, key) not in seen:
                        seen.add((scope, key))
                        secrets.append({
                            "scope": scope,
                            "key": key,
                            "description": f"Parameter Context reference: {pname}",
                            "source_processor": proc.name,
                            "original_value": "[PARAMETER_REFERENCE]",
                        })

    logger.info("Secret detection: %d secret(s) identified", len(secrets))
    return secrets


# ---------------------------------------------------------------------------
# 5. Package scanning
# ---------------------------------------------------------------------------

def _scan_packages(
    assessment: AssessmentResult,
    analysis_result: AnalysisResult,
) -> list[str]:
    """Scan assessment to determine required Python/cluster packages.

    Returns package install strings (e.g., ``boto3``, ``requests``).
    """
    packages: set[str] = set()

    # Check all mapping code for third-party imports
    for mapping in assessment.mappings:
        if not mapping.code:
            continue
        for imp in _extract_imports_from_code(mapping.code):
            module = _extract_module_name(imp)
            if module and module not in _STDLIB_MODULES and not any(
                module.startswith(p) for p in _PYSPARK_PREFIXES
            ):
                packages.add(module)

    # Check external systems from analysis
    for ext in analysis_result.external_systems:
        ext_type = ext.get("type", "").lower()
        if "kafka" in ext_type:
            packages.add("confluent-kafka")  # Optional, Spark has built-in Kafka
        if "elasticsearch" in ext_type:
            packages.add("elasticsearch")
        if "mongo" in ext_type:
            packages.add("pymongo")
        if "dynamodb" in ext_type or "s3" in ext_type or "sqs" in ext_type or "sns" in ext_type:
            packages.add("boto3")
        if "redis" in ext_type:
            packages.add("redis")
        if "slack" in ext_type:
            packages.add("requests")
        if "azure" in ext_type:
            packages.add("azure-storage-blob")
        if "gcs" in ext_type or "pubsub" in ext_type:
            packages.add("google-cloud-storage")

    return sorted(packages)

"""Unity Catalog helper — generates UC setup code cells.

Creates catalog, schema, external locations, volumes, secret scopes,
and permission grants based on the parsed flow definition.
"""

import logging
import re

from app.models.config import DatabricksConfig
from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)


def generate_uc_setup(
    parse_result: ParseResult,
    config: DatabricksConfig | None = None,
) -> list[dict]:
    """Generate Unity Catalog setup notebook cells.

    Returns list of notebook cell dicts: {type, source, label}.
    """
    if config is None:
        config = DatabricksConfig()

    logger.info("Generating UC setup: catalog=%s, schema=%s", config.catalog, config.schema_name)
    cells: list[dict] = []

    # Cell 1: Create catalog + schema
    cells.append({
        "type": "code",
        "source": (
            f'-- Unity Catalog Setup: Create catalog and schema\n'
            f'CREATE CATALOG IF NOT EXISTS {config.catalog};\n'
            f'USE CATALOG {config.catalog};\n'
            f'CREATE SCHEMA IF NOT EXISTS {config.schema_name}\n'
            f'  COMMENT "Migrated from {parse_result.platform} flow";\n'
            f'USE SCHEMA {config.schema_name};'
        ),
        "label": "uc_catalog_schema",
    })

    # Cell 2: External locations for cloud storage paths
    cloud_paths = _extract_cloud_paths(parse_result)
    if cloud_paths:
        loc_lines = ["-- External Locations for cloud storage"]
        for i, (path, provider) in enumerate(cloud_paths):
            safe_loc = re.sub(r"[^a-zA-Z0-9_]", "_", path.split("/")[-1] or f"loc_{i}")[:60]
            loc_lines.append(
                f'CREATE EXTERNAL LOCATION IF NOT EXISTS `{safe_loc}`\n'
                f'  URL "{path}"\n'
                f'  WITH (STORAGE CREDENTIAL `{config.secret_scope}_credential`);'
            )
        cells.append({
            "type": "code",
            "source": "\n\n".join(loc_lines),
            "label": "uc_external_locations",
        })
    else:
        cells.append({
            "type": "code",
            "source": "-- No external cloud paths detected; add external locations as needed.",
            "label": "uc_external_locations",
        })

    # Cell 3: Create volumes for file-based sources
    file_paths = _extract_file_paths(parse_result)
    vol_lines = ["-- Volumes for file-based data landing"]
    if file_paths:
        seen: set[str] = set()
        for fp in file_paths:
            vol_name = re.sub(r"[^a-zA-Z0-9_]", "_", fp.rstrip("/").split("/")[-1])[:60].lower()
            if vol_name and vol_name not in seen:
                seen.add(vol_name)
                vol_lines.append(
                    f'CREATE VOLUME IF NOT EXISTS {config.catalog}.{config.schema_name}.{vol_name}\n'
                    f'  COMMENT "Landing zone for {vol_name}";'
                )
    vol_lines.append(
        f'CREATE VOLUME IF NOT EXISTS {config.catalog}.{config.schema_name}.checkpoints\n'
        f'  COMMENT "Streaming checkpoint location";'
    )
    cells.append({
        "type": "code",
        "source": "\n\n".join(vol_lines),
        "label": "uc_volumes",
    })

    # Cell 4: Secret scope setup
    secrets = _extract_secret_references(parse_result)
    secret_lines = [
        f'# Secret scope setup for "{config.secret_scope}"',
        f'# Run via Databricks CLI or REST API:',
        f'# databricks secrets create-scope {config.secret_scope}',
    ]
    if secrets:
        for sec_name, sec_hint in secrets:
            secret_lines.append(
                f'# databricks secrets put-secret {config.secret_scope} {sec_name}  '
                f'# {sec_hint}'
            )
    else:
        secret_lines.append('# No credential references detected; add secrets as needed.')
    cells.append({
        "type": "code",
        "source": "\n".join(secret_lines),
        "label": "uc_secrets",
    })

    # Cell 5: Grant permissions
    cells.append({
        "type": "code",
        "source": (
            f'-- Permissions: grant access to migration service principal\n'
            f'GRANT USE CATALOG ON CATALOG {config.catalog} TO `migration_sp`;\n'
            f'GRANT USE SCHEMA ON SCHEMA {config.catalog}.{config.schema_name} TO `migration_sp`;\n'
            f'GRANT SELECT ON SCHEMA {config.catalog}.{config.schema_name} TO `migration_sp`;\n'
            f'GRANT MODIFY ON SCHEMA {config.catalog}.{config.schema_name} TO `migration_sp`;'
        ),
        "label": "uc_permissions",
    })

    return cells


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

_S3_RE = re.compile(r's3[a]?://[^\s"\'<>,]+', re.IGNORECASE)
_ADLS_RE = re.compile(r'abfss?://[^\s"\'<>,]+', re.IGNORECASE)
_GCS_RE = re.compile(r'gs://[^\s"\'<>,]+', re.IGNORECASE)
_LOCAL_PATH_RE = re.compile(r'(?:/opt/nifi/|/tmp/|/var/|/data/|/mnt/|C:\\\\|D:\\\\)[^\s"\'<>,]*', re.IGNORECASE)

_CRED_KEYS = re.compile(
    r'(password|secret|api[_-]?key|token|access[_-]?key|private[_-]?key|connection[_-]?string|credential)',
    re.IGNORECASE,
)


def _extract_cloud_paths(parse_result: ParseResult) -> list[tuple[str, str]]:
    """Extract S3/ADLS/GCS paths from all processor properties."""
    paths: list[tuple[str, str]] = []
    seen: set[str] = set()

    for proc in parse_result.processors:
        text = str(proc.properties)
        for m in _S3_RE.finditer(text):
            p = m.group(0)
            if p not in seen:
                seen.add(p)
                paths.append((p, "aws"))
        for m in _ADLS_RE.finditer(text):
            p = m.group(0)
            if p not in seen:
                seen.add(p)
                paths.append((p, "azure"))
        for m in _GCS_RE.finditer(text):
            p = m.group(0)
            if p not in seen:
                seen.add(p)
                paths.append((p, "gcp"))

    return paths


def _extract_file_paths(parse_result: ParseResult) -> list[str]:
    """Extract local filesystem paths from processor properties."""
    paths: list[str] = []
    seen: set[str] = set()

    for proc in parse_result.processors:
        text = str(proc.properties)
        for m in _LOCAL_PATH_RE.finditer(text):
            p = m.group(0).rstrip("'\")")
            if p not in seen:
                seen.add(p)
                paths.append(p)

    return paths


def _extract_secret_references(parse_result: ParseResult) -> list[tuple[str, str]]:
    """Extract credential property names from processors and controller services."""
    secrets: list[tuple[str, str]] = []
    seen: set[str] = set()

    for proc in parse_result.processors:
        for key, val in proc.properties.items():
            if _CRED_KEYS.search(key) and key not in seen:
                seen.add(key)
                hint = f"from {proc.type}: {key}"
                safe_key = re.sub(r"[^a-zA-Z0-9_]", "_", key).lower()
                secrets.append((safe_key, hint))

    for svc in parse_result.controller_services:
        for key, val in svc.properties.items():
            if _CRED_KEYS.search(key) and key not in seen:
                seen.add(key)
                hint = f"from controller service {svc.type}: {key}"
                safe_key = re.sub(r"[^a-zA-Z0-9_]", "_", key).lower()
                secrets.append((safe_key, hint))

    return secrets

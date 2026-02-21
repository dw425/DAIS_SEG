"""Detect file format by extension + content sniffing and route to correct parser."""

import json
import logging
import zipfile
from io import BytesIO
from pathlib import Path

from app.models.pipeline import ParseResult
from app.utils.errors import UnsupportedFormatError

logger = logging.getLogger(__name__)

# Extension to parser mapping (lazy imports below)
_EXT_MAP: dict[str, str] = {
    ".xml": "_detect_xml",
    ".dtsx": "ssis",
    ".ktr": "pentaho",
    ".kjb": "pentaho",
    ".dsx": "datastage",
    ".json": "_detect_json",
    ".sql": "_detect_sql",
    ".py": "_detect_python",
    ".zip": "_detect_zip",
    ".yml": "dbt",
    ".yaml": "dbt",
    ".item": "talend",
}


def parse_flow(content: bytes, filename: str) -> ParseResult:
    """Detect file format and dispatch to the correct parser."""
    ext = Path(filename).suffix.lower()
    handler_key = _EXT_MAP.get(ext)

    if handler_key is None:
        handler_key = _sniff_content(content)

    if handler_key is None:
        raise UnsupportedFormatError(f"Unsupported file format: {ext} ({filename})")

    # Internal dispatchers for ambiguous extensions
    if handler_key.startswith("_detect_"):
        handler_key = globals()[handler_key](content, filename)

    return _dispatch(handler_key, content, filename)


def _sniff_content(content: bytes) -> str | None:
    """Attempt to determine format from content bytes."""
    head = content[:2000].strip()
    if head.startswith(b"<?xml") or head.startswith(b"<"):
        return "_detect_xml"
    if head.startswith(b"{") or head.startswith(b"["):
        return "_detect_json"
    if b"CREATE " in head.upper() or b"SELECT " in head.upper():
        return "sql"
    if b"def " in head or b"import " in head or b"from " in head:
        return "_detect_python"
    return None


def _detect_xml(content: bytes, filename: str) -> str:
    """Determine XML sub-type."""
    head = content[:5000].decode("utf-8", errors="replace").lower()
    if "dtsx:" in head or "dts:executable" in head or "<dts:" in head:
        return "ssis"
    if "informatica" in head or "<folder " in head or "<mapping " in head:
        return "informatica"
    if "<template>" in head or "<snippet>" in head or "nifi" in head.lower():
        return "nifi_xml"
    if "<flowcontroller" in head or "<rootgroup" in head:
        return "nifi_xml"
    if "<processgroupflow" in head:
        return "nifi_xml"
    if "<transformation" in head or "<job" in head:
        # Could be Pentaho
        if "<transformation" in head:
            return "pentaho"
    if "oracle" in head and ("<folder" in head or "<package" in head):
        return "oracle_odi"
    # Generic XML fallback: try NiFi
    return "nifi_xml"


def _detect_json(content: bytes, filename: str) -> str:
    """Determine JSON sub-type."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        raise UnsupportedFormatError("Invalid JSON")

    if isinstance(data, dict):
        # NiFi Registry JSON
        if "flowContents" in data or "versionedFlowSnapshot" in data:
            return "nifi_json"
        if "processors" in data and ("connections" in data or "processGroups" in data):
            return "nifi_json"
        # ADF ARM template
        if "resources" in data and any(
            r.get("type", "").startswith("Microsoft.DataFactory")
            for r in data.get("resources", [])
            if isinstance(r, dict)
        ):
            return "azure_adf"
        # AWS Glue
        if "JobName" in data or ("Job" in data and "Command" in data.get("Job", {})):
            return "aws_glue"
        # dbt manifest
        if "metadata" in data and "nodes" in data:
            return "dbt"
        # Matillion
        if "group" in data and "components" in data:
            return "matillion"
        # Fivetran
        if "service" in data and ("schema" in data or "sync_mode" in data):
            return "fivetran"
        # Airbyte
        if "catalog" in data or ("syncCatalog" in data):
            return "airbyte"
        # Stitch
        if "integration_type" in data or "replication_keys" in data:
            return "stitch"
        # Fallback: try NiFi
        if "name" in data:
            return "nifi_json"

    raise UnsupportedFormatError("Could not identify JSON format")


def _detect_python(content: bytes, filename: str) -> str:
    """Determine Python file sub-type."""
    text = content.decode("utf-8", errors="replace")
    if "from airflow" in text or "from airflow." in text or "DAG(" in text:
        return "airflow"
    if "from prefect" in text or "@flow" in text:
        return "prefect"
    if "from dagster" in text or "@op" in text or "@job" in text or "@asset" in text:
        return "dagster"
    if "SparkSession" in text or "spark.read" in text or "spark.sql" in text:
        return "spark"
    # Fallback to airflow if it has task-like patterns
    if "def " in text:
        return "spark"
    raise UnsupportedFormatError("Could not identify Python file type")


def _detect_sql(content: bytes, filename: str) -> str:
    """Determine SQL file sub-type."""
    text = content.decode("utf-8", errors="replace").upper()
    if "CREATE PIPE" in text or "CREATE TASK" in text or "CREATE STREAM" in text:
        return "snowflake"
    return "sql"


def _detect_zip(content: bytes, filename: str) -> str:
    """Detect ZIP contents."""
    try:
        with zipfile.ZipFile(BytesIO(content)) as zf:
            names = zf.namelist()
            if any(n.endswith(".item") for n in names):
                return "talend"
    except zipfile.BadZipFile:
        pass
    raise UnsupportedFormatError("Unrecognized ZIP archive")


def _dispatch(parser_key: str, content: bytes, filename: str) -> ParseResult:
    """Import and call the appropriate parser."""
    parsers = {
        "nifi_xml": lambda c, f: _import("app.engines.parsers.nifi_xml", "parse_nifi_xml")(c, f),
        "nifi_json": lambda c, f: _import("app.engines.parsers.nifi_json", "parse_nifi_json")(c, f),
        "ssis": lambda c, f: _import("app.engines.parsers.ssis_parser", "parse_ssis")(c, f),
        "informatica": lambda c, f: _import("app.engines.parsers.informatica_parser", "parse_informatica")(c, f),
        "talend": lambda c, f: _import("app.engines.parsers.talend_parser", "parse_talend")(c, f),
        "airflow": lambda c, f: _import("app.engines.parsers.airflow_parser", "parse_airflow")(c, f),
        "dbt": lambda c, f: _import("app.engines.parsers.dbt_parser", "parse_dbt")(c, f),
        "oracle_odi": lambda c, f: _import("app.engines.parsers.oracle_parser", "parse_oracle_odi")(c, f),
        "snowflake": lambda c, f: _import("app.engines.parsers.snowflake_parser", "parse_snowflake")(c, f),
        "azure_adf": lambda c, f: _import("app.engines.parsers.azure_adf_parser", "parse_adf")(c, f),
        "aws_glue": lambda c, f: _import("app.engines.parsers.aws_glue_parser", "parse_glue")(c, f),
        "matillion": lambda c, f: _import("app.engines.parsers.matillion_parser", "parse_matillion")(c, f),
        "pentaho": lambda c, f: _import("app.engines.parsers.pentaho_parser", "parse_pentaho")(c, f),
        "datastage": lambda c, f: _import("app.engines.parsers.datastage_parser", "parse_datastage")(c, f),
        "fivetran": lambda c, f: _import("app.engines.parsers.fivetran_parser", "parse_fivetran")(c, f),
        "airbyte": lambda c, f: _import("app.engines.parsers.airbyte_parser", "parse_airbyte")(c, f),
        "prefect": lambda c, f: _import("app.engines.parsers.prefect_parser", "parse_prefect")(c, f),
        "dagster": lambda c, f: _import("app.engines.parsers.dagster_parser", "parse_dagster")(c, f),
        "stitch": lambda c, f: _import("app.engines.parsers.stitch_parser", "parse_stitch")(c, f),
        "sql": lambda c, f: _import("app.engines.parsers.sql_parser", "parse_sql")(c, f),
        "spark": lambda c, f: _import("app.engines.parsers.spark_parser", "parse_spark")(c, f),
    }
    fn = parsers.get(parser_key)
    if fn is None:
        raise UnsupportedFormatError(f"No parser for key: {parser_key}")
    return fn(content, filename)


def _import(module_path: str, attr: str):
    """Lazy import a parser function."""
    import importlib

    mod = importlib.import_module(module_path)
    return getattr(mod, attr)

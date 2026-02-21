"""Controller Service to Unity Catalog Connection generator.

Maps NiFi DBCPConnectionPool controller services to Unity Catalog Connection
objects, extracts JDBC configuration, generates Databricks Secrets provisioning
commands, and maps OAuth providers to Service Principals.
"""

import logging
import re

from app.models.pipeline import ParseResult
from app.models.processor import ControllerService

logger = logging.getLogger(__name__)

# DBCP connection pool service types
_DBCP_RE = re.compile(r"(DBCP|ConnectionPool|HikariCP|DBCPService)", re.I)

# OAuth/token provider service types
_OAUTH_RE = re.compile(r"(OAuth|AccessToken|TokenProvider|Credential)", re.I)

# JDBC URL pattern — captures host, optional :port, and optional /database
_JDBC_URL_RE = re.compile(r"jdbc:(\w+)://([^:/;?]+)(?::(\d+))?(?:/([^;?]+))?", re.I)

# Property keys for connection details
_URL_PROPS = re.compile(r"(Database\s*Connection\s*URL|url|jdbc[\.\-_]?url|connection[\.\-_]?string)", re.I)
_DRIVER_PROPS = re.compile(r"(Database\s*Driver\s*Class\s*Name|driver|driver[\.\-_]?class)", re.I)
_USER_PROPS = re.compile(r"(Database\s*User|user|username|db[\.\-_]?user)", re.I)
_PASSWORD_PROPS = re.compile(r"(Password|password|secret|credential)", re.I)
_TIMEOUT_PROPS = re.compile(r"(Max\s*Wait\s*Time|timeout|connection[\.\-_]?timeout|max[\.\-_]?wait)", re.I)

# Default ports per database type (used when JDBC URL omits port)
_DEFAULT_PORTS = {
    "mysql": "3306",
    "postgresql": "5432",
    "sqlserver": "1433",
    "oracle": "1521",
    "redshift": "5439",
    "snowflake": "443",
    "bigquery": "443",
}

# Database type to Unity Catalog connection type mapping
_DB_TYPE_MAP = {
    "mysql": "MYSQL",
    "postgresql": "POSTGRESQL",
    "sqlserver": "SQLSERVER",
    "oracle": "ORACLE",
    "redshift": "REDSHIFT",
    "snowflake": "SNOWFLAKE",
    "bigquery": "BIGQUERY",
}


def generate_connections(parse_result: ParseResult) -> dict:
    """Generate Unity Catalog Connection configurations from controller services.

    Returns:
        {
            "connection_mappings": [...],
            "secrets_provisioning": [...],
            "service_principal_mappings": [...],
            "sql_statements": [...],
            "summary": {...},
        }
    """
    controllers = parse_result.controller_services if parse_result.controller_services else []

    connection_mappings = []
    secrets_provisioning = []
    sp_mappings = []
    sql_statements = []

    for cs in controllers:
        if _DBCP_RE.search(cs.type):
            mapping = _map_dbcp_service(cs)
            connection_mappings.append(mapping)

            secrets = _generate_secrets(cs, mapping)
            secrets_provisioning.extend(secrets)

            sql = _generate_connection_sql(cs, mapping)
            sql_statements.append(sql)

        elif _OAUTH_RE.search(cs.type):
            sp = _map_oauth_to_service_principal(cs)
            sp_mappings.append(sp)

    db_types = [m["database_type"] for m in connection_mappings]
    logger.info("Connection generation: %d DBCP services, %d secrets, database types: %s", len(connection_mappings), len(secrets_provisioning), db_types)
    return {
        "connection_mappings": connection_mappings,
        "secrets_provisioning": secrets_provisioning,
        "service_principal_mappings": sp_mappings,
        "sql_statements": sql_statements,
        "summary": {
            "dbcp_services": len(connection_mappings),
            "secrets_needed": len(secrets_provisioning),
            "service_principals": len(sp_mappings),
        },
    }


def _map_dbcp_service(cs: ControllerService) -> dict:
    """Map a DBCP controller service to Unity Catalog Connection config."""
    jdbc_url = _extract_prop(cs, _URL_PROPS)
    driver_class = _extract_prop(cs, _DRIVER_PROPS)
    username = _extract_prop(cs, _USER_PROPS)
    password = _extract_prop(cs, _PASSWORD_PROPS)
    timeout = _extract_prop(cs, _TIMEOUT_PROPS)

    # Parse JDBC URL for database type, host, port, and database name
    db_type = "generic"
    host = ""
    port = ""
    database = ""
    if jdbc_url:
        m = _JDBC_URL_RE.search(jdbc_url)
        if m:
            db_type = m.group(1).lower()
            host = m.group(2)
            port = m.group(3) or ""
            database = m.group(4) or ""

    # Derive port from database type if not explicit in the URL
    if not port:
        port = _DEFAULT_PORTS.get(db_type, "443")

    uc_type = _DB_TYPE_MAP.get(db_type, "GENERIC")
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", cs.name).strip("_").lower()

    # Detect if credentials are hardcoded
    has_hardcoded_creds = bool(password and not password.startswith("${"))

    return {
        "service_name": cs.name,
        "service_type": cs.type,
        "jdbc_url": jdbc_url,
        "driver_class": driver_class,
        "database_type": db_type,
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "has_hardcoded_credentials": has_hardcoded_creds,
        "timeout": timeout,
        "unity_catalog_connection_type": uc_type,
        "connection_name": safe_name,
        "code_snippet": _generate_connection_code(safe_name, db_type, table_name=database or safe_name),
    }


def _generate_secrets(cs: ControllerService, mapping: dict) -> list[dict]:
    """Generate Databricks Secrets provisioning commands."""
    safe_name = mapping["connection_name"]
    scope = f"migration_{safe_name}"
    secrets = []

    if mapping["jdbc_url"]:
        secrets.append({
            "scope": scope,
            "key": "jdbc_url",
            "description": f"JDBC URL for {cs.name}",
            "code": f'# Store: databricks secrets put-secret {scope} jdbc_url\n'
                    f'jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc_url")',
        })

    if mapping["username"]:
        secrets.append({
            "scope": scope,
            "key": "jdbc_user",
            "description": f"Database username for {cs.name}",
            "code": f'# Store: databricks secrets put-secret {scope} jdbc_user\n'
                    f'jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc_user")',
        })

    # Always generate a password secret
    secrets.append({
        "scope": scope,
        "key": "jdbc_password",
        "description": f"Database password for {cs.name}",
        "code": f'# Store: databricks secrets put-secret {scope} jdbc_password\n'
                f'jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc_password")',
        "warning": (
            "HARDCODED CREDENTIAL DETECTED — migrate to Databricks Secrets immediately"
            if mapping["has_hardcoded_credentials"]
            else None
        ),
    })

    return secrets


def _generate_connection_sql(cs: ControllerService, mapping: dict) -> dict:
    """Generate Unity Catalog CREATE CONNECTION SQL statement."""
    safe_name = mapping["connection_name"]
    uc_type = mapping["unity_catalog_connection_type"]
    scope = f"migration_{safe_name}"

    port = mapping.get("port", "443")
    sql = (
        f"-- Unity Catalog Connection for NiFi service '{cs.name}'\n"
        f"CREATE CONNECTION IF NOT EXISTS {safe_name}\n"
        f"  TYPE {uc_type}\n"
        f"  OPTIONS (\n"
        f"    host '{mapping['host']}',\n"
        f"    port '{port}',\n"
        f"    user secret('{scope}', 'jdbc_user'),\n"
        f"    password secret('{scope}', 'jdbc_password')\n"
        f"  );"
    )

    if mapping["database"]:
        sql += (
            f"\n\n-- Create foreign catalog from this connection\n"
            f"CREATE FOREIGN CATALOG IF NOT EXISTS {safe_name}_catalog\n"
            f"  USING CONNECTION {safe_name}\n"
            f"  OPTIONS (database '{mapping['database']}');"
        )

    return {
        "service_name": cs.name,
        "connection_name": safe_name,
        "sql": sql,
    }


def _map_oauth_to_service_principal(cs: ControllerService) -> dict:
    """Map OAuth/token provider to Databricks Service Principal."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", cs.name).strip("_").lower()

    # Extract OAuth properties
    client_id = ""
    token_url = ""
    for key, val in cs.properties.items():
        if not isinstance(val, str):
            continue
        if "client" in key.lower() and "id" in key.lower():
            client_id = val
        elif "token" in key.lower() and "url" in key.lower():
            token_url = val

    code = (
        f"# Service Principal mapping for NiFi OAuth service '{cs.name}'\n"
        f"# Replace NiFi OAuth2 with Databricks Service Principal\n"
        f"#\n"
        f"# 1. Create Service Principal in Databricks Account Console\n"
        f"# 2. Store credentials in Secret Scope:\n"
        f'#    databricks secrets put-secret sp_{safe_name} client_id\n'
        f'#    databricks secrets put-secret sp_{safe_name} client_secret\n'
        f"#\n"
        f"# Usage in notebook:\n"
        f'client_id = dbutils.secrets.get(scope="sp_{safe_name}", key="client_id")\n'
        f'client_secret = dbutils.secrets.get(scope="sp_{safe_name}", key="client_secret")'
    )

    return {
        "service_name": cs.name,
        "service_type": cs.type,
        "original_client_id": client_id,
        "original_token_url": token_url,
        "databricks_scope": f"sp_{safe_name}",
        "code_snippet": code,
    }


def _generate_connection_code(safe_name: str, db_type: str, table_name: str = "") -> str:
    """Generate PySpark code to use the Unity Catalog Connection."""
    scope = f"migration_{safe_name}"
    dbtable = table_name or safe_name

    return (
        f"# Read from {db_type} via Unity Catalog Connection\n"
        f"jdbc_url = dbutils.secrets.get(scope=\"{scope}\", key=\"jdbc_url\")\n"
        f"jdbc_user = dbutils.secrets.get(scope=\"{scope}\", key=\"jdbc_user\")\n"
        f"jdbc_password = dbutils.secrets.get(scope=\"{scope}\", key=\"jdbc_password\")\n"
        f"\n"
        f"df = (\n"
        f"    spark.read\n"
        f"    .format(\"jdbc\")\n"
        f"    .option(\"url\", jdbc_url)\n"
        f"    .option(\"dbtable\", \"{dbtable}\")\n"
        f"    .option(\"user\", jdbc_user)\n"
        f"    .option(\"password\", jdbc_password)\n"
        f"    .load()\n"
        f")\n"
        f"\n"
        f"# Alternative: Use Unity Catalog foreign catalog\n"
        f"# df = spark.table(\"{safe_name}_catalog.schema.table\")"
    )


def _extract_prop(cs: ControllerService, pattern: re.Pattern) -> str:
    """Extract a property value matching a key pattern."""
    for key, val in cs.properties.items():
        if pattern.search(key) and isinstance(val, str):
            return val
    return ""

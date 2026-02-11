"""Environment parser — reads .env files and credential strings into structured source credentials.

Accepts source database credentials in multiple formats:
- .env file (key=value lines)
- Raw text (pasted into Streamlit)
- Dictionary (programmatic use)

Supports: Oracle, SQL Server, PostgreSQL, MySQL, Snowflake, Teradata, DB2
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# Default ports per source type
DEFAULT_PORTS = {
    "oracle": "1521",
    "sqlserver": "1433",
    "postgresql": "5432",
    "mysql": "3306",
    "snowflake": "443",
    "teradata": "1025",
    "db2": "50000",
}

# Map user-friendly names to Databricks CONNECTION TYPE values
DATABRICKS_CONNECTION_TYPES = {
    "oracle": "ORACLE",
    "sqlserver": "SQLSERVER",
    "sql_server": "SQLSERVER",
    "mssql": "SQLSERVER",
    "postgresql": "POSTGRESQL",
    "postgres": "POSTGRESQL",
    "mysql": "MYSQL",
    "snowflake": "SNOWFLAKE",
    "teradata": "TERADATA",
    "db2": "DB2",
    "redshift": "REDSHIFT",
}


@dataclass
class SourceCredentials:
    """Structured credentials for a source database system."""

    source_type: str
    host: str
    port: str
    database: str
    user: str
    password: str
    connection_name: str = ""
    extra_options: dict[str, str] = field(default_factory=dict)

    # Snowflake-specific
    warehouse: str = ""
    role: str = ""
    account: str = ""

    def __post_init__(self):
        """Normalize source type and set defaults."""
        self.source_type = self.source_type.lower().strip()
        if not self.port:
            self.port = DEFAULT_PORTS.get(self.source_type, "")
        if not self.connection_name:
            safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", self.host.split(".")[0])
            self.connection_name = f"seg_{self.source_type}_{safe_name}"

    @property
    def databricks_type(self) -> str:
        """Get the Databricks CONNECTION TYPE string."""
        return DATABRICKS_CONNECTION_TYPES.get(self.source_type, self.source_type.upper())

    def validate(self) -> list[str]:
        """Validate credentials and return list of errors (empty = valid)."""
        errors = []
        if not self.source_type:
            errors.append("source_type is required")
        elif self.source_type not in DATABRICKS_CONNECTION_TYPES:
            errors.append(f"Unsupported source_type: {self.source_type}. Supported: {', '.join(sorted(set(DATABRICKS_CONNECTION_TYPES.values())))}")
        if not self.host:
            errors.append("host is required")
        if not self.database and self.source_type != "snowflake":
            errors.append("database is required")
        if not self.user:
            errors.append("user is required")
        if not self.password:
            errors.append("password is required")
        if self.source_type == "snowflake" and not self.account:
            errors.append("account is required for Snowflake")
        return errors


class EnvParser:
    """Parses source database credentials from .env files and text."""

    # Keys to look for in .env files (case-insensitive)
    KEY_MAP = {
        "SOURCE_TYPE": "source_type",
        "SOURCE_HOST": "host",
        "SOURCE_PORT": "port",
        "SOURCE_DATABASE": "database",
        "SOURCE_DB": "database",
        "SOURCE_USER": "user",
        "SOURCE_USERNAME": "user",
        "SOURCE_PASSWORD": "password",
        "SOURCE_PASS": "password",
        "SOURCE_CONNECTION_NAME": "connection_name",
        # Snowflake-specific
        "SOURCE_WAREHOUSE": "warehouse",
        "SOURCE_ROLE": "role",
        "SOURCE_ACCOUNT": "account",
        # Also accept without SOURCE_ prefix
        "DB_TYPE": "source_type",
        "DB_HOST": "host",
        "DB_PORT": "port",
        "DB_NAME": "database",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        # JDBC-style
        "JDBC_HOST": "host",
        "JDBC_PORT": "port",
        "JDBC_USER": "user",
        "JDBC_PASSWORD": "password",
        "JDBC_DATABASE": "database",
    }

    @classmethod
    def parse_env_file(cls, file_path: str | Path) -> SourceCredentials:
        """Parse a .env file into SourceCredentials."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Env file not found: {file_path}")
        return cls.parse_env_string(path.read_text())

    @classmethod
    def parse_env_string(cls, content: str) -> SourceCredentials:
        """Parse .env-formatted text into SourceCredentials."""
        env_dict = cls._parse_dotenv(content)
        return cls.parse_dict(env_dict)

    @classmethod
    def parse_dict(cls, env_dict: dict[str, str]) -> SourceCredentials:
        """Parse a dictionary of environment variables into SourceCredentials."""
        mapped = {}
        extra = {}

        for key, value in env_dict.items():
            upper_key = key.upper().strip()
            if upper_key in cls.KEY_MAP:
                attr = cls.KEY_MAP[upper_key]
                mapped[attr] = value.strip()
            elif upper_key.startswith("SOURCE_"):
                # Store unrecognized SOURCE_ vars as extra options
                extra_key = upper_key.replace("SOURCE_", "").lower()
                extra[extra_key] = value.strip()

        # Also check for JDBC URL that encodes everything
        jdbc_url = env_dict.get("JDBC_URL", env_dict.get("SOURCE_JDBC_URL", ""))
        if jdbc_url and "host" not in mapped:
            parsed = cls._parse_jdbc_url(jdbc_url)
            for k, v in parsed.items():
                if k not in mapped or not mapped[k]:
                    mapped[k] = v

        return SourceCredentials(
            source_type=mapped.get("source_type", ""),
            host=mapped.get("host", ""),
            port=mapped.get("port", ""),
            database=mapped.get("database", ""),
            user=mapped.get("user", ""),
            password=mapped.get("password", ""),
            connection_name=mapped.get("connection_name", ""),
            warehouse=mapped.get("warehouse", ""),
            role=mapped.get("role", ""),
            account=mapped.get("account", ""),
            extra_options=extra,
        )

    @staticmethod
    def _parse_dotenv(content: str) -> dict[str, str]:
        """Parse .env file content into a flat dictionary."""
        result = {}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Handle KEY=VALUE (with optional quotes)
            match = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)=(.*)$', line)
            if match:
                key = match.group(1)
                value = match.group(2).strip()
                # Remove surrounding quotes
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                result[key] = value
        return result

    @staticmethod
    def _parse_jdbc_url(url: str) -> dict[str, str]:
        """Extract host, port, database, and source_type from a JDBC URL."""
        result = {}
        url = url.strip()

        # jdbc:oracle:thin:@host:port:sid
        oracle_match = re.match(r'jdbc:oracle:thin:@([^:]+):(\d+):(\w+)', url)
        if oracle_match:
            result["source_type"] = "oracle"
            result["host"] = oracle_match.group(1)
            result["port"] = oracle_match.group(2)
            result["database"] = oracle_match.group(3)
            return result

        # jdbc:sqlserver://host:port;database=db
        sqlserver_match = re.match(r'jdbc:sqlserver://([^:;]+):?(\d*);?.*database=(\w+)', url, re.I)
        if sqlserver_match:
            result["source_type"] = "sqlserver"
            result["host"] = sqlserver_match.group(1)
            result["port"] = sqlserver_match.group(2) or "1433"
            result["database"] = sqlserver_match.group(3)
            return result

        # jdbc:postgresql://host:port/database
        pg_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)/(\w+)', url)
        if pg_match:
            result["source_type"] = "postgresql"
            result["host"] = pg_match.group(1)
            result["port"] = pg_match.group(2)
            result["database"] = pg_match.group(3)
            return result

        # jdbc:mysql://host:port/database
        mysql_match = re.match(r'jdbc:mysql://([^:]+):(\d+)/(\w+)', url)
        if mysql_match:
            result["source_type"] = "mysql"
            result["host"] = mysql_match.group(1)
            result["port"] = mysql_match.group(2)
            result["database"] = mysql_match.group(3)
            return result

        # jdbc:snowflake://account.snowflakecomputing.com
        snow_match = re.match(r'jdbc:snowflake://([^/]+)', url)
        if snow_match:
            result["source_type"] = "snowflake"
            result["host"] = snow_match.group(1)
            result["account"] = snow_match.group(1).split(".")[0]
            return result

        return result

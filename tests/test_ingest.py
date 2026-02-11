"""Tests for the Ingest module (EnvParser, SourceCredentials)."""

import pytest

from dais_seg.ingest.env_parser import (
    SourceCredentials,
    EnvParser,
    DEFAULT_PORTS,
    DATABRICKS_CONNECTION_TYPES,
)


class TestSourceCredentials:
    """Tests for SourceCredentials dataclass."""

    def test_basic_creation(self):
        creds = SourceCredentials(
            source_type="oracle",
            host="oracle.prod.example.com",
            port="1521",
            database="PROD",
            user="admin",
            password="secret",
        )
        assert creds.source_type == "oracle"
        assert creds.host == "oracle.prod.example.com"
        assert creds.port == "1521"
        assert creds.database == "PROD"

    def test_source_type_normalized_to_lowercase(self):
        creds = SourceCredentials(
            source_type="  ORACLE  ",
            host="h", port="", database="d", user="u", password="p",
        )
        assert creds.source_type == "oracle"

    def test_default_port_applied(self):
        for source_type, expected_port in DEFAULT_PORTS.items():
            creds = SourceCredentials(
                source_type=source_type,
                host="h", port="", database="d", user="u", password="p",
            )
            assert creds.port == expected_port, f"Default port for {source_type}"

    def test_explicit_port_not_overridden(self):
        creds = SourceCredentials(
            source_type="oracle",
            host="h", port="9999", database="d", user="u", password="p",
        )
        assert creds.port == "9999"

    def test_auto_connection_name(self):
        creds = SourceCredentials(
            source_type="postgresql",
            host="pg.prod.example.com",
            port="", database="mydb", user="u", password="p",
        )
        assert creds.connection_name.startswith("seg_postgresql_")
        assert "pg" in creds.connection_name

    def test_explicit_connection_name(self):
        creds = SourceCredentials(
            source_type="oracle",
            host="h", port="", database="d", user="u", password="p",
            connection_name="my_conn",
        )
        assert creds.connection_name == "my_conn"

    def test_databricks_type_mapping(self):
        assert SourceCredentials(
            source_type="oracle", host="h", port="", database="d", user="u", password="p"
        ).databricks_type == "ORACLE"

        assert SourceCredentials(
            source_type="postgresql", host="h", port="", database="d", user="u", password="p"
        ).databricks_type == "POSTGRESQL"

        assert SourceCredentials(
            source_type="sqlserver", host="h", port="", database="d", user="u", password="p"
        ).databricks_type == "SQLSERVER"

    def test_validate_valid_oracle(self):
        creds = SourceCredentials(
            source_type="oracle", host="h", port="1521",
            database="d", user="u", password="p",
        )
        assert creds.validate() == []

    def test_validate_missing_fields(self):
        creds = SourceCredentials(
            source_type="", host="", port="",
            database="", user="", password="",
        )
        errors = creds.validate()
        assert any("source_type" in e for e in errors)
        assert any("host" in e for e in errors)
        assert any("user" in e for e in errors)
        assert any("password" in e for e in errors)

    def test_validate_unsupported_type(self):
        creds = SourceCredentials(
            source_type="nosqldb", host="h", port="",
            database="d", user="u", password="p",
        )
        errors = creds.validate()
        assert any("Unsupported" in e for e in errors)

    def test_validate_snowflake_requires_account(self):
        creds = SourceCredentials(
            source_type="snowflake", host="h", port="",
            database="", user="u", password="p", account="",
        )
        errors = creds.validate()
        assert any("account" in e for e in errors)

    def test_validate_snowflake_no_database_required(self):
        creds = SourceCredentials(
            source_type="snowflake", host="h", port="",
            database="", user="u", password="p", account="myacct",
        )
        errors = creds.validate()
        # database should NOT be required for snowflake
        assert not any("database" in e for e in errors)


class TestEnvParser:
    """Tests for EnvParser."""

    SAMPLE_ENV = """\
# Databricks workspace
DATABRICKS_HOST=https://my-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi123

# Source system
SOURCE_TYPE=oracle
SOURCE_HOST=oracle.prod.example.com
SOURCE_PORT=1521
SOURCE_DATABASE=PROD
SOURCE_USER=migration_user
SOURCE_PASSWORD=secret123
SOURCE_CONNECTION_NAME=oracle_prod
"""

    def test_parse_env_string_oracle(self):
        creds = EnvParser.parse_env_string(self.SAMPLE_ENV)
        assert creds.source_type == "oracle"
        assert creds.host == "oracle.prod.example.com"
        assert creds.port == "1521"
        assert creds.database == "PROD"
        assert creds.user == "migration_user"
        assert creds.password == "secret123"
        assert creds.connection_name == "oracle_prod"

    def test_parse_env_string_with_quotes(self):
        env = 'SOURCE_TYPE="postgresql"\nSOURCE_HOST=\'pg.local\'\nSOURCE_DATABASE=mydb\nSOURCE_USER=u\nSOURCE_PASSWORD="pass word"'
        creds = EnvParser.parse_env_string(env)
        assert creds.source_type == "postgresql"
        assert creds.host == "pg.local"
        assert creds.password == "pass word"

    def test_parse_env_string_ignores_comments(self):
        env = "# This is a comment\nSOURCE_TYPE=mysql\nSOURCE_HOST=h\nSOURCE_DATABASE=d\nSOURCE_USER=u\nSOURCE_PASSWORD=p"
        creds = EnvParser.parse_env_string(env)
        assert creds.source_type == "mysql"

    def test_parse_env_string_ignores_blank_lines(self):
        env = "\n\nSOURCE_TYPE=mysql\n\nSOURCE_HOST=h\nSOURCE_DATABASE=d\nSOURCE_USER=u\nSOURCE_PASSWORD=p\n\n"
        creds = EnvParser.parse_env_string(env)
        assert creds.source_type == "mysql"
        assert creds.host == "h"

    def test_parse_dict(self):
        env = {
            "SOURCE_TYPE": "sqlserver",
            "SOURCE_HOST": "sql.local",
            "SOURCE_PORT": "1433",
            "SOURCE_DATABASE": "AdventureWorks",
            "SOURCE_USER": "sa",
            "SOURCE_PASSWORD": "p@ss",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "sqlserver"
        assert creds.database == "AdventureWorks"

    def test_parse_dict_with_alternate_keys(self):
        env = {
            "DB_TYPE": "mysql",
            "DB_HOST": "mysql.local",
            "DB_NAME": "shop",
            "DB_USER": "root",
            "DB_PASSWORD": "secret",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "mysql"
        assert creds.host == "mysql.local"
        assert creds.database == "shop"

    def test_parse_dict_extra_source_vars(self):
        env = {
            "SOURCE_TYPE": "oracle",
            "SOURCE_HOST": "h",
            "SOURCE_DATABASE": "d",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
            "SOURCE_CUSTOM_FLAG": "true",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.extra_options.get("custom_flag") == "true"


class TestJdbcUrlParsing:
    """Tests for JDBC URL parsing."""

    def test_oracle_jdbc(self):
        env = {
            "JDBC_URL": "jdbc:oracle:thin:@oracle.prod.example.com:1521:PROD",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "oracle"
        assert creds.host == "oracle.prod.example.com"
        assert creds.port == "1521"
        assert creds.database == "PROD"

    def test_sqlserver_jdbc(self):
        env = {
            "JDBC_URL": "jdbc:sqlserver://sql.prod.example.com:1433;database=AdventureWorks",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "sqlserver"
        assert creds.host == "sql.prod.example.com"
        assert creds.database == "AdventureWorks"

    def test_postgresql_jdbc(self):
        env = {
            "JDBC_URL": "jdbc:postgresql://pg.example.com:5432/analytics",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "postgresql"
        assert creds.host == "pg.example.com"
        assert creds.port == "5432"
        assert creds.database == "analytics"

    def test_mysql_jdbc(self):
        env = {
            "JDBC_URL": "jdbc:mysql://mysql.example.com:3306/shop",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "mysql"
        assert creds.host == "mysql.example.com"
        assert creds.database == "shop"

    def test_snowflake_jdbc(self):
        env = {
            "JDBC_URL": "jdbc:snowflake://myaccount.snowflakecomputing.com",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        assert creds.source_type == "snowflake"
        assert creds.account == "myaccount"

    def test_jdbc_does_not_override_explicit_fields(self):
        env = {
            "JDBC_URL": "jdbc:oracle:thin:@jdbc-host:1521:JDBCDB",
            "SOURCE_HOST": "explicit-host",
            "SOURCE_USER": "u",
            "SOURCE_PASSWORD": "p",
        }
        creds = EnvParser.parse_dict(env)
        # Explicit SOURCE_HOST should take precedence
        assert creds.host == "explicit-host"


class TestEnvFile:
    """Tests for file-based parsing."""

    def test_parse_env_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "SOURCE_TYPE=db2\nSOURCE_HOST=db2.local\nSOURCE_PORT=50000\n"
            "SOURCE_DATABASE=SAMPLE\nSOURCE_USER=db2inst1\nSOURCE_PASSWORD=secret\n"
        )
        creds = EnvParser.parse_env_file(str(env_file))
        assert creds.source_type == "db2"
        assert creds.port == "50000"

    def test_parse_env_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            EnvParser.parse_env_file("/nonexistent/.env")

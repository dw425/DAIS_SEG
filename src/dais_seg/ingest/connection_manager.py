"""Connection manager — creates and manages Lakehouse Federation connections in Databricks.

Translates SourceCredentials into Databricks CREATE CONNECTION / CREATE FOREIGN CATALOG
SQL statements, executes them via the Statement Execution API, and provides
connection lifecycle management (create, test, drop).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from dais_seg.config import get_config, SEGConfig
from dais_seg.ingest.env_parser import SourceCredentials

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Creates and manages Lakehouse Federation connections via the Databricks SDK.

    Handles the full lifecycle:
    1. CREATE CONNECTION from source credentials
    2. CREATE FOREIGN CATALOG using the connection
    3. Test connectivity by querying SHOW SCHEMAS
    4. DROP CONNECTION for cleanup
    """

    def __init__(
        self,
        client: Optional[WorkspaceClient] = None,
        config: Optional[SEGConfig] = None,
    ):
        self.client = client or WorkspaceClient()
        self.config = config or get_config()

    def create_connection(self, creds: SourceCredentials) -> str:
        """Create a Lakehouse Federation connection from source credentials.

        Executes CREATE CONNECTION with the appropriate TYPE and OPTIONS
        for the source database system.

        Returns the connection name.
        """
        errors = creds.validate()
        if errors:
            raise ValueError(f"Invalid credentials: {'; '.join(errors)}")

        connection_name = creds.connection_name
        db_type = creds.databricks_type
        options = self._build_connection_options(creds)

        options_sql = ",\n    ".join(f"{k} '{v}'" for k, v in options.items())

        statement = f"""
            CREATE CONNECTION IF NOT EXISTS `{connection_name}`
            TYPE {db_type}
            OPTIONS (
                {options_sql}
            )
        """

        logger.info(f"Creating connection: {connection_name} (type={db_type})")
        self._execute_sql(statement)
        logger.info(f"Connection created: {connection_name}")
        return connection_name

    def create_foreign_catalog(
        self,
        connection_name: str,
        database_name: str,
        catalog_alias: Optional[str] = None,
    ) -> str:
        """Create a foreign catalog in Unity Catalog for a federation connection.

        Args:
            connection_name: Name of the connection created by create_connection().
            database_name: Name of the database/catalog on the source system.
            catalog_alias: Optional alias for the foreign catalog. Defaults to seg_fed_{connection_name}.

        Returns the foreign catalog name in Unity Catalog.
        """
        catalog_name = catalog_alias or f"seg_fed_{connection_name}"

        statement = f"""
            CREATE FOREIGN CATALOG IF NOT EXISTS `{catalog_name}`
            USING CONNECTION `{connection_name}`
            OPTIONS (database '{database_name}')
        """

        logger.info(f"Creating foreign catalog: {catalog_name} -> {connection_name}/{database_name}")
        self._execute_sql(statement)
        logger.info(f"Foreign catalog created: {catalog_name}")
        return catalog_name

    def test_connection(self, foreign_catalog: str) -> dict:
        """Test a federation connection by querying available schemas.

        Returns dict with success status and schema list or error message.
        """
        try:
            result = self._execute_sql_with_result(
                f"SHOW SCHEMAS IN `{foreign_catalog}`"
            )
            schemas = [row[0] for row in result] if result else []
            logger.info(f"Connection test OK: {foreign_catalog} ({len(schemas)} schemas)")
            return {
                "success": True,
                "foreign_catalog": foreign_catalog,
                "schemas": schemas,
                "schema_count": len(schemas),
            }
        except Exception as e:
            logger.error(f"Connection test failed for {foreign_catalog}: {e}")
            return {
                "success": False,
                "foreign_catalog": foreign_catalog,
                "error": str(e),
            }

    def drop_connection(self, connection_name: str) -> None:
        """Drop a federation connection."""
        logger.info(f"Dropping connection: {connection_name}")
        self._execute_sql(f"DROP CONNECTION IF EXISTS `{connection_name}`")

    def drop_foreign_catalog(self, catalog_name: str) -> None:
        """Drop a foreign catalog."""
        logger.info(f"Dropping foreign catalog: {catalog_name}")
        self._execute_sql(f"DROP CATALOG IF EXISTS `{catalog_name}`")

    def list_connections(self) -> list[dict]:
        """List all available connections."""
        try:
            result = self._execute_sql_with_result("SHOW CONNECTIONS")
            return [{"name": row[0], "type": row[1] if len(row) > 1 else ""} for row in result] if result else []
        except Exception:
            return []

    def setup_source(self, creds: SourceCredentials) -> dict:
        """Full setup: create connection + foreign catalog + test.

        This is the main entry point for ingesting a new source system.
        Returns a result dict with connection details and test results.
        """
        # Step 1: Create connection
        connection_name = self.create_connection(creds)

        # Step 2: Create foreign catalog
        foreign_catalog = self.create_foreign_catalog(
            connection_name, creds.database
        )

        # Step 3: Test the connection
        test_result = self.test_connection(foreign_catalog)

        return {
            "connection_name": connection_name,
            "foreign_catalog": foreign_catalog,
            "source_type": creds.databricks_type,
            "test_result": test_result,
        }

    def _build_connection_options(self, creds: SourceCredentials) -> dict[str, str]:
        """Build connection OPTIONS dict based on source type."""
        options: dict[str, str] = {
            "host": creds.host,
            "port": creds.port,
            "user": creds.user,
            "password": creds.password,
        }

        source = creds.source_type

        if source == "snowflake":
            options["sfWarehouse"] = creds.warehouse or "COMPUTE_WH"
            if creds.role:
                options["sfRole"] = creds.role
            if creds.account:
                options["account"] = creds.account
            if creds.database:
                options["database"] = creds.database
        elif source == "oracle":
            options["database"] = creds.database
        elif source in ("sqlserver", "mssql"):
            options["database"] = creds.database
        elif source in ("postgresql", "postgres"):
            options["database"] = creds.database
        elif source == "mysql":
            options["database"] = creds.database
        elif source == "teradata":
            options["database"] = creds.database
        elif source == "db2":
            options["database"] = creds.database
        else:
            if creds.database:
                options["database"] = creds.database

        # Add any extra options from the .env
        for k, v in creds.extra_options.items():
            if k not in options:
                options[k] = v

        return options

    def _execute_sql(self, statement: str) -> None:
        """Execute a SQL statement via the Databricks Statement Execution API."""
        warehouse_id = self.config.warehouse_id
        if not warehouse_id:
            raise ValueError(
                "SEG_WAREHOUSE_ID is required for SQL execution. "
                "Set it in your .env or app configuration."
            )

        response = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            wait_timeout="60s",
        )

        if response.status and response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown SQL error"
            raise RuntimeError(f"SQL execution failed: {error_msg}")

    def _execute_sql_with_result(self, statement: str) -> list[list]:
        """Execute a SQL statement and return result rows."""
        warehouse_id = self.config.warehouse_id
        if not warehouse_id:
            raise ValueError("SEG_WAREHOUSE_ID is required")

        response = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            wait_timeout="60s",
        )

        if response.status and response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown SQL error"
            raise RuntimeError(f"SQL query failed: {error_msg}")

        if response.result and response.result.data_array:
            return response.result.data_array
        return []

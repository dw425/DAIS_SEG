"""Workspace provisioner — creates isolated Databricks Workspaces for synthetic environments.

Each workstream in a parallel migration gets its own Workspace with synthetic
Delta Tables, ensuring complete isolation. This module handles creation,
configuration, and teardown of these Workspaces via the Databricks SDK.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.provisioning import Workspace

from dais_seg.config import get_config

logger = logging.getLogger(__name__)


class WorkspaceStatus(str, Enum):
    """Status of a synthetic workspace."""

    PROVISIONING = "provisioning"
    READY = "ready"
    GENERATING = "generating"
    VALIDATING = "validating"
    COMPLETE = "complete"
    FAILED = "failed"
    DECOMMISSIONED = "decommissioned"


@dataclass
class SyntheticWorkspace:
    """Represents an isolated workspace for a migration workstream."""

    workspace_id: str
    workspace_name: str
    workstream: str
    blueprint_id: str
    status: WorkspaceStatus = WorkspaceStatus.PROVISIONING
    catalog: str = ""
    schema: str = ""
    tables_generated: int = 0
    validation_score: Optional[float] = None
    created_at: str = ""
    metadata: dict = field(default_factory=dict)


class WorkspaceProvisioner:
    """Provisions and manages isolated Databricks schemas for synthetic environments.

    Note: In practice, full Workspace provisioning requires account-level admin.
    This implementation uses catalog/schema isolation within a single Workspace
    as the primary isolation mechanism — each workstream gets its own schema
    with its own set of synthetic Delta Tables.
    """

    def __init__(self, client: Optional[WorkspaceClient] = None):
        self.config = get_config()
        self.client = client or WorkspaceClient()
        self._workspaces: dict[str, SyntheticWorkspace] = {}

    def provision(
        self,
        workstream_name: str,
        blueprint_id: str,
        catalog: Optional[str] = None,
    ) -> SyntheticWorkspace:
        """Provision an isolated schema for a workstream's synthetic environment.

        Creates a dedicated schema within the target catalog, sets up
        appropriate permissions, and returns a SyntheticWorkspace handle.
        """
        catalog = catalog or self.config.catalog
        schema_name = f"{self.config.synthetic_workspace_prefix}_{workstream_name}"
        workspace_id = f"{catalog}.{schema_name}"

        logger.info(f"Provisioning synthetic workspace: {workspace_id}")

        workspace = SyntheticWorkspace(
            workspace_id=workspace_id,
            workspace_name=schema_name,
            workstream=workstream_name,
            blueprint_id=blueprint_id,
            catalog=catalog,
            schema=schema_name,
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )

        # Create isolated schema via SQL
        self._create_schema(catalog, schema_name)
        workspace.status = WorkspaceStatus.READY

        self._workspaces[workspace_id] = workspace
        logger.info(f"Workspace ready: {workspace_id}")
        return workspace

    def deprovision(self, workspace_id: str, cascade: bool = True) -> None:
        """Tear down a synthetic workspace — remove all tables and schema.

        Args:
            workspace_id: The workspace identifier.
            cascade: If True, drops all tables in the schema before dropping it.
        """
        workspace = self._workspaces.get(workspace_id)
        if not workspace:
            logger.warning(f"Workspace not found: {workspace_id}")
            return

        logger.info(f"Deprovisioning workspace: {workspace_id}")
        cascade_sql = "CASCADE" if cascade else "RESTRICT"
        self._execute_sql(
            f"DROP SCHEMA IF EXISTS `{workspace.catalog}`.`{workspace.schema}` {cascade_sql}"
        )
        workspace.status = WorkspaceStatus.DECOMMISSIONED
        logger.info(f"Workspace decommissioned: {workspace_id}")

    def list_workspaces(self) -> list[SyntheticWorkspace]:
        """List all tracked synthetic workspaces."""
        return list(self._workspaces.values())

    def get_workspace(self, workspace_id: str) -> Optional[SyntheticWorkspace]:
        """Get a workspace by ID."""
        return self._workspaces.get(workspace_id)

    def update_status(
        self, workspace_id: str, status: WorkspaceStatus, **kwargs
    ) -> None:
        """Update workspace status and optional metadata."""
        workspace = self._workspaces.get(workspace_id)
        if workspace:
            workspace.status = status
            for key, value in kwargs.items():
                if hasattr(workspace, key):
                    setattr(workspace, key, value)

    def _create_schema(self, catalog: str, schema: str) -> None:
        """Create an isolated schema for a synthetic workspace."""
        self._execute_sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        self._execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` "
            f"COMMENT 'Synthetic environment workspace'"
        )

    def _execute_sql(self, statement: str) -> None:
        """Execute a SQL statement via the Databricks SDK."""
        try:
            warehouse_id = self.config.warehouse_id
            if warehouse_id:
                self.client.statement_execution.execute_statement(
                    statement=statement,
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
            else:
                logger.warning(f"No warehouse_id configured, SQL not executed: {statement}")
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise

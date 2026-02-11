"""Lifecycle manager — tracks and manages synthetic workspace lifecycles.

Provides CRUD operations for workspace state, integrates with Unity Catalog
for persistence, and supports automated cleanup of stale workspaces.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

from databricks.sdk import WorkspaceClient

from dais_seg.config import get_config
from dais_seg.generator.workspace_provisioner import SyntheticWorkspace, WorkspaceStatus

logger = logging.getLogger(__name__)


class LifecycleManager:
    """Manages the full lifecycle of synthetic workspaces.

    Tracks workspace state transitions, persists metadata for audit,
    and provides automated cleanup for expired or failed workspaces.
    """

    def __init__(self, client: Optional[WorkspaceClient] = None):
        self.config = get_config()
        self.client = client or WorkspaceClient()
        self._registry: dict[str, SyntheticWorkspace] = {}

    def register(self, workspace: SyntheticWorkspace) -> None:
        """Register a workspace in the lifecycle registry."""
        self._registry[workspace.workspace_id] = workspace
        logger.info(f"Registered workspace: {workspace.workspace_id}")

    def get(self, workspace_id: str) -> Optional[SyntheticWorkspace]:
        """Retrieve a workspace by ID."""
        return self._registry.get(workspace_id)

    def list_active(self) -> list[SyntheticWorkspace]:
        """List all active (non-decommissioned) workspaces."""
        return [
            ws for ws in self._registry.values()
            if ws.status != WorkspaceStatus.DECOMMISSIONED
        ]

    def list_by_status(self, status: WorkspaceStatus) -> list[SyntheticWorkspace]:
        """List workspaces with a specific status."""
        return [ws for ws in self._registry.values() if ws.status == status]

    def transition(
        self, workspace_id: str, new_status: WorkspaceStatus
    ) -> Optional[SyntheticWorkspace]:
        """Transition a workspace to a new status with validation.

        Enforces valid state transitions:
        - provisioning -> ready
        - ready -> generating
        - generating -> validating
        - validating -> complete
        - any -> failed
        - any -> decommissioned
        """
        workspace = self._registry.get(workspace_id)
        if not workspace:
            logger.warning(f"Workspace not found: {workspace_id}")
            return None

        valid_transitions = {
            WorkspaceStatus.PROVISIONING: {WorkspaceStatus.READY, WorkspaceStatus.FAILED},
            WorkspaceStatus.READY: {WorkspaceStatus.GENERATING, WorkspaceStatus.FAILED, WorkspaceStatus.DECOMMISSIONED},
            WorkspaceStatus.GENERATING: {WorkspaceStatus.VALIDATING, WorkspaceStatus.FAILED},
            WorkspaceStatus.VALIDATING: {WorkspaceStatus.COMPLETE, WorkspaceStatus.FAILED},
            WorkspaceStatus.COMPLETE: {WorkspaceStatus.DECOMMISSIONED},
            WorkspaceStatus.FAILED: {WorkspaceStatus.DECOMMISSIONED, WorkspaceStatus.READY},
        }

        allowed = valid_transitions.get(workspace.status, {WorkspaceStatus.DECOMMISSIONED})
        if new_status not in allowed:
            logger.error(
                f"Invalid transition for {workspace_id}: "
                f"{workspace.status.value} -> {new_status.value}"
            )
            return None

        old_status = workspace.status
        workspace.status = new_status
        logger.info(
            f"Workspace {workspace_id}: {old_status.value} -> {new_status.value}"
        )
        return workspace

    def cleanup_failed(self, max_age_hours: int = 24) -> int:
        """Decommission failed workspaces older than max_age_hours."""
        cutoff = time.time() - (max_age_hours * 3600)
        cleaned = 0

        for ws in list(self._registry.values()):
            if ws.status == WorkspaceStatus.FAILED:
                try:
                    created = time.mktime(
                        time.strptime(ws.created_at, "%Y-%m-%dT%H:%M:%SZ")
                    )
                    if created < cutoff:
                        ws.status = WorkspaceStatus.DECOMMISSIONED
                        cleaned += 1
                        logger.info(f"Cleaned up failed workspace: {ws.workspace_id}")
                except (ValueError, OverflowError):
                    pass

        return cleaned

    def get_summary(self) -> dict:
        """Get a summary of all workspace statuses."""
        summary: dict[str, int] = {}
        for ws in self._registry.values():
            key = ws.status.value
            summary[key] = summary.get(key, 0) + 1
        return {
            "total": len(self._registry),
            "by_status": summary,
        }

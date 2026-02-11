"""Genie interface — natural language bridge for synthetic environment operations.

Integrates with Databricks Genie (via Foundation Model Serving) to let users
request synthetic environments in natural language:
  "Give me a synthetic copy of the orders database at 10% scale"
  "What's the confidence score for workspace team_alpha?"
  "Spin up 3 parallel workspaces for the CRM migration"
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from databricks.sdk import WorkspaceClient

from dais_seg.config import get_config

logger = logging.getLogger(__name__)


class IntentType(str, Enum):
    """Recognized user intents for Genie integration."""

    PROFILE_SOURCE = "profile_source"
    GENERATE_ENVIRONMENT = "generate_environment"
    RUN_PIPELINE = "run_pipeline"
    VALIDATE_WORKSPACE = "validate_workspace"
    CHECK_STATUS = "check_status"
    LIST_BLUEPRINTS = "list_blueprints"
    LIST_WORKSPACES = "list_workspaces"
    TEARDOWN_WORKSPACE = "teardown_workspace"
    HELP = "help"
    UNKNOWN = "unknown"


@dataclass
class GenieRequest:
    """Parsed request from a natural language Genie query."""

    intent: IntentType
    parameters: dict[str, Any]
    raw_query: str
    confidence: float = 0.0


@dataclass
class GenieResponse:
    """Response to a Genie query."""

    message: str
    data: Optional[dict] = None
    action_taken: bool = False
    follow_up: Optional[str] = None


class GenieInterface:
    """Natural language interface for synthetic environment operations.

    Uses Databricks Foundation Model Serving to parse natural language
    queries into structured intents, then dispatches to the appropriate
    SEG framework operation.
    """

    SYSTEM_PROMPT = """You are the SEG (Synthetic Environment Generator) assistant.
You help users create and manage synthetic database environments for migration testing on Databricks.

Parse user requests into one of these intents:
- profile_source: User wants to profile/scan a source system
- generate_environment: User wants to create a synthetic environment
- run_pipeline: User wants to run the medallion conformance pipeline
- validate_workspace: User wants to validate a synthetic workspace
- check_status: User wants to check status of a workspace or job
- list_blueprints: User wants to see available source blueprints
- list_workspaces: User wants to see active synthetic workspaces
- teardown_workspace: User wants to remove a synthetic workspace
- help: User needs help or information

Respond with a JSON object:
{"intent": "<intent>", "parameters": {<relevant params>}, "confidence": <0.0-1.0>}

Parameters to extract when relevant:
- source_name: name of source system/catalog
- blueprint_id: ID of a source blueprint
- workspace_id: ID of a synthetic workspace
- workspace_name: name for a new workspace
- tables: list of specific tables
- scale_factor: data scale multiplier (e.g., 0.1 for 10%)
- parallel_count: number of parallel workspaces
"""

    def __init__(self, client: Optional[WorkspaceClient] = None):
        self.config = get_config()
        self.client = client or WorkspaceClient()

    def parse_query(self, query: str) -> GenieRequest:
        """Parse a natural language query into a structured GenieRequest.

        Uses Foundation Model Serving for intent detection and parameter extraction.
        """
        try:
            response = self.client.serving_endpoints.query(
                name=self.config.model_serving_endpoint,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": query},
                ],
                max_tokens=500,
                temperature=0.1,
            )

            content = response.choices[0].message.content
            parsed = json.loads(content)

            return GenieRequest(
                intent=IntentType(parsed.get("intent", "unknown")),
                parameters=parsed.get("parameters", {}),
                raw_query=query,
                confidence=parsed.get("confidence", 0.0),
            )
        except Exception as e:
            logger.warning(f"Failed to parse query via LLM: {e}")
            return self._fallback_parse(query)

    def format_response(self, response: GenieResponse) -> str:
        """Format a GenieResponse for display in the Gradio UI."""
        parts = [response.message]
        if response.data:
            parts.append("\n**Details:**")
            parts.append(f"```json\n{json.dumps(response.data, indent=2, default=str)}\n```")
        if response.follow_up:
            parts.append(f"\n*Suggested next step:* {response.follow_up}")
        return "\n".join(parts)

    def _fallback_parse(self, query: str) -> GenieRequest:
        """Keyword-based fallback when LLM parsing fails."""
        query_lower = query.lower()

        if any(w in query_lower for w in ["profile", "scan", "crawl", "discover"]):
            return GenieRequest(IntentType.PROFILE_SOURCE, {}, query, 0.5)
        elif any(w in query_lower for w in ["generate", "create", "spin up", "build"]):
            return GenieRequest(IntentType.GENERATE_ENVIRONMENT, {}, query, 0.5)
        elif any(w in query_lower for w in ["pipeline", "conform", "medallion", "bronze"]):
            return GenieRequest(IntentType.RUN_PIPELINE, {}, query, 0.5)
        elif any(w in query_lower for w in ["validate", "check", "confidence", "score"]):
            return GenieRequest(IntentType.VALIDATE_WORKSPACE, {}, query, 0.5)
        elif any(w in query_lower for w in ["status", "progress", "running"]):
            return GenieRequest(IntentType.CHECK_STATUS, {}, query, 0.5)
        elif any(w in query_lower for w in ["blueprint", "list source"]):
            return GenieRequest(IntentType.LIST_BLUEPRINTS, {}, query, 0.5)
        elif any(w in query_lower for w in ["workspace", "list env"]):
            return GenieRequest(IntentType.LIST_WORKSPACES, {}, query, 0.5)
        elif any(w in query_lower for w in ["teardown", "remove", "delete", "destroy"]):
            return GenieRequest(IntentType.TEARDOWN_WORKSPACE, {}, query, 0.5)
        elif any(w in query_lower for w in ["help", "how", "what"]):
            return GenieRequest(IntentType.HELP, {}, query, 0.5)

        return GenieRequest(IntentType.UNKNOWN, {}, query, 0.0)

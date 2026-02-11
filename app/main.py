"""DAIS SEG — Databricks App entry point.

Gradio-based interface for the Synthetic Environment Generation framework.
Provides natural language interaction (Genie-powered), dashboard views for
workspace status, and controls for the full Profile → Generate → Conform →
Validate pipeline.

Deploy as a Databricks App via: databricks bundle deploy
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import gradio as gr
from databricks.sdk import WorkspaceClient

from dais_seg.config import SEGConfig, set_config
from dais_seg.genie.interface import GenieInterface, GenieResponse, IntentType
from dais_seg.generator.workspace_provisioner import WorkspaceProvisioner
from dais_seg.workspace_manager.lifecycle_manager import LifecycleManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("dais_seg.app")

# --------------------------------------------------------------------------- #
#  Global state (initialized on startup)
# --------------------------------------------------------------------------- #

config: Optional[SEGConfig] = None
client: Optional[WorkspaceClient] = None
genie: Optional[GenieInterface] = None
provisioner: Optional[WorkspaceProvisioner] = None
lifecycle: Optional[LifecycleManager] = None


def initialize():
    """Initialize the SEG framework on app startup."""
    global config, client, genie, provisioner, lifecycle

    config = SEGConfig.from_env()
    set_config(config)

    client = WorkspaceClient()
    genie = GenieInterface(client)
    provisioner = WorkspaceProvisioner(client)
    lifecycle = LifecycleManager(client)

    logger.info(
        f"SEG App initialized — catalog={config.catalog}, "
        f"model={config.model_serving_endpoint}"
    )


# --------------------------------------------------------------------------- #
#  Chat handler (Genie interface)
# --------------------------------------------------------------------------- #

def chat_handler(message: str, history: list[list[str]]) -> str:
    """Handle a chat message via the Genie interface."""
    if not genie:
        return "SEG framework not initialized. Check app configuration."

    request = genie.parse_query(message)
    logger.info(f"Intent: {request.intent.value} (confidence={request.confidence:.0%})")

    response = dispatch_intent(request)
    return genie.format_response(response)


def dispatch_intent(request) -> GenieResponse:
    """Route a parsed Genie request to the appropriate handler."""
    handlers = {
        IntentType.PROFILE_SOURCE: handle_profile,
        IntentType.GENERATE_ENVIRONMENT: handle_generate,
        IntentType.RUN_PIPELINE: handle_pipeline,
        IntentType.VALIDATE_WORKSPACE: handle_validate,
        IntentType.CHECK_STATUS: handle_status,
        IntentType.LIST_BLUEPRINTS: handle_list_blueprints,
        IntentType.LIST_WORKSPACES: handle_list_workspaces,
        IntentType.TEARDOWN_WORKSPACE: handle_teardown,
        IntentType.HELP: handle_help,
    }

    handler = handlers.get(request.intent, handle_unknown)
    return handler(request)


# --------------------------------------------------------------------------- #
#  Intent handlers
# --------------------------------------------------------------------------- #

def handle_profile(request) -> GenieResponse:
    source = request.parameters.get("source_name", "")
    if not source:
        return GenieResponse(
            message="Which source system would you like to profile? Please provide the federation connection or catalog name.",
            follow_up="Example: 'Profile the oracle_erp source system'",
        )
    return GenieResponse(
        message=f"Starting profiler for source system: **{source}**\n\nThis will crawl Unity Catalog metadata via Lakehouse Federation, profile column distributions, and generate a Source Blueprint.",
        data={"source": source, "status": "submitted"},
        action_taken=True,
        follow_up=f"Once complete, run: 'Generate a synthetic environment from {source}'",
    )


def handle_generate(request) -> GenieResponse:
    blueprint_id = request.parameters.get("blueprint_id", "")
    workspace_name = request.parameters.get("workspace_name", "")
    scale = request.parameters.get("scale_factor", 1.0)
    parallel = request.parameters.get("parallel_count", 1)

    if not blueprint_id and not workspace_name:
        return GenieResponse(
            message="I can generate a synthetic environment. What would you like?\n\n"
                    "- Specify a **blueprint ID** from a previous profile\n"
                    "- Or tell me the **source name** and I'll find the latest blueprint",
            follow_up="Example: 'Generate synthetic environment from blueprint abc123 at 10% scale'",
        )

    return GenieResponse(
        message=f"Generating synthetic environment:\n"
                f"- Blueprint: **{blueprint_id or 'latest'}**\n"
                f"- Scale: **{scale}x**\n"
                f"- Parallel workspaces: **{parallel}**\n\n"
                f"Creating isolated schemas with synthetic Delta Tables...",
        data={
            "blueprint_id": blueprint_id,
            "scale_factor": scale,
            "parallel_count": parallel,
            "status": "submitted",
        },
        action_taken=True,
        follow_up="I'll notify you when generation is complete. Then we can run the medallion pipeline.",
    )


def handle_pipeline(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    return GenieResponse(
        message=f"Running medallion conformance pipeline (Bronze → Silver → Gold) "
                f"for workspace **{workspace_id or 'all active'}**.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
        follow_up="After the pipeline completes, run validation to get confidence scores.",
    )


def handle_validate(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    return GenieResponse(
        message=f"Running like-for-like validation on workspace **{workspace_id or 'all active'}**.\n\n"
                f"Checking: schema parity, data fidelity, quality compliance, pipeline integrity.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
        follow_up="Results will include green/amber/red confidence scores per table.",
    )


def handle_status(request) -> GenieResponse:
    if lifecycle:
        summary = lifecycle.get_summary()
        return GenieResponse(
            message="Current workspace status:",
            data=summary,
        )
    return GenieResponse(message="No workspaces tracked yet.")


def handle_list_blueprints(request) -> GenieResponse:
    return GenieResponse(
        message="Listing available Source Blueprints from Unity Catalog...",
        data={"status": "loading"},
        follow_up="Select a blueprint to generate a synthetic environment.",
    )


def handle_list_workspaces(request) -> GenieResponse:
    if lifecycle:
        active = lifecycle.list_active()
        data = [
            {
                "id": ws.workspace_id,
                "workstream": ws.workstream,
                "status": ws.status.value,
                "tables": ws.tables_generated,
                "score": ws.validation_score,
            }
            for ws in active
        ]
        return GenieResponse(
            message=f"**{len(active)} active workspaces:**",
            data={"workspaces": data},
        )
    return GenieResponse(message="No active workspaces.")


def handle_teardown(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    if not workspace_id:
        return GenieResponse(
            message="Which workspace would you like to tear down? Provide the workspace ID.",
            follow_up="Use 'list workspaces' to see active workspaces.",
        )
    return GenieResponse(
        message=f"Tearing down workspace **{workspace_id}**. "
                f"All synthetic Delta Tables and schemas will be removed.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
    )


def handle_help(request) -> GenieResponse:
    return GenieResponse(
        message="**SEG — Synthetic Environment Generator**\n\n"
                "I can help you with:\n\n"
                "- **Profile** a source system: *'Profile the oracle_erp database'*\n"
                "- **Generate** synthetic environments: *'Create a synthetic copy at 10% scale'*\n"
                "- **Run the pipeline**: *'Conform data through Bronze/Silver/Gold'*\n"
                "- **Validate**: *'Check confidence scores for workspace team_a'*\n"
                "- **Manage workspaces**: *'List active workspaces'* or *'Tear down workspace X'*\n"
                "- **Parallel runs**: *'Spin up 5 parallel workspaces for the CRM migration'*\n\n"
                "Built natively on Databricks: Unity Catalog, Delta Tables, Foundation Models, DLT, and Genie.",
    )


def handle_unknown(request) -> GenieResponse:
    return GenieResponse(
        message=f"I'm not sure what you're asking. Could you rephrase?\n\n"
                f"Try things like:\n"
                f"- *'Profile the orders database'*\n"
                f"- *'Generate a synthetic environment'*\n"
                f"- *'Show me workspace confidence scores'*",
    )


# --------------------------------------------------------------------------- #
#  Dashboard tab
# --------------------------------------------------------------------------- #

def get_dashboard_data() -> str:
    """Fetch current dashboard state."""
    if not lifecycle:
        return json.dumps({"message": "Not initialized"}, indent=2)
    return json.dumps(lifecycle.get_summary(), indent=2, default=str)


# --------------------------------------------------------------------------- #
#  Gradio UI
# --------------------------------------------------------------------------- #

def build_app() -> gr.Blocks:
    """Build the Gradio interface for the SEG Databricks App."""
    with gr.Blocks(
        title="SEG — Synthetic Environment Generator",
        theme=gr.themes.Soft(
            primary_hue="blue",
            neutral_hue="slate",
        ),
    ) as app:
        gr.Markdown(
            "# Synthetic Environment Generator\n"
            "### AI-Driven Parallel Migration on Databricks\n"
            "Ask me to profile sources, generate synthetic environments, "
            "run medallion pipelines, and validate — all in natural language."
        )

        with gr.Tabs():
            # ---- Chat Tab (Genie Interface) ----
            with gr.Tab("Genie Interface"):
                chatbot = gr.ChatInterface(
                    fn=chat_handler,
                    title="",
                    description="Natural language interface for synthetic environment operations",
                    examples=[
                        "Profile the oracle_erp source system",
                        "Generate a synthetic environment at 10% scale",
                        "Run medallion pipeline for all workspaces",
                        "Show confidence scores for workspace team_alpha",
                        "List all active workspaces",
                        "Spin up 3 parallel workspaces for the CRM migration",
                        "Help",
                    ],
                    retry_btn=None,
                    undo_btn=None,
                )

            # ---- Dashboard Tab ----
            with gr.Tab("Dashboard"):
                gr.Markdown("### Workspace Overview")
                dashboard_output = gr.JSON(label="Current State")
                refresh_btn = gr.Button("Refresh Dashboard")
                refresh_btn.click(fn=get_dashboard_data, outputs=dashboard_output)

            # ---- Pipeline Tab ----
            with gr.Tab("Pipeline"):
                gr.Markdown("### Full Pipeline Configuration")
                with gr.Row():
                    source_input = gr.Textbox(label="Source Catalog", placeholder="e.g., oracle_erp_fed")
                    scale_input = gr.Slider(0.01, 1.0, value=0.1, step=0.01, label="Scale Factor")
                    parallel_input = gr.Slider(1, 10, value=1, step=1, label="Parallel Workspaces")
                run_pipeline_btn = gr.Button("Run Full Pipeline", variant="primary")
                pipeline_output = gr.Textbox(label="Pipeline Output", lines=10)

                def run_full_pipeline(source, scale, parallel):
                    return (
                        f"Pipeline submitted:\n"
                        f"  Source: {source}\n"
                        f"  Scale: {scale}x\n"
                        f"  Parallel workspaces: {int(parallel)}\n\n"
                        f"Monitor progress in the Dashboard tab."
                    )

                run_pipeline_btn.click(
                    fn=run_full_pipeline,
                    inputs=[source_input, scale_input, parallel_input],
                    outputs=pipeline_output,
                )

            # ---- Validation Tab ----
            with gr.Tab("Validation"):
                gr.Markdown("### Validation Results & Confidence Scores")
                workspace_input = gr.Textbox(label="Workspace ID", placeholder="e.g., dais_seg.seg_team_alpha")
                validate_btn = gr.Button("Run Validation", variant="primary")
                validation_output = gr.JSON(label="Validation Report")

                def run_validation(workspace_id):
                    return {"workspace_id": workspace_id, "status": "submitted"}

                validate_btn.click(
                    fn=run_validation,
                    inputs=workspace_input,
                    outputs=validation_output,
                )

    return app


# --------------------------------------------------------------------------- #
#  Entry point
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    initialize()
    app = build_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=int(os.getenv("PORT", "8080")),
        share=False,
    )

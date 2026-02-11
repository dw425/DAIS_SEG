"""DAIS SEG — Databricks App entry point (Streamlit).

Streamlit-based interface for the Synthetic Environment Generation framework.
Provides natural language interaction (Genie-powered), dashboard views for
workspace status, and controls for the full Profile → Generate → Conform →
Validate pipeline.

Deploy as a Databricks App via: databricks bundle deploy
Run locally: streamlit run app/main.py
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import streamlit as st
from databricks.sdk import WorkspaceClient

from dais_seg.config import SEGConfig, set_config
from dais_seg.genie.interface import GenieInterface, GenieResponse, IntentType
from dais_seg.generator.workspace_provisioner import WorkspaceProvisioner
from dais_seg.workspace_manager.lifecycle_manager import LifecycleManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("dais_seg.app")

# --------------------------------------------------------------------------- #
#  Page config
# --------------------------------------------------------------------------- #

st.set_page_config(
    page_title="SEG — Synthetic Environment Generator",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------- #
#  Initialize framework (cached across reruns)
# --------------------------------------------------------------------------- #


@st.cache_resource
def initialize():
    """Initialize the SEG framework on app startup."""
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
    return config, client, genie, provisioner, lifecycle


config, client, genie, provisioner, lifecycle = initialize()


# --------------------------------------------------------------------------- #
#  Intent dispatch
# --------------------------------------------------------------------------- #

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
            message="I can generate a synthetic environment. What would you like?\n\n- Specify a **blueprint ID** from a previous profile\n- Or tell me the **source name** and I'll find the latest blueprint",
            follow_up="Example: 'Generate synthetic environment from blueprint abc123 at 10% scale'",
        )
    return GenieResponse(
        message=f"Generating synthetic environment:\n- Blueprint: **{blueprint_id or 'latest'}**\n- Scale: **{scale}x**\n- Parallel workspaces: **{parallel}**\n\nCreating isolated schemas with synthetic Delta Tables...",
        data={"blueprint_id": blueprint_id, "scale_factor": scale, "parallel_count": parallel, "status": "submitted"},
        action_taken=True,
        follow_up="I'll notify you when generation is complete. Then we can run the medallion pipeline.",
    )


def handle_pipeline(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    return GenieResponse(
        message=f"Running medallion conformance pipeline (Bronze → Silver → Gold) for workspace **{workspace_id or 'all active'}**.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
        follow_up="After the pipeline completes, run validation to get confidence scores.",
    )


def handle_validate(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    return GenieResponse(
        message=f"Running like-for-like validation on workspace **{workspace_id or 'all active'}**.\n\nChecking: schema parity, data fidelity, quality compliance, pipeline integrity.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
        follow_up="Results will include green/amber/red confidence scores per table.",
    )


def handle_status(request) -> GenieResponse:
    summary = lifecycle.get_summary()
    return GenieResponse(message="Current workspace status:", data=summary)


def handle_list_blueprints(request) -> GenieResponse:
    return GenieResponse(
        message="Listing available Source Blueprints from Unity Catalog...",
        data={"status": "loading"},
        follow_up="Select a blueprint to generate a synthetic environment.",
    )


def handle_list_workspaces(request) -> GenieResponse:
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


def handle_teardown(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    if not workspace_id:
        return GenieResponse(
            message="Which workspace would you like to tear down? Provide the workspace ID.",
            follow_up="Use 'list workspaces' to see active workspaces.",
        )
    return GenieResponse(
        message=f"Tearing down workspace **{workspace_id}**. All synthetic Delta Tables and schemas will be removed.",
        data={"workspace_id": workspace_id, "status": "submitted"},
        action_taken=True,
    )


def handle_help(request) -> GenieResponse:
    return GenieResponse(
        message=(
            "**SEG — Synthetic Environment Generator**\n\n"
            "I can help you with:\n\n"
            "- **Profile** a source system: *'Profile the oracle_erp database'*\n"
            "- **Generate** synthetic environments: *'Create a synthetic copy at 10% scale'*\n"
            "- **Run the pipeline**: *'Conform data through Bronze/Silver/Gold'*\n"
            "- **Validate**: *'Check confidence scores for workspace team_a'*\n"
            "- **Manage workspaces**: *'List active workspaces'* or *'Tear down workspace X'*\n"
            "- **Parallel runs**: *'Spin up 5 parallel workspaces for the CRM migration'*\n\n"
            "Built natively on Databricks: Unity Catalog, Delta Tables, Foundation Models, DLT, and Genie."
        ),
    )


def handle_unknown(request) -> GenieResponse:
    return GenieResponse(
        message=(
            "I'm not sure what you're asking. Could you rephrase?\n\n"
            "Try things like:\n"
            "- *'Profile the orders database'*\n"
            "- *'Generate a synthetic environment'*\n"
            "- *'Show me workspace confidence scores'*"
        ),
    )


# --------------------------------------------------------------------------- #
#  Sidebar
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.image("https://databricks.com/wp-content/uploads/2023/07/databricks-logo.png", width=180)
    st.markdown("---")
    st.markdown("### Configuration")
    st.text_input("Catalog", value=config.catalog, key="sidebar_catalog", disabled=True)
    st.text_input("Model Endpoint", value=config.model_serving_endpoint, key="sidebar_model", disabled=True)
    st.markdown("---")
    st.markdown(
        "**SEG** — Synthetic Environment Generator  \n"
        "Data + AI Summit 2026 | Blueprint"
    )


# --------------------------------------------------------------------------- #
#  Tabs
# --------------------------------------------------------------------------- #

tab_genie, tab_dashboard, tab_pipeline, tab_validation = st.tabs(
    ["Genie Interface", "Dashboard", "Pipeline", "Validation"]
)

# ---- Tab 1: Genie Interface (Chat) ---- #
with tab_genie:
    st.header("Genie Interface")
    st.caption("Natural language interface for synthetic environment operations")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input
    if prompt := st.chat_input("Ask me to profile, generate, validate, or manage environments..."):
        # Show user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Parse and dispatch
        request = genie.parse_query(prompt)
        response = dispatch_intent(request)

        # Build assistant response
        reply_parts = [response.message]
        if response.data:
            reply_parts.append(f"\n```json\n{json.dumps(response.data, indent=2, default=str)}\n```")
        if response.follow_up:
            reply_parts.append(f"\n> **Next step:** {response.follow_up}")
        reply = "\n".join(reply_parts)

        st.session_state.messages.append({"role": "assistant", "content": reply})
        with st.chat_message("assistant"):
            st.markdown(reply)

    # Example prompts
    with st.expander("Example prompts"):
        examples = [
            "Profile the oracle_erp source system",
            "Generate a synthetic environment at 10% scale",
            "Run medallion pipeline for all workspaces",
            "Show confidence scores for workspace team_alpha",
            "List all active workspaces",
            "Spin up 3 parallel workspaces for the CRM migration",
            "Help",
        ]
        for ex in examples:
            st.code(ex, language=None)


# ---- Tab 2: Dashboard ---- #
with tab_dashboard:
    st.header("Workspace Dashboard")

    col1, col2, col3 = st.columns(3)
    summary = lifecycle.get_summary()
    by_status = summary.get("by_status", {})

    col1.metric("Total Workspaces", summary.get("total", 0))
    col2.metric("Active", by_status.get("ready", 0) + by_status.get("generating", 0) + by_status.get("validating", 0))
    col3.metric("Complete", by_status.get("complete", 0))

    st.markdown("---")

    if st.button("Refresh", key="refresh_dashboard"):
        st.rerun()

    active = lifecycle.list_active()
    if active:
        ws_data = [
            {
                "Workspace ID": ws.workspace_id,
                "Workstream": ws.workstream,
                "Status": ws.status.value,
                "Tables": ws.tables_generated,
                "Confidence": f"{ws.validation_score:.1%}" if ws.validation_score else "—",
            }
            for ws in active
        ]
        st.dataframe(ws_data, use_container_width=True)
    else:
        st.info("No active workspaces. Use the Genie Interface or Pipeline tab to create one.")


# ---- Tab 3: Pipeline ---- #
with tab_pipeline:
    st.header("Full Pipeline Configuration")
    st.markdown("Configure and launch the end-to-end SEG pipeline: **Profile → Generate → Conform → Validate**")

    with st.form("pipeline_form"):
        col1, col2 = st.columns(2)
        with col1:
            source_catalog = st.text_input("Source Catalog", placeholder="e.g., oracle_erp_fed")
            blueprint_id = st.text_input("Blueprint ID", placeholder="Leave blank to auto-profile")
        with col2:
            scale_factor = st.slider("Scale Factor", 0.01, 1.0, 0.1, 0.01, help="Multiply source row counts by this factor")
            parallel_count = st.slider("Parallel Workspaces", 1, 10, 1, 1, help="Number of isolated workspaces to spin up")

        workstream_names = st.text_input(
            "Workstream Names (comma-separated)",
            value="team_alpha",
            help="Each workstream gets its own isolated synthetic workspace",
        )

        submitted = st.form_submit_button("Run Full Pipeline", type="primary")

        if submitted:
            if not source_catalog and not blueprint_id:
                st.error("Provide either a Source Catalog or Blueprint ID.")
            else:
                st.success(
                    f"Pipeline submitted:\n\n"
                    f"- **Source**: {source_catalog or 'from blueprint'}\n"
                    f"- **Blueprint**: {blueprint_id or 'auto-profile'}\n"
                    f"- **Scale**: {scale_factor}x\n"
                    f"- **Workspaces**: {parallel_count}\n"
                    f"- **Workstreams**: {workstream_names}\n\n"
                    f"Monitor progress in the **Dashboard** tab."
                )


# ---- Tab 4: Validation ---- #
with tab_validation:
    st.header("Validation & Confidence Scores")
    st.markdown("Run like-for-like validation and review green/amber/red confidence scores.")

    with st.form("validation_form"):
        val_workspace = st.text_input("Workspace ID", placeholder="e.g., dais_seg.seg_team_alpha")
        val_blueprint = st.text_input("Blueprint ID", placeholder="Required for validation")
        val_submitted = st.form_submit_button("Run Validation", type="primary")

        if val_submitted:
            if not val_blueprint:
                st.error("Blueprint ID is required for validation.")
            else:
                st.info(f"Validation submitted for workspace **{val_workspace or 'all active'}** against blueprint **{val_blueprint}**")

    st.markdown("---")
    st.subheader("Scoring Dimensions")

    score_cols = st.columns(4)
    score_cols[0].markdown("**Schema Parity**\n\nColumns, types, constraints\n\n*Weight: 25%*")
    score_cols[1].markdown("**Data Fidelity**\n\nDistributions, nulls, cardinality\n\n*Weight: 35%*")
    score_cols[2].markdown("**Quality Compliance**\n\nDLT expectations pass rate\n\n*Weight: 20%*")
    score_cols[3].markdown("**Pipeline Integrity**\n\nMedallion conformance\n\n*Weight: 20%*")

    st.markdown("---")
    st.markdown(
        "| Level | Threshold | Meaning |\n"
        "|-------|-----------|--------|\n"
        "| 🟢 Green | ≥ 90% | Ready for production cutover |\n"
        "| 🟡 Amber | ≥ 70% | Needs attention before cutover |\n"
        "| 🔴 Red | < 70% | Not ready — review recommendations |"
    )

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
from dais_seg.ingest import EnvParser, SourceCredentials, ConnectionManager
from dais_seg.pipeline import PipelineRunner, PipelineResult, PipelineStatus
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
    conn_mgr = ConnectionManager(client, config)
    pipeline = PipelineRunner(client, config)
    lifecycle = LifecycleManager(client)

    logger.info(
        f"SEG App initialized — catalog={config.catalog}, "
        f"model={config.model_serving_endpoint}"
    )
    return config, client, genie, conn_mgr, pipeline, lifecycle


config, client, genie, conn_mgr, pipeline_runner, lifecycle = initialize()


# --------------------------------------------------------------------------- #
#  Session state defaults
# --------------------------------------------------------------------------- #

if "messages" not in st.session_state:
    st.session_state.messages = []
if "credentials" not in st.session_state:
    st.session_state.credentials = None
if "pipeline_results" not in st.session_state:
    st.session_state.pipeline_results = []


# --------------------------------------------------------------------------- #
#  Sidebar: credentials input
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.markdown("### Source Credentials")

    # File upload
    uploaded_env = st.file_uploader(
        "Upload .env file",
        type=["env", "txt"],
        help="Upload a .env file with SOURCE_TYPE, SOURCE_HOST, etc.",
    )
    if uploaded_env:
        content = uploaded_env.read().decode("utf-8")
        try:
            creds = EnvParser.parse_env_string(content)
            errors = creds.validate()
            if errors:
                st.error(f"Validation errors: {', '.join(errors)}")
            else:
                st.session_state.credentials = creds
                st.success(f"Loaded: {creds.source_type.upper()} @ {creds.host}")
        except Exception as e:
            st.error(f"Parse error: {e}")

    st.markdown("**— or enter manually —**")

    with st.expander("Manual credentials", expanded=not bool(st.session_state.credentials)):
        source_type = st.selectbox(
            "Source Type",
            ["oracle", "sqlserver", "postgresql", "mysql", "snowflake", "teradata", "db2"],
        )
        host = st.text_input("Host", placeholder="oracle.prod.example.com")
        port = st.text_input("Port", placeholder="Auto-detected from type")
        database = st.text_input("Database", placeholder="PROD")
        user = st.text_input("User", placeholder="migration_user")
        password = st.text_input("Password", type="password")
        conn_name = st.text_input("Connection Name", placeholder="Auto-generated")

        if st.button("Set Credentials"):
            creds = SourceCredentials(
                source_type=source_type,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connection_name=conn_name,
            )
            errors = creds.validate()
            if errors:
                st.error(f"Validation: {', '.join(errors)}")
            else:
                st.session_state.credentials = creds
                st.success(f"Set: {creds.source_type.upper()} @ {creds.host}")

    st.markdown("---")

    # Show current credentials status
    creds = st.session_state.credentials
    if creds:
        st.markdown(f"**Active source:** {creds.source_type.upper()}")
        st.markdown(f"**Host:** {creds.host}:{creds.port}")
        st.markdown(f"**Database:** {creds.database}")
        st.markdown(f"**Connection:** {creds.connection_name}")
    else:
        st.info("No source credentials loaded. Upload a .env file or enter manually.")

    st.markdown("---")
    st.markdown("### Framework Config")
    st.text_input("Catalog", value=config.catalog, disabled=True)
    st.text_input("Cluster ID", value=config.cluster_id or "not set", disabled=True)
    st.markdown(
        "**SEG** — Synthetic Environment Generator  \n"
        "Data + AI Summit 2026 | Blueprint"
    )


# --------------------------------------------------------------------------- #
#  Intent dispatch (connected to real execution)
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
#  Intent handlers — wired to real execution
# --------------------------------------------------------------------------- #

def handle_profile(request) -> GenieResponse:
    creds = st.session_state.credentials
    if not creds:
        return GenieResponse(
            message="No source credentials loaded. Please upload a .env file or enter credentials in the sidebar first.",
        )
    try:
        setup = conn_mgr.setup_source(creds)
        test = setup["test_result"]
        if test["success"]:
            # Submit profiling notebook
            result = pipeline_runner.run_single_stage("profile", {
                "source_catalog": setup["foreign_catalog"],
                "catalog": config.catalog,
                "schema": config.schema,
            })
            if result.status == "success":
                return GenieResponse(
                    message=f"Profiling complete for **{creds.host}** ({creds.source_type.upper()}).\n\nBlueprint ID: `{result.output.get('blueprint_id', 'N/A')}`",
                    data={"connection": setup, "profile_result": result.output},
                    action_taken=True,
                    follow_up="Now run: 'Generate a synthetic environment'",
                )
            else:
                return GenieResponse(message=f"Profiling failed: {result.error}", data={"error": result.error})
        else:
            return GenieResponse(message=f"Connection test failed: {test.get('error')}", data=test)
    except Exception as e:
        return GenieResponse(message=f"Error: {e}")


def handle_generate(request) -> GenieResponse:
    blueprint_id = request.parameters.get("blueprint_id", "")
    scale = request.parameters.get("scale_factor", 0.1)
    if not blueprint_id:
        return GenieResponse(
            message="Please provide a blueprint ID. You can find it from a previous profile run.",
            follow_up="Example: 'Generate environment from blueprint abc-123 at 10% scale'",
        )
    try:
        result = pipeline_runner.run_single_stage("generate", {
            "blueprint_id": blueprint_id,
            "catalog": config.catalog,
            "target_schema": "synthetic",
            "scale_factor": str(scale),
        })
        if result.status == "success":
            return GenieResponse(
                message=f"Synthetic environment generated. **{result.output.get('tables_generated', 0)} tables** created.",
                data=result.output,
                action_taken=True,
                follow_up="Run: 'Conform data through medallion pipeline'",
            )
        return GenieResponse(message=f"Generation failed: {result.error}")
    except Exception as e:
        return GenieResponse(message=f"Error: {e}")


def handle_pipeline(request) -> GenieResponse:
    blueprint_id = request.parameters.get("blueprint_id", "")
    if not blueprint_id:
        return GenieResponse(message="Provide a blueprint_id to run the medallion pipeline.")
    try:
        result = pipeline_runner.run_single_stage("conform", {
            "blueprint_id": blueprint_id,
            "catalog": config.catalog,
            "source_schema": "synthetic",
        })
        if result.status == "success":
            return GenieResponse(
                message="Medallion pipeline complete (Bronze → Silver → Gold).",
                data=result.output,
                action_taken=True,
                follow_up="Run: 'Validate the workspace'",
            )
        return GenieResponse(message=f"Pipeline failed: {result.error}")
    except Exception as e:
        return GenieResponse(message=f"Error: {e}")


def handle_validate(request) -> GenieResponse:
    blueprint_id = request.parameters.get("blueprint_id", "")
    workspace_id = request.parameters.get("workspace_id", "")
    if not blueprint_id:
        return GenieResponse(message="Provide a blueprint_id to validate against.")
    try:
        result = pipeline_runner.run_single_stage("validate", {
            "blueprint_id": blueprint_id,
            "workspace_id": workspace_id or "default",
            "catalog": config.catalog,
            "gold_schema": config.gold_schema,
            "scale_factor": "0.1",
        })
        if result.status == "success":
            score = result.output.get("score", 0)
            level = result.output.get("level", "unknown").upper()
            return GenieResponse(
                message=f"Validation complete: **{level}** ({score:.1%})\n\n{result.output.get('summary', '')}",
                data=result.output,
                action_taken=True,
            )
        return GenieResponse(message=f"Validation failed: {result.error}")
    except Exception as e:
        return GenieResponse(message=f"Error: {e}")


def handle_status(request) -> GenieResponse:
    summary = lifecycle.get_summary()
    results = st.session_state.pipeline_results
    recent = results[-1].to_dict() if results else None
    return GenieResponse(
        message="Current status:",
        data={"workspaces": summary, "last_pipeline": recent},
    )


def handle_list_blueprints(request) -> GenieResponse:
    try:
        rows = conn_mgr._execute_sql_with_result(
            f"SELECT blueprint_id, source_name, source_type, profiled_at, table_count "
            f"FROM `{config.catalog}`.`{config.schema}`.`source_blueprints` "
            f"ORDER BY profiled_at DESC LIMIT 20"
        )
        if rows:
            blueprints = [
                {"id": r[0], "source": r[1], "type": r[2], "profiled_at": r[3], "tables": r[4]}
                for r in rows
            ]
            return GenieResponse(message=f"**{len(blueprints)} blueprints found:**", data={"blueprints": blueprints})
        return GenieResponse(message="No blueprints found. Profile a source system first.")
    except Exception as e:
        return GenieResponse(message=f"Could not query blueprints: {e}")


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
        message=f"**{len(active)} active workspaces:**" if active else "No active workspaces.",
        data={"workspaces": data} if data else None,
    )


def handle_teardown(request) -> GenieResponse:
    workspace_id = request.parameters.get("workspace_id", "")
    connection_name = request.parameters.get("connection_name", "")
    if not workspace_id and not connection_name:
        return GenieResponse(message="Provide a workspace_id or connection_name to tear down.")
    try:
        if connection_name:
            conn_mgr.drop_connection(connection_name)
        return GenieResponse(
            message=f"Teardown complete for **{workspace_id or connection_name}**.",
            action_taken=True,
        )
    except Exception as e:
        return GenieResponse(message=f"Teardown error: {e}")


def handle_help(request) -> GenieResponse:
    return GenieResponse(
        message=(
            "**SEG — Synthetic Environment Generator**\n\n"
            "I can help you with:\n\n"
            "1. **Upload** source credentials via the sidebar (.env file or manual entry)\n"
            "2. **Profile** a source system: *'Profile the source database'*\n"
            "3. **Generate** synthetic environments: *'Generate environment from blueprint X at 10% scale'*\n"
            "4. **Conform** data: *'Run medallion pipeline for blueprint X'*\n"
            "5. **Validate**: *'Validate workspace against blueprint X'*\n"
            "6. **Full pipeline**: Use the Pipeline tab for one-click end-to-end execution\n\n"
            "Built natively on Databricks: Unity Catalog, Delta Tables, Foundation Models, DLT."
        ),
    )


def handle_unknown(request) -> GenieResponse:
    return GenieResponse(
        message=(
            "I'm not sure what you're asking. Could you rephrase?\n\n"
            "Try: *'Profile the source'*, *'Generate a synthetic environment'*, or *'Help'*"
        ),
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

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask me to profile, generate, validate, or manage environments..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.spinner("Processing..."):
            request = genie.parse_query(prompt)
            response = dispatch_intent(request)

        reply_parts = [response.message]
        if response.data:
            reply_parts.append(f"\n```json\n{json.dumps(response.data, indent=2, default=str)}\n```")
        if response.follow_up:
            reply_parts.append(f"\n> **Next step:** {response.follow_up}")
        reply = "\n".join(reply_parts)

        st.session_state.messages.append({"role": "assistant", "content": reply})
        with st.chat_message("assistant"):
            st.markdown(reply)

    with st.expander("Example prompts"):
        for ex in [
            "Profile the source database",
            "Generate environment from blueprint abc-123 at 10% scale",
            "Run medallion pipeline for blueprint abc-123",
            "Validate workspace against blueprint abc-123",
            "List blueprints",
            "Help",
        ]:
            st.code(ex, language=None)


# ---- Tab 2: Dashboard ---- #
with tab_dashboard:
    st.header("Workspace Dashboard")

    col1, col2, col3, col4 = st.columns(4)
    summary = lifecycle.get_summary()
    by_status = summary.get("by_status", {})
    pipeline_count = len(st.session_state.pipeline_results)

    col1.metric("Total Workspaces", summary.get("total", 0))
    col2.metric("Active", by_status.get("ready", 0) + by_status.get("generating", 0) + by_status.get("validating", 0))
    col3.metric("Complete", by_status.get("complete", 0))
    col4.metric("Pipeline Runs", pipeline_count)

    st.markdown("---")

    if st.session_state.pipeline_results:
        st.subheader("Recent Pipeline Runs")
        for i, pr in enumerate(reversed(st.session_state.pipeline_results[-5:])):
            level_icon = {"green": "🟢", "amber": "🟡", "red": "🔴"}.get(pr.confidence_level, "⚪")
            with st.expander(
                f"{level_icon} Run #{pipeline_count - i}: {pr.status.value} — "
                f"{pr.confidence_level.upper() if pr.confidence_level else 'N/A'} "
                f"({pr.total_duration_seconds:.0f}s)"
            ):
                st.json(pr.to_dict())
    else:
        st.info("No pipeline runs yet. Use the Pipeline tab to start one.")

    if st.button("Refresh", key="refresh_dashboard"):
        st.rerun()


# ---- Tab 3: Pipeline ---- #
with tab_pipeline:
    st.header("Full Pipeline — End to End")
    st.markdown(
        "Runs the complete SEG pipeline: "
        "**Connect → Profile → Generate → Conform → Validate**"
    )

    creds = st.session_state.credentials

    if not creds:
        st.warning("Load source credentials in the sidebar first (upload .env or enter manually).")
    else:
        st.success(f"Source: **{creds.source_type.upper()}** @ {creds.host}:{creds.port}/{creds.database}")

        with st.form("pipeline_form"):
            col1, col2 = st.columns(2)
            with col1:
                scale_factor = st.slider("Scale Factor", 0.01, 1.0, 0.1, 0.01)
                workspace_name = st.text_input("Workspace Name", value="default")
            with col2:
                st.markdown(f"**Connection:** `{creds.connection_name}`")
                st.markdown(f"**Catalog:** `{config.catalog}`")
                st.markdown(f"**Cluster:** `{config.cluster_id or 'not set'}`")

            submitted = st.form_submit_button("Run Full Pipeline", type="primary")

        if submitted:
            if not config.cluster_id:
                st.error("SEG_CLUSTER_ID is not set. Configure it in your .env or app settings.")
            elif not config.warehouse_id:
                st.error("SEG_WAREHOUSE_ID is not set. Configure it in your .env or app settings.")
            else:
                status_container = st.status("Running SEG pipeline...", expanded=True)
                progress = st.progress(0)
                stage_messages = {
                    PipelineStatus.CONNECTING: ("Connecting to source...", 0.10),
                    PipelineStatus.PROFILING: ("Profiling source system...", 0.25),
                    PipelineStatus.GENERATING: ("Generating synthetic Delta Tables...", 0.50),
                    PipelineStatus.CONFORMING: ("Running medallion pipeline...", 0.70),
                    PipelineStatus.VALIDATING: ("Validating synthetic environment...", 0.85),
                    PipelineStatus.COMPLETE: ("Pipeline complete!", 1.0),
                }

                def on_status(status: PipelineStatus, message: str):
                    msg, pct = stage_messages.get(status, (message, 0))
                    progress.progress(pct)
                    status_container.update(label=msg)

                result = pipeline_runner.run(
                    credentials=creds,
                    scale_factor=scale_factor,
                    workspace_name=workspace_name,
                    on_status_change=on_status,
                )

                st.session_state.pipeline_results.append(result)

                if result.status == PipelineStatus.COMPLETE:
                    progress.progress(1.0)
                    status_container.update(label="Pipeline complete!", state="complete")
                    st.balloons()

                    col1, col2, col3 = st.columns(3)
                    level_icon = {"green": "🟢", "amber": "🟡", "red": "🔴"}.get(result.confidence_level, "⚪")
                    col1.metric("Confidence", f"{level_icon} {result.confidence_level.upper()}")
                    col2.metric("Tables", result.tables_generated)
                    col3.metric("Duration", f"{result.total_duration_seconds:.0f}s")

                    st.markdown(f"**Blueprint ID:** `{result.blueprint_id}`")
                    st.markdown(f"**Workspace:** `{result.workspace_id}`")

                    with st.expander("Full pipeline details"):
                        st.json(result.to_dict())
                else:
                    status_container.update(label=f"Pipeline failed: {result.error}", state="error")
                    st.error(f"Pipeline failed: {result.error}")
                    with st.expander("Stage details"):
                        st.json(result.to_dict())


# ---- Tab 4: Validation ---- #
with tab_validation:
    st.header("Validation & Confidence Scores")

    with st.form("validation_form"):
        val_blueprint = st.text_input("Blueprint ID", placeholder="Required")
        val_workspace = st.text_input("Workspace ID", placeholder="e.g., dais_seg.seg_default")
        val_scale = st.text_input("Scale Factor", value="0.1")
        val_submitted = st.form_submit_button("Run Validation", type="primary")

    if val_submitted:
        if not val_blueprint:
            st.error("Blueprint ID is required.")
        elif not config.cluster_id:
            st.error("SEG_CLUSTER_ID is not set.")
        else:
            with st.spinner("Running validation..."):
                result = pipeline_runner.run_single_stage("validate", {
                    "blueprint_id": val_blueprint,
                    "workspace_id": val_workspace or "default",
                    "catalog": config.catalog,
                    "gold_schema": config.gold_schema,
                    "scale_factor": val_scale,
                })

            if result.status == "success":
                score = result.output.get("score", 0)
                level = result.output.get("level", "unknown")
                level_icon = {"green": "🟢", "amber": "🟡", "red": "🔴"}.get(level, "⚪")

                st.markdown(f"### {level_icon} {level.upper()} — {score:.1%}")
                st.markdown(result.output.get("summary", ""))
                st.json(result.output)
            else:
                st.error(f"Validation failed: {result.error}")

    st.markdown("---")
    st.subheader("Scoring Dimensions")

    score_cols = st.columns(4)
    score_cols[0].markdown("**Schema Parity**\n\nColumns, types, constraints\n\n*Weight: 25%*")
    score_cols[1].markdown("**Data Fidelity**\n\nDistributions, nulls, cardinality\n\n*Weight: 35%*")
    score_cols[2].markdown("**Quality Compliance**\n\nDLT expectations pass rate\n\n*Weight: 20%*")
    score_cols[3].markdown("**Pipeline Integrity**\n\nMedallion conformance\n\n*Weight: 20%*")

    st.markdown(
        "| Level | Threshold | Meaning |\n"
        "|-------|-----------|--------|\n"
        "| 🟢 Green | >= 90% | Ready for production cutover |\n"
        "| 🟡 Amber | >= 70% | Needs attention before cutover |\n"
        "| 🔴 Red | < 70% | Not ready — review recommendations |"
    )

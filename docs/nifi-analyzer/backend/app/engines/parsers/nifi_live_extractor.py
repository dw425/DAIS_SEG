"""NiFi live extractor — connects to a running NiFi instance via NiPyApi.

Uses the NiFi REST API to pull flow definitions in real-time,
eliminating the manual XML export step.

NiPyApi is an optional dependency — the module gracefully degrades
if it is not installed.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.models.pipeline import ParseResult
from app.models.processor import Connection, ControllerService, Processor, ProcessGroup

logger = logging.getLogger(__name__)

try:
    import nipyapi  # type: ignore[import-untyped]
    NIPYAPI_AVAILABLE = True
except ImportError:
    nipyapi = None  # type: ignore[assignment]
    NIPYAPI_AVAILABLE = False


def is_available() -> bool:
    """Check if NiPyApi is installed."""
    return NIPYAPI_AVAILABLE


def connect(
    nifi_url: str,
    username: str | None = None,
    password: str | None = None,
    token: str | None = None,
    verify_ssl: bool = True,
) -> dict:
    """Test connection to a NiFi instance.

    Returns {"connected": True, "version": "...", "clustered": bool}
    on success, or {"connected": False, "error": "..."} on failure.
    """
    if not NIPYAPI_AVAILABLE:
        return {"connected": False, "error": "nipyapi is not installed. Install with: pip install nipyapi"}

    try:
        # Configure NiPyApi connection
        nipyapi.config.nifi_config.host = nifi_url.rstrip("/") + "/nifi-api"
        nipyapi.config.nifi_config.verify_ssl = verify_ssl

        if username and password:
            nipyapi.security.service_login(
                service="nifi",
                username=username,
                password=password,
            )
        elif token:
            nipyapi.config.nifi_config.api_key["tokenAuth"] = token

        # Test connection by fetching system diagnostics
        about = nipyapi.system.get_system_diagnostics()
        cluster_info = nipyapi.system.get_cluster() if hasattr(nipyapi.system, "get_cluster") else None

        return {
            "connected": True,
            "version": getattr(about, "version", "unknown"),
            "clustered": cluster_info is not None,
        }
    except Exception as exc:
        logger.warning("NiFi connection failed: %s", exc)
        return {"connected": False, "error": str(exc)}


def list_process_groups(
    nifi_url: str,
    username: str | None = None,
    password: str | None = None,
    token: str | None = None,
    verify_ssl: bool = True,
) -> list[dict]:
    """List available process groups from a NiFi instance.

    Returns a list of {id, name, processorCount, connectionCount}.
    """
    if not NIPYAPI_AVAILABLE:
        return []

    try:
        _configure_connection(nifi_url, username, password, token, verify_ssl)

        root_pg = nipyapi.canvas.get_root_pg_id()
        groups = _list_groups_recursive(root_pg)
        return groups
    except Exception as exc:
        logger.exception("Failed to list NiFi process groups")
        raise ValueError(f"Failed to list process groups: {exc}") from exc


def extract_flow(
    nifi_url: str,
    process_group_id: str = "root",
    username: str | None = None,
    password: str | None = None,
    token: str | None = None,
    verify_ssl: bool = True,
) -> ParseResult:
    """Extract a flow from a live NiFi instance and return a ParseResult.

    This produces the same ParseResult shape as the XML/JSON parsers,
    allowing seamless integration with the rest of the pipeline.
    """
    if not NIPYAPI_AVAILABLE:
        raise RuntimeError("nipyapi is not installed. Install with: pip install nipyapi")

    _configure_connection(nifi_url, username, password, token, verify_ssl)

    try:
        # Resolve "root" to the actual root PG ID
        if process_group_id == "root":
            process_group_id = nipyapi.canvas.get_root_pg_id()

        # Extract processors, connections, controller services recursively
        processors: list[Processor] = []
        connections: list[Connection] = []
        process_groups: list[ProcessGroup] = []
        controller_services: list[ControllerService] = []

        _extract_group_recursive(
            process_group_id,
            processors,
            connections,
            process_groups,
            controller_services,
        )

        # Get NiFi version for metadata
        version = "unknown"
        try:
            about = nipyapi.system.get_system_diagnostics()
            version = getattr(about, "version", "unknown")
        except Exception:
            pass

        return ParseResult(
            platform="nifi",
            version=version,
            processors=processors,
            connections=connections,
            process_groups=process_groups,
            controller_services=controller_services,
            parameter_contexts=[],
            metadata={
                "source": "live_nifi",
                "nifi_url": nifi_url,
                "process_group_id": process_group_id,
            },
            warnings=[],
        )
    except Exception as exc:
        logger.exception("Failed to extract NiFi flow")
        raise ValueError(f"Flow extraction failed: {exc}") from exc


# ── Internal helpers ──


def _configure_connection(
    nifi_url: str,
    username: str | None,
    password: str | None,
    token: str | None,
    verify_ssl: bool,
) -> None:
    """Configure the NiPyApi connection settings."""
    nipyapi.config.nifi_config.host = nifi_url.rstrip("/") + "/nifi-api"
    nipyapi.config.nifi_config.verify_ssl = verify_ssl

    if username and password:
        nipyapi.security.service_login(
            service="nifi",
            username=username,
            password=password,
        )
    elif token:
        nipyapi.config.nifi_config.api_key["tokenAuth"] = token


def _list_groups_recursive(pg_id: str, depth: int = 0) -> list[dict]:
    """Recursively list process groups."""
    results: list[dict] = []
    try:
        pg = nipyapi.canvas.get_process_group(pg_id, identifier_type="id")
        status = pg.status if hasattr(pg, "status") else None

        results.append({
            "id": pg_id,
            "name": getattr(pg.component, "name", "Unknown") if hasattr(pg, "component") else "root",
            "depth": depth,
            "processorCount": getattr(status, "aggregate_snapshot", {}).get("processor_count", 0) if status else 0,
        })

        # Get child groups
        children = nipyapi.canvas.list_all_process_groups(pg_id)
        if children:
            for child in children:
                child_id = child.id if hasattr(child, "id") else str(child)
                results.extend(_list_groups_recursive(child_id, depth + 1))
    except Exception as exc:
        logger.warning("Failed to list group %s: %s", pg_id, exc)

    return results


def _extract_group_recursive(
    pg_id: str,
    processors: list[Processor],
    connections: list[Connection],
    process_groups: list[ProcessGroup],
    controller_services: list[ControllerService],
    group_path: str = "",
) -> None:
    """Recursively extract processors, connections, and services from a process group."""
    try:
        # Get process group details
        pg = nipyapi.canvas.get_process_group(pg_id, identifier_type="id")
        pg_name = getattr(pg.component, "name", "root") if hasattr(pg, "component") else "root"
        current_path = f"{group_path}/{pg_name}" if group_path else pg_name

        # Extract processors in this group
        proc_names: list[str] = []
        try:
            procs = nipyapi.canvas.list_all_processors(pg_id)
            if procs:
                for p in procs:
                    comp = p.component if hasattr(p, "component") else p
                    name = getattr(comp, "name", f"proc_{p.id}" if hasattr(p, "id") else "unknown")
                    proc_type = getattr(comp, "type", "Unknown")
                    # Extract short type name (strip Java package prefix)
                    short_type = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type

                    # Extract properties
                    props: dict[str, str] = {}
                    raw_props = getattr(comp, "config", None)
                    if raw_props:
                        prop_dict = getattr(raw_props, "properties", None)
                        if prop_dict and isinstance(prop_dict, dict):
                            props = {k: str(v) for k, v in prop_dict.items() if v is not None}

                    state = getattr(comp, "state", "STOPPED") if hasattr(comp, "state") else "STOPPED"

                    # Scheduling
                    scheduling: dict[str, Any] = {}
                    if raw_props:
                        scheduling = {
                            "strategy": getattr(raw_props, "scheduling_strategy", "TIMER_DRIVEN"),
                            "period": getattr(raw_props, "scheduling_period", "0 sec"),
                            "concurrentTasks": getattr(raw_props, "concurrent_tasks", "1"),
                        }

                    processors.append(Processor(
                        name=name,
                        type=short_type,
                        platform="nifi",
                        properties=props,
                        group=current_path,
                        state=state,
                        scheduling=scheduling,
                        resolved_services=None,
                    ))
                    proc_names.append(name)
        except Exception as exc:
            logger.warning("Failed to list processors in %s: %s", pg_id, exc)

        # Extract connections in this group
        try:
            conns = nipyapi.canvas.list_all_connections(pg_id)
            if conns:
                for c in conns:
                    comp = c.component if hasattr(c, "component") else c
                    src_name = _resolve_component_name(comp, "source")
                    dst_name = _resolve_component_name(comp, "destination")
                    rels = getattr(comp, "selected_relationships", ["success"])
                    rel = rels[0] if rels else "success"

                    bp_obj = getattr(comp, "back_pressure_object_threshold", 10000)
                    bp_data = getattr(comp, "back_pressure_data_size_threshold", "1 GB")

                    connections.append(Connection(
                        source_name=src_name,
                        destination_name=dst_name,
                        relationship=rel,
                        back_pressure_object_threshold=int(bp_obj) if bp_obj else 10000,
                        back_pressure_data_size_threshold=str(bp_data) if bp_data else "1 GB",
                    ))
        except Exception as exc:
            logger.warning("Failed to list connections in %s: %s", pg_id, exc)

        # Record process group
        process_groups.append(ProcessGroup(
            name=current_path,
            processors=proc_names,
        ))

        # Extract controller services
        try:
            services = nipyapi.canvas.list_all_controller_services(pg_id)
            if services:
                for svc in services:
                    comp = svc.component if hasattr(svc, "component") else svc
                    svc_name = getattr(comp, "name", "unknown")
                    svc_type = getattr(comp, "type", "Unknown")
                    short_svc_type = svc_type.rsplit(".", 1)[-1] if "." in svc_type else svc_type

                    svc_props: dict[str, str] = {}
                    raw_svc_props = getattr(comp, "properties", None)
                    if raw_svc_props and isinstance(raw_svc_props, dict):
                        svc_props = {k: str(v) for k, v in raw_svc_props.items() if v is not None}

                    controller_services.append(ControllerService(
                        name=svc_name,
                        type=short_svc_type,
                        properties=svc_props,
                    ))
        except Exception as exc:
            logger.warning("Failed to list controller services in %s: %s", pg_id, exc)

        # Recurse into child process groups
        try:
            children = nipyapi.canvas.list_all_process_groups(pg_id)
            if children:
                for child in children:
                    child_id = child.id if hasattr(child, "id") else str(child)
                    _extract_group_recursive(
                        child_id,
                        processors,
                        connections,
                        process_groups,
                        controller_services,
                        current_path,
                    )
        except Exception as exc:
            logger.warning("Failed to recurse into child groups of %s: %s", pg_id, exc)

    except Exception as exc:
        logger.exception("Failed to extract group %s", pg_id)
        raise


def _resolve_component_name(component: Any, direction: str) -> str:
    """Resolve source/destination component name from a connection component."""
    try:
        endpoint = getattr(component, direction, None)
        if endpoint:
            return getattr(endpoint, "name", getattr(endpoint, "id", "unknown"))
    except Exception:
        pass
    return f"unknown_{direction}"

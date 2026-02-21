"""Total Cost of Ownership calculator — current platform vs Databricks over 1/3/5 years.

Estimates infrastructure, licensing, operational, and migration costs to provide
a comprehensive TCO comparison.
"""

import logging

from app.engines.reporters.roi_comparison import compute_roi_comparison
from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Reference baseline: 478 processors
_REFERENCE_PROCESSOR_COUNT = 478

# Default cost assumptions
_DEFAULT_CONFIG = {
    # Current platform (NiFi)
    "nifi_nodes": 3,
    "nifi_node_cost_monthly": 500,          # $/mo per node
    "ops_fte_fraction": 0.5,                 # FTEs dedicated to ops
    "ops_fte_annual_salary": 150_000,        # $/yr
    "storage_per_tb_monthly": 100,           # $/mo per TB
    "estimated_storage_tb": 2,               # TB managed
    # Databricks
    "dbu_batch_per_processor_hourly": 0.5,   # DBU/h per batch processor
    "dbu_streaming_per_processor_hourly": 2.0,  # DBU/h per streaming processor
    "dbu_cost": 0.15,                        # $/DBU
    "databricks_storage_per_tb_monthly": 23, # $/mo per TB (Delta Lake on cloud)
    "unity_catalog_cost": 0,                 # included
    "databricks_ops_fte_fraction": 0.2,      # less ops needed
    # General
    "annual_infra_growth": 0.05,             # 5% annual infra cost growth
    "hourly_rate": 150,                      # for migration cost
}

_STREAMING_TYPES = {
    "consumekafka", "consumemqtt", "consumejms", "listens3",
    "listenhttp", "listenudp", "listensyslog", "consumeamqp",
    "consumewindowseventlog", "gettwitter", "listeneventhub",
}


def compute_tco(
    parse_result: ParseResult,
    assessment: AssessmentResult,
    config: dict | None = None,
) -> dict:
    """Compute Total Cost of Ownership comparison.

    Returns dict with currentPlatform, databricks, migrationCost, and savings
    over 1, 3, and 5 year horizons.
    """
    if not parse_result or not parse_result.processors:
        return _empty_tco()
    if not assessment:
        return _empty_tco()

    cfg = {**_DEFAULT_CONFIG, **(config or {})}

    # Validate config values
    if cfg.get("nifi_nodes", 0) < 0:
        cfg["nifi_nodes"] = 0
    if cfg.get("estimated_storage_tb", 0) < 0:
        cfg["estimated_storage_tb"] = 0

    total_processors = len(parse_result.processors)
    if total_processors == 0:
        return _empty_tco()

    # Scale factor relative to reference baseline
    scale = total_processors / _REFERENCE_PROCESSOR_COUNT

    # Classify streaming vs batch processors
    streaming_count = sum(
        1 for p in parse_result.processors
        if p.type.lower() in _STREAMING_TYPES
    )
    batch_count = total_processors - streaming_count

    # --- Current platform annual cost ---
    infra_annual = cfg["nifi_nodes"] * cfg["nifi_node_cost_monthly"] * 12 * scale
    ops_annual = cfg["ops_fte_fraction"] * cfg["ops_fte_annual_salary"] * scale
    storage_annual = cfg["storage_per_tb_monthly"] * cfg["estimated_storage_tb"] * 12
    current_annual = infra_annual + ops_annual + storage_annual

    # --- Databricks annual cost ---
    # Compute hours: assume 8h/day active for batch, 24h/day for streaming
    batch_dbu_annual = (
        batch_count * cfg["dbu_batch_per_processor_hourly"] * 8 * 365 * cfg["dbu_cost"]
    )
    streaming_dbu_annual = (
        streaming_count * cfg["dbu_streaming_per_processor_hourly"] * 24 * 365 * cfg["dbu_cost"]
    )
    dbx_compute_annual = batch_dbu_annual + streaming_dbu_annual
    dbx_storage_annual = cfg["databricks_storage_per_tb_monthly"] * cfg["estimated_storage_tb"] * 12
    dbx_ops_annual = cfg["databricks_ops_fte_fraction"] * cfg["ops_fte_annual_salary"] * scale
    dbx_annual = dbx_compute_annual + dbx_storage_annual + dbx_ops_annual

    # --- Migration one-time cost ---
    roi = compute_roi_comparison(parse_result, assessment, rate=cfg["hourly_rate"])
    # Use the cheaper scenario as migration cost estimate
    migration_cost = min(roi["liftAndShift"]["cost"], roi["refactor"]["cost"])

    logger.info("TCO: current_annual=$%d, databricks_annual=$%d, migration=$%d", round(current_annual), round(dbx_annual), migration_cost)
    # --- Multi-year projections ---
    growth = cfg["annual_infra_growth"]

    def _project(annual: float, years: int, growth_rate: float) -> int:
        """Project cost over N years with annual growth."""
        total = 0.0
        for y in range(years):
            total += annual * ((1 + growth_rate) ** y)
        return round(total)

    current_y1 = _project(current_annual, 1, growth)
    current_y3 = _project(current_annual, 3, growth)
    current_y5 = _project(current_annual, 5, growth)

    dbx_y1 = _project(dbx_annual, 1, growth) + migration_cost
    dbx_y3 = _project(dbx_annual, 3, growth) + migration_cost
    dbx_y5 = _project(dbx_annual, 5, growth) + migration_cost

    return {
        "currentPlatform": {
            "year1": current_y1,
            "year3": current_y3,
            "year5": current_y5,
            "annualBreakdown": {
                "infrastructure": round(infra_annual),
                "operations": round(ops_annual),
                "storage": round(storage_annual),
            },
        },
        "databricks": {
            "year1": dbx_y1,
            "year3": dbx_y3,
            "year5": dbx_y5,
            "annualBreakdown": {
                "compute": round(dbx_compute_annual),
                "storage": round(dbx_storage_annual),
                "operations": round(dbx_ops_annual),
            },
        },
        "migrationCost": migration_cost,
        "savingsYear1": current_y1 - dbx_y1,
        "savingsYear3": current_y3 - dbx_y3,
        "savingsYear5": current_y5 - dbx_y5,
        "processorBreakdown": {
            "total": total_processors,
            "batch": batch_count,
            "streaming": streaming_count,
        },
        "assumptions": {
            "referenceBaseline": _REFERENCE_PROCESSOR_COUNT,
            "scaleFactor": round(scale, 3),
            "annualGrowthRate": growth,
        },
    }


def _empty_tco() -> dict:
    """Return empty TCO when no processors are present."""
    zero = {"year1": 0, "year3": 0, "year5": 0, "annualBreakdown": {}}
    return {
        "currentPlatform": zero,
        "databricks": zero,
        "migrationCost": 0,
        "savingsYear1": 0,
        "savingsYear3": 0,
        "savingsYear5": 0,
        "processorBreakdown": {"total": 0, "batch": 0, "streaming": 0},
        "assumptions": {},
    }

"""License savings calculator — compare platform licensing costs.

Estimates annual licensing costs for the source ETL platform versus
Databricks to quantify potential savings.
"""

import logging

from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)

# Annual license cost estimates by platform
_PLATFORM_LICENSES: dict[str, dict] = {
    "nifi": {
        "name": "Apache NiFi",
        "license": 0,
        "infrastructure": 50_000,
        "notes": "Open source — cost is infrastructure and operations only",
    },
    "ssis": {
        "name": "SQL Server Integration Services",
        "license": 15_000,
        "infrastructure": 20_000,
        "notes": "Per SQL Server Enterprise license; Standard edition ~$3,700",
    },
    "informatica": {
        "name": "Informatica PowerCenter",
        "license": 200_000,
        "infrastructure": 30_000,
        "notes": "Per production license; IICS cloud ~$100K-$300K/yr",
    },
    "talend": {
        "name": "Talend Data Integration",
        "license": 50_000,
        "infrastructure": 15_000,
        "notes": "Per named user; Talend Cloud ~$1,170/user/mo",
    },
    "datastage": {
        "name": "IBM DataStage",
        "license": 150_000,
        "infrastructure": 25_000,
        "notes": "Per PVU; CP4D licensing varies by deployment",
    },
    "pentaho": {
        "name": "Hitachi Vantara Pentaho",
        "license": 30_000,
        "infrastructure": 10_000,
        "notes": "Per server; community edition is free",
    },
    "airflow": {
        "name": "Apache Airflow",
        "license": 0,
        "infrastructure": 40_000,
        "notes": "Open source — cost is infrastructure (MWAA ~$350/mo base)",
    },
    "matillion": {
        "name": "Matillion ETL",
        "license": 60_000,
        "infrastructure": 10_000,
        "notes": "Per instance; usage-based pricing available",
    },
    "fivetran": {
        "name": "Fivetran",
        "license": 24_000,
        "infrastructure": 0,
        "notes": "SaaS; ~$2/credit, ~1K credits/mo baseline",
    },
    "oracle": {
        "name": "Oracle Data Integrator",
        "license": 120_000,
        "infrastructure": 25_000,
        "notes": "Per processor license; Named User Plus ~$5K each",
    },
}

# Databricks cost estimate
_DATABRICKS_BASE = {
    "name": "Databricks",
    "license": 0,  # Pay-per-use, not license-based
    "infrastructure": 0,  # Cloud-native, included in DBU pricing
    "notes": "Usage-based DBU pricing; Unity Catalog included",
}


def compute_license_savings(parse_result: ParseResult) -> dict:
    """Compute license savings by comparing source platform to Databricks.

    Returns dict with currentLicense, databricksLicense, annualSavings,
    and platform comparison details.
    """
    platform = parse_result.platform.lower().strip()
    processor_count = len(parse_result.processors)

    # Look up source platform
    platform_info = _PLATFORM_LICENSES.get(platform)
    if not platform_info:
        # Try partial matching
        for key, info in _PLATFORM_LICENSES.items():
            if key in platform or platform in key:
                platform_info = info
                break

    is_estimated = False
    estimation_warnings: list[str] = []
    if not platform_info:
        logger.warning("Platform %r not found in known catalog, using default estimates", parse_result.platform)
        is_estimated = True
        estimation_warnings.append(
            f"Platform '{parse_result.platform}' is not in the known platform catalog. "
            "License cost defaulted to $0 and infrastructure to $30,000/yr."
        )
        estimation_warnings.append(
            "Actual costs may differ significantly — verify with vendor pricing."
        )
        platform_info = {
            "name": parse_result.platform,
            "license": 0,
            "infrastructure": 30_000,
            "notes": f"Unknown platform '{parse_result.platform}' — using default infrastructure estimate",
        }

    current_license = platform_info["license"]
    current_infra = platform_info["infrastructure"]
    current_total = current_license + current_infra

    # Scale infrastructure by processor count (using 100 as baseline)
    scale = max(processor_count / 100, 0.5)
    scaled_infra = round(current_infra * scale)
    scaled_total = current_license + scaled_infra

    # Databricks estimated cost (DBU-based, scaled by processors)
    # Rough estimate: $0.15/DBU * 0.5 DBU/h * 8h/day * 365 days per processor
    dbx_annual = round(processor_count * 0.15 * 0.5 * 8 * 365)

    annual_savings = scaled_total - dbx_annual
    logger.info("License savings: %s annual=$%d, Databricks=$%d, savings=$%d", platform_info["name"], scaled_total, dbx_annual, annual_savings)

    # Build platform comparison list
    platforms = []
    for key, info in _PLATFORM_LICENSES.items():
        total = info["license"] + info["infrastructure"]
        platforms.append({
            "platform": info["name"],
            "annualLicense": info["license"],
            "annualInfrastructure": info["infrastructure"],
            "annualTotal": total,
            "notes": info["notes"],
            "isCurrent": key == platform,
        })

    return {
        "currentPlatform": {
            "name": platform_info["name"],
            "annualLicense": current_license,
            "annualInfrastructure": scaled_infra,
            "annualTotal": scaled_total,
            "notes": platform_info["notes"],
        },
        "databricks": {
            "name": "Databricks",
            "annualCompute": dbx_annual,
            "annualLicense": 0,
            "annualTotal": dbx_annual,
            "notes": _DATABRICKS_BASE["notes"],
        },
        "annualSavings": annual_savings,
        "savingsPercent": round(annual_savings / max(scaled_total, 1) * 100, 1),
        "platforms": platforms,
        "processorCount": processor_count,
        "scaleFactor": round(scale, 3),
        "estimated": is_estimated,
        "warnings": estimation_warnings,
    }

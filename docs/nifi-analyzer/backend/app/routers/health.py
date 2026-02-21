"""Health router — detailed health checks and mapping coverage statistics."""

import logging
import sys
import time
from pathlib import Path

from fastapi import APIRouter

from app.engines.mappers.cache import mapping_cache
from app.engines.parsers.parse_cache import parse_cache
from app.processing_status import processing_status

router = APIRouter()
logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).resolve().parent.parent / "constants" / "processor_maps"
_START_TIME = time.monotonic()


@router.get("/health/detailed")
async def health_detailed() -> dict:
    """Return detailed health: mapping counts, cache stats, uptime, Python version."""
    mapping_counts = _count_mappings_per_platform()
    return {
        "status": "healthy",
        "uptime_seconds": round(time.monotonic() - _START_TIME, 1),
        "python_version": sys.version,
        "mapping_counts": mapping_counts,
        "mapping_cache": mapping_cache.cache_stats(),
        "parse_cache": parse_cache.stats(),
        "total_platforms": len(mapping_counts),
    }


@router.get("/health/heartbeat")
async def health_heartbeat() -> dict:
    """Fast heartbeat — reports whether the backend is actively processing a step."""
    return processing_status.status()


@router.get("/health/mappings")
async def health_mappings() -> dict:
    """Return coverage statistics per platform YAML file."""
    results: dict[str, dict] = {}

    if not _YAML_DIR.is_dir():
        return {"error": f"YAML directory not found: {_YAML_DIR}", "platforms": {}}

    for path in sorted(_YAML_DIR.glob("*.yaml")):
        try:
            import yaml

            with open(path, encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}

            if isinstance(data, dict):
                # Count entries that have a "databricks" or mapping target key
                total_entries = len(data)
                mapped = sum(
                    1 for v in data.values()
                    if isinstance(v, dict) and v.get("databricks")
                )
                results[path.stem] = {
                    "file": path.name,
                    "total_entries": total_entries,
                    "mapped_entries": mapped,
                    "coverage_pct": round(mapped / max(total_entries, 1) * 100, 1),
                }
            else:
                results[path.stem] = {
                    "file": path.name,
                    "total_entries": 0,
                    "mapped_entries": 0,
                    "coverage_pct": 0,
                    "note": "YAML root is not a dict",
                }
        except Exception as exc:
            results[path.stem] = {
                "file": path.name,
                "error": str(exc),
            }

    return {"platforms": results, "total_files": len(results)}


def _count_mappings_per_platform() -> dict[str, int]:
    """Quick count of top-level keys in each platform YAML."""
    counts: dict[str, int] = {}
    if not _YAML_DIR.is_dir():
        return counts

    for path in sorted(_YAML_DIR.glob("*.yaml")):
        try:
            import yaml

            with open(path, encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            counts[path.stem] = len(data) if isinstance(data, dict) else 0
        except Exception:
            counts[path.stem] = -1  # indicates error
    return counts

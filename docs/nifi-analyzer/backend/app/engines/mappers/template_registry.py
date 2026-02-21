"""Template registry — central registry for all platform-to-Databricks mappings.

Loads every YAML file from constants/processor_maps/ and exposes a unified
lookup, search, and coverage API.
"""

import logging
import re
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).parent.parent.parent / "constants" / "processor_maps"


class TemplateRegistry:
    """Central registry for all platform processor mapping templates."""

    def __init__(self) -> None:
        self._registry: dict[str, dict[str, dict]] = {}
        self._load_all()

    def _load_all(self) -> None:
        """Load all YAML files from the processor_maps directory."""
        if not _YAML_DIR.exists():
            logger.warning("Processor maps directory not found: %s", _YAML_DIR)
            return

        for yaml_path in sorted(_YAML_DIR.glob("*_databricks.yaml")):
            platform = yaml_path.stem.replace("_databricks", "")
            try:
                with open(yaml_path) as f:
                    raw = yaml.safe_load(f) or {}

                mapping: dict[str, dict] = {}
                if "mappings" in raw and isinstance(raw["mappings"], list):
                    for entry in raw["mappings"]:
                        if isinstance(entry, dict) and "type" in entry:
                            mapping[entry["type"]] = entry
                elif isinstance(raw, dict):
                    # Filter out metadata keys
                    mapping = {
                        k: v for k, v in raw.items()
                        if isinstance(v, dict) and k not in ("version", "metadata", "description")
                    }

                self._registry[platform] = mapping
                logger.debug("Loaded %d mappings for %s", len(mapping), platform)

            except Exception as exc:
                logger.warning("Failed to load %s: %s", yaml_path, exc)

    def get_template(self, platform: str, proc_type: str) -> dict | None:
        """Return the mapping entry for a specific platform + processor type."""
        platform_map = self._registry.get(platform, {})
        return platform_map.get(proc_type)

    def list_templates(self, platform: str) -> list[dict]:
        """Return all mapping entries for a platform."""
        platform_map = self._registry.get(platform, {})
        results: list[dict] = []
        for proc_type, entry in platform_map.items():
            item = dict(entry)
            item.setdefault("type", proc_type)
            item.setdefault("platform", platform)
            results.append(item)
        return results

    def get_coverage(self, platform: str) -> dict:
        """Return coverage stats for a platform.

        Returns: {total, mapped, unmapped, coverage_pct}
        """
        platform_map = self._registry.get(platform, {})
        total = len(platform_map)
        mapped = sum(
            1 for entry in platform_map.values()
            if isinstance(entry, dict) and entry.get("template")
        )
        unmapped = total - mapped

        return {
            "platform": platform,
            "total": total,
            "mapped": mapped,
            "unmapped": unmapped,
            "coverage_pct": round((mapped / total * 100) if total else 0, 1),
        }

    def get_all_coverage(self) -> list[dict]:
        """Return coverage stats across all loaded platforms."""
        return [
            self.get_coverage(platform) for platform in sorted(self._registry)
        ]

    def search_templates(self, query: str) -> list[dict]:
        """Search all mappings by type, category, or description keywords."""
        query_lower = query.lower()
        query_tokens = set(re.split(r'[\s_-]+', query_lower))
        results: list[dict] = []

        for platform, mapping in self._registry.items():
            for proc_type, entry in mapping.items():
                if not isinstance(entry, dict):
                    continue

                # Build searchable text
                searchable = " ".join([
                    proc_type,
                    entry.get("category", ""),
                    entry.get("description", ""),
                    entry.get("notes", ""),
                    entry.get("databricks_type", ""),
                ]).lower()

                # Check if query tokens overlap with searchable text
                if query_lower in searchable or any(t in searchable for t in query_tokens if len(t) > 2):
                    item = dict(entry)
                    item["type"] = proc_type
                    item["platform"] = platform
                    results.append(item)

        return results

    @property
    def platforms(self) -> list[str]:
        """Return list of all loaded platform names."""
        return sorted(self._registry.keys())


# Module-level singleton
template_registry = TemplateRegistry()

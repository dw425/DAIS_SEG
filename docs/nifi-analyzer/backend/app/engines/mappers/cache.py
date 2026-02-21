"""LRU + TTL cache for YAML mapping files."""

import logging
import time
from pathlib import Path
from threading import Lock

import yaml

logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).resolve().parent.parent.parent / "constants" / "processor_maps"
_DEFAULT_TTL = 300  # seconds


class MappingCache:
    """LRU cache with TTL for platform YAML mapping files."""

    def __init__(self, ttl: float = _DEFAULT_TTL) -> None:
        self._ttl = ttl
        self._cache: dict[str, tuple[float, dict]] = {}
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    def get_mapping(self, platform: str) -> dict:
        """Return the cached YAML dict for *platform*, loading on miss or expiry."""
        now = time.monotonic()
        with self._lock:
            entry = self._cache.get(platform)
            if entry and (now - entry[0]) < self._ttl:
                self._hits += 1
                return entry[1]
            self._misses += 1

        # Load outside lock to avoid blocking other readers
        data = self._load_yaml(platform)
        with self._lock:
            self._cache[platform] = (time.monotonic(), data)
        return data

    def invalidate(self, platform: str) -> None:
        """Remove a specific platform from the cache."""
        with self._lock:
            self._cache.pop(platform, None)

    def invalidate_all(self) -> None:
        """Clear the entire cache."""
        with self._lock:
            self._cache.clear()

    def cache_stats(self) -> dict:
        """Return hit/miss statistics."""
        with self._lock:
            total = self._hits + self._misses
            return {
                "hits": self._hits,
                "misses": self._misses,
                "total": total,
                "hit_rate": round(self._hits / max(total, 1), 3),
                "cached_platforms": list(self._cache.keys()),
                "ttl_seconds": self._ttl,
            }

    # ------------------------------------------------------------------
    @staticmethod
    def _load_yaml(platform: str) -> dict:
        """Read and parse the YAML mapping file for *platform*."""
        name_map = {
            "azure_adf": "adf_databricks",
            "aws_glue": "glue_databricks",
        }
        stem = name_map.get(platform, f"{platform}_databricks")
        path = _YAML_DIR / f"{stem}.yaml"

        if not path.exists():
            logger.warning("No YAML mapping file for platform '%s' at %s", platform, path)
            return {}

        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        logger.debug("Loaded mapping for '%s' (%d keys)", platform, len(data))
        return data


# Module-level singleton
mapping_cache = MappingCache()

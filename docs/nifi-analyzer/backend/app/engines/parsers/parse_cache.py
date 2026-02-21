"""Content-hash based LRU cache for parse results."""

import hashlib
import logging
from collections import OrderedDict
from threading import Lock

from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)

_MAX_ENTRIES = 50


class ParseCache:
    """LRU cache keyed by content SHA-256 hash."""

    def __init__(self, max_entries: int = _MAX_ENTRIES) -> None:
        self._max = max_entries
        self._cache: OrderedDict[str, ParseResult] = OrderedDict()
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def content_hash(content: bytes) -> str:
        """Return the SHA-256 hex digest of *content*."""
        return hashlib.sha256(content).hexdigest()

    def get_cached(self, content_hash: str) -> ParseResult | None:
        """Return the cached ParseResult or ``None`` on miss."""
        with self._lock:
            result = self._cache.get(content_hash)
            if result is not None:
                # Move to end (most recently used)
                self._cache.move_to_end(content_hash)
                self._hits += 1
                return result
            self._misses += 1
            return None

    def set_cached(self, content_hash: str, result: ParseResult) -> None:
        """Store a ParseResult, evicting the oldest entry if at capacity."""
        with self._lock:
            if content_hash in self._cache:
                self._cache.move_to_end(content_hash)
                self._cache[content_hash] = result
                return
            if len(self._cache) >= self._max:
                evicted_key, _ = self._cache.popitem(last=False)
                logger.debug("Evicted parse cache entry %s", evicted_key[:12])
            self._cache[content_hash] = result

    def invalidate(self, content_hash: str) -> None:
        """Remove a specific entry."""
        with self._lock:
            self._cache.pop(content_hash, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()

    def stats(self) -> dict:
        """Return cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            return {
                "entries": len(self._cache),
                "max_entries": self._max,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / max(total, 1), 3),
            }


# Module-level singleton
parse_cache = ParseCache()

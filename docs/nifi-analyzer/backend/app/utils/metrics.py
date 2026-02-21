"""In-memory request metrics collector (singleton)."""

import time
from threading import Lock
from typing import Any


class MetricsCollector:
    """Tracks request counts, latencies, and error rates.

    Access the singleton via :data:`metrics`.
    """

    _instance: "MetricsCollector | None" = None
    _lock = Lock()

    def __new__(cls) -> "MetricsCollector":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init_state()
            return cls._instance

    def _init_state(self) -> None:
        self._start_time = time.monotonic()
        self._requests: dict[str, int] = {}
        self._errors: dict[str, int] = {}
        self._latencies: dict[str, list[float]] = {}
        self._total_requests = 0
        self._total_errors = 0
        self._data_lock = Lock()

    def record_request(self, endpoint: str, duration_ms: float, status_code: int) -> None:
        """Record a single request."""
        with self._data_lock:
            self._total_requests += 1
            self._requests[endpoint] = self._requests.get(endpoint, 0) + 1

            # Track latencies (keep last 1000 per endpoint)
            latencies = self._latencies.setdefault(endpoint, [])
            latencies.append(duration_ms)
            if len(latencies) > 1000:
                self._latencies[endpoint] = latencies[-1000:]

            if status_code >= 400:
                self._total_errors += 1
                self._errors[endpoint] = self._errors.get(endpoint, 0) + 1

    def get_metrics(self) -> dict[str, Any]:
        """Return a snapshot of all collected metrics."""
        with self._data_lock:
            uptime = time.monotonic() - self._start_time
            per_endpoint: dict[str, dict] = {}
            for ep, count in self._requests.items():
                latencies = self._latencies.get(ep, [])
                per_endpoint[ep] = {
                    "requests": count,
                    "errors": self._errors.get(ep, 0),
                    "avg_latency_ms": round(sum(latencies) / max(len(latencies), 1), 1),
                    "p95_latency_ms": round(_percentile(latencies, 0.95), 1) if latencies else 0,
                    "max_latency_ms": round(max(latencies), 1) if latencies else 0,
                }

            error_rate = self._total_errors / max(self._total_requests, 1)
            return {
                "uptime_seconds": round(uptime, 1),
                "total_requests": self._total_requests,
                "total_errors": self._total_errors,
                "error_rate": round(error_rate, 4),
                "endpoints": per_endpoint,
            }

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        self._init_state()


def _percentile(data: list[float], pct: float) -> float:
    """Compute an approximate percentile from a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


# Module-level singleton
metrics = MetricsCollector()

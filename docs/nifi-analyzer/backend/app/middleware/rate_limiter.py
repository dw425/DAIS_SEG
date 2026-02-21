"""In-memory token-bucket rate limiter middleware for FastAPI."""

import time
from threading import Lock

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


class _TokenBucket:
    """Simple token-bucket for a single client."""

    __slots__ = ("tokens", "last_refill", "capacity", "refill_rate")

    def __init__(self, capacity: float, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.tokens = capacity
        self.last_refill = time.monotonic()

    def consume(self) -> bool:
        """Try to consume one token. Returns True on success."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """Per-IP token-bucket rate limiter.

    Parameters
    ----------
    app : FastAPI / Starlette application
    requests_per_minute : int
        Maximum sustained request rate per client IP (default 60).
    """

    def __init__(self, app, requests_per_minute: int = 60) -> None:  # noqa: ANN001
        super().__init__(app)
        self._capacity = float(requests_per_minute)
        self._refill_rate = requests_per_minute / 60.0  # tokens per second
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = Lock()

    def _get_bucket(self, key: str) -> _TokenBucket:
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _TokenBucket(self._capacity, self._refill_rate)
                self._buckets[key] = bucket
                # Periodic pruning: cap total tracked IPs at 10 000
                if len(self._buckets) > 10_000:
                    oldest = next(iter(self._buckets))
                    del self._buckets[oldest]
            return bucket

    async def dispatch(self, request: Request, call_next) -> Response:  # noqa: ANN001
        client_ip = request.client.host if request.client else "unknown"
        bucket = self._get_bucket(client_ip)

        if not bucket.consume():
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Please retry later."},
                headers={"Retry-After": "60"},
            )

        return await call_next(request)

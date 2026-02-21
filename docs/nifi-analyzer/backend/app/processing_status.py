"""Global processing status tracker — allows health endpoint to report what's running."""

import threading
import time


class ProcessingStatus:
    """Thread-safe tracker for current backend processing state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current_step: str | None = None
        self._started_at: float | None = None
        self._last_progress_at: float | None = None
        self._processor_count: int = 0

    def start(self, step: str, processor_count: int = 0) -> None:
        with self._lock:
            self._current_step = step
            self._started_at = time.time()
            self._last_progress_at = time.time()
            self._processor_count = processor_count

    def tick(self) -> None:
        """Call periodically from long-running operations to indicate progress."""
        with self._lock:
            self._last_progress_at = time.time()

    def finish(self) -> None:
        with self._lock:
            self._current_step = None
            self._started_at = None
            self._last_progress_at = None
            self._processor_count = 0

    def status(self) -> dict:
        with self._lock:
            if not self._current_step:
                return {"active": False, "step": None, "elapsedSeconds": 0, "lastProgressSeconds": 0, "processorCount": 0}
            now = time.time()
            return {
                "active": True,
                "step": self._current_step,
                "elapsedSeconds": round(now - (self._started_at or now)),
                "lastProgressSeconds": round(now - (self._last_progress_at or now)),
                "processorCount": self._processor_count,
            }


# Singleton instance
processing_status = ProcessingStatus()

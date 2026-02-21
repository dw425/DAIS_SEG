"""Structured logging with error persistence for admin endpoint."""

import logging
from collections import deque
from datetime import datetime, timezone

# Ring buffer of recent errors for the /admin/logs endpoint
_error_buffer: deque[dict] = deque(maxlen=200)


class ErrorCapture(logging.Handler):
    """Logging handler that captures ERROR+ records into a ring buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        _error_buffer.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            }
        )


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger with structured format and error capture."""
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

    capture = ErrorCapture()
    capture.setLevel(logging.ERROR)
    capture.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(capture)


def get_recent_errors() -> list[dict]:
    """Return the most recent error log entries."""
    return list(_error_buffer)

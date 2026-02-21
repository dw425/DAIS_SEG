"""JSON-formatted structured logging and pipeline timing helpers."""

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("etl_pipeline")


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = str(record.exc_info[1])
        # Attach extra fields added by log_pipeline_event
        for key in ("step", "duration_ms", "status", "metadata"):
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val
        return json.dumps(entry, default=str)


def setup_json_logging(level: str = "INFO") -> None:
    """Add the JSON formatter to the root logger's stream handler."""
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def log_pipeline_event(
    step: str,
    duration_ms: float,
    status: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Emit a structured pipeline event at INFO level."""
    logger.info(
        "Pipeline step %s completed in %.1fms [%s]",
        step,
        duration_ms,
        status,
        extra={
            "step": step,
            "duration_ms": round(duration_ms, 1),
            "status": status,
            "metadata": metadata or {},
        },
    )


@contextmanager
def timed_step(step_name: str):
    """Context manager that logs the duration of a pipeline step.

    Usage::

        with timed_step("parse"):
            result = parse_flow(content, filename)
    """
    t0 = time.monotonic()
    status = "ok"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        elapsed = (time.monotonic() - t0) * 1000
        log_pipeline_event(step_name, elapsed, status)

"""Checkpoint manager — persist and restore pipeline session state.

Saves step results to /tmp/etl-checkpoints/{session_id}.json so that
long-running migration workflows can be resumed after interruption.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CHECKPOINT_DIR = Path("/tmp/etl-checkpoints")


def _ensure_dir() -> None:
    _CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def save_checkpoint(session_id: str, step: str, data: dict) -> None:
    """Save (or update) a checkpoint for the given session.

    Each checkpoint stores: session_id, timestamp, current_step,
    and pipeline_data (all step results accumulated so far).
    """
    _ensure_dir()
    path = _CHECKPOINT_DIR / f"{session_id}.json"

    existing: dict = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}

    pipeline_data = existing.get("pipeline_data", {})
    pipeline_data[step] = data

    checkpoint = {
        "session_id": session_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "current_step": step,
        "pipeline_data": pipeline_data,
    }

    path.write_text(json.dumps(checkpoint, indent=2, default=str))
    logger.info("Checkpoint saved: session=%s step=%s", session_id, step)


def load_checkpoint(session_id: str) -> dict | None:
    """Load a checkpoint by session_id. Returns None if not found."""
    path = _CHECKPOINT_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load checkpoint %s: %s", session_id, exc)
        return None


def list_checkpoints() -> list[dict]:
    """Return metadata for all saved checkpoints."""
    _ensure_dir()
    results: list[dict] = []

    for f in sorted(_CHECKPOINT_DIR.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            results.append({
                "session_id": data.get("session_id", f.stem),
                "timestamp": data.get("timestamp", ""),
                "current_step": data.get("current_step", ""),
                "steps_completed": list(data.get("pipeline_data", {}).keys()),
            })
        except (json.JSONDecodeError, OSError):
            continue

    return results


def delete_checkpoint(session_id: str) -> bool:
    """Delete a checkpoint file. Returns True if deleted, False if not found."""
    path = _CHECKPOINT_DIR / f"{session_id}.json"
    if path.exists():
        path.unlink()
        logger.info("Checkpoint deleted: session=%s", session_id)
        return True
    return False

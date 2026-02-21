"""WebSocket presence router — real-time user presence tracking."""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()
logger = logging.getLogger(__name__)

# Active connections: {ws_id: {"ws": WebSocket, "user": str, "color": str, "step": int}}
_connections: dict[str, dict] = {}

AVATAR_COLORS = [
    "#FF4B4B", "#21C354", "#EAB308", "#3B82F6", "#A855F7",
    "#EC4899", "#06B6D4", "#F97316", "#14B8A6", "#8B5CF6",
]


@router.websocket("/ws/presence")
async def presence_socket(ws: WebSocket):
    """WebSocket endpoint for real-time presence tracking."""
    await ws.accept()
    ws_id = str(uuid.uuid4())[:8]
    color = AVATAR_COLORS[len(_connections) % len(AVATAR_COLORS)]

    _connections[ws_id] = {
        "ws": ws,
        "user": f"User-{ws_id}",
        "color": color,
        "step": 0,
        "joined_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("Presence: %s connected (%d total)", ws_id, len(_connections))

    # Broadcast updated presence list
    await _broadcast_presence()

    try:
        while True:
            data = await ws.receive_json()
            # Update user state
            if "user" in data:
                _connections[ws_id]["user"] = data["user"]
            if "step" in data:
                _connections[ws_id]["step"] = data["step"]
            await _broadcast_presence()
    except WebSocketDisconnect:
        logger.info("Presence: %s disconnected", ws_id)
    except Exception as exc:
        logger.warning("Presence WS error: %s", exc)
    finally:
        _connections.pop(ws_id, None)
        await _broadcast_presence()


async def _broadcast_presence():
    """Send current presence list to all connected clients."""
    presence = [
        {
            "id": wid,
            "user": info["user"],
            "color": info["color"],
            "step": info["step"],
            "joinedAt": info["joined_at"],
        }
        for wid, info in _connections.items()
    ]
    payload = {"type": "presence", "users": presence}
    disconnected = []
    for wid, info in _connections.items():
        try:
            await info["ws"].send_json(payload)
        except Exception as exc:
            logger.debug("WebSocket send failed for %s: %s", wid, exc)
            disconnected.append(wid)
    for wid in disconnected:
        _connections.pop(wid, None)

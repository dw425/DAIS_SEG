"""Audit middleware — logs API calls to the audit_logs table."""

import json
import logging
import time

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.auth.jwt_handler import verify_token
from app.db.engine import SessionLocal
from app.db.models.audit import AuditLog

logger = logging.getLogger(__name__)

# Paths to skip auditing (high-frequency or non-meaningful)
_SKIP_PATHS = {"/api/health/detailed", "/api/health/mappings", "/api/admin/health", "/docs", "/openapi.json"}


class AuditMiddleware(BaseHTTPMiddleware):
    """Log all mutating API calls to the audit_logs table."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip non-mutating GETs and health endpoints
        path = request.url.path
        method = request.method

        if path in _SKIP_PATHS or path.startswith("/docs") or path.startswith("/openapi"):
            return await call_next(request)

        # Only audit mutating requests (POST, PUT, DELETE) and auth endpoints
        if method not in ("POST", "PUT", "DELETE", "PATCH"):
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        duration_ms = round((time.monotonic() - start) * 1000)

        # Extract user info from token (best-effort)
        user_id = None
        try:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                payload = verify_token(auth_header[7:])
                if payload:
                    user_id = payload.get("sub")
        except Exception:
            pass

        # Determine action and resource from path
        action = f"{method} {path}"
        resource_type = _extract_resource_type(path)
        resource_id = _extract_resource_id(path)
        ip_address = request.client.host if request.client else None

        details = json.dumps({
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "method": method,
            "path": path,
        })

        # Write to DB in a separate session (fire-and-forget)
        try:
            db = SessionLocal()
            audit_log = AuditLog(
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                details=details,
                ip_address=ip_address,
            )
            db.add(audit_log)
            db.commit()
            db.close()
        except Exception as exc:
            logger.warning("Failed to write audit log: %s", exc)

        return response


def _extract_resource_type(path: str) -> str | None:
    """Extract resource type from URL path (e.g., /api/projects/123 -> projects)."""
    parts = [p for p in path.split("/") if p and p != "api"]
    return parts[0] if parts else None


def _extract_resource_id(path: str) -> str | None:
    """Extract resource ID from URL path (e.g., /api/projects/123 -> 123)."""
    parts = [p for p in path.split("/") if p and p != "api"]
    return parts[1] if len(parts) > 1 else None

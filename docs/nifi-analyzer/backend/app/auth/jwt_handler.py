"""JWT token creation and verification using HMAC-SHA256 (stdlib only)."""

import base64
import hashlib
import hmac
import json
import logging
import os
import time

logger = logging.getLogger(__name__)

# Secret key — in production, set the ETL_JWT_SECRET environment variable.
# The hardcoded fallback is for local development only and MUST NOT be used in production.
_ENV = os.environ.get("ETL_ENV", "dev")
if _ENV != "dev" and "ETL_JWT_SECRET" not in os.environ:
    raise RuntimeError("ETL_JWT_SECRET must be set in production")
JWT_SECRET = os.environ.get("ETL_JWT_SECRET", "dev-secret-do-not-use-in-prod")
logger.info("JWT secret loaded (env=%s)", _ENV)
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = 3600  # 1 hour
JWT_REFRESH_EXPIRY_SECONDS = 86400 * 7  # 7 days


def _b64url_encode(data: bytes) -> str:
    """Base64url encode without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    """Base64url decode, adding padding as needed."""
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def _sign(header_b64: str, payload_b64: str) -> str:
    """Create HMAC-SHA256 signature."""
    message = f"{header_b64}.{payload_b64}".encode("ascii")
    sig = hmac.new(JWT_SECRET.encode("utf-8"), message, hashlib.sha256).digest()
    return _b64url_encode(sig)


def create_access_token(user_id: str, email: str, role: str, expires_in: int | None = None) -> str:
    """Create a JWT access token."""
    now = int(time.time())
    exp = now + (expires_in or JWT_EXPIRY_SECONDS)

    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "iat": now,
        "exp": exp,
    }

    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = _sign(header_b64, payload_b64)

    return f"{header_b64}.{payload_b64}.{signature}"


def create_refresh_token(user_id: str, email: str, role: str) -> str:
    """Create a longer-lived refresh token."""
    return create_access_token(user_id, email, role, expires_in=JWT_REFRESH_EXPIRY_SECONDS)


def verify_token(token: str) -> dict | None:
    """Verify and decode a JWT token. Returns the payload dict or None if invalid."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None

        header_b64, payload_b64, signature = parts

        # Verify signature
        expected_sig = _sign(header_b64, payload_b64)
        if not hmac.compare_digest(signature, expected_sig):
            return None

        # Decode payload
        payload = json.loads(_b64url_decode(payload_b64))

        # Check expiration
        if payload.get("exp", 0) < int(time.time()):
            return None

        return payload
    except Exception as exc:
        logger.debug("Token verification failed: %s", exc)
        return None

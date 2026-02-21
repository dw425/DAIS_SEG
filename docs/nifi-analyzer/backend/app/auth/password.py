"""Password hashing using stdlib hashlib.pbkdf2_hmac — no external deps needed."""

import hashlib
import os
import secrets


def hash_password(password: str) -> str:
    """Hash a password with PBKDF2-SHA256 and a random salt.

    Returns a string in the format: salt_hex$hash_hex
    """
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations=100_000)
    return f"{salt.hex()}${dk.hex()}"


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a stored hash."""
    try:
        salt_hex, hash_hex = hashed.split("$", 1)
        salt = bytes.fromhex(salt_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations=100_000)
        return secrets.compare_digest(dk.hex(), hash_hex)
    except (ValueError, AttributeError):
        return False

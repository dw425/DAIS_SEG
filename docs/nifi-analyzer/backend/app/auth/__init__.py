"""Authentication package — JWT, password hashing, RBAC."""

from app.auth.password import hash_password, verify_password
from app.auth.jwt_handler import create_access_token, verify_token
from app.auth.dependencies import get_current_user, get_optional_user
from app.auth.rbac import require_role

__all__ = [
    "hash_password",
    "verify_password",
    "create_access_token",
    "verify_token",
    "get_current_user",
    "get_optional_user",
    "require_role",
]

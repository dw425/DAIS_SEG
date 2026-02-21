"""Role-Based Access Control — roles and permissions."""

from fastapi import Depends, HTTPException, status

from app.auth.dependencies import get_current_user
from app.db.models.user import User

# Role hierarchy: admin > analyst > viewer
ROLES = {
    "admin": 3,
    "analyst": 2,
    "viewer": 1,
}

ROLE_PERMISSIONS = {
    "admin": ["read", "write", "delete", "admin", "manage_users", "view_audit", "manage_api_keys"],
    "analyst": ["read", "write", "delete"],
    "viewer": ["read"],
}


def require_role(*allowed_roles: str):
    """FastAPI dependency factory — raises 403 if the user's role is not in allowed_roles.

    Usage:
        @router.get("/admin/users", dependencies=[Depends(require_role("admin"))])
        async def list_users(...): ...
    """

    def _checker(user: User = Depends(get_current_user)) -> User:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{user.role}' is not authorized. Required: {', '.join(allowed_roles)}",
            )
        return user

    return _checker


def has_permission(user: User, permission: str) -> bool:
    """Check if a user's role grants a specific permission."""
    return permission in ROLE_PERMISSIONS.get(user.role, [])

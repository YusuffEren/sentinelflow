# =============================================================================
# SentinelFlow - Authentication Package
# =============================================================================
"""
JWT-based authentication and authorization.
"""

from sentinelflow.auth.service import AuthService
from sentinelflow.auth.dependencies import (
    get_current_user,
    get_current_active_user,
    require_role,
    require_admin,
    require_analyst,
)
from sentinelflow.auth.password import hash_password, verify_password

__all__ = [
    "AuthService",
    "get_current_user",
    "get_current_active_user",
    "require_role",
    "require_admin",
    "require_analyst",
    "hash_password",
    "verify_password",
]

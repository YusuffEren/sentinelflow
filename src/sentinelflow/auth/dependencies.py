# =============================================================================
# SentinelFlow - Auth Dependencies for FastAPI
# =============================================================================
"""
FastAPI dependencies for authentication and authorization.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from sentinelflow.api.deps import get_db_session
from sentinelflow.auth.service import AuthService
from sentinelflow.contracts import User, UserRole


# HTTP Bearer token scheme
security = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    session: Session = Depends(get_db_session),
) -> User | None:
    """
    Get current user from JWT token (optional auth).
    
    Returns None if no token or invalid token.
    """
    if not credentials:
        return None
    
    auth_service = AuthService(session)
    user = auth_service.get_user_from_token(credentials.credentials)
    
    return user


async def get_current_active_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    session: Session = Depends(get_db_session),
) -> User:
    """
    Get current active user (required auth).
    
    Raises 401 if not authenticated or user inactive.
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    auth_service = AuthService(session)
    user = auth_service.get_user_from_token(credentials.credentials)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is not active",
        )
    
    return user


def require_role(*roles: UserRole):
    """
    Dependency factory for role-based access control.
    
    Usage:
        @router.get("/admin-only")
        async def admin_endpoint(user: User = Depends(require_role(UserRole.ADMIN))):
            ...
    """
    async def role_checker(
        user: User = Depends(get_current_active_user),
    ) -> User:
        if user.role not in [r.value for r in roles]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required roles: {[r.value for r in roles]}",
            )
        return user
    
    return role_checker


# Convenience dependencies
require_admin = require_role(UserRole.ADMIN)
require_analyst = require_role(UserRole.ADMIN, UserRole.ANALYST)
require_viewer = require_role(UserRole.ADMIN, UserRole.ANALYST, UserRole.VIEWER)

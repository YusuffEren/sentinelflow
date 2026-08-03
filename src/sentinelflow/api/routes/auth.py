# =============================================================================
# SentinelFlow API - Authentication Routes
# =============================================================================
"""
Authentication endpoints: login, logout, register, token refresh.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPBearer

from sentinelflow.api.deps import get_db_session
from sentinelflow.auth.dependencies import get_current_active_user
from sentinelflow.auth.service import AuthService
from sentinelflow.contracts import (
    LoginRequest,
    TokenResponse,
    User,
    UserCreate,
    UserPublic,
)
from sentinelflow.contracts.user import PasswordChangeRequest, RefreshRequest

router = APIRouter(prefix="/auth", tags=["Authentication"])
security = HTTPBearer(auto_error=False)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="User login",
    description="Authenticate with username/email and password to receive JWT tokens.",
)
async def login(
    login_data: LoginRequest,
    request: Request,
    session=Depends(get_db_session),
):
    """Authenticate user and return tokens."""
    auth_service = AuthService(session)

    try:
        # Get client info
        user_agent = request.headers.get("user-agent")
        ip_address = request.client.host if request.client else None

        tokens = auth_service.login(
            username=login_data.username,
            password=login_data.password,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        session.commit()
        return tokens

    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=401, detail=str(e))


@router.post(
    "/logout",
    summary="User logout",
    description="Revoke refresh tokens to logout user.",
)
async def logout(
    user: User = Depends(get_current_active_user),
    refresh_token: str | None = None,
    session=Depends(get_db_session),
):
    """Logout user by revoking tokens."""
    auth_service = AuthService(session)
    auth_service.logout(user.user_id, refresh_token)
    session.commit()

    return {"message": "Logged out successfully"}


@router.post(
    "/register",
    response_model=UserPublic,
    summary="Register new user",
    description="Create a new user account. Default role is 'viewer'.",
)
async def register(
    user_data: UserCreate,
    session=Depends(get_db_session),
):
    """Register a new user."""
    auth_service = AuthService(session)

    try:
        user = auth_service.register(user_data)
        session.commit()

        return UserPublic(
            user_id=user.user_id,
            username=user.username,
            full_name=user.full_name,
            role=user.role,
            team=user.team,
        )

    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh access token",
    description="Get new access token using refresh token.",
)
async def refresh_tokens(
    refresh_data: RefreshRequest,
    request: Request,
    session=Depends(get_db_session),
):
    """Refresh access token."""
    auth_service = AuthService(session)

    try:
        user_agent = request.headers.get("user-agent")
        ip_address = request.client.host if request.client else None

        tokens = auth_service.refresh_tokens(
            refresh_token=refresh_data.refresh_token,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        session.commit()
        return tokens

    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=401, detail=str(e))


@router.get(
    "/me",
    response_model=User,
    summary="Get current user",
    description="Returns the currently authenticated user's profile.",
)
async def get_me(
    user: User = Depends(get_current_active_user),
):
    """Get current user profile."""
    return user


@router.post(
    "/change-password",
    summary="Change password",
    description="Change the current user's password.",
)
async def change_password(
    password_data: PasswordChangeRequest,
    user: User = Depends(get_current_active_user),
    session=Depends(get_db_session),
):
    """Change user password."""
    auth_service = AuthService(session)

    try:
        auth_service.change_password(
            user_id=user.user_id,
            current_password=password_data.current_password,
            new_password=password_data.new_password,
        )
        session.commit()

        return {"message": "Password changed successfully"}

    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))

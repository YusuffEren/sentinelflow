# =============================================================================
# SentinelFlow - Authentication Service
# =============================================================================
"""
Core authentication service for user login, token management.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from typing import Any

from jose import jwt, JWTError
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from loguru import logger

from sentinelflow.auth.config import auth_config
from sentinelflow.auth.password import hash_password, verify_password
from sentinelflow.database.models import UserModel, RefreshTokenModel
from sentinelflow.contracts import (
    User,
    UserCreate,
    UserRole,
    UserStatus,
    TokenResponse,
    TokenPayload,
)


class AuthService:
    """
    Authentication service.

    Handles:
    - User registration
    - Login/logout
    - Token creation and validation
    - Password management
    """

    def __init__(self, session: Session):
        self._session = session

    # =========================================================================
    # User Registration
    # =========================================================================

    def register(self, user_data: UserCreate) -> User:
        """
        Register a new user.

        Raises:
            ValueError: If username or email already exists
        """
        # Check existing username
        if self._get_user_by_username(user_data.username):
            raise ValueError(f"Username '{user_data.username}' already exists")

        # Check existing email
        if self._get_user_by_email(user_data.email):
            raise ValueError(f"Email '{user_data.email}' already registered")

        # Create user
        user_model = UserModel(
            username=user_data.username,
            email=user_data.email,
            password_hash=hash_password(user_data.password),
            full_name=user_data.full_name,
            role=user_data.role.value if isinstance(user_data.role, UserRole) else user_data.role,
            status="active",
            team=user_data.team,
        )

        self._session.add(user_model)
        self._session.flush()

        logger.info(f"User registered: {user_model.username} ({user_model.user_id})")

        return User.model_validate(user_model.to_dict())

    # =========================================================================
    # Login
    # =========================================================================

    def login(
        self,
        username: str,
        password: str,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> TokenResponse:
        """
        Authenticate user and return tokens.

        Args:
            username: Username or email
            password: Plain text password
            user_agent: Client user agent
            ip_address: Client IP

        Returns:
            TokenResponse with access and refresh tokens

        Raises:
            ValueError: Invalid credentials or account locked
        """
        # Find user by username or email
        user = self._get_user_by_username(username)
        if not user:
            user = self._get_user_by_email(username)

        if not user:
            raise ValueError("Invalid username or password")

        # Check if account is locked
        if user.locked_until and user.locked_until > datetime.now(timezone.utc):
            remaining = (user.locked_until - datetime.now(timezone.utc)).seconds // 60
            raise ValueError(f"Account locked. Try again in {remaining} minutes")

        # Check if account is active
        if user.status != "active":
            raise ValueError(f"Account is {user.status}")

        # Verify password
        if not verify_password(password, user.password_hash):
            self._handle_failed_login(user)
            raise ValueError("Invalid username or password")

        # Reset failed attempts on successful login
        self._reset_failed_attempts(user)

        # Update last login
        self._update_last_login(user)

        # Create tokens
        access_token = self._create_access_token(user)
        refresh_token = self._create_refresh_token(user, user_agent, ip_address)

        logger.info(f"User logged in: {user.username}")

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=auth_config.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        )

    def logout(self, user_id: str, refresh_token: str | None = None) -> bool:
        """
        Logout user by revoking tokens.

        If refresh_token provided, revoke only that token.
        Otherwise, revoke all tokens for the user.
        """
        if refresh_token:
            token_hash = self._hash_token(refresh_token)
            stmt = (
                update(RefreshTokenModel)
                .where(RefreshTokenModel.token_hash == token_hash)
                .values(is_revoked=True)
            )
        else:
            stmt = (
                update(RefreshTokenModel)
                .where(RefreshTokenModel.user_id == user_id)
                .values(is_revoked=True)
            )

        result = self._session.execute(stmt)
        self._session.flush()

        logger.info(f"User logged out: {user_id}")
        return result.rowcount > 0

    # =========================================================================
    # Token Management
    # =========================================================================

    def refresh_tokens(
        self,
        refresh_token: str,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> TokenResponse:
        """
        Refresh access token using refresh token.

        Raises:
            ValueError: Invalid or expired refresh token
        """
        # Find token in database
        token_hash = self._hash_token(refresh_token)
        stmt = select(RefreshTokenModel).where(
            RefreshTokenModel.token_hash == token_hash,
            RefreshTokenModel.is_revoked == False,
        )
        result = self._session.execute(stmt)
        token_model = result.scalar_one_or_none()

        if not token_model:
            raise ValueError("Invalid refresh token")

        if token_model.expires_at < datetime.now(timezone.utc):
            raise ValueError("Refresh token expired")

        # Get user
        user = self._get_user_by_id(token_model.user_id)
        if not user or user.status != "active":
            raise ValueError("User not found or inactive")

        # Revoke old refresh token
        token_model.is_revoked = True

        # Create new tokens
        access_token = self._create_access_token(user)
        new_refresh_token = self._create_refresh_token(user, user_agent, ip_address)

        self._session.flush()

        return TokenResponse(
            access_token=access_token,
            refresh_token=new_refresh_token,
            expires_in=auth_config.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        )

    def validate_token(self, token: str) -> TokenPayload | None:
        """
        Validate JWT access token.

        Returns:
            TokenPayload if valid, None otherwise
        """
        try:
            payload = jwt.decode(
                token,
                auth_config.SECRET_KEY,
                algorithms=[auth_config.ALGORITHM],
            )

            return TokenPayload(
                sub=payload["sub"],
                username=payload["username"],
                role=payload["role"],
                exp=payload["exp"],
                iat=payload["iat"],
                type=payload.get("type", "access"),
            )

        except JWTError as e:
            logger.debug(f"Token validation failed: {e}")
            return None

    def get_user_from_token(self, token: str) -> User | None:
        """Get user from access token."""
        payload = self.validate_token(token)
        if not payload:
            return None

        user = self._get_user_by_id(payload.sub)
        if not user or user.status != "active":
            return None

        return User.model_validate(user.to_dict())

    # =========================================================================
    # Password Management
    # =========================================================================

    def change_password(
        self,
        user_id: str,
        current_password: str,
        new_password: str,
    ) -> bool:
        """
        Change user password.

        Raises:
            ValueError: Invalid current password
        """
        user = self._get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")

        if not verify_password(current_password, user.password_hash):
            raise ValueError("Invalid current password")

        user.password_hash = hash_password(new_password)
        self._session.flush()

        # Revoke all refresh tokens
        self.logout(user_id)

        logger.info(f"Password changed for user: {user.username}")
        return True

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_user_by_id(self, user_id: str) -> UserModel | None:
        stmt = select(UserModel).where(UserModel.user_id == user_id)
        result = self._session.execute(stmt)
        return result.scalar_one_or_none()

    def _get_user_by_username(self, username: str) -> UserModel | None:
        stmt = select(UserModel).where(UserModel.username == username)
        result = self._session.execute(stmt)
        return result.scalar_one_or_none()

    def _get_user_by_email(self, email: str) -> UserModel | None:
        stmt = select(UserModel).where(UserModel.email == email)
        result = self._session.execute(stmt)
        return result.scalar_one_or_none()

    def _create_access_token(self, user: UserModel) -> str:
        """Create JWT access token."""
        now = datetime.now(timezone.utc)
        expire = now + auth_config.access_token_expire

        payload = {
            "sub": user.user_id,
            "username": user.username,
            "role": user.role,
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
            "type": "access",
        }

        return jwt.encode(payload, auth_config.SECRET_KEY, algorithm=auth_config.ALGORITHM)

    def _create_refresh_token(
        self,
        user: UserModel,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> str:
        """Create and store refresh token."""
        token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(token)

        expires_at = datetime.now(timezone.utc) + auth_config.refresh_token_expire

        refresh_model = RefreshTokenModel(
            user_id=user.user_id,
            token_hash=token_hash,
            expires_at=expires_at,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        self._session.add(refresh_model)
        return token

    def _hash_token(self, token: str) -> str:
        """Hash refresh token for storage."""
        return hashlib.sha256(token.encode()).hexdigest()

    def _handle_failed_login(self, user: UserModel) -> None:
        """Handle failed login attempt."""
        user.failed_login_attempts += 1

        if user.failed_login_attempts >= auth_config.MAX_LOGIN_ATTEMPTS:
            user.locked_until = datetime.now(timezone.utc) + timedelta(
                minutes=auth_config.LOCKOUT_DURATION_MINUTES
            )
            logger.warning(f"Account locked due to failed attempts: {user.username}")

        self._session.flush()

    def _reset_failed_attempts(self, user: UserModel) -> None:
        """Reset failed login counter."""
        user.failed_login_attempts = 0
        user.locked_until = None
        self._session.flush()

    def _update_last_login(self, user: UserModel) -> None:
        """Update last login timestamp."""
        user.last_login = datetime.now(timezone.utc)
        self._session.flush()

# =============================================================================
# SentinelFlow - User Contracts
# =============================================================================
"""
User and authentication schema definitions.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field, EmailStr, field_validator

from sentinelflow.contracts.base import ContractBase, utc_now, generate_id


class UserRole(str, Enum):
    """User roles for RBAC."""

    ADMIN = "admin"  # Full access, user management
    ANALYST = "analyst"  # Case management, alert handling
    VIEWER = "viewer"  # Read-only access
    API = "api"  # API-only access (service accounts)


class UserStatus(str, Enum):
    """User account status."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    PENDING = "pending"  # Awaiting email verification


# =============================================================================
# User Schemas
# =============================================================================


class UserCreate(ContractBase):
    """Schema for creating a new user."""

    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        pattern=r"^[a-zA-Z0-9_]+$",
        description="Username (alphanumeric + underscore)",
    )
    email: EmailStr = Field(..., description="Email address")
    password: str = Field(
        ...,
        min_length=8,
        max_length=100,
        description="Password (min 8 characters)",
    )
    full_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Full name",
    )
    role: UserRole = Field(default=UserRole.VIEWER)
    team: str | None = Field(default=None, max_length=100)

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Basic password validation."""
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")
        return v


class User(ContractBase):
    """Full user record."""

    user_id: str = Field(default_factory=lambda: generate_id("USR"))
    username: str
    email: str
    full_name: str
    role: UserRole = UserRole.VIEWER
    status: UserStatus = UserStatus.ACTIVE
    team: str | None = None

    # Timestamps
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    last_login: datetime | None = None

    # Settings
    preferences: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.status == UserStatus.ACTIVE

    @property
    def is_admin(self) -> bool:
        return self.role == UserRole.ADMIN


class UserPublic(ContractBase):
    """Public user info (no sensitive data)."""

    user_id: str
    username: str
    full_name: str
    role: UserRole
    team: str | None = None


class UserUpdate(ContractBase):
    """Schema for updating user."""

    full_name: str | None = None
    email: EmailStr | None = None
    role: UserRole | None = None
    status: UserStatus | None = None
    team: str | None = None
    preferences: dict[str, Any] | None = None


# =============================================================================
# Authentication Schemas
# =============================================================================


class LoginRequest(ContractBase):
    """Login request."""

    username: str = Field(..., description="Username or email")
    password: str = Field(..., description="Password")


class TokenResponse(ContractBase):
    """JWT token response."""

    access_token: str
    refresh_token: str | None = None
    token_type: str = "bearer"
    expires_in: int = Field(default=3600, description="Token expiry in seconds")


class TokenPayload(ContractBase):
    """JWT token payload."""

    sub: str  # user_id
    username: str
    role: str
    exp: int  # expiry timestamp
    iat: int  # issued at timestamp
    type: str = "access"  # access or refresh


class RefreshRequest(ContractBase):
    """Token refresh request."""

    refresh_token: str


class PasswordChangeRequest(ContractBase):
    """Password change request."""

    current_password: str
    new_password: str = Field(..., min_length=8)

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")
        return v

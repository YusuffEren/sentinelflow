# =============================================================================
# SentinelFlow - Auth Configuration
# =============================================================================
"""
Authentication configuration from environment variables.
"""

import os
from datetime import timedelta


class AuthConfig:
    """Authentication configuration."""

    # JWT Settings
    SECRET_KEY: str = os.getenv(
        "JWT_SECRET_KEY", "sentinelflow-super-secret-key-change-in-production-2026"
    )
    ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")

    # Token expiry
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("JWT_ACCESS_EXPIRE_MINUTES", "30"))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS", "7"))

    @property
    def access_token_expire(self) -> timedelta:
        return timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)

    @property
    def refresh_token_expire(self) -> timedelta:
        return timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)

    # Security settings
    MAX_LOGIN_ATTEMPTS: int = 5
    LOCKOUT_DURATION_MINUTES: int = 15


auth_config = AuthConfig()

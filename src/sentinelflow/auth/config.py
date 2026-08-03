# =============================================================================
# SentinelFlow - Auth Configuration
# =============================================================================
"""Authentication configuration from environment variables."""

from datetime import timedelta
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger


class AuthSettings(BaseSettings):
    """Authentication configuration loaded from environment / .env."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # JWT Settings
    JWT_SECRET_KEY: str = Field(
        default="",
        description="JWT signing key. MUST be set via environment or .env.",
    )
    JWT_ALGORITHM: str = Field(default="HS256")
    JWT_ACCESS_EXPIRE_MINUTES: int = Field(default=30)
    JWT_REFRESH_EXPIRE_DAYS: int = Field(default=7)

    # Convenience aliases used throughout the auth module
    @property
    def SECRET_KEY(self) -> str:
        return self.JWT_SECRET_KEY

    @property
    def ALGORITHM(self) -> str:
        return self.JWT_ALGORITHM

    @property
    def ACCESS_TOKEN_EXPIRE_MINUTES(self) -> int:
        return self.JWT_ACCESS_EXPIRE_MINUTES

    @property
    def REFRESH_TOKEN_EXPIRE_DAYS(self) -> int:
        return self.JWT_REFRESH_EXPIRE_DAYS

    @property
    def access_token_expire(self) -> timedelta:
        return timedelta(minutes=self.JWT_ACCESS_EXPIRE_MINUTES)

    @property
    def refresh_token_expire(self) -> timedelta:
        return timedelta(days=self.JWT_REFRESH_EXPIRE_DAYS)

    # Security settings
    MAX_LOGIN_ATTEMPTS: int = 5
    LOCKOUT_DURATION_MINUTES: int = 15


@lru_cache
def _get_auth_config() -> AuthSettings:
    return AuthSettings()


auth_config = _get_auth_config()

if not auth_config.JWT_SECRET_KEY:
    logger.warning(
        "JWT_SECRET_KEY is empty – using insecure default for local development. "
        "Set JWT_SECRET_KEY in .env before deploying."
    )
    # Provide a dev-only fallback so the app can still start locally
    object.__setattr__(auth_config, "JWT_SECRET_KEY", "dev-only-insecure-key-do-not-deploy")

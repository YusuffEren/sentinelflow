# =============================================================================
# SentinelFlow - Rate Limiting
# =============================================================================
"""
API rate limiting for SentinelFlow.

Provides:
- Per-IP rate limiting
- Per-user rate limiting
- Endpoint-specific limits
- Sliding window algorithm
- Integration with FastAPI

Example:
    >>> limiter = RateLimiter()
    >>> allowed, retry_after = limiter.check("192.168.1.1", "submit_transaction")
    >>> if not allowed:
    ...     raise HTTPException(429, headers={"Retry-After": str(retry_after)})
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger


# =============================================================================
# Rate Limit Configuration
# =============================================================================


@dataclass
class RateLimitConfig:
    """Rate limit configuration for an endpoint."""

    requests: int  # Max requests allowed
    window_seconds: int  # Time window in seconds
    burst: int = 0  # Additional burst allowance

    @property
    def per_second(self) -> float:
        return self.requests / self.window_seconds


# Default rate limits
DEFAULT_LIMITS = {
    "default": RateLimitConfig(requests=100, window_seconds=60),
    "submit_transaction": RateLimitConfig(requests=1000, window_seconds=60),
    "get_alerts": RateLimitConfig(requests=200, window_seconds=60),
    "predict": RateLimitConfig(requests=500, window_seconds=60),
    "login": RateLimitConfig(requests=5, window_seconds=60),
    "register": RateLimitConfig(requests=3, window_seconds=3600),
    "websocket": RateLimitConfig(requests=10, window_seconds=60),
}


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class RateLimitState:
    """State for a rate limit bucket."""

    tokens: float
    last_update: float
    request_count: int = 0


@dataclass
class RateLimitResult:
    """Result of rate limit check."""

    allowed: bool
    remaining: int
    limit: int
    reset_after: float  # Seconds until limit resets
    retry_after: float | None = None  # Seconds to wait if blocked

    def to_headers(self) -> dict[str, str]:
        """Generate rate limit headers."""
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(int(self.reset_after)),
        }
        if self.retry_after is not None:
            headers["Retry-After"] = str(int(self.retry_after))
        return headers


# =============================================================================
# Rate Limiter
# =============================================================================


class RateLimiter:
    """
    Token bucket rate limiter with sliding window.

    Example:
        >>> limiter = RateLimiter()
        >>> result = limiter.check("192.168.1.1", "submit_transaction")
        >>> if not result.allowed:
        ...     print(f"Rate limited. Retry in {result.retry_after}s")
    """

    def __init__(
        self,
        limits: dict[str, RateLimitConfig] | None = None,
        default_limit: RateLimitConfig | None = None,
    ):
        """
        Initialize rate limiter.

        Args:
            limits: Endpoint-specific limits
            default_limit: Default limit for unlisted endpoints
        """
        self._limits = {**DEFAULT_LIMITS, **(limits or {})}
        self._default_limit = default_limit or self._limits.get(
            "default", RateLimitConfig(requests=100, window_seconds=60)
        )

        # State storage: {client_key: {endpoint: RateLimitState}}
        self._state: dict[str, dict[str, RateLimitState]] = defaultdict(dict)

        logger.info("RateLimiter initialized")

    def check(
        self,
        client_key: str,
        endpoint: str = "default",
        cost: int = 1,
    ) -> RateLimitResult:
        """
        Check if request is allowed.

        Args:
            client_key: Client identifier (IP, user ID, etc.)
            endpoint: Endpoint name
            cost: Request cost (usually 1)

        Returns:
            RateLimitResult with allowed status
        """
        limit = self._limits.get(endpoint, self._default_limit)
        now = time.time()

        # Get or create state
        if endpoint not in self._state[client_key]:
            self._state[client_key][endpoint] = RateLimitState(
                tokens=float(limit.requests + limit.burst),
                last_update=now,
            )

        state = self._state[client_key][endpoint]

        # Refill tokens based on time elapsed
        elapsed = now - state.last_update
        refill = elapsed * limit.per_second
        state.tokens = min(limit.requests + limit.burst, state.tokens + refill)
        state.last_update = now

        # Check if request allowed
        if state.tokens >= cost:
            state.tokens -= cost
            state.request_count += 1

            return RateLimitResult(
                allowed=True,
                remaining=int(state.tokens),
                limit=limit.requests,
                reset_after=limit.window_seconds,
            )
        else:
            # Calculate retry time
            tokens_needed = cost - state.tokens
            retry_after = tokens_needed / limit.per_second

            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=limit.requests,
                reset_after=limit.window_seconds,
                retry_after=retry_after,
            )

    def reset(self, client_key: str, endpoint: str | None = None) -> None:
        """Reset rate limit state for a client."""
        if endpoint:
            if client_key in self._state and endpoint in self._state[client_key]:
                del self._state[client_key][endpoint]
        else:
            if client_key in self._state:
                del self._state[client_key]

    def get_limit(self, endpoint: str) -> RateLimitConfig:
        """Get rate limit config for endpoint."""
        return self._limits.get(endpoint, self._default_limit)

    def set_limit(self, endpoint: str, limit: RateLimitConfig) -> None:
        """Set rate limit for endpoint."""
        self._limits[endpoint] = limit
        logger.info(f"Rate limit updated: {endpoint} = {limit.requests}/{limit.window_seconds}s")

    def cleanup_old_entries(self, max_age_seconds: int = 3600) -> int:
        """Remove old entries to prevent memory growth."""
        now = time.time()
        removed = 0

        clients_to_remove = []

        for client_key, endpoints in self._state.items():
            endpoints_to_remove = []

            for endpoint, state in endpoints.items():
                if now - state.last_update > max_age_seconds:
                    endpoints_to_remove.append(endpoint)

            for endpoint in endpoints_to_remove:
                del endpoints[endpoint]
                removed += 1

            if not endpoints:
                clients_to_remove.append(client_key)

        for client_key in clients_to_remove:
            del self._state[client_key]

        return removed


# =============================================================================
# FastAPI Integration
# =============================================================================


def create_fastapi_rate_limiter(
    limiter: RateLimiter,
    key_func: Callable | None = None,
):
    """
    Create FastAPI middleware for rate limiting.

    Args:
        limiter: RateLimiter instance
        key_func: Function to extract client key from request
    """
    from fastapi import Request, Response, HTTPException
    from starlette.middleware.base import BaseHTTPMiddleware

    class RateLimitMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            # Extract client key
            if key_func:
                client_key = key_func(request)
            else:
                # Default: use client IP
                client_key = request.client.host if request.client else "unknown"

            # Get endpoint name
            endpoint = request.url.path.strip("/").replace("/", "_") or "default"

            # Check rate limit
            result = limiter.check(client_key, endpoint)

            if not result.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers=result.to_headers(),
                )

            # Process request
            response: Response = await call_next(request)

            # Add rate limit headers
            for header, value in result.to_headers().items():
                response.headers[header] = value

            return response

    return RateLimitMiddleware


def create_rate_limit_dependency(
    limiter: RateLimiter,
    endpoint: str = "default",
):
    """Create a FastAPI dependency for rate limiting specific endpoints."""
    from fastapi import Request, HTTPException, Depends

    async def rate_limit_check(request: Request):
        client_key = request.client.host if request.client else "unknown"
        result = limiter.check(client_key, endpoint)

        if not result.allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Retry in {result.retry_after:.0f} seconds.",
                headers=result.to_headers(),
            )

        return result

    return rate_limit_check

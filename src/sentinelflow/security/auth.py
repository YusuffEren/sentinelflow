# =============================================================================
# SentinelFlow - Authentication Module
# =============================================================================
"""
JWT-based authentication for SentinelFlow API.

Features:
- JWT access and refresh tokens
- Role-based access control (RBAC)
- API key authentication
- Password hashing
- Token blacklisting

Example:
    >>> auth = AuthManager(secret_key="your-secret")
    >>> token = auth.create_access_token(user_id="user1", roles=["analyst"])
    >>> user = auth.verify_token(token)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

from loguru import logger

try:
    from jose import JWTError, jwt
    from passlib.context import CryptContext
    HAS_AUTH_LIBS = True
except ImportError:
    HAS_AUTH_LIBS = False
    logger.warning("jose/passlib not available, auth disabled")


# =============================================================================
# Enums and Constants
# =============================================================================

class UserRole(str, Enum):
    """User roles for RBAC."""
    
    ADMIN = "admin"
    COMPLIANCE_OFFICER = "compliance_officer"
    ANALYST = "analyst"
    VIEWER = "viewer"
    API_CLIENT = "api_client"


class TokenType(str, Enum):
    """Token types."""
    
    ACCESS = "access"
    REFRESH = "refresh"
    API_KEY = "api_key"


# Default settings
DEFAULT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
DEFAULT_REFRESH_TOKEN_EXPIRE_DAYS = 7
DEFAULT_ALGORITHM = "HS256"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class User:
    """User model for authentication."""
    
    user_id: str
    username: str
    email: str = ""
    full_name: str = ""
    roles: list[UserRole] = field(default_factory=list)
    is_active: bool = True
    is_superuser: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_login: str | None = None
    
    @property
    def is_admin(self) -> bool:
        return UserRole.ADMIN in self.roles or self.is_superuser
    
    @property
    def is_compliance_officer(self) -> bool:
        return UserRole.COMPLIANCE_OFFICER in self.roles or self.is_admin
    
    def has_role(self, role: UserRole) -> bool:
        return role in self.roles or self.is_admin
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "roles": [r.value for r in self.roles],
            "is_active": self.is_active,
            "is_superuser": self.is_superuser,
        }


@dataclass
class TokenData:
    """Decoded token data."""
    
    user_id: str
    username: str
    roles: list[str]
    token_type: TokenType
    exp: datetime
    iat: datetime
    jti: str = ""  # JWT ID for blacklisting
    
    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.exp


# =============================================================================
# Auth Manager
# =============================================================================

class AuthManager:
    """
    JWT authentication manager.
    
    Handles:
    - Token creation and verification
    - Password hashing
    - Token blacklisting
    
    Example:
        >>> auth = AuthManager()
        >>> token = auth.create_access_token(user_id="user1", username="john")
        >>> data = auth.verify_token(token)
    """
    
    def __init__(
        self,
        secret_key: str | None = None,
        algorithm: str = DEFAULT_ALGORITHM,
        access_token_expire_minutes: int = DEFAULT_ACCESS_TOKEN_EXPIRE_MINUTES,
        refresh_token_expire_days: int = DEFAULT_REFRESH_TOKEN_EXPIRE_DAYS,
    ):
        """
        Initialize auth manager.
        
        Args:
            secret_key: JWT secret key (from env if not provided)
            algorithm: JWT algorithm
            access_token_expire_minutes: Access token TTL
            refresh_token_expire_days: Refresh token TTL
        """
        self._secret_key = secret_key or os.getenv("JWT_SECRET_KEY", "dev-secret-change-me")
        self._algorithm = algorithm
        self._access_expire = timedelta(minutes=access_token_expire_minutes)
        self._refresh_expire = timedelta(days=refresh_token_expire_days)
        
        # Password hashing
        if HAS_AUTH_LIBS:
            self._pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        else:
            self._pwd_context = None
        
        # Token blacklist (in production, use Redis)
        self._blacklist: set[str] = set()
        
        # User store (in production, use database)
        self._users: dict[str, dict] = {}
        
        logger.info("AuthManager initialized")
    
    # =========================================================================
    # Password Hashing
    # =========================================================================
    
    def hash_password(self, password: str) -> str:
        """Hash a password."""
        if not HAS_AUTH_LIBS or self._pwd_context is None:
            return password  # Fallback (NOT SECURE)
        
        return self._pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against hash."""
        if not HAS_AUTH_LIBS or self._pwd_context is None:
            return plain_password == hashed_password  # Fallback (NOT SECURE)
        
        return self._pwd_context.verify(plain_password, hashed_password)
    
    # =========================================================================
    # Token Creation
    # =========================================================================
    
    def create_access_token(
        self,
        user_id: str,
        username: str,
        roles: list[str] | None = None,
        expires_delta: timedelta | None = None,
    ) -> str:
        """
        Create an access token.
        
        Args:
            user_id: User ID
            username: Username
            roles: User roles
            expires_delta: Custom expiration
        
        Returns:
            JWT token string
        """
        if not HAS_AUTH_LIBS:
            return "auth-disabled"
        
        import uuid
        
        expire = datetime.now(timezone.utc) + (expires_delta or self._access_expire)
        
        payload = {
            "sub": user_id,
            "username": username,
            "roles": roles or [],
            "type": TokenType.ACCESS.value,
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }
        
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)
    
    def create_refresh_token(
        self,
        user_id: str,
        username: str,
        expires_delta: timedelta | None = None,
    ) -> str:
        """Create a refresh token."""
        if not HAS_AUTH_LIBS:
            return "auth-disabled"
        
        import uuid
        
        expire = datetime.now(timezone.utc) + (expires_delta or self._refresh_expire)
        
        payload = {
            "sub": user_id,
            "username": username,
            "type": TokenType.REFRESH.value,
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }
        
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)
    
    def create_api_key(
        self,
        client_id: str,
        client_name: str,
        expires_days: int = 365,
    ) -> str:
        """Create an API key for service-to-service auth."""
        if not HAS_AUTH_LIBS:
            return "auth-disabled"
        
        import uuid
        
        expire = datetime.now(timezone.utc) + timedelta(days=expires_days)
        
        payload = {
            "sub": client_id,
            "username": client_name,
            "roles": [UserRole.API_CLIENT.value],
            "type": TokenType.API_KEY.value,
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }
        
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)
    
    # =========================================================================
    # Token Verification
    # =========================================================================
    
    def verify_token(self, token: str) -> TokenData | None:
        """
        Verify and decode a token.
        
        Args:
            token: JWT token string
        
        Returns:
            TokenData if valid, None otherwise
        """
        if not HAS_AUTH_LIBS:
            return None
        
        try:
            payload = jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
            )
            
            # Check blacklist
            jti = payload.get("jti", "")
            if jti and jti in self._blacklist:
                logger.warning(f"Blacklisted token used: {jti[:8]}...")
                return None
            
            return TokenData(
                user_id=payload.get("sub", ""),
                username=payload.get("username", ""),
                roles=payload.get("roles", []),
                token_type=TokenType(payload.get("type", "access")),
                exp=datetime.fromtimestamp(payload.get("exp", 0), tz=timezone.utc),
                iat=datetime.fromtimestamp(payload.get("iat", 0), tz=timezone.utc),
                jti=jti,
            )
            
        except JWTError as e:
            logger.warning(f"Token verification failed: {e}")
            return None
    
    def refresh_access_token(self, refresh_token: str) -> str | None:
        """
        Get new access token using refresh token.
        
        Args:
            refresh_token: Valid refresh token
        
        Returns:
            New access token or None
        """
        token_data = self.verify_token(refresh_token)
        
        if not token_data:
            return None
        
        if token_data.token_type != TokenType.REFRESH:
            logger.warning("Non-refresh token used for refresh")
            return None
        
        if token_data.is_expired:
            return None
        
        return self.create_access_token(
            user_id=token_data.user_id,
            username=token_data.username,
            roles=token_data.roles,
        )
    
    # =========================================================================
    # Token Blacklisting
    # =========================================================================
    
    def blacklist_token(self, token: str) -> bool:
        """
        Add a token to the blacklist (logout).
        
        Args:
            token: Token to blacklist
        
        Returns:
            True if blacklisted successfully
        """
        token_data = self.verify_token(token)
        
        if token_data and token_data.jti:
            self._blacklist.add(token_data.jti)
            logger.info(f"Token blacklisted: {token_data.jti[:8]}...")
            return True
        
        return False
    
    # =========================================================================
    # User Management (Demo)
    # =========================================================================
    
    def create_user(
        self,
        username: str,
        password: str,
        email: str = "",
        full_name: str = "",
        roles: list[UserRole] | None = None,
    ) -> User:
        """Create a new user (demo - use database in production)."""
        import uuid
        
        user_id = str(uuid.uuid4())
        hashed_password = self.hash_password(password)
        
        self._users[username] = {
            "user_id": user_id,
            "username": username,
            "email": email,
            "full_name": full_name,
            "hashed_password": hashed_password,
            "roles": roles or [UserRole.VIEWER],
            "is_active": True,
        }
        
        return User(
            user_id=user_id,
            username=username,
            email=email,
            full_name=full_name,
            roles=roles or [UserRole.VIEWER],
        )
    
    def authenticate_user(self, username: str, password: str) -> User | None:
        """Authenticate a user by username and password."""
        user_data = self._users.get(username)
        
        if not user_data:
            return None
        
        if not self.verify_password(password, user_data["hashed_password"]):
            return None
        
        if not user_data.get("is_active", True):
            return None
        
        return User(
            user_id=user_data["user_id"],
            username=user_data["username"],
            email=user_data.get("email", ""),
            full_name=user_data.get("full_name", ""),
            roles=user_data.get("roles", []),
        )
    
    # =========================================================================
    # Authorization
    # =========================================================================
    
    def require_role(self, token_data: TokenData, required_role: UserRole) -> bool:
        """Check if token has required role."""
        if UserRole.ADMIN.value in token_data.roles:
            return True  # Admin has all roles
        
        return required_role.value in token_data.roles
    
    def require_any_role(self, token_data: TokenData, roles: list[UserRole]) -> bool:
        """Check if token has any of the required roles."""
        return any(self.require_role(token_data, role) for role in roles)


# =============================================================================
# FastAPI Integration
# =============================================================================

def create_fastapi_auth_dependencies(auth_manager: AuthManager):
    """Create FastAPI dependency functions for authentication."""
    from fastapi import Depends, HTTPException, status
    from fastapi.security import OAuth2PasswordBearer
    
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")
    
    async def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenData:
        """Get current authenticated user from token."""
        token_data = auth_manager.verify_token(token)
        
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        if token_data.is_expired:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        return token_data
    
    def require_roles(*roles: UserRole):
        """Dependency to require specific roles."""
        async def check_roles(user: TokenData = Depends(get_current_user)):
            if not auth_manager.require_any_role(user, list(roles)):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions",
                )
            return user
        return check_roles
    
    return {
        "get_current_user": get_current_user,
        "require_roles": require_roles,
        "oauth2_scheme": oauth2_scheme,
    }

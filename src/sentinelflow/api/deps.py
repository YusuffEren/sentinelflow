# =============================================================================
# SentinelFlow API - Dependencies
# =============================================================================
"""
FastAPI dependency injection utilities.
"""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from sentinelflow.database.postgres import get_session


def get_db_session() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session.

    Yields a session and ensures it's closed after the request.
    """
    session = get_session()
    try:
        yield session
    finally:
        session.close()


# =============================================================================
# Future: Auth dependencies
# =============================================================================

# async def get_current_user(token: str = Depends(oauth2_scheme)):
#     """Get current authenticated user from JWT token."""
#     credentials_exception = HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Could not validate credentials",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         username: str = payload.get("sub")
#         if username is None:
#             raise credentials_exception
#     except JWTError:
#         raise credentials_exception
#
#     user = get_user(username)
#     if user is None:
#         raise credentials_exception
#     return user

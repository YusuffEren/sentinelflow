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

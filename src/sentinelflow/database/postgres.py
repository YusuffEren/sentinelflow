# =============================================================================
# SentinelFlow - PostgreSQL Connection
# =============================================================================
"""
PostgreSQL database connection management.

Uses SQLAlchemy 2.0 with both sync and async support.
"""

from __future__ import annotations

import os
from contextlib import contextmanager, asynccontextmanager
from typing import Generator, AsyncGenerator

from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from loguru import logger


def get_database_url(async_driver: bool = False) -> str:
    """
    Build database URL from environment variables.

    Environment variables:
        POSTGRES_HOST: Database host (default: localhost)
        POSTGRES_PORT: Database port (default: 5432)
        POSTGRES_USER: Database user (default: sentinelflow)
        POSTGRES_PASSWORD: Database password (default: sentinelflow_secret)
        POSTGRES_DB: Database name (default: sentinelflow)
        DATABASE_URL: Full URL override (takes precedence)
    """
    # Check for full URL override
    full_url = os.getenv("DATABASE_URL")
    if full_url:
        if async_driver and "postgresql://" in full_url:
            return full_url.replace("postgresql://", "postgresql+asyncpg://")
        return full_url

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "sentinelflow")
    password = os.getenv("POSTGRES_PASSWORD", "sentinelflow_secret")
    database = os.getenv("POSTGRES_DB", "sentinelflow")

    if async_driver:
        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


# =============================================================================
# Sync Engine & Session
# =============================================================================

_engine = None
_session_factory = None


def get_engine():
    """Get or create SQLAlchemy engine (singleton)."""
    global _engine
    if _engine is None:
        url = get_database_url(async_driver=False)
        _engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_pre_ping=True,
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        )
        logger.info(
            f"PostgreSQL engine created: {url.split('@')[1] if '@' in url else 'configured'}"
        )
    return _engine


def get_session_factory():
    """Get or create session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _session_factory


def get_session() -> Session:
    """Create a new database session."""
    return get_session_factory()()


@contextmanager
def DatabaseSession() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Usage:
        with DatabaseSession() as db:
            db.query(AlertModel).all()
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# =============================================================================
# Async Engine & Session
# =============================================================================

_async_engine = None
_async_session_factory = None


def get_async_engine():
    """Get or create async SQLAlchemy engine (singleton)."""
    global _async_engine
    if _async_engine is None:
        url = get_database_url(async_driver=True)
        _async_engine = create_async_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        )
        logger.info("Async PostgreSQL engine created")
    return _async_engine


def get_async_session_factory():
    """Get or create async session factory."""
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            bind=get_async_engine(),
            class_=AsyncSession,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _async_session_factory


async def get_async_session() -> AsyncSession:
    """Create a new async database session."""
    return get_async_session_factory()()


@asynccontextmanager
async def AsyncDatabaseSession() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database sessions.

    Usage:
        async with AsyncDatabaseSession() as db:
            result = await db.execute(select(AlertModel))
    """
    session = await get_async_session()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


# =============================================================================
# Database Initialization
# =============================================================================


def init_db(drop_all: bool = False) -> None:
    """
    Initialize database tables.

    Args:
        drop_all: If True, drop all tables before creating (DANGER!)
    """
    from sentinelflow.database.models import Base

    engine = get_engine()

    if drop_all:
        logger.warning("Dropping all tables!")
        Base.metadata.drop_all(bind=engine)

    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified")


async def init_db_async(drop_all: bool = False) -> None:
    """Async version of init_db."""
    from sentinelflow.database.models import Base

    engine = get_async_engine()

    async with engine.begin() as conn:
        if drop_all:
            logger.warning("Dropping all tables!")
            await conn.run_sync(Base.metadata.drop_all)

        await conn.run_sync(Base.metadata.create_all)

    logger.info("Database tables created/verified (async)")

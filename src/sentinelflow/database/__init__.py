# =============================================================================
# SentinelFlow - Database Package
# =============================================================================
"""
Database connectors and ORM models.

Supports:
- PostgreSQL (primary store for alerts, cases, audit)
- Neo4j (graph database for ring detection)
- Redis (geo-spatial cache, rate limiting)
"""

from sentinelflow.database.models import (
    AlertModel,
    Base,
    CaseEventModel,
    CaseModel,
    ModelVersionModel,
    RefreshTokenModel,
    TransactionSummaryModel,
    UserModel,
)
from sentinelflow.database.postgres import (
    AsyncDatabaseSession,
    DatabaseSession,
    get_async_session,
    get_engine,
    get_session,
    init_db,
)

__all__ = [
    # Connection
    "get_engine",
    "get_session",
    "get_async_session",
    "init_db",
    "DatabaseSession",
    "AsyncDatabaseSession",
    # Models
    "Base",
    "AlertModel",
    "CaseModel",
    "CaseEventModel",
    "TransactionSummaryModel",
    "ModelVersionModel",
    "UserModel",
    "RefreshTokenModel",
]

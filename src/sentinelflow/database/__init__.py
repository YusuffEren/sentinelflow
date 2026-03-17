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

from sentinelflow.database.postgres import (
    get_engine,
    get_session,
    get_async_session,
    init_db,
    DatabaseSession,
    AsyncDatabaseSession,
)
from sentinelflow.database.models import (
    Base,
    AlertModel,
    CaseModel,
    CaseEventModel,
    TransactionSummaryModel,
    ModelVersionModel,
    UserModel,
    RefreshTokenModel,
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

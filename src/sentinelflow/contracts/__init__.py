# =============================================================================
# SentinelFlow - Contract Schemas (Single Source of Truth)
# =============================================================================
"""
Central schema definitions used across all SentinelFlow services.

This module defines the canonical data contracts for:
- Transactions
- Alerts  
- Cases
- Events (audit log)

All services (API, Detector, Dashboard) MUST use these schemas to ensure
consistency across Kafka messages, database records, and API responses.

Schema versioning: All messages include `schema_version` field.
Current version: 1.0.0
"""

from sentinelflow.contracts.enums import (
    FraudType,
    Severity,
    CaseStatus,
    CasePriority,
    EventType,
)
from sentinelflow.contracts.transaction import (
    TransactionCreate,
    TransactionSummary,
    TransactionKafkaMessage,
)
from sentinelflow.contracts.alert import (
    AlertCreate,
    Alert,
    AlertKafkaMessage,
    Evidence,
)
from sentinelflow.contracts.case import (
    CaseCreate,
    Case,
    CaseEvent,
)
from sentinelflow.contracts.user import (
    UserRole,
    UserStatus,
    UserCreate,
    User,
    UserPublic,
    UserUpdate,
    LoginRequest,
    TokenResponse,
    TokenPayload,
)
from sentinelflow.contracts.base import SCHEMA_VERSION

__all__ = [
    # Version
    "SCHEMA_VERSION",
    # Enums
    "FraudType",
    "Severity", 
    "CaseStatus",
    "CasePriority",
    "EventType",
    # Transaction
    "TransactionCreate",
    "TransactionSummary",
    "TransactionKafkaMessage",
    # Alert
    "AlertCreate",
    "Alert",
    "AlertKafkaMessage",
    "Evidence",
    # Case
    "CaseCreate",
    "Case",
    "CaseEvent",
    # User & Auth
    "UserRole",
    "UserStatus",
    "UserCreate",
    "User",
    "UserPublic",
    "UserUpdate",
    "LoginRequest",
    "TokenResponse",
    "TokenPayload",
]

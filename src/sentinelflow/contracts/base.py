# =============================================================================
# SentinelFlow - Base Contract Definitions
# =============================================================================
"""
Base classes and utilities for contract schemas.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Current schema version - increment on breaking changes
SCHEMA_VERSION = "1.0.0"


class ContractBase(BaseModel):
    """Base class for all contract schemas."""

    model_config = ConfigDict(
        from_attributes=True,  # Enable ORM mode for SQLAlchemy
        populate_by_name=True,
        use_enum_values=True,
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None,
        },
    )


class KafkaMessageBase(ContractBase):
    """Base class for Kafka messages with versioning."""

    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Schema version for compatibility checking",
    )
    produced_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when message was produced",
    )

    def to_kafka_dict(self) -> dict[str, Any]:
        """Serialize for Kafka with proper datetime handling."""
        data = self.model_dump(mode="json")
        return data


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def generate_id(prefix: str) -> str:
    """Generate a unique ID with prefix."""
    from uuid import uuid4

    return f"{prefix}-{uuid4().hex[:12].upper()}"

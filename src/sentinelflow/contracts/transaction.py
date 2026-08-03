# =============================================================================
# SentinelFlow - Transaction Contracts
# =============================================================================
"""
Transaction schema definitions.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from sentinelflow.contracts.base import (
    ContractBase,
    KafkaMessageBase,
    generate_id,
    utc_now,
)


class TransactionCreate(ContractBase):
    """
    Schema for creating/submitting a transaction.
    Used by API endpoints and Kafka producers.
    """

    transaction_id: str | None = Field(
        default=None,
        description="Unique transaction ID (auto-generated if empty)",
        examples=["TXN-A1B2C3D4E5F6"],
    )

    # Sender info
    sender_iban: str = Field(
        ...,
        min_length=15,
        max_length=34,
        description="Sender IBAN",
        examples=["TR330006100519786457841326"],
    )
    sender_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Sender full name",
        examples=["Ahmet Yılmaz"],
    )
    sender_city: str = Field(
        default="",
        max_length=100,
        description="Sender city",
        examples=["İstanbul"],
    )

    # Receiver info
    receiver_iban: str = Field(
        ...,
        min_length=15,
        max_length=34,
        description="Receiver IBAN",
        examples=["TR110006400000468521793064"],
    )
    receiver_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Receiver full name",
        examples=["Mehmet Demir"],
    )
    receiver_city: str = Field(
        default="",
        max_length=100,
        description="Receiver city",
        examples=["Ankara"],
    )

    # Transaction details
    amount: float = Field(
        ...,
        gt=0,
        description="Transfer amount",
        examples=[25000.00],
    )
    currency: str = Field(
        default="TRY",
        max_length=3,
        description="Currency code (ISO 4217)",
    )
    description: str = Field(
        default="",
        max_length=500,
        description="Transaction description/note",
        examples=["Kira ödemesi"],
    )

    # Timestamp
    timestamp: datetime | None = Field(
        default=None,
        description="Transaction timestamp (auto-set if empty)",
    )

    # Optional geo/device info
    sender_latitude: float | None = Field(default=None, ge=-90, le=90)
    sender_longitude: float | None = Field(default=None, ge=-180, le=180)
    sender_ip: str | None = Field(default=None, max_length=45)
    device_id: str | None = Field(default=None, max_length=100)
    channel: str = Field(default="web", description="Transaction channel: web, mobile, atm, branch")

    def with_defaults(self) -> TransactionCreate:
        """Return a copy with auto-generated defaults filled in."""
        data = self.model_dump()
        if not data.get("transaction_id"):
            data["transaction_id"] = generate_id("TXN")
        if not data.get("timestamp"):
            data["timestamp"] = utc_now()
        return TransactionCreate(**data)

    @field_validator("sender_iban", "receiver_iban")
    @classmethod
    def validate_iban(cls, v: str) -> str:
        """Basic IBAN validation."""
        v = v.strip().upper().replace(" ", "")
        if len(v) < 15:
            raise ValueError("IBAN too short")
        return v


class TransactionSummary(ContractBase):
    """
    Summarized transaction record for database storage.
    Includes processing results.
    """

    transaction_id: str

    # Core fields
    sender_iban: str
    sender_name: str
    sender_city: str
    receiver_iban: str
    receiver_name: str
    receiver_city: str
    amount: float
    currency: str
    description: str
    timestamp: datetime
    channel: str = "web"

    # Processing results
    is_fraud: bool = False
    fraud_score: float = Field(default=0.0, ge=0.0, le=1.0)
    alert_ids: list[str] = Field(default_factory=list)
    case_id: str | None = None

    # Metadata
    processed_at: datetime = Field(default_factory=utc_now)
    processing_time_ms: float = 0.0
    detector_versions: dict[str, str] = Field(default_factory=dict)


class TransactionKafkaMessage(KafkaMessageBase, TransactionCreate):
    """
    Transaction message format for Kafka `transactions` topic.
    Combines TransactionCreate with Kafka metadata.
    """

    transaction_id: str = Field(
        default_factory=lambda: generate_id("TXN"),
        description="Unique transaction ID",
    )
    timestamp: datetime = Field(
        default_factory=utc_now,
        description="Transaction timestamp",
    )

    # Source tracking
    source_system: str = Field(
        default="api",
        description="Originating system: api, batch, replay, external",
    )
    correlation_id: str | None = Field(
        default=None,
        description="Correlation ID for request tracing",
    )

    def kafka_key(self) -> str:
        """Return Kafka message key (for partitioning)."""
        return self.sender_iban

# =============================================================================
# SentinelFlow - Alert Contracts
# =============================================================================
"""
Alert schema definitions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field

from sentinelflow.contracts.base import (
    ContractBase,
    KafkaMessageBase,
    utc_now,
    generate_id,
)
from sentinelflow.contracts.enums import FraudType, Severity, DetectorType


class Evidence(ContractBase):
    """
    Evidence supporting a fraud detection.
    Structured for explainability and audit.
    """

    detector_type: DetectorType = Field(
        ..., description="Detection engine that produced this evidence"
    )
    detector_version: str = Field(default="1.0.0")

    # What was detected
    rule_id: str | None = Field(default=None, description="Rule ID if rule-based")
    pattern_name: str | None = Field(
        default=None, description="Pattern name (e.g., 'circular_ring_3_hop')"
    )

    # Scores
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Detection confidence")
    contribution: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Contribution to final score"
    )

    # Details (flexible structure per detector type)
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Detector-specific evidence details",
    )

    # Human-readable explanation
    summary: str = Field(default="", description="Human-readable explanation")

    # For graph-based evidence
    related_entities: list[str] = Field(
        default_factory=list,
        description="Related accounts/entities (IBANs, names)",
    )
    related_transactions: list[str] = Field(
        default_factory=list,
        description="Related transaction IDs",
    )


class AlertCreate(ContractBase):
    """
    Schema for creating a new alert.
    Used internally by detectors.
    """

    fraud_type: FraudType
    severity: Severity
    confidence: float = Field(..., ge=0.0, le=1.0)

    # Related transaction
    transaction_id: str
    sender_iban: str
    sender_name: str
    sender_city: str = ""
    receiver_iban: str
    receiver_name: str
    receiver_city: str = ""
    amount: float
    currency: str = "TRY"

    # Description
    title: str = Field(default="", max_length=200, description="Short alert title")
    description: str = Field(default="", max_length=2000, description="Detailed description")

    # Evidence (multiple detectors can contribute)
    evidence: list[Evidence] = Field(default_factory=list)

    # Related entities for correlation
    related_transactions: list[str] = Field(default_factory=list)
    related_accounts: list[str] = Field(default_factory=list)


class Alert(ContractBase):
    """
    Full alert record with database fields.
    """

    # Identity
    alert_id: str = Field(default_factory=lambda: generate_id("ALERT"))

    # Core fields from AlertCreate
    fraud_type: FraudType
    severity: Severity
    confidence: float = Field(..., ge=0.0, le=1.0)

    # Transaction context
    transaction_id: str
    sender_iban: str
    sender_name: str
    sender_city: str = ""
    receiver_iban: str
    receiver_name: str
    receiver_city: str = ""
    amount: float
    currency: str = "TRY"

    # Description
    title: str = ""
    description: str = ""

    # Evidence
    evidence: list[Evidence] = Field(default_factory=list)

    # Relations
    related_transactions: list[str] = Field(default_factory=list)
    related_accounts: list[str] = Field(default_factory=list)
    case_id: str | None = Field(default=None, description="Linked case ID (if correlated)")

    # Timestamps
    detected_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    # Status
    is_dismissed: bool = Field(default=False, description="Manually dismissed by analyst")
    dismissed_by: str | None = None
    dismissed_at: datetime | None = None
    dismissed_reason: str | None = None

    # Metadata
    detector_versions: dict[str, str] = Field(
        default_factory=dict,
        description="Version of each detector that contributed",
    )
    processing_time_ms: float = 0.0

    @classmethod
    def from_create(cls, create: AlertCreate, **kwargs: Any) -> "Alert":
        """Create Alert from AlertCreate with additional fields."""
        data = create.model_dump()
        data.update(kwargs)
        return cls(**data)

    @property
    def severity_order(self) -> int:
        """Numeric severity for sorting."""
        return self.severity.priority_order


class AlertKafkaMessage(KafkaMessageBase):
    """
    Alert message format for Kafka `alerts` topic.
    """

    # Full alert data
    alert: Alert

    # Event metadata
    event_type: str = Field(default="alert_created")
    correlation_id: str | None = None

    def kafka_key(self) -> str:
        """Return Kafka message key."""
        return self.alert.alert_id


# =============================================================================
# API Response schemas (for backwards compatibility)
# =============================================================================


class AlertResponse(Alert):
    """Alert response for API (alias for Alert)."""

    pass


class AlertListResponse(ContractBase):
    """Paginated list of alerts."""

    total: int = Field(..., description="Total number of matching alerts")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    alerts: list[Alert] = Field(default_factory=list)

    # Filters applied
    filters: dict[str, Any] = Field(default_factory=dict)

# =============================================================================
# SentinelFlow - Case Contracts
# =============================================================================
"""
Case management schema definitions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field

from sentinelflow.contracts.base import (
    ContractBase,
    utc_now,
    generate_id,
)
from sentinelflow.contracts.enums import (
    CaseStatus,
    CasePriority,
    Severity,
    FraudType,
    EventType,
)


class CaseCreate(ContractBase):
    """
    Schema for creating a new case.
    Cases are created from correlated alerts.
    """
    
    title: str = Field(..., min_length=1, max_length=300)
    description: str = Field(default="", max_length=5000)
    
    # Initial alerts
    alert_ids: list[str] = Field(default_factory=list, min_length=1)
    
    # Priority (can be auto-calculated from alerts)
    priority: CasePriority = Field(default=CasePriority.P3_MEDIUM)
    
    # Primary fraud type (from highest severity alert)
    primary_fraud_type: FraudType | None = None
    
    # Tags for categorization
    tags: list[str] = Field(default_factory=list)
    
    # Assignment
    assigned_to: str | None = Field(default=None, description="Username of assigned analyst")
    assigned_team: str | None = Field(default=None, description="Team name")


class Case(ContractBase):
    """
    Full case record with database fields.
    """
    
    # Identity
    case_id: str = Field(default_factory=lambda: generate_id("CASE"))
    
    # Core fields
    title: str
    description: str = ""
    status: CaseStatus = Field(default=CaseStatus.NEW)
    priority: CasePriority = Field(default=CasePriority.P3_MEDIUM)
    
    # Fraud classification
    primary_fraud_type: FraudType | None = None
    fraud_types: list[FraudType] = Field(
        default_factory=list,
        description="All fraud types from linked alerts",
    )
    
    # Linked alerts
    alert_ids: list[str] = Field(default_factory=list)
    alert_count: int = 0
    
    # Aggregated metrics from alerts
    total_amount: float = Field(default=0.0, description="Sum of transaction amounts")
    max_severity: Severity = Field(default=Severity.MEDIUM)
    avg_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    
    # Related entities (deduplicated from alerts)
    involved_accounts: list[str] = Field(default_factory=list)
    involved_transactions: list[str] = Field(default_factory=list)
    
    # Assignment
    assigned_to: str | None = None
    assigned_team: str | None = None
    
    # Tags
    tags: list[str] = Field(default_factory=list)
    
    # Timestamps
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    first_alert_at: datetime | None = None
    last_alert_at: datetime | None = None
    
    # SLA tracking
    sla_due_at: datetime | None = None
    sla_breached: bool = False
    
    # Resolution
    resolution: str | None = Field(default=None, description="Resolution summary")
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    
    # Compliance
    str_required: bool = Field(default=False, description="STR filing required")
    str_filed: bool = False
    str_filed_at: datetime | None = None
    str_reference: str | None = None
    
    # Notes (latest; full history in case_events)
    notes_count: int = 0
    last_note: str | None = None
    
    @classmethod
    def from_create(cls, create: CaseCreate, **kwargs: Any) -> "Case":
        """Create Case from CaseCreate with additional fields."""
        data = create.model_dump()
        data["alert_count"] = len(data.get("alert_ids", []))
        data.update(kwargs)
        return cls(**data)
    
    @property
    def is_open(self) -> bool:
        """Whether case is still open."""
        return self.status.is_open


class CaseEvent(ContractBase):
    """
    Audit log entry for case events.
    Provides full history/audit trail.
    """
    
    # Identity
    event_id: str = Field(default_factory=lambda: generate_id("EVT"))
    case_id: str
    
    # Event type
    event_type: EventType
    
    # Who/what triggered
    actor: str = Field(
        default="system",
        description="Username or 'system' for automated events",
    )
    actor_type: str = Field(
        default="system",
        description="user, system, api",
    )
    
    # What changed
    description: str = Field(default="", max_length=2000)
    
    # State change (for status/priority changes)
    previous_value: str | None = None
    new_value: str | None = None
    
    # Additional data
    extra_data: dict[str, Any] = Field(default_factory=dict)
    
    # Related entities
    alert_id: str | None = None
    transaction_id: str | None = None
    
    # Timestamp
    created_at: datetime = Field(default_factory=utc_now)
    
    # IP/request tracking for audit
    ip_address: str | None = None
    user_agent: str | None = None


class CaseUpdate(ContractBase):
    """
    Schema for updating a case.
    """
    
    status: CaseStatus | None = None
    priority: CasePriority | None = None
    assigned_to: str | None = None
    assigned_team: str | None = None
    tags: list[str] | None = None
    resolution: str | None = None
    
    # For adding notes
    note: str | None = Field(default=None, max_length=5000)


class CaseListResponse(ContractBase):
    """Paginated list of cases."""
    
    total: int
    page: int = 1
    page_size: int = 20
    cases: list[Case] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)


class CaseSummary(ContractBase):
    """
    Lightweight case summary for lists and dashboards.
    """
    
    case_id: str
    title: str
    status: CaseStatus
    priority: CasePriority
    primary_fraud_type: FraudType | None
    alert_count: int
    total_amount: float
    max_severity: Severity
    assigned_to: str | None
    created_at: datetime
    updated_at: datetime
    sla_breached: bool = False

# =============================================================================
# SentinelFlow - SQLAlchemy ORM Models
# =============================================================================
"""
Database models for PostgreSQL.

Tables:
- alerts: Fraud detection alerts
- cases: Aggregated investigation cases
- case_events: Audit log for case actions
- transactions_summary: Processed transaction summaries
- model_versions: ML model version tracking
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import (
    Column,
    String,
    Float,
    Boolean,
    Integer,
    DateTime,
    Text,
    ForeignKey,
    Index,
    Enum as SQLEnum,
    JSON,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column

from sentinelflow.contracts.enums import (
    FraudType,
    Severity,
    CaseStatus,
    CasePriority,
    EventType,
)


def generate_id(prefix: str) -> str:
    """Generate prefixed UUID."""
    return f"{prefix}-{uuid4().hex[:12].upper()}"


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


# =============================================================================
# Base
# =============================================================================

class Base(DeclarativeBase):
    """Base class for all models."""
    
    type_annotation_map = {
        dict[str, Any]: JSONB,
        list[str]: ARRAY(String),
    }


# =============================================================================
# Alert Model
# =============================================================================

class AlertModel(Base):
    """Fraud detection alert."""
    
    __tablename__ = "alerts"
    
    # Primary key
    alert_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("ALERT"),
    )
    
    # Fraud classification
    fraud_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
    )
    severity: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True,
    )
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    
    # Transaction context
    transaction_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    sender_iban: Mapped[str] = mapped_column(String(34), nullable=False, index=True)
    sender_name: Mapped[str] = mapped_column(String(200), nullable=False)
    sender_city: Mapped[str] = mapped_column(String(100), default="")
    receiver_iban: Mapped[str] = mapped_column(String(34), nullable=False, index=True)
    receiver_name: Mapped[str] = mapped_column(String(200), nullable=False)
    receiver_city: Mapped[str] = mapped_column(String(100), default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="TRY")
    
    # Description
    title: Mapped[str] = mapped_column(String(200), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    
    # Evidence (JSONB for flexible structure)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Relations
    related_transactions: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    related_accounts: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    case_id: Mapped[Optional[str]] = mapped_column(
        String(50),
        ForeignKey("cases.case_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    
    # Timestamps
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )
    
    # Status
    is_dismissed: Mapped[bool] = mapped_column(Boolean, default=False)
    dismissed_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    dismissed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    dismissed_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Metadata
    detector_versions: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    processing_time_ms: Mapped[float] = mapped_column(Float, default=0.0)
    
    # Relationships
    case: Mapped[Optional["CaseModel"]] = relationship("CaseModel", back_populates="alerts")
    
    # Indexes
    __table_args__ = (
        Index("ix_alerts_detected_at_desc", detected_at.desc()),
        Index("ix_alerts_fraud_severity", fraud_type, severity),
        Index("ix_alerts_sender_receiver", sender_iban, receiver_iban),
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "fraud_type": self.fraud_type,
            "severity": self.severity,
            "confidence": self.confidence,
            "transaction_id": self.transaction_id,
            "sender_iban": self.sender_iban,
            "sender_name": self.sender_name,
            "sender_city": self.sender_city,
            "receiver_iban": self.receiver_iban,
            "receiver_name": self.receiver_name,
            "receiver_city": self.receiver_city,
            "amount": self.amount,
            "currency": self.currency,
            "title": self.title,
            "description": self.description,
            "evidence": self.evidence,
            "related_transactions": self.related_transactions,
            "related_accounts": self.related_accounts,
            "case_id": self.case_id,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_dismissed": self.is_dismissed,
            "detector_versions": self.detector_versions,
            "processing_time_ms": self.processing_time_ms,
        }


# =============================================================================
# Case Model
# =============================================================================

class CaseModel(Base):
    """Investigation case (aggregates alerts)."""
    
    __tablename__ = "cases"
    
    # Primary key
    case_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("CASE"),
    )
    
    # Core fields
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(50), default="new", index=True)
    priority: Mapped[str] = mapped_column(String(10), default="P3", index=True)
    
    # Fraud classification
    primary_fraud_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    fraud_types: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    
    # Aggregated metrics
    alert_count: Mapped[int] = mapped_column(Integer, default=0)
    total_amount: Mapped[float] = mapped_column(Float, default=0.0)
    max_severity: Mapped[str] = mapped_column(String(20), default="medium")
    avg_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    
    # Related entities
    involved_accounts: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    involved_transactions: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    
    # Assignment
    assigned_to: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    assigned_team: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Tags
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )
    first_alert_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_alert_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # SLA
    sla_due_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_breached: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Resolution
    resolution: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Compliance
    str_required: Mapped[bool] = mapped_column(Boolean, default=False)
    str_filed: Mapped[bool] = mapped_column(Boolean, default=False)
    str_filed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    str_reference: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Notes
    notes_count: Mapped[int] = mapped_column(Integer, default=0)
    last_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    alerts: Mapped[list["AlertModel"]] = relationship("AlertModel", back_populates="case")
    events: Mapped[list["CaseEventModel"]] = relationship("CaseEventModel", back_populates="case")
    
    # Indexes
    __table_args__ = (
        Index("ix_cases_status_priority", status, priority),
        Index("ix_cases_created_at_desc", created_at.desc()),
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "case_id": self.case_id,
            "title": self.title,
            "description": self.description,
            "status": self.status,
            "priority": self.priority,
            "primary_fraud_type": self.primary_fraud_type,
            "fraud_types": self.fraud_types,
            "alert_count": self.alert_count,
            "total_amount": self.total_amount,
            "max_severity": self.max_severity,
            "avg_confidence": self.avg_confidence,
            "involved_accounts": self.involved_accounts,
            "assigned_to": self.assigned_to,
            "assigned_team": self.assigned_team,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "sla_breached": self.sla_breached,
            "str_required": self.str_required,
            "str_filed": self.str_filed,
        }


# =============================================================================
# Case Event Model (Audit Log)
# =============================================================================

class CaseEventModel(Base):
    """Audit log entry for case actions."""
    
    __tablename__ = "case_events"
    
    # Primary key
    event_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("EVT"),
    )
    
    # Case reference
    case_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("cases.case_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Event type
    event_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    
    # Actor
    actor: Mapped[str] = mapped_column(String(100), default="system")
    actor_type: Mapped[str] = mapped_column(String(20), default="system")
    
    # Change details
    description: Mapped[str] = mapped_column(Text, default="")
    previous_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Extra data
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Related entities
    alert_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    transaction_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Timestamp
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        index=True,
    )
    
    # Audit fields
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    case: Mapped["CaseModel"] = relationship("CaseModel", back_populates="events")
    
    # Indexes
    __table_args__ = (
        Index("ix_case_events_case_created", case_id, created_at.desc()),
    )


# =============================================================================
# Transaction Summary Model
# =============================================================================

class TransactionSummaryModel(Base):
    """Processed transaction summary."""
    
    __tablename__ = "transactions_summary"
    
    # Primary key
    transaction_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    
    # Core fields
    sender_iban: Mapped[str] = mapped_column(String(34), nullable=False, index=True)
    sender_name: Mapped[str] = mapped_column(String(200), nullable=False)
    sender_city: Mapped[str] = mapped_column(String(100), default="")
    receiver_iban: Mapped[str] = mapped_column(String(34), nullable=False, index=True)
    receiver_name: Mapped[str] = mapped_column(String(200), nullable=False)
    receiver_city: Mapped[str] = mapped_column(String(100), default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="TRY")
    description: Mapped[str] = mapped_column(Text, default="")
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(20), default="web")
    
    # Processing results
    is_fraud: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    fraud_score: Mapped[float] = mapped_column(Float, default=0.0)
    alert_ids: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    case_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Metadata
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    processing_time_ms: Mapped[float] = mapped_column(Float, default=0.0)
    detector_versions: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Indexes
    __table_args__ = (
        Index("ix_txn_timestamp_desc", timestamp.desc()),
        Index("ix_txn_fraud", is_fraud, timestamp.desc()),
    )


# =============================================================================
# Model Version Model (ML tracking)
# =============================================================================

class ModelVersionModel(Base):
    """ML model version tracking."""
    
    __tablename__ = "model_versions"
    
    # Primary key
    version_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("MDL"),
    )
    
    # Model info
    model_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    version: Mapped[str] = mapped_column(String(20), nullable=False)
    
    # Status
    stage: Mapped[str] = mapped_column(
        String(20),
        default="development",  # development, staging, production, archived
        index=True,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    
    # Metrics
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Training info
    training_dataset: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    training_samples: Mapped[int] = mapped_column(Integer, default=0)
    training_time_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    hyperparameters: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Artifact
    artifact_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    artifact_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    deployed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Metadata
    description: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str] = mapped_column(String(100), default="system")


# =============================================================================
# User Model
# =============================================================================

class UserModel(Base):
    """User account for authentication."""
    
    __tablename__ = "users"
    
    # Primary key
    user_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("USR"),
    )
    
    # Credentials
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Profile
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    role: Mapped[str] = mapped_column(String(20), default="viewer", index=True)
    status: Mapped[str] = mapped_column(String(20), default="active", index=True)
    team: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Settings
    preferences: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Security
    failed_login_attempts: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary (excluding password)."""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "role": self.role,
            "status": self.status,
            "team": self.team,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "preferences": self.preferences or {},
        }


# =============================================================================
# Refresh Token Model
# =============================================================================

class RefreshTokenModel(Base):
    """Refresh token storage for JWT auth."""
    
    __tablename__ = "refresh_tokens"
    
    token_id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        default=lambda: generate_id("RTK"),
    )
    
    user_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    
    # Metadata
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    
    is_revoked: Mapped[bool] = mapped_column(Boolean, default=False)

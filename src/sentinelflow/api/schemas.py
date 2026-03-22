# =============================================================================
# SentinelFlow API - Pydantic Schemas
# =============================================================================
"""
Request/response schemas for the SentinelFlow REST API.

This module re-exports contract schemas and adds API-specific extensions.
All core schemas come from sentinelflow.contracts (single source of truth).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# =============================================================================
# Re-export from contracts (single source of truth)
# =============================================================================
from sentinelflow.contracts import (
    # Version
    SCHEMA_VERSION,
    # Alert
    Alert,
    AlertCreate,
    # Case
    Case,
    CaseCreate,
    CaseEvent,
    CasePriority,
    CaseStatus,
    EventType,
    Evidence,
    # Enums
    FraudType,
    Severity,
    # Transaction
    TransactionCreate,
    TransactionSummary,
)
from sentinelflow.contracts.alert import AlertListResponse
from sentinelflow.contracts.case import CaseListResponse, CaseSummary, CaseUpdate

# Backwards compatibility aliases
FraudTypeEnum = FraudType
SeverityEnum = Severity
TransactionSubmit = TransactionCreate
AlertResponse = Alert


# =============================================================================
# API-specific Response Schemas
# =============================================================================


class TransactionResponse(BaseModel):
    """Response after submitting a transaction."""

    transaction_id: str
    status: str = Field(default="analyzed", description="Processing status")
    message: str = Field(default="Transaction analyzed successfully")
    is_fraud: bool = Field(default=False, description="Fraud detection result")
    fraud_score: float = Field(default=0.0, ge=0.0, le=1.0, description="ML ensemble score")
    alerts: list[Alert] | None = Field(default=None, description="Generated alerts, if any")
    processing_time_ms: float = Field(default=0.0)


class BatchTransactionResponse(BaseModel):
    """Response for batch transaction submission."""

    total_processed: int
    fraud_detected: int
    results: list[TransactionResponse]
    processing_time_ms: float


# =============================================================================
# ML Pipeline Schemas
# =============================================================================


class MLFeatureResponse(BaseModel):
    """Response with extracted features."""

    transaction_id: str
    features: dict[str, float] = Field(..., description="Extracted feature values")
    num_features: int = Field(default=21)


class MLPredictionResponse(BaseModel):
    """Response with ML prediction."""

    transaction_id: str
    is_fraud: bool
    ensemble_score: float = Field(..., ge=0.0, le=1.0)
    model_scores: dict[str, float] = Field(default_factory=dict)
    explanation: dict[str, Any] | None = None


class ModelInfo(BaseModel):
    """Information about a single ML model."""

    name: str
    version: str = "1.0.0"
    ready: bool = False
    last_trained: datetime | None = None
    metrics: dict[str, float] = Field(default_factory=dict)


class ModelStatusResponse(BaseModel):
    """Status of all ML models."""

    isolation_forest: ModelInfo = Field(default_factory=lambda: ModelInfo(name="IsolationForest"))
    xgboost: ModelInfo = Field(default_factory=lambda: ModelInfo(name="XGBoost"))
    autoencoder: ModelInfo = Field(default_factory=lambda: ModelInfo(name="AutoEncoder"))
    ensemble_ready: bool = False
    ensemble_threshold: float = 0.5


class TrainRequest(BaseModel):
    """Request to trigger model training."""

    n_samples: int = Field(
        default=5000, ge=100, le=100000, description="Number of synthetic samples"
    )
    fraud_ratio: float = Field(default=0.05, ge=0.01, le=0.5, description="Fraud ratio")


class TrainResponse(BaseModel):
    """Response with training results."""

    status: str = "completed"
    training_time_seconds: float = 0.0
    dataset_size: int = 0
    metrics: dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# System Schemas
# =============================================================================


class ComponentStatus(BaseModel):
    """Status of a system component."""

    name: str
    status: str = "unknown"  # healthy, degraded, down, unknown
    latency_ms: float | None = None
    message: str | None = None


class HealthResponse(BaseModel):
    """System health check response."""

    status: str = Field(default="healthy", description="Overall system status")
    version: str = Field(default="2.0.0")
    schema_version: str = Field(default=SCHEMA_VERSION)
    uptime_seconds: float = Field(default=0.0)
    components: dict[str, ComponentStatus] = Field(default_factory=dict)


class StatsResponse(BaseModel):
    """System statistics response."""

    transactions_processed: int = 0
    fraud_detected: int = 0
    alerts_created: int = 0
    cases_open: int = 0
    cases_resolved: int = 0

    # By fraud type
    by_fraud_type: dict[str, int] = Field(default_factory=dict)

    # By severity
    by_severity: dict[str, int] = Field(default_factory=dict)

    # Performance
    avg_detection_latency_ms: float = 0.0
    p95_detection_latency_ms: float = 0.0

    # Time range
    period_start: datetime | None = None
    period_end: datetime | None = None
    uptime_seconds: float = 0.0

    # Rates
    fraud_rate: float = Field(default=0.0, description="Fraud detection rate (percentage)")
    alerts_per_minute: float = 0.0


# =============================================================================
# WebSocket Message Schemas
# =============================================================================


class WSMessage(BaseModel):
    """WebSocket message format."""

    type: str = Field(..., description="Message type: alert, stats, heartbeat, error")
    data: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now())


class WSAlertMessage(BaseModel):
    """WebSocket alert notification."""

    type: str = "alert"
    alert: Alert


class WSStatsMessage(BaseModel):
    """WebSocket stats update."""

    type: str = "stats"
    stats: StatsResponse


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Version
    "SCHEMA_VERSION",
    # Enums
    "FraudType",
    "FraudTypeEnum",  # backwards compat
    "Severity",
    "SeverityEnum",  # backwards compat
    "CaseStatus",
    "CasePriority",
    "EventType",
    # Transaction
    "TransactionCreate",
    "TransactionSubmit",  # backwards compat
    "TransactionSummary",
    "TransactionResponse",
    "BatchTransactionResponse",
    # Alert
    "Alert",
    "AlertCreate",
    "AlertResponse",  # backwards compat
    "AlertListResponse",
    "Evidence",
    # Case
    "Case",
    "CaseCreate",
    "CaseUpdate",
    "CaseEvent",
    "CaseListResponse",
    "CaseSummary",
    # ML
    "MLFeatureResponse",
    "MLPredictionResponse",
    "ModelInfo",
    "ModelStatusResponse",
    "TrainRequest",
    "TrainResponse",
    # System
    "ComponentStatus",
    "HealthResponse",
    "StatsResponse",
    # WebSocket
    "WSMessage",
    "WSAlertMessage",
    "WSStatsMessage",
]

# =============================================================================
# SentinelFlow - Fraud Detector Types
# =============================================================================
"""Enums, keyword lists and dataclasses shared across the detector package."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4


class FraudType(str, Enum):
    """Types of fraud detected by the system."""

    CIRCULAR_RING = "circular_ring"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    BLACKLIST_KEYWORD = "blacklist_keyword"
    MULE_ACCOUNT = "mule_account"
    AI_DETECTED_ANOMALY = "ai_detected_anomaly"
    ML_ENSEMBLE = "ml_ensemble"


# Blacklisted keywords for NLP check (Turkish + English)
BLACKLIST_KEYWORDS: list[str] = [
    # Gambling / Betting
    "bahis",
    "casino",
    "kumar",
    "poker",
    "rulet",
    "slot",
    "bet365",
    "betting",
    # Cryptocurrency (suspicious transfers)
    "kripto",
    "bitcoin",
    "btc",
    "ethereum",
    "usdt",
    "binance",
    "crypto",
    # Offshore / Anonymous
    "offshore",
    "anonim",
    "anonymous",
    "gizli",
    "secret",
    # Urgency patterns (social engineering)
    "acil",
    "urgent",
    "hemen",
    "immediately",
]


@dataclass
class FraudAlert:
    """Represents a detected fraud case."""

    alert_id: str = field(default_factory=lambda: f"ALERT-{uuid4().hex[:12].upper()}")
    fraud_type: FraudType = FraudType.CIRCULAR_RING
    severity: str = "high"  # low, medium, high, critical
    confidence: float = 0.9

    # Transaction details
    transaction_id: str = ""
    sender_iban: str = ""
    sender_name: str = ""
    receiver_iban: str = ""
    receiver_name: str = ""
    amount: float = 0.0

    # Fraud details
    description: str = ""
    evidence: dict = field(default_factory=dict)
    related_transactions: list[str] = field(default_factory=list)

    # Metadata
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    detector_version: str = "1.0.0"

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return {
            "alert_id": self.alert_id,
            "fraud_type": (
                self.fraud_type.value if isinstance(self.fraud_type, FraudType) else self.fraud_type
            ),
            "severity": self.severity,
            "confidence": self.confidence,
            "transaction_id": self.transaction_id,
            "sender_iban": self.sender_iban,
            "sender_name": self.sender_name,
            "receiver_iban": self.receiver_iban,
            "receiver_name": self.receiver_name,
            "amount": self.amount,
            "description": self.description,
            "evidence": self.evidence,
            "related_transactions": self.related_transactions,
            "detected_at": self.detected_at,
            "detector_version": self.detector_version,
        }


@dataclass
class DetectorStats:
    """Statistics for the detector service."""

    transactions_processed: int = 0
    fraud_detected: int = 0
    circular_rings: int = 0
    impossible_travel: int = 0
    blacklist_hits: int = 0
    ai_anomalies: int = 0
    ml_ensemble_hits: int = 0
    errors: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()

    @property
    def fraud_rate(self) -> float:
        if self.transactions_processed == 0:
            return 0.0
        return self.fraud_detected / self.transactions_processed

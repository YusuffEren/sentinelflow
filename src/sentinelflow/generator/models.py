# =============================================================================
# SentinelFlow - Transaction Data Models
# =============================================================================
"""
Pydantic models for bank transactions and related entities.

These models ensure type safety and validation throughout the pipeline,
from generation to Kafka serialization to database storage.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class FraudType(str, Enum):
    """Types of fraud patterns detected by the system."""
    
    NONE = "none"
    CIRCULAR_RING = "circular_ring"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    BLACKLIST_KEYWORD = "blacklist_keyword"
    MULE_ACCOUNT = "mule_account"
    HIGH_VALUE_ANOMALY = "high_value_anomaly"  # Whale transactions for AI detector


class TransactionStatus(str, Enum):
    """Transaction processing status."""
    
    PENDING = "pending"
    COMPLETED = "completed"
    FLAGGED = "flagged"
    BLOCKED = "blocked"


class City(BaseModel):
    """Turkish city with geographical coordinates."""
    
    name: str
    latitude: float
    longitude: float
    
    class Config:
        frozen = True


class Account(BaseModel):
    """Bank account entity."""
    
    account_id: UUID = Field(default_factory=uuid4)
    iban: str = Field(..., pattern=r"^TR\d{24}$")
    holder_name: str = Field(..., min_length=2, max_length=100)
    bank_name: str = Field(default="SentinelBank")
    city: str = Field(..., description="City where account is registered")
    
    @field_validator("iban")
    @classmethod
    def validate_iban(cls, v: str) -> str:
        """Ensure IBAN format is valid."""
        if not v.startswith("TR"):
            raise ValueError("Turkish IBAN must start with 'TR'")
        return v.upper()


class Transaction(BaseModel):
    """
    Core transaction model representing a bank transfer.
    
    This is the primary data structure flowing through Kafka
    and stored in Neo4j for graph analysis.
    """
    
    transaction_id: UUID = Field(default_factory=uuid4)
    
    # Sender information
    sender_account_id: UUID
    sender_iban: str
    sender_name: str
    sender_city: str
    
    # Receiver information
    receiver_account_id: UUID
    receiver_iban: str
    receiver_name: str
    receiver_city: str
    
    # Transaction details
    amount: Decimal = Field(..., gt=0, description="Amount in TRY")
    currency: str = Field(default="TRY")
    description: str = Field(default="", max_length=500)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Fraud detection metadata
    status: TransactionStatus = Field(default=TransactionStatus.PENDING)
    fraud_type: FraudType = Field(default=FraudType.NONE)
    fraud_score: float = Field(default=0.0, ge=0.0, le=1.0)
    is_flagged: bool = Field(default=False)
    
    # Pattern tracking (for injected fraud scenarios)
    ring_id: Optional[str] = Field(default=None, description="ID linking circular transactions")
    pattern_label: Optional[str] = Field(default=None, description="Label for injected patterns")
    
    def to_kafka_dict(self) -> dict:
        """Serialize for Kafka producer."""
        return {
            "transaction_id": str(self.transaction_id),
            "sender_account_id": str(self.sender_account_id),
            "sender_iban": self.sender_iban,
            "sender_name": self.sender_name,
            "sender_city": self.sender_city,
            "receiver_account_id": str(self.receiver_account_id),
            "receiver_iban": self.receiver_iban,
            "receiver_name": self.receiver_name,
            "receiver_city": self.receiver_city,
            "amount": float(self.amount),
            "currency": self.currency,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "fraud_type": self.fraud_type.value,
            "fraud_score": self.fraud_score,
            "is_flagged": self.is_flagged,
            "ring_id": self.ring_id,
            "pattern_label": self.pattern_label,
        }
    
    class Config:
        json_encoders = {
            UUID: str,
            Decimal: float,
            datetime: lambda v: v.isoformat(),
        }


class FraudAlert(BaseModel):
    """Alert generated when fraud is detected."""
    
    alert_id: UUID = Field(default_factory=uuid4)
    transaction_id: UUID
    fraud_type: FraudType
    severity: str = Field(..., pattern=r"^(low|medium|high|critical)$")
    confidence: float = Field(..., ge=0.0, le=1.0)
    description: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    related_transactions: list[UUID] = Field(default_factory=list)
    
    def to_kafka_dict(self) -> dict:
        """Serialize for Kafka producer."""
        return {
            "alert_id": str(self.alert_id),
            "transaction_id": str(self.transaction_id),
            "fraud_type": self.fraud_type.value,
            "severity": self.severity,
            "confidence": self.confidence,
            "description": self.description,
            "detected_at": self.detected_at.isoformat(),
            "related_transactions": [str(t) for t in self.related_transactions],
        }

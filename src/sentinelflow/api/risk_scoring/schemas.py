# =============================================================================
# SentinelFlow - Risk Scoring Schemas
# =============================================================================
"""Request/response models for the risk scoring API."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class RiskDecision(str, Enum):
    """Risk karar seviyeleri."""

    ALLOW = "allow"
    REVIEW = "review"
    BLOCK = "block"
    CRITICAL = "critical"


class RiskLevel(str, Enum):
    """Risk seviyeleri."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskScoringRequest(BaseModel):
    """Risk skorlama isteği."""

    transaction_id: str | None = Field(None, description="İşlem ID")
    sender_iban: str = Field(..., description="Gönderen IBAN")
    sender_name: str = Field(..., description="Gönderen adı")
    sender_city: str = Field("İstanbul", description="Gönderen şehir")
    receiver_iban: str = Field(..., description="Alıcı IBAN")
    receiver_name: str = Field(..., description="Alıcı adı")
    receiver_city: str = Field("Ankara", description="Alıcı şehir")
    amount: float = Field(..., gt=0, description="Tutar (TL)")
    currency: str = Field("TRY", description="Para birimi")
    description: str = Field("", description="Açıklama")
    timestamp: str | None = Field(None, description="ISO 8601 timestamp")
    channel: str = Field("mobile", description="Kanal (mobile, web, atm)")
    device_id: str | None = Field(None, description="Cihaz ID")

    class Config:
        json_schema_extra = {
            "example": {
                "sender_iban": "TR330006100519786457841326",
                "sender_name": "Ahmet Yılmaz",
                "sender_city": "İstanbul",
                "receiver_iban": "TR110006400000478893400002",
                "receiver_name": "Mehmet Kaya",
                "receiver_city": "Ankara",
                "amount": 15000.00,
                "description": "Kira ödemesi",
                "channel": "mobile",
            }
        }


class RiskFactor(BaseModel):
    """Tek bir risk faktörü açıklaması."""

    feature: str = Field(..., description="Özellik adı")
    impact: float = Field(..., description="Risk etkisi (-1 to 1)")
    direction: str = Field(..., description="increases_risk / decreases_risk")
    explanation: str = Field(..., description="Türkçe açıklama")
    value: float | None = Field(None, description="Özellik değeri")


class SimilarCase(BaseModel):
    """Benzer fraud vakası."""

    case_id: str
    similarity_score: float
    fraud_type: str
    amount: float
    description: str


class RiskScoringResponse(BaseModel):
    """Risk skorlama yanıtı."""

    # Temel bilgiler
    transaction_id: str
    timestamp: str

    # Risk skorları
    risk_score: float = Field(..., ge=0, le=1, description="Risk skoru (0-1)")
    confidence: float = Field(..., ge=0, le=1, description="Güven skoru")

    # Karar
    decision: RiskDecision
    risk_level: RiskLevel

    # Performans
    latency_ms: float = Field(..., description="İşlem süresi (ms)")

    # Model detayları
    model_scores: dict[str, float] = Field(default_factory=dict)
    ensemble_method: str = Field("stacking", description="Ensemble yöntemi")

    # Açıklanabilirlik
    top_risk_factors: list[RiskFactor] = Field(default_factory=list)
    explanation_summary: str = Field("", description="Özet açıklama")

    # Ek bilgiler
    similar_cases: list[SimilarCase] = Field(default_factory=list)
    recommended_action: str = Field("", description="Önerilen aksiyon")

    # Feature sayıları
    num_features_extracted: int = Field(0)

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "TX-ABC123",
                "timestamp": "2024-01-15T14:30:00Z",
                "risk_score": 0.87,
                "confidence": 0.92,
                "decision": "block",
                "risk_level": "high",
                "latency_ms": 25.4,
                "model_scores": {
                    "LightGBM": 0.91,
                    "XGBoost": 0.85,
                    "CatBoost": 0.88,
                },
                "explanation_summary": "Yüksek tutarlı işlem, yeni alıcı, gece saatinde",
            }
        }


class BatchRiskRequest(BaseModel):
    """Toplu risk skorlama isteği."""

    transactions: list[RiskScoringRequest]
    parallel: bool = Field(True, description="Paralel işleme")


class BatchRiskResponse(BaseModel):
    """Toplu risk skorlama yanıtı."""

    total: int
    processed: int
    avg_latency_ms: float
    high_risk_count: int
    results: list[RiskScoringResponse]

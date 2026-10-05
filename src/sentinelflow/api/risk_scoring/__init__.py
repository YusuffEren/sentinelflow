# =============================================================================
# SentinelFlow - Real-Time Risk Scoring API (TEKNOFEST Edition)
# =============================================================================
"""
Yüksek performanslı, gerçek zamanlı risk skorlama servisi.

Özellikler:
- <30ms latency hedefi
- Parallel feature extraction
- Cached model predictions
- Comprehensive SHAP explanations
- Async/await optimized

TEKNOFEST jürisi için kritik:
- Hızlı yanıt süresi
- Açıklanabilir AI
- Güvenilir risk skorları
"""

from .engine import RiskScoringEngine
from .routes import get_engine, router
from .schemas import (
    BatchRiskRequest,
    BatchRiskResponse,
    RiskDecision,
    RiskFactor,
    RiskLevel,
    RiskScoringRequest,
    RiskScoringResponse,
    SimilarCase,
)

__all__ = [
    "BatchRiskRequest",
    "BatchRiskResponse",
    "RiskDecision",
    "RiskFactor",
    "RiskLevel",
    "RiskScoringEngine",
    "RiskScoringRequest",
    "RiskScoringResponse",
    "SimilarCase",
    "get_engine",
    "router",
]

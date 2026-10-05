# =============================================================================
# SentinelFlow - Risk Scoring Routes
# =============================================================================
"""FastAPI router for the real-time risk scoring endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from sentinelflow.auth.dependencies import require_analyst, require_viewer
from sentinelflow.contracts import User
from sentinelflow.ml.advanced_features import ADVANCED_FEATURE_NAMES
from sentinelflow.ml.feature_engine import FEATURE_NAMES

from .engine import RiskScoringEngine
from .schemas import (
    BatchRiskRequest,
    BatchRiskResponse,
    RiskScoringRequest,
    RiskScoringResponse,
)

router = APIRouter(prefix="/api/v1/risk", tags=["Risk Scoring"])

# Global engine instance
_engine: RiskScoringEngine | None = None


def get_engine() -> RiskScoringEngine:
    """Get or create risk scoring engine."""
    global _engine
    if _engine is None:
        _engine = RiskScoringEngine()
    return _engine


@router.post(
    "/score",
    response_model=RiskScoringResponse,
    summary="Real-time risk scoring",
    description="Score a single transaction for fraud risk. Target latency: <30ms",
)
async def score_transaction(
    request: RiskScoringRequest, _user: User = Depends(require_analyst)
) -> RiskScoringResponse:
    """
    Real-time fraud risk scoring.

    Hedef: <30ms yanıt süresi, %99.5+ doğruluk

    Özellikler:
    - 53+ özellik çıkarımı
    - 5 model ensemble (IF, XGB, AE, LightGBM, CatBoost)
    - SHAP tabanlı açıklama
    - Türkçe risk özeti
    """
    engine = get_engine()
    return await engine.score(request)


@router.post(
    "/batch",
    response_model=BatchRiskResponse,
    summary="Batch risk scoring",
    description="Score multiple transactions in parallel",
)
async def score_batch(
    request: BatchRiskRequest, _user: User = Depends(require_analyst)
) -> BatchRiskResponse:
    """Batch risk scoring with parallel processing."""
    engine = get_engine()
    return await engine.score_batch(request.transactions, request.parallel)


@router.get(
    "/stats",
    summary="Risk scoring statistics",
)
async def get_stats(_user: User = Depends(require_viewer)) -> dict[str, Any]:
    """Get risk scoring engine statistics."""
    engine = get_engine()
    return {
        "total_predictions": engine.total_predictions,
        "avg_latency_ms": round(engine.avg_latency_ms, 2),
        "target_latency_ms": 30,
        "performance_status": "optimal" if engine.avg_latency_ms < 30 else "degraded",
    }


@router.get(
    "/features",
    summary="List available features",
)
async def list_features(_user: User = Depends(require_viewer)) -> dict[str, Any]:
    """List all available features and their descriptions."""
    engine = get_engine()
    return {
        "base_features": FEATURE_NAMES,
        "advanced_features": ADVANCED_FEATURE_NAMES,
        "total": len(FEATURE_NAMES) + len(ADVANCED_FEATURE_NAMES),
        "descriptions": engine._feature_explanations,
    }

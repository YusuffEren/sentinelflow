# =============================================================================
# SentinelFlow API - KYC / Screening Routes
# =============================================================================
"""
KYC screening endpoints.

Provides PEP & sanctions screening for customers, backed by
``sentinelflow.kyc.screening.CombinedScreener``.

Note: The underlying databases are demo fixtures. In production these should
be wired to external providers (World-Check, Dow Jones, OFAC SDN, MASAK).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from loguru import logger

from sentinelflow.api.schemas import KYCScreenRequest, KYCScreenResponse
from sentinelflow.auth.dependencies import require_analyst
from sentinelflow.contracts import User

router = APIRouter(prefix="/kyc", tags=["KYC & Compliance"])

# Lazily-created shared screener (stateless apart from a counter).
_screener = None


def _get_screener():
    global _screener
    if _screener is None:
        from sentinelflow.kyc.screening import CombinedScreener

        _screener = CombinedScreener()
        logger.info("CombinedScreener initialized for KYC API")
    return _screener


@router.post(
    "/screen",
    response_model=KYCScreenResponse,
    summary="Screen a customer (PEP & Sanctions)",
    description=(
        "Performs combined PEP and sanctions screening on a customer name and "
        "returns matches, an aggregate risk score, and a recommendation."
    ),
)
async def screen_customer(
    request: KYCScreenRequest,
    user: User = Depends(require_analyst),
) -> KYCScreenResponse:
    """Run PEP + sanctions screening for a customer."""
    screener = _get_screener()

    result = screener.screen_full(
        name=request.name,
        country=request.country,
        additional_info=request.additional_info,
    )

    logger.info(
        f"KYC screen by {user.username}: '{request.name}' -> "
        f"matches={len(result.matches)} risk={result.risk_score}"
    )

    return KYCScreenResponse(**result.to_dict())

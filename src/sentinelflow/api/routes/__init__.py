# =============================================================================
# SentinelFlow API - Routes Package
# =============================================================================
"""
API route modules.
"""

from sentinelflow.api.routes.alerts import router as alerts_router
from sentinelflow.api.routes.cases import router as cases_router
from sentinelflow.api.routes.auth import router as auth_router
from sentinelflow.api.routes.ml import router as ml_router

__all__ = ["alerts_router", "cases_router", "auth_router", "ml_router"]

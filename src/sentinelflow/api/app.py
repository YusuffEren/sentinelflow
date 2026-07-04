# =============================================================================
# SentinelFlow - FastAPI Application
# =============================================================================
"""
Main FastAPI application for SentinelFlow REST API.

Run:
    uvicorn sentinelflow.api.app:app --host 0.0.0.0 --port 8000 --reload

Or:
    python -m sentinelflow.api.app
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from sentinelflow.api.schemas import (
    Alert,
    AlertListResponse,
    FraudType,
    Severity,
    HealthResponse,
    StatsResponse,
    TransactionResponse,
    TransactionCreate,
    SCHEMA_VERSION,
)

# Import routes
from sentinelflow.api.routes.alerts import router as alerts_router
from sentinelflow.api.routes.cases import router as cases_router
from sentinelflow.api.routes.auth import router as auth_router
from sentinelflow.api.routes.ml import router as ml_router
from sentinelflow.api.routes.graph import router as graph_router
from sentinelflow.api.routes.chat import router as chat_router
from sentinelflow.api.risk_scoring import router as risk_router


# =============================================================================
# Application State
# =============================================================================


class AppState:
    """Holds application-wide state."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.transactions_processed = 0
        self.fraud_detected = 0
        self.errors = 0

        # WebSocket connections for real-time alerts
        self.ws_clients: list[WebSocket] = []

        # Database initialized flag
        self.db_initialized = False

        # ML components (lazy loaded)
        self._ml_loaded = False
        self._feature_engine = None
        self._ensemble = None
        self._explainer = None

    def init_database(self) -> bool:
        """Initialize database connection and tables."""
        try:
            from sentinelflow.database.postgres import init_db

            init_db(drop_all=False)
            self.db_initialized = True
            logger.info("Database initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False

    def load_ml_components(self) -> bool:
        """Lazy load ML components (optional, may fail on some systems)."""
        if self._ml_loaded:
            return True

        try:
            from sentinelflow.ml.feature_engine import TransactionFeatureEngine, NUM_FEATURES
            from sentinelflow.ml.models import (
                IsolationForestModel,
                XGBoostFraudModel,
                AutoEncoderModel,
            )
            from sentinelflow.ml.ensemble import EnsembleVoter
            from sentinelflow.ml.explainer import FraudExplainer

            self._feature_engine = TransactionFeatureEngine(history_window_size=1000)

            if_model = IsolationForestModel(
                contamination=0.05, n_estimators=200, min_samples_to_train=50
            )
            xgb_model = XGBoostFraudModel(n_estimators=300, max_depth=6, learning_rate=0.05)
            ae_model = AutoEncoderModel(input_dim=NUM_FEATURES, encoding_dim=8)

            self._ensemble = EnsembleVoter(threshold=0.7)
            self._ensemble.add_model(if_model, weight=0.4)
            self._ensemble.add_model(xgb_model, weight=0.4)
            self._ensemble.add_model(ae_model, weight=0.2)

            self._explainer = FraudExplainer(enable_shap=False)  # Disable SHAP for faster startup

            self._ml_loaded = True
            logger.info("ML components loaded")
            return True

        except Exception as e:
            logger.warning(f"ML components not loaded (optional): {e}")
            return False

    async def broadcast_alert(self, alert_data: dict) -> None:
        """Broadcast alert to all WebSocket clients."""
        disconnected: list[WebSocket] = []
        message = json.dumps(alert_data, ensure_ascii=False, default=str)

        for ws in self.ws_clients:
            try:
                await ws.send_text(message)
            except Exception:
                disconnected.append(ws)

        for ws in disconnected:
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)


# Module-level state
state = AppState()


# =============================================================================
# App Lifecycle
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App startup/shutdown lifecycle."""
    logger.info("SentinelFlow API starting...")

    # Initialize database
    state.init_database()

    # Try to load ML components (optional)
    state.load_ml_components()

    yield

    logger.info("SentinelFlow API shutting down...")


# =============================================================================
# FastAPI App
# =============================================================================

app = FastAPI(
    title="SentinelFlow - Fraud Detection API",
    description="""
## SentinelFlow Real-Time Fraud Detection Platform

**TEKNOFEST 2026 Finans Teknolojileri** yarışması için geliştirilmiş, 
yapay zeka destekli dolandırıcılık tespit platformu.

### Özellikler
- **ML Ensemble**: IsolationForest + XGBoost + AutoEncoder ile çoklu model oylama
- **PostgreSQL**: Kalıcı alert ve case yönetimi
- **Case Management**: Alert korelasyonu, triage, audit log
- **WebSocket**: Canlı alert akışı
- **Explainability**: Neden dolandırıcılık tespit edildiğini açıklar

### API Grupları
- `/api/v1/alerts` - Alarm listesi ve detayları
- `/api/v1/cases` - Vaka yönetimi
- `/api/v1/transactions` - İşlem analizi
- `/api/v1/system` - Sistem sağlık ve istatistikler
- `/ws/alerts` - WebSocket canlı alert akışı
    """,
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")
app.include_router(cases_router, prefix="/api/v1")
app.include_router(ml_router, prefix="/api/v1")
app.include_router(graph_router, prefix="/api/v1")
app.include_router(chat_router, prefix="/api/v1")
app.include_router(risk_router)


# =============================================================================
# Transaction Endpoints
# =============================================================================


@app.post(
    "/api/v1/transactions",
    response_model=TransactionResponse,
    tags=["Transactions"],
    summary="Submit a transaction for fraud analysis",
)
async def submit_transaction(tx: TransactionCreate) -> TransactionResponse:
    """Analyze a transaction for fraud."""
    from sentinelflow.api.deps import get_db_session
    from sentinelflow.repository import AlertRepository
    from sentinelflow.processor.alert_writer import AlertWriter, create_alert_from_detection

    start_time = time.perf_counter()

    try:
        tx_with_defaults = tx.with_defaults()
        tx_id = tx_with_defaults.transaction_id
        state.transactions_processed += 1

        is_fraud = False
        fraud_score = 0.0
        alerts_list = []

        # Run ML detection if available
        if state._ml_loaded and state._ensemble:
            import numpy as np
            from sentinelflow.ml.feature_engine import FEATURE_NAMES

            tx_data = tx_with_defaults.model_dump()
            features_dict = state._feature_engine.extract(tx_data)
            features_vector = np.array([features_dict[name] for name in FEATURE_NAMES])

            prediction = state._ensemble.predict(features_vector)
            fraud_score = prediction.final_score
            is_fraud = prediction.is_fraud

            if is_fraud:
                state.fraud_detected += 1

                # Create alert and persist
                alert_create = create_alert_from_detection(
                    fraud_type=FraudType.ML_ENSEMBLE,
                    severity=Severity.CRITICAL if fraud_score > 0.85 else Severity.HIGH,
                    confidence=fraud_score,
                    tx_data=tx_data,
                    description=f"ML Ensemble detected fraud (score: {fraud_score:.2f})",
                )

                # Persist to database
                try:
                    session = next(get_db_session())
                    repo = AlertRepository(session)
                    alert = repo.create(alert_create)
                    session.commit()
                    alerts_list.append(alert)

                    # Broadcast to WebSocket
                    await state.broadcast_alert(alert.model_dump(mode="json"))
                except Exception as e:
                    logger.error(f"Failed to persist alert: {e}")

        elapsed = (time.perf_counter() - start_time) * 1000

        return TransactionResponse(
            transaction_id=tx_id,
            status="analyzed",
            message="Fraud detected!" if is_fraud else "Transaction is clean",
            is_fraud=is_fraud,
            fraud_score=round(fraud_score, 4),
            alerts=alerts_list if alerts_list else None,
            processing_time_ms=round(elapsed, 2),
        )

    except Exception as e:
        state.errors += 1
        logger.error(f"Transaction analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# System Endpoints
# =============================================================================


@app.get(
    "/api/v1/system/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health check",
)
async def health_check() -> HealthResponse:
    """Check system health."""
    from sentinelflow.api.schemas import ComponentStatus

    uptime = time.time() - state.start_time

    components = {
        "api": ComponentStatus(name="api", status="healthy"),
        "database": ComponentStatus(
            name="database",
            status="healthy" if state.db_initialized else "degraded",
        ),
        "ml_ensemble": ComponentStatus(
            name="ml_ensemble",
            status="healthy" if state._ml_loaded else "not_loaded",
        ),
    }

    overall = "healthy" if state.db_initialized else "degraded"

    return HealthResponse(
        status=overall,
        version="2.1.0",
        schema_version=SCHEMA_VERSION,
        uptime_seconds=round(uptime, 2),
        components=components,
    )


@app.get(
    "/api/v1/system/stats",
    response_model=StatsResponse,
    tags=["System"],
    summary="System statistics",
)
async def system_stats() -> StatsResponse:
    """Get system-wide statistics."""
    from sentinelflow.api.deps import get_db_session
    from sentinelflow.repository import AlertRepository, CaseRepository

    uptime = time.time() - state.start_time

    # Get database stats
    alerts_created = 0
    cases_open = 0
    cases_resolved = 0
    by_fraud_type = {}
    by_severity = {}

    try:
        session = next(get_db_session())
        alert_repo = AlertRepository(session)
        case_repo = CaseRepository(session)

        alert_stats = alert_repo.get_stats()
        case_stats = case_repo.get_stats()

        alerts_created = alert_stats.get("total", 0)
        by_fraud_type = alert_stats.get("by_fraud_type", {})
        by_severity = alert_stats.get("by_severity", {})
        cases_open = case_stats.get("open", 0)
        cases_resolved = case_stats.get("closed", 0)

        session.close()
    except Exception as e:
        logger.warning(f"Could not fetch DB stats: {e}")

    fraud_rate = (
        (state.fraud_detected / state.transactions_processed * 100)
        if state.transactions_processed > 0
        else 0.0
    )

    return StatsResponse(
        transactions_processed=state.transactions_processed,
        fraud_detected=state.fraud_detected,
        alerts_created=alerts_created,
        cases_open=cases_open,
        cases_resolved=cases_resolved,
        by_fraud_type=by_fraud_type,
        by_severity=by_severity,
        uptime_seconds=round(uptime, 2),
        fraud_rate=round(fraud_rate, 2),
    )


# =============================================================================
# WebSocket Endpoint
# =============================================================================


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint for real-time fraud alert streaming.

    Connect to receive alerts as they are detected.
    """
    await websocket.accept()
    state.ws_clients.append(websocket)
    logger.info(f"WebSocket client connected (total: {len(state.ws_clients)})")

    try:
        await websocket.send_json(
            {
                "type": "connection",
                "message": "Connected to SentinelFlow alert stream",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        while True:
            data = await websocket.receive_text()

            if data == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if websocket in state.ws_clients:
            state.ws_clients.remove(websocket)


# =============================================================================
# Root & Metrics
# =============================================================================


@app.get("/", tags=["Root"])
async def root():
    """API root - service info."""
    return {
        "service": "SentinelFlow Fraud Detection API",
        "version": "2.1.0",
        "schema_version": SCHEMA_VERSION,
        "docs": "/docs",
        "health": "/api/v1/system/health",
    }


@app.get("/metrics", tags=["Monitoring"])
async def metrics():
    """Prometheus-compatible metrics endpoint."""
    uptime = time.time() - state.start_time

    lines = [
        "# HELP sentinelflow_transactions_processed_total Total transactions processed",
        "# TYPE sentinelflow_transactions_processed_total counter",
        f"sentinelflow_transactions_processed_total {state.transactions_processed}",
        "",
        "# HELP sentinelflow_fraud_detected_total Total fraud cases detected",
        "# TYPE sentinelflow_fraud_detected_total counter",
        f"sentinelflow_fraud_detected_total {state.fraud_detected}",
        "",
        "# HELP sentinelflow_errors_total Total errors",
        "# TYPE sentinelflow_errors_total counter",
        f"sentinelflow_errors_total {state.errors}",
        "",
        "# HELP sentinelflow_uptime_seconds API uptime in seconds",
        "# TYPE sentinelflow_uptime_seconds gauge",
        f"sentinelflow_uptime_seconds {uptime:.2f}",
        "",
        "# HELP sentinelflow_websocket_clients Active WebSocket connections",
        "# TYPE sentinelflow_websocket_clients gauge",
        f"sentinelflow_websocket_clients {len(state.ws_clients)}",
        "",
    ]

    return "\n".join(lines)


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """Run the API server."""
    import uvicorn

    host = os.getenv("SENTINELFLOW_API_HOST", "0.0.0.0")
    port = int(os.getenv("SENTINELFLOW_API_PORT", "8000"))

    logger.info(f"Starting SentinelFlow API on {host}:{port}")
    uvicorn.run(
        "sentinelflow.api.app:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()

# =============================================================================
# SentinelFlow API - ML Routes
# =============================================================================
"""
Machine Learning model management endpoints.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from loguru import logger

from sentinelflow.api.schemas import (
    ModelInfo,
    ModelStatusResponse,
    TrainRequest,
    TrainResponse,
)
from sentinelflow.auth.dependencies import require_analyst
from sentinelflow.contracts import User

router = APIRouter(prefix="/ml", tags=["Machine Learning"])

# Global state for training status
_training_status = {
    "is_training": False,
    "started_at": None,
    "progress": 0,
    "message": "",
}


@router.get(
    "/models",
    response_model=ModelStatusResponse,
    summary="Get ML model status",
    description="Returns the status of all ML models in the ensemble.",
)
async def get_model_status():
    """Get status of ML models."""
    try:

        models_dir = os.path.join(os.getcwd(), "models")

        # Check IsolationForest
        if_path = os.path.join(models_dir, "isolation_forest.pkl")
        if_ready = os.path.exists(if_path)
        if_info = ModelInfo(
            name="IsolationForest",
            version="1.0.0",
            ready=if_ready,
            last_trained=(
                datetime.fromtimestamp(os.path.getmtime(if_path), tz=timezone.utc)
                if if_ready
                else None
            ),
        )

        # Check XGBoost
        xgb_path = os.path.join(models_dir, "xgboost_fraud.json")
        xgb_ready = os.path.exists(xgb_path)
        xgb_info = ModelInfo(
            name="XGBoost",
            version="1.0.0",
            ready=xgb_ready,
            last_trained=(
                datetime.fromtimestamp(os.path.getmtime(xgb_path), tz=timezone.utc)
                if xgb_ready
                else None
            ),
        )

        # Check AutoEncoder
        ae_path = os.path.join(models_dir, "autoencoder.pt")
        ae_ready = os.path.exists(ae_path)
        ae_info = ModelInfo(
            name="AutoEncoder",
            version="1.0.0",
            ready=ae_ready,
            last_trained=(
                datetime.fromtimestamp(os.path.getmtime(ae_path), tz=timezone.utc)
                if ae_ready
                else None
            ),
        )

        ensemble_ready = if_ready and xgb_ready

        return ModelStatusResponse(
            isolation_forest=if_info,
            xgboost=xgb_info,
            autoencoder=ae_info,
            ensemble_ready=ensemble_ready,
            ensemble_threshold=0.7,
        )

    except Exception as e:
        logger.error(f"Error getting model status: {e}")
        return ModelStatusResponse()


@router.post(
    "/train",
    response_model=TrainResponse,
    summary="Train ML models",
    description="Trigger training of all ML models with synthetic data.",
)
async def train_models(
    request: TrainRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_analyst),
):
    """Train ML models."""
    global _training_status

    if _training_status["is_training"]:
        raise HTTPException(
            status_code=409,
            detail="Training already in progress",
        )

    _training_status["is_training"] = True
    _training_status["started_at"] = datetime.now(timezone.utc)
    _training_status["progress"] = 0
    _training_status["message"] = "Starting training..."

    # Run training in background
    background_tasks.add_task(
        _run_training,
        n_samples=request.n_samples,
        fraud_ratio=request.fraud_ratio,
    )

    return TrainResponse(
        status="started",
        training_time_seconds=0,
        dataset_size=request.n_samples,
        metrics={"message": "Training started in background"},
    )


@router.get(
    "/train/status",
    summary="Get training status",
)
async def get_training_status():
    """Get current training status."""
    global _training_status

    return {
        "is_training": _training_status["is_training"],
        "started_at": _training_status["started_at"],
        "progress": _training_status["progress"],
        "message": _training_status["message"],
    }


async def _run_training(n_samples: int, fraud_ratio: float):
    """Background training task."""
    global _training_status

    start_time = time.time()

    try:
        from sentinelflow.ml.train_pipeline import TrainPipeline

        _training_status["message"] = "Starting training pipeline..."
        _training_status["progress"] = 10

        # pipeline.run() kendi sentetik dataset'ini üretir; ayrıca loader çağırmaya gerek yok
        pipeline = TrainPipeline(output_dir="models")

        _training_status["progress"] = 30
        _training_status["message"] = "Training models (IsolationForest + XGBoost + AutoEncoder)..."
        pipeline.run(
            n_samples=n_samples,
            fraud_ratio=fraud_ratio,
        )

        elapsed = time.time() - start_time
        _training_status["progress"] = 100
        _training_status["message"] = f"Training completed in {elapsed:.1f}s"
        logger.info(f"Training completed: {n_samples} samples, {elapsed:.1f}s")
    except Exception as e:
        logger.error(f"Training failed: {e}")
        _training_status["message"] = f"Training failed: {str(e)}"
    finally:
        _training_status["is_training"] = False


@router.get(
    "/features",
    summary="Get feature definitions",
    description="Returns the list of features used by the ML ensemble.",
)
async def get_features():
    """Get ML feature definitions."""
    try:
        from sentinelflow.ml.feature_engine import FEATURE_DESCRIPTIONS, FEATURE_NAMES

        features = []
        for name in FEATURE_NAMES:
            features.append(
                {
                    "name": name,
                    "description": FEATURE_DESCRIPTIONS.get(name, ""),
                }
            )

        return {
            "total_features": len(FEATURE_NAMES),
            "features": features,
        }

    except Exception as e:
        logger.error(f"Error getting features: {e}")
        return {"total_features": 0, "features": []}

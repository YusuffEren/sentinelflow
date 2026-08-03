# =============================================================================
# SentinelFlow - Ensemble Voter
# =============================================================================
"""
Multi-model ensemble voting for fraud detection.

Combines predictions from multiple ML models using weighted averaging
to produce a final fraud probability and decision.

Usage:
    voter = EnsembleVoter()
    voter.add_model(isolation_forest, weight=0.3)
    voter.add_model(xgboost_model, weight=0.5)
    voter.add_model(autoencoder, weight=0.2)

    result = voter.predict(features)
    # result = {"is_fraud": True, "final_score": 0.82, "model_scores": {...}}
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
from loguru import logger

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class EnsemblePrediction:
    """Result of ensemble prediction."""

    is_fraud: bool = False
    final_score: float = 0.0
    model_scores: dict[str, float] = field(default_factory=dict)
    model_weights: dict[str, float] = field(default_factory=dict)
    active_models: int = 0
    threshold_used: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for evidence/serialization."""
        return {
            "is_fraud": self.is_fraud,
            "ensemble_score": round(self.final_score, 4),
            "model_scores": {k: round(v, 4) for k, v in self.model_scores.items()},
            "model_weights": {k: round(v, 4) for k, v in self.model_weights.items()},
            "active_models": self.active_models,
            "threshold": self.threshold_used,
        }


# =============================================================================
# Ensemble Voter
# =============================================================================


class EnsembleVoter:
    """
    Weighted ensemble voter for multiple fraud detection models.

    Only uses models that are ready (trained). Weights are automatically
    normalized among active models.
    """

    def __init__(self, threshold: float = 0.65) -> None:
        """
        Initialize ensemble voter.

        Args:
            threshold: Fraud decision threshold (0-1). Scores above this
                       are classified as fraud.
        """
        self._models: list[tuple[Any, float]] = []  # (model, weight)
        self._threshold = threshold

        logger.info(f"EnsembleVoter initialized (threshold={threshold})")

    def add_model(self, model: Any, weight: float = 1.0) -> None:
        """
        Add a model to the ensemble.

        Args:
            model: A BaseFraudModel instance
            weight: Relative weight for this model's predictions
        """
        self._models.append((model, weight))
        logger.info(f"Added model '{model.name}' with weight {weight}")

    def predict(self, features: np.ndarray) -> EnsemblePrediction:
        """
        Get ensemble prediction for a single feature vector.

        Args:
            features: 1D numpy array of features

        Returns:
            EnsemblePrediction with final score and per-model breakdown
        """
        result = EnsemblePrediction(threshold_used=self._threshold)

        active_scores: list[tuple[str, float, float]] = []  # (name, score, weight)

        for model, weight in self._models:
            if not model.is_ready:
                continue

            try:
                score = model.predict_single(features)
                active_scores.append((model.name, float(score), weight))
                result.model_scores[model.name] = float(score)
                result.model_weights[model.name] = weight
            except Exception as e:
                logger.error(f"Model '{model.name}' prediction error: {e}")

        result.active_models = len(active_scores)

        if not active_scores:
            return result

        # Weighted average
        total_weight = sum(w for _, _, w in active_scores)
        if total_weight > 0:
            result.final_score = (
                sum(score * weight for _, score, weight in active_scores) / total_weight
            )

        result.is_fraud = result.final_score >= self._threshold

        return result

    def predict_batch(self, X: np.ndarray) -> list[EnsemblePrediction]:
        """
        Get ensemble predictions for a batch of feature vectors.

        Args:
            X: 2D numpy array of shape (n_samples, n_features)

        Returns:
            List of EnsemblePrediction objects
        """
        return [self.predict(X[i]) for i in range(len(X))]

    @property
    def threshold(self) -> float:
        """Current fraud decision threshold."""
        return self._threshold

    @threshold.setter
    def threshold(self, value: float) -> None:
        """Update fraud decision threshold."""
        self._threshold = max(0.0, min(1.0, value))
        logger.info(f"Ensemble threshold updated to {self._threshold}")

    @property
    def num_models(self) -> int:
        """Total number of models in ensemble."""
        return len(self._models)

    @property
    def num_ready_models(self) -> int:
        """Number of models that are trained and ready."""
        return sum(1 for model, _ in self._models if model.is_ready)

    @property
    def model_summary(self) -> dict[str, dict[str, Any]]:
        """Summary of all models and their status."""
        return {
            model.name: {
                "weight": weight,
                "is_ready": model.is_ready,
            }
            for model, weight in self._models
        }

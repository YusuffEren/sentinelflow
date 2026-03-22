# =============================================================================
# SentinelFlow - ML Fraud Detection Models
# =============================================================================
"""
Multiple ML models for fraud detection, each implementing a common interface.

Models:
1. IsolationForestModel  - Unsupervised anomaly detection with full feature vectors
2. XGBoostFraudModel     - Supervised gradient boosting (requires training data)
3. AutoEncoderModel      - Deep learning reconstruction-based anomaly detection

Each model implements:
    - fit(X): Train on feature matrix
    - predict(X): Returns fraud probability [0, 1]
    - is_ready: Whether model has been trained
"""

from __future__ import annotations

import os
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.warning("scikit-learn not available, IsolationForest disabled")

try:
    import xgboost as xgb

    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    logger.warning("xgboost not available, XGBoost model disabled")

try:
    import torch
    import torch.nn as nn

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    logger.warning("torch not available, AutoEncoder model disabled")


# =============================================================================
# Base Model Interface
# =============================================================================


class BaseFraudModel(ABC):
    """Abstract base class for all fraud detection models."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray | None = None) -> None:
        """Train the model on feature matrix X and optional labels y."""
        ...

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return fraud probability for each sample. Shape: (n_samples,)."""
        ...

    @abstractmethod
    def predict_single(self, features: np.ndarray) -> float:
        """Return fraud probability for a single feature vector."""
        ...

    def predict(self, features: np.ndarray) -> float:
        """Backward-compatible alias for single-vector prediction."""
        return self.predict_single(features)

    @property
    @abstractmethod
    def is_ready(self) -> bool:
        """Whether the model has been trained and is ready for prediction."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable model name."""
        ...

    def save(self, path: str | None = None) -> None:
        """Save model to disk. Override in subclasses for custom serialization."""
        raise NotImplementedError

    def load(self, path: str) -> None:
        """Load model from disk. Override in subclasses for custom deserialization."""
        raise NotImplementedError


# =============================================================================
# Isolation Forest Model (Unsupervised)
# =============================================================================


class IsolationForestModel(BaseFraudModel):
    """
    Enhanced Isolation Forest using full feature vectors.

    Unlike the basic version that only uses amount, this uses all 21 features
    from the TransactionFeatureEngine for much better anomaly detection.
    """

    def __init__(
        self,
        contamination: float = 0.05,
        n_estimators: int = 200,
        min_samples_to_train: int = 100,
        retrain_interval: int = 500,
    ) -> None:
        self._contamination = contamination
        self._n_estimators = n_estimators
        self._min_samples = min_samples_to_train
        self._retrain_interval = retrain_interval

        self._model: IsolationForest | None = None
        self._scaler = StandardScaler() if HAS_SKLEARN else None
        self._is_fitted = False
        self._samples_since_train = 0
        self._training_buffer: list[np.ndarray] = []

        logger.info(
            f"IsolationForestModel initialized "
            f"(contamination={contamination}, n_estimators={n_estimators})"
        )

    def fit(self, X: np.ndarray, y: np.ndarray | None = None) -> None:
        """Train IsolationForest on feature matrix."""
        if not HAS_SKLEARN:
            return

        if len(X) < self._min_samples:
            logger.debug(f"IsolationForest: Need {self._min_samples} samples, have {len(X)}")
            return

        # Scale features
        X_scaled = self._scaler.fit_transform(X)

        self._model = IsolationForest(
            contamination=self._contamination,
            n_estimators=self._n_estimators,
            random_state=42,
            max_samples="auto",
            n_jobs=-1,
        )
        self._model.fit(X_scaled)
        self._is_fitted = True
        self._samples_since_train = 0

        logger.info(f"IsolationForest trained on {len(X)} samples")

    def add_sample_and_maybe_retrain(self, features: np.ndarray) -> None:
        """Add a sample to the buffer and retrain if interval reached."""
        self._training_buffer.append(features.copy())
        self._samples_since_train += 1

        # Keep buffer manageable
        if len(self._training_buffer) > 5000:
            self._training_buffer = self._training_buffer[-3000:]

        # Auto-retrain at intervals
        if (
            self._samples_since_train >= self._retrain_interval
            and len(self._training_buffer) >= self._min_samples
        ):
            X = np.array(self._training_buffer)
            self.fit(X)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return anomaly scores as probabilities [0, 1]."""
        if not self._is_fitted or self._model is None or self._scaler is None:
            return np.full(len(X), 0.0)

        X_scaled = self._scaler.transform(X)
        # decision_function: negative = anomaly, positive = normal
        scores = self._model.decision_function(X_scaled)
        # Convert to probability: lower score = higher fraud probability
        # Normalize scores to [0, 1] range
        probs = 1.0 / (1.0 + np.exp(scores * 5))  # Sigmoid transformation
        return probs

    def predict_single(self, features: np.ndarray) -> float:
        """Return fraud probability for a single feature vector."""
        if not self._is_fitted:
            return 0.0
        X = features.reshape(1, -1)
        return float(self.predict_proba(X)[0])

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return "IsolationForest"

    def save(self, path: str | None = None) -> None:
        """Save IsolationForest model and scaler using pickle."""
        if not self._is_fitted:
            return
        path = path or "models/isolation_forest.pkl"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({"model": self._model, "scaler": self._scaler}, f)
        logger.info(f"IsolationForest saved to {path}")

    def load(self, path: str) -> None:
        """Load IsolationForest model and scaler from pickle."""
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
            self._model = data["model"]
            self._scaler = data["scaler"]
            self._is_fitted = True
            logger.info(f"IsolationForest loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load IsolationForest: {e}")


# =============================================================================
# XGBoost Model (Supervised)
# =============================================================================


class XGBoostFraudModel(BaseFraudModel):
    """
    XGBoost gradient boosting classifier for fraud detection.

    Can be trained on labeled datasets (e.g., Kaggle IEEE-CIS)
    or used with a pre-trained model file.
    """

    def __init__(
        self,
        model_path: str | None = None,
        n_estimators: int = 300,
        max_depth: int = 6,
        learning_rate: float = 0.1,
        scale_pos_weight: float = 20.0,  # Handle class imbalance
    ) -> None:
        self._n_estimators = n_estimators
        self._max_depth = max_depth
        self._learning_rate = learning_rate
        self._scale_pos_weight = scale_pos_weight
        self._model: Any = None
        self._scaler = StandardScaler() if HAS_SKLEARN else None
        self._is_fitted = False

        # Load pre-trained model if path provided
        if model_path and os.path.exists(model_path) and HAS_XGBOOST:
            self._load_model(model_path)

        logger.info(
            f"XGBoostFraudModel initialized "
            f"(n_estimators={n_estimators}, max_depth={max_depth})"
        )

    def _load_model(self, path: str) -> None:
        """Load a pre-trained XGBoost model from file."""
        try:
            self._model = xgb.XGBClassifier()
            self._model.load_model(path)
            self._is_fitted = True
            logger.info(f"XGBoost model loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load XGBoost model: {e}")
            self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray | None = None) -> None:
        """Train XGBoost on labeled data."""
        if not HAS_XGBOOST or y is None:
            logger.warning("XGBoost requires labels for training")
            return

        if len(X) < 50:
            logger.debug(f"XGBoost: Need at least 50 samples, have {len(X)}")
            return

        # Scale features
        X_scaled = self._scaler.fit_transform(X)

        self._model = xgb.XGBClassifier(
            n_estimators=self._n_estimators,
            max_depth=self._max_depth,
            learning_rate=self._learning_rate,
            scale_pos_weight=self._scale_pos_weight,
            random_state=42,
            eval_metric="logloss",
            use_label_encoder=False,
        )
        self._model.fit(X_scaled, y)
        self._is_fitted = True

        logger.info(f"XGBoost trained on {len(X)} samples ({int(y.sum())} fraud)")

    def save(self, path: str | None = None) -> None:
        """Save trained model to file."""
        if self._model is not None:
            path = path or "models/xgboost_fraud.json"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._model.save_model(path)
            logger.info(f"XGBoost model saved to {path}")

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return fraud probability for each sample."""
        if not self._is_fitted or self._model is None or self._scaler is None:
            return np.full(len(X), 0.0)

        X_scaled = self._scaler.transform(X)
        # predict_proba returns [prob_class_0, prob_class_1]
        probs = self._model.predict_proba(X_scaled)[:, 1]
        return probs

    def predict_single(self, features: np.ndarray) -> float:
        """Return fraud probability for a single feature vector."""
        if not self._is_fitted:
            return 0.0
        X = features.reshape(1, -1)
        return float(self.predict_proba(X)[0])

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return "XGBoost"


# =============================================================================
# AutoEncoder Model (Deep Learning Anomaly Detection)
# =============================================================================


class _AutoEncoderNetwork(nn.Module if HAS_TORCH else object):
    """PyTorch AutoEncoder for learning normal transaction patterns."""

    def __init__(self, input_dim: int = 21, encoding_dim: int = 8):
        if not HAS_TORCH:
            return
        super().__init__()

        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.BatchNorm1d(32),
            nn.Linear(32, encoding_dim),
            nn.ReLU(),
        )

        self.decoder = nn.Sequential(
            nn.Linear(encoding_dim, 32),
            nn.ReLU(),
            nn.BatchNorm1d(32),
            nn.Dropout(0.2),
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Linear(64, input_dim),
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded


class AutoEncoderModel(BaseFraudModel):
    """
    AutoEncoder-based anomaly detection.

    Learns to reconstruct 'normal' transactions. High reconstruction error
    indicates anomalous/fraudulent transactions.
    """

    def __init__(
        self,
        input_dim: int = 21,
        encoding_dim: int = 8,
        threshold_percentile: float = 95.0,
        model_path: str | None = None,
    ) -> None:
        self._input_dim = input_dim
        self._encoding_dim = encoding_dim
        self._threshold_percentile = threshold_percentile
        self._threshold: float | None = None
        self._network: _AutoEncoderNetwork | None = None
        self._scaler = StandardScaler() if HAS_SKLEARN else None
        self._is_fitted = False

        if HAS_TORCH:
            self._network = _AutoEncoderNetwork(input_dim, encoding_dim)

            if model_path and os.path.exists(model_path):
                self._load_model(model_path)

        logger.info(
            f"AutoEncoderModel initialized " f"(input_dim={input_dim}, encoding_dim={encoding_dim})"
        )

    def _load_model(self, path: str) -> None:
        """Load pre-trained AutoEncoder weights and scaler."""
        try:
            if HAS_TORCH and self._network is not None:
                checkpoint = torch.load(path, map_location="cpu", weights_only=False)
                self._network.load_state_dict(checkpoint["model_state_dict"])
                self._threshold = checkpoint.get("threshold", None)
                self._scaler = checkpoint.get("scaler", self._scaler)
                self._is_fitted = True
                logger.info(f"AutoEncoder loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load AutoEncoder: {e}")

    def fit(self, X: np.ndarray, y: np.ndarray | None = None, epochs: int = 50) -> None:
        """
        Train AutoEncoder on normal transaction data.

        Args:
            X: Feature matrix (preferably only normal transactions)
            y: Labels (optional, used to filter normal transactions)
            epochs: Number of training epochs
        """
        if not HAS_TORCH or self._network is None or self._scaler is None:
            return

        if len(X) < 100:
            logger.debug(f"AutoEncoder: Need at least 100 samples, have {len(X)}")
            return

        # If labels provided, train only on normal transactions
        if y is not None:
            normal_mask = y == 0
            X_train = X[normal_mask]
        else:
            X_train = X

        if len(X_train) < 50:
            return

        # Scale features
        X_scaled = self._scaler.fit_transform(X_train)
        X_tensor = torch.FloatTensor(X_scaled)

        # Training
        self._network.train()
        optimizer = torch.optim.Adam(self._network.parameters(), lr=0.001)
        criterion = nn.MSELoss()

        dataset = torch.utils.data.TensorDataset(X_tensor)
        loader = torch.utils.data.DataLoader(dataset, batch_size=64, shuffle=True)

        for epoch in range(epochs):
            total_loss = 0.0
            for (batch,) in loader:
                optimizer.zero_grad()
                reconstructed = self._network(batch)
                loss = criterion(reconstructed, batch)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()

            if (epoch + 1) % 10 == 0:
                avg_loss = total_loss / len(loader)
                logger.debug(f"AutoEncoder epoch {epoch+1}/{epochs}, loss: {avg_loss:.6f}")

        # Calculate threshold from training data reconstruction errors
        self._network.eval()
        with torch.no_grad():
            reconstructed = self._network(X_tensor)
            errors = torch.mean((X_tensor - reconstructed) ** 2, dim=1).numpy()
            self._threshold = float(np.percentile(errors, self._threshold_percentile))

        self._is_fitted = True
        logger.info(
            f"AutoEncoder trained on {len(X_train)} samples, " f"threshold: {self._threshold:.6f}"
        )

    def save(self, path: str | None = None) -> None:
        """Save trained AutoEncoder."""
        if HAS_TORCH and self._network is not None and self._is_fitted:
            path = path or "models/autoencoder.pt"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            torch.save(
                {
                    "model_state_dict": self._network.state_dict(),
                    "threshold": self._threshold,
                    "scaler": self._scaler,
                },
                path,
            )
            logger.info(f"AutoEncoder saved to {path}")

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return anomaly scores based on reconstruction error."""
        if not self._is_fitted or self._network is None or self._scaler is None:
            return np.full(len(X), 0.0)

        X_scaled = self._scaler.transform(X)
        X_tensor = torch.FloatTensor(X_scaled)

        self._network.eval()
        with torch.no_grad():
            reconstructed = self._network(X_tensor)
            errors = torch.mean((X_tensor - reconstructed) ** 2, dim=1).numpy()

        # Convert errors to probabilities using threshold
        if self._threshold and self._threshold > 0:
            probs = np.clip(errors / (self._threshold * 2), 0.0, 1.0)
        else:
            probs = np.zeros(len(X))

        return probs

    def predict_single(self, features: np.ndarray) -> float:
        """Return anomaly score for a single feature vector."""
        if not self._is_fitted:
            return 0.0
        X = features.reshape(1, -1)
        return float(self.predict_proba(X)[0])

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return "AutoEncoder"

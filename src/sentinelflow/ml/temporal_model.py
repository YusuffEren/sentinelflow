# =============================================================================
# SentinelFlow - Temporal Pattern Detection Model
# =============================================================================
"""
LSTM and Transformer-based temporal sequence models for fraud detection.

This module provides sequence-based anomaly detection that analyzes
temporal patterns in account transaction histories to detect:
- Behavior drift (sudden changes in spending patterns)
- Temporal anomalies (unusual transaction timing)
- Velocity attacks (rapid successive transactions)
- Sequence-based fraud patterns

Architecture:
    - LSTM-based sequence model for transaction history
    - Transformer encoder for attention-based pattern learning
    - Per-account history tracking
    - Sliding window analysis

Usage:
    from sentinelflow.ml.temporal_model import TemporalFraudModel

    model = TemporalFraudModel(model_type="lstm")
    model.fit(X_sequences, y_labels)
    risk_score = model.predict_sequence(account_history)
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import math

import numpy as np
from loguru import logger

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.optim import Adam
    from torch.optim.lr_scheduler import ReduceLROnPlateau
    from torch.utils.data import Dataset, DataLoader

    HAS_TORCH = True
except (ImportError, OSError):
    HAS_TORCH = False
    logger.warning("PyTorch not available, temporal model disabled")


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class TransactionSequence:
    """A sequence of transactions for an account."""

    iban: str
    features: np.ndarray  # Shape: (seq_len, num_features)
    timestamps: list[float]
    is_fraud: bool = False

    @property
    def length(self) -> int:
        return len(self.features)


@dataclass
class TemporalPrediction:
    """Prediction result from temporal model."""

    risk_score: float
    is_fraud: bool
    confidence: float
    behavior_drift_score: float  # How much behavior has changed
    velocity_score: float  # Transaction velocity anomaly
    attention_weights: np.ndarray | None = None  # For Transformer

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_score": round(self.risk_score, 4),
            "is_fraud": self.is_fraud,
            "confidence": round(self.confidence, 4),
            "behavior_drift_score": round(self.behavior_drift_score, 4),
            "velocity_score": round(self.velocity_score, 4),
        }


# =============================================================================
# Sequence Feature Extractor
# =============================================================================


class SequenceFeatureExtractor:
    """
    Extracts sequence features from transaction history.

    Features per transaction:
        - amount_normalized
        - hour_of_day (encoded)
        - day_of_week (encoded)
        - time_since_last_tx
        - amount_diff_from_avg
        - is_weekend
        - is_night
        - velocity (tx count in last hour)
    """

    FEATURE_NAMES = [
        "amount_normalized",
        "amount_log",
        "hour_sin",
        "hour_cos",
        "day_sin",
        "day_cos",
        "time_since_last",
        "amount_diff_avg",
        "is_weekend",
        "is_night",
        "velocity_1h",
        "cumsum_normalized",
    ]

    NUM_FEATURES = len(FEATURE_NAMES)

    def __init__(self, max_amount: float = 1_000_000.0):
        self._max_amount = max_amount
        self._account_histories: dict[str, deque] = {}
        self._account_stats: dict[str, dict] = {}

    def extract_sequence(
        self,
        transactions: list[dict],
        iban: str | None = None,
    ) -> np.ndarray:
        """
        Extract feature sequence from a list of transactions.

        Args:
            transactions: List of transaction dicts with amount, timestamp, etc.
            iban: Account IBAN for history tracking

        Returns:
            Feature array of shape (len(transactions), NUM_FEATURES)
        """
        if not transactions:
            return np.zeros((0, self.NUM_FEATURES), dtype=np.float32)

        features = []
        prev_timestamp = None
        cumsum = 0.0

        # Get account stats
        if iban and iban in self._account_stats:
            avg_amount = self._account_stats[iban].get("avg_amount", 5000.0)
        else:
            amounts = [float(tx.get("amount", 0)) for tx in transactions]
            avg_amount = np.mean(amounts) if amounts else 5000.0

        for i, tx in enumerate(transactions):
            amount = float(tx.get("amount", 0.0))
            cumsum += amount

            # Parse timestamp
            ts_str = tx.get("timestamp", "")
            try:
                from datetime import datetime

                if "T" in ts_str:
                    ts = datetime.fromisoformat(ts_str.replace("Z", ""))
                else:
                    ts = datetime.now()
                timestamp = ts.timestamp()
                hour = ts.hour
                day = ts.weekday()
            except:
                from datetime import datetime

                ts = datetime.now()
                timestamp = ts.timestamp()
                hour = ts.hour
                day = ts.weekday()

            # Time since last transaction
            if prev_timestamp is not None:
                time_since_last = (timestamp - prev_timestamp) / 3600.0  # hours
            else:
                time_since_last = 24.0  # Default to 24 hours

            # Calculate velocity (transactions in last hour)
            if iban and iban in self._account_histories:
                history = self._account_histories[iban]
                one_hour_ago = timestamp - 3600
                velocity = sum(1 for t in history if t > one_hour_ago)
            else:
                velocity = i + 1  # Approximate from current sequence

            # Build feature vector
            feature_vec = [
                amount / self._max_amount,  # amount_normalized
                np.log1p(amount) / 15.0,  # amount_log (normalized)
                math.sin(2 * math.pi * hour / 24),  # hour_sin
                math.cos(2 * math.pi * hour / 24),  # hour_cos
                math.sin(2 * math.pi * day / 7),  # day_sin
                math.cos(2 * math.pi * day / 7),  # day_cos
                min(time_since_last, 168.0) / 168.0,  # time_since_last (capped at 1 week)
                (amount - avg_amount) / (avg_amount + 1e-6),  # amount_diff_avg
                1.0 if day >= 5 else 0.0,  # is_weekend
                1.0 if hour < 6 or hour >= 23 else 0.0,  # is_night
                min(velocity, 20) / 20.0,  # velocity_1h (normalized)
                cumsum / (self._max_amount * len(transactions)),  # cumsum_normalized
            ]

            features.append(feature_vec)
            prev_timestamp = timestamp

        return np.array(features, dtype=np.float32)

    def update_account_history(self, iban: str, timestamp: float, amount: float) -> None:
        """Update account history for velocity calculation."""
        if iban not in self._account_histories:
            self._account_histories[iban] = deque(maxlen=1000)
            self._account_stats[iban] = {"amounts": deque(maxlen=500), "avg_amount": 5000.0}

        self._account_histories[iban].append(timestamp)
        self._account_stats[iban]["amounts"].append(amount)
        amounts = list(self._account_stats[iban]["amounts"])
        self._account_stats[iban]["avg_amount"] = np.mean(amounts) if amounts else 5000.0


# =============================================================================
# Dataset for Training
# =============================================================================

if HAS_TORCH:

    class TransactionSequenceDataset(Dataset):
        """PyTorch Dataset for transaction sequences."""

        def __init__(
            self,
            sequences: list[np.ndarray],
            labels: list[int],
            max_seq_len: int = 50,
        ):
            self.sequences = sequences
            self.labels = labels
            self.max_seq_len = max_seq_len

        def __len__(self) -> int:
            return len(self.sequences)

        def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            seq = self.sequences[idx]
            label = self.labels[idx]

            # Pad or truncate sequence
            seq_len = len(seq)
            if seq_len > self.max_seq_len:
                seq = seq[-self.max_seq_len :]  # Take last N transactions
                seq_len = self.max_seq_len
            elif seq_len < self.max_seq_len:
                padding = np.zeros((self.max_seq_len - seq_len, seq.shape[1]), dtype=np.float32)
                seq = np.vstack([padding, seq])

            # Create mask (1 for real data, 0 for padding)
            mask = np.zeros(self.max_seq_len, dtype=np.float32)
            mask[-seq_len:] = 1.0

            return (
                torch.FloatTensor(seq),
                torch.FloatTensor(mask),
                torch.FloatTensor([label]),
            )


# =============================================================================
# LSTM Network
# =============================================================================

if HAS_TORCH:

    class LSTMFraudNetwork(nn.Module):
        """
        LSTM-based network for sequence fraud detection.

        Uses bidirectional LSTM to capture temporal dependencies
        in both directions, followed by attention-weighted pooling.
        """

        def __init__(
            self,
            input_dim: int,
            hidden_dim: int = 64,
            num_layers: int = 2,
            dropout: float = 0.3,
            bidirectional: bool = True,
        ):
            super().__init__()

            self.hidden_dim = hidden_dim
            self.num_layers = num_layers
            self.bidirectional = bidirectional
            self.num_directions = 2 if bidirectional else 1

            # Input projection
            self.input_proj = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.ReLU(),
                nn.Dropout(dropout),
            )

            # LSTM layers
            self.lstm = nn.LSTM(
                input_size=hidden_dim,
                hidden_size=hidden_dim,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout if num_layers > 1 else 0,
                bidirectional=bidirectional,
            )

            lstm_output_dim = hidden_dim * self.num_directions

            # Attention layer
            self.attention = nn.Sequential(
                nn.Linear(lstm_output_dim, hidden_dim),
                nn.Tanh(),
                nn.Linear(hidden_dim, 1),
            )

            # Classification head
            self.classifier = nn.Sequential(
                nn.Linear(lstm_output_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(hidden_dim, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

            # Behavior drift head
            self.drift_head = nn.Sequential(
                nn.Linear(lstm_output_dim, 32),
                nn.ReLU(),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

            # Velocity anomaly head
            self.velocity_head = nn.Sequential(
                nn.Linear(lstm_output_dim, 32),
                nn.ReLU(),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

        def forward(
            self,
            x: torch.Tensor,
            mask: torch.Tensor | None = None,
        ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            Forward pass.

            Args:
                x: Input sequence (batch, seq_len, input_dim)
                mask: Attention mask (batch, seq_len)

            Returns:
                fraud_prob, drift_score, velocity_score, attention_weights
            """
            batch_size, seq_len, _ = x.shape

            # Project input
            x = self.input_proj(x)

            # LSTM
            lstm_out, _ = self.lstm(x)

            # Attention pooling
            attn_scores = self.attention(lstm_out).squeeze(-1)

            if mask is not None:
                attn_scores = attn_scores.masked_fill(mask == 0, float("-inf"))

            attn_weights = F.softmax(attn_scores, dim=1)
            context = torch.bmm(attn_weights.unsqueeze(1), lstm_out).squeeze(1)

            # Predictions
            fraud_prob = self.classifier(context)
            drift_score = self.drift_head(context)
            velocity_score = self.velocity_head(context)

            return fraud_prob, drift_score, velocity_score, attn_weights


# =============================================================================
# Transformer Network
# =============================================================================

if HAS_TORCH:

    class PositionalEncoding(nn.Module):
        """Sinusoidal positional encoding for Transformer."""

        def __init__(self, d_model: int, max_len: int = 500, dropout: float = 0.1):
            super().__init__()
            self.dropout = nn.Dropout(p=dropout)

            pe = torch.zeros(max_len, d_model)
            position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
            div_term = torch.exp(
                torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model)
            )

            pe[:, 0::2] = torch.sin(position * div_term)
            pe[:, 1::2] = torch.cos(position * div_term)
            pe = pe.unsqueeze(0)

            self.register_buffer("pe", pe)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            x = x + self.pe[:, : x.size(1)]
            return self.dropout(x)

    class TransformerFraudNetwork(nn.Module):
        """
        Transformer-based network for sequence fraud detection.

        Uses self-attention to capture long-range dependencies
        and complex temporal patterns in transaction history.
        """

        def __init__(
            self,
            input_dim: int,
            d_model: int = 64,
            nhead: int = 4,
            num_layers: int = 3,
            dim_feedforward: int = 128,
            dropout: float = 0.3,
            max_seq_len: int = 100,
        ):
            super().__init__()

            self.d_model = d_model

            # Input projection
            self.input_proj = nn.Linear(input_dim, d_model)

            # Positional encoding
            self.pos_encoder = PositionalEncoding(d_model, max_seq_len, dropout)

            # Transformer encoder
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=d_model,
                nhead=nhead,
                dim_feedforward=dim_feedforward,
                dropout=dropout,
                batch_first=True,
            )
            self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

            # CLS token (learnable)
            self.cls_token = nn.Parameter(torch.randn(1, 1, d_model))

            # Classification head
            self.classifier = nn.Sequential(
                nn.Linear(d_model, d_model // 2),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(d_model // 2, 1),
                nn.Sigmoid(),
            )

            # Drift and velocity heads
            self.drift_head = nn.Sequential(
                nn.Linear(d_model, 32),
                nn.ReLU(),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

            self.velocity_head = nn.Sequential(
                nn.Linear(d_model, 32),
                nn.ReLU(),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

        def forward(
            self,
            x: torch.Tensor,
            mask: torch.Tensor | None = None,
        ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            Forward pass.

            Args:
                x: Input sequence (batch, seq_len, input_dim)
                mask: Padding mask (batch, seq_len)

            Returns:
                fraud_prob, drift_score, velocity_score, attention_weights
            """
            batch_size, seq_len, _ = x.shape

            # Project input
            x = self.input_proj(x) * math.sqrt(self.d_model)

            # Add CLS token
            cls_tokens = self.cls_token.expand(batch_size, -1, -1)
            x = torch.cat([cls_tokens, x], dim=1)

            # Add positional encoding
            x = self.pos_encoder(x)

            # Create attention mask (if provided)
            if mask is not None:
                # Add mask for CLS token (always attend)
                cls_mask = torch.ones(batch_size, 1, device=mask.device)
                full_mask = torch.cat([cls_mask, mask], dim=1)
                src_key_padding_mask = full_mask == 0
            else:
                src_key_padding_mask = None

            # Transformer
            transformer_out = self.transformer(x, src_key_padding_mask=src_key_padding_mask)

            # Use CLS token output
            cls_output = transformer_out[:, 0, :]

            # Predictions
            fraud_prob = self.classifier(cls_output)
            drift_score = self.drift_head(cls_output)
            velocity_score = self.velocity_head(cls_output)

            # Attention weights (simplified - actual attention is in transformer)
            attn_weights = torch.ones(batch_size, seq_len, device=x.device) / seq_len

            return fraud_prob, drift_score, velocity_score, attn_weights


# =============================================================================
# Main Temporal Fraud Model
# =============================================================================


class TemporalFraudModel:
    """
    Temporal sequence-based fraud detection model.

    Uses either LSTM or Transformer architecture to analyze
    transaction sequences and detect temporal anomalies.

    Example:
        >>> model = TemporalFraudModel(model_type="lstm")
        >>> model.fit(sequences, labels)
        >>> pred = model.predict_sequence(account_history)
    """

    def __init__(
        self,
        model_type: str = "lstm",  # "lstm" or "transformer"
        input_dim: int = 12,
        hidden_dim: int = 64,
        num_layers: int = 2,
        dropout: float = 0.3,
        max_seq_len: int = 50,
        learning_rate: float = 0.001,
        threshold: float = 0.5,
        model_path: str | None = None,
    ):
        """
        Initialize temporal model.

        Args:
            model_type: "lstm" or "transformer"
            input_dim: Number of input features per time step
            hidden_dim: Hidden layer size
            num_layers: Number of layers
            dropout: Dropout rate
            max_seq_len: Maximum sequence length
            learning_rate: Learning rate
            threshold: Classification threshold
            model_path: Path to load pre-trained model
        """
        self._model_type = model_type
        self._input_dim = input_dim
        self._hidden_dim = hidden_dim
        self._num_layers = num_layers
        self._dropout = dropout
        self._max_seq_len = max_seq_len
        self._learning_rate = learning_rate
        self._threshold = threshold

        self._network: nn.Module | None = None
        self._is_fitted = False
        self._device = (
            torch.device("cuda" if torch.cuda.is_available() else "cpu") if HAS_TORCH else None
        )

        self._feature_extractor = SequenceFeatureExtractor()

        if model_path and os.path.exists(model_path):
            self.load(model_path)
        else:
            self._build_network()

        logger.info(
            f"TemporalFraudModel initialized (type={model_type}, "
            f"hidden={hidden_dim}, layers={num_layers})"
        )

    def _build_network(self) -> None:
        """Build the neural network."""
        if not HAS_TORCH:
            return

        if self._model_type == "lstm":
            self._network = LSTMFraudNetwork(
                input_dim=self._input_dim,
                hidden_dim=self._hidden_dim,
                num_layers=self._num_layers,
                dropout=self._dropout,
            )
        elif self._model_type == "transformer":
            self._network = TransformerFraudNetwork(
                input_dim=self._input_dim,
                d_model=self._hidden_dim,
                num_layers=self._num_layers,
                dropout=self._dropout,
                max_seq_len=self._max_seq_len + 1,  # +1 for CLS token
            )
        else:
            raise ValueError(f"Unknown model type: {self._model_type}")

        self._network = self._network.to(self._device)

    def fit(
        self,
        sequences: list[np.ndarray],
        labels: list[int],
        epochs: int = 50,
        batch_size: int = 32,
        patience: int = 10,
        val_ratio: float = 0.2,
    ) -> dict[str, list[float]]:
        """
        Train the temporal model.

        Args:
            sequences: List of feature sequences
            labels: List of fraud labels (0 or 1)
            epochs: Number of training epochs
            batch_size: Training batch size
            patience: Early stopping patience
            val_ratio: Validation split ratio

        Returns:
            Training history dict
        """
        if not HAS_TORCH:
            return {"error": ["PyTorch not available"]}

        if self._network is None:
            self._build_network()

        logger.info(f"Starting temporal model training for {epochs} epochs...")

        # Split data
        n_val = int(len(sequences) * val_ratio)
        indices = np.random.permutation(len(sequences))
        val_indices = indices[:n_val]
        train_indices = indices[n_val:]

        train_seqs = [sequences[i] for i in train_indices]
        train_labels = [labels[i] for i in train_indices]
        val_seqs = [sequences[i] for i in val_indices]
        val_labels = [labels[i] for i in val_indices]

        # Create datasets
        train_dataset = TransactionSequenceDataset(train_seqs, train_labels, self._max_seq_len)
        val_dataset = TransactionSequenceDataset(val_seqs, val_labels, self._max_seq_len)

        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

        # Setup training
        optimizer = Adam(self._network.parameters(), lr=self._learning_rate)
        scheduler = ReduceLROnPlateau(optimizer, mode="min", patience=5, factor=0.5)

        # Class weights
        pos_count = sum(labels)
        neg_count = len(labels) - pos_count
        pos_weight = torch.tensor([neg_count / (pos_count + 1e-6)]).to(self._device)
        criterion = nn.BCELoss()

        history = {"train_loss": [], "val_loss": [], "val_auc": []}
        best_val_loss = float("inf")
        patience_counter = 0

        for epoch in range(epochs):
            # Training
            self._network.train()
            train_loss = 0.0

            for seq, mask, label in train_loader:
                seq = seq.to(self._device)
                mask = mask.to(self._device)
                label = label.to(self._device)

                optimizer.zero_grad()
                fraud_prob, _, _, _ = self._network(seq, mask)
                loss = criterion(fraud_prob, label)
                loss.backward()
                optimizer.step()

                train_loss += loss.item()

            train_loss /= len(train_loader)

            # Validation
            self._network.eval()
            val_loss = 0.0
            val_preds = []
            val_true = []

            with torch.no_grad():
                for seq, mask, label in val_loader:
                    seq = seq.to(self._device)
                    mask = mask.to(self._device)
                    label = label.to(self._device)

                    fraud_prob, _, _, _ = self._network(seq, mask)
                    loss = criterion(fraud_prob, label)
                    val_loss += loss.item()

                    val_preds.extend(fraud_prob.cpu().numpy().flatten())
                    val_true.extend(label.cpu().numpy().flatten())

            val_loss /= len(val_loader)

            # AUC
            try:
                from sklearn.metrics import roc_auc_score

                if len(np.unique(val_true)) > 1:
                    val_auc = roc_auc_score(val_true, val_preds)
                else:
                    val_auc = 0.5
            except:
                val_auc = 0.5

            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_loss)
            history["val_auc"].append(val_auc)

            scheduler.step(val_loss)

            # Early stopping
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
            else:
                patience_counter += 1

            if patience_counter >= patience:
                logger.info(f"Early stopping at epoch {epoch + 1}")
                break

            if (epoch + 1) % 10 == 0:
                logger.info(
                    f"Epoch {epoch + 1}/{epochs}: "
                    f"Train Loss={train_loss:.4f}, "
                    f"Val Loss={val_loss:.4f}, "
                    f"Val AUC={val_auc:.4f}"
                )

        self._is_fitted = True
        logger.info(f"Training complete. Best Val Loss: {best_val_loss:.4f}")

        return history

    def predict_sequence(self, sequence: np.ndarray) -> TemporalPrediction:
        """
        Predict fraud risk for a single sequence.

        Args:
            sequence: Feature sequence (seq_len, num_features)

        Returns:
            TemporalPrediction with risk scores
        """
        if not self._is_fitted or self._network is None:
            return TemporalPrediction(
                risk_score=0.0,
                is_fraud=False,
                confidence=0.0,
                behavior_drift_score=0.0,
                velocity_score=0.0,
            )

        self._network.eval()

        # Prepare input
        seq_len = len(sequence)
        if seq_len > self._max_seq_len:
            sequence = sequence[-self._max_seq_len :]
            seq_len = self._max_seq_len
        elif seq_len < self._max_seq_len:
            padding = np.zeros((self._max_seq_len - seq_len, sequence.shape[1]), dtype=np.float32)
            sequence = np.vstack([padding, sequence])

        mask = np.zeros(self._max_seq_len, dtype=np.float32)
        mask[-seq_len:] = 1.0

        seq_tensor = torch.FloatTensor(sequence).unsqueeze(0).to(self._device)
        mask_tensor = torch.FloatTensor(mask).unsqueeze(0).to(self._device)

        with torch.no_grad():
            fraud_prob, drift_score, velocity_score, attn_weights = self._network(
                seq_tensor, mask_tensor
            )

        risk = float(fraud_prob.cpu().numpy()[0, 0])
        drift = float(drift_score.cpu().numpy()[0, 0])
        velocity = float(velocity_score.cpu().numpy()[0, 0])

        return TemporalPrediction(
            risk_score=risk,
            is_fraud=risk >= self._threshold,
            confidence=abs(risk - 0.5) * 2,
            behavior_drift_score=drift,
            velocity_score=velocity,
            attention_weights=attn_weights.cpu().numpy()[0] if attn_weights is not None else None,
        )

    def predict_from_transactions(
        self,
        transactions: list[dict],
        iban: str | None = None,
    ) -> TemporalPrediction:
        """
        Predict from raw transaction list.

        Args:
            transactions: List of transaction dicts
            iban: Account IBAN

        Returns:
            TemporalPrediction
        """
        if not transactions:
            return TemporalPrediction(
                risk_score=0.0,
                is_fraud=False,
                confidence=0.0,
                behavior_drift_score=0.0,
                velocity_score=0.0,
            )

        sequence = self._feature_extractor.extract_sequence(transactions, iban)
        return self.predict_sequence(sequence)

    def save(self, path: str | None = None) -> None:
        """Save the model to disk."""
        if not self._is_fitted or self._network is None:
            return

        path = path or "models/temporal_fraud_model.pt"
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        checkpoint = {
            "model_state_dict": self._network.state_dict(),
            "model_type": self._model_type,
            "input_dim": self._input_dim,
            "hidden_dim": self._hidden_dim,
            "num_layers": self._num_layers,
            "dropout": self._dropout,
            "max_seq_len": self._max_seq_len,
            "threshold": self._threshold,
        }

        torch.save(checkpoint, path)
        logger.info(f"Temporal model saved to {path}")

    def load(self, path: str) -> None:
        """Load a model from disk."""
        if not HAS_TORCH:
            return

        try:
            checkpoint = torch.load(path, map_location=self._device, weights_only=False)

            self._model_type = checkpoint.get("model_type", "lstm")
            self._input_dim = checkpoint.get("input_dim", 12)
            self._hidden_dim = checkpoint.get("hidden_dim", 64)
            self._num_layers = checkpoint.get("num_layers", 2)
            self._dropout = checkpoint.get("dropout", 0.3)
            self._max_seq_len = checkpoint.get("max_seq_len", 50)
            self._threshold = checkpoint.get("threshold", 0.5)

            self._build_network()
            self._network.load_state_dict(checkpoint["model_state_dict"])
            self._network.eval()
            self._is_fitted = True

            logger.info(f"Temporal model loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load temporal model: {e}")

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return f"Temporal-{self._model_type.upper()}"

    def predict_single(self, features: np.ndarray) -> float:
        """
        Interface for ensemble compatibility.

        For temporal model, this expects a sequence. If given a single
        feature vector, returns 0.0.
        """
        if len(features.shape) == 1:
            return 0.0
        pred = self.predict_sequence(features)
        return pred.risk_score


# =============================================================================
# Utility Functions
# =============================================================================


def generate_synthetic_sequences(
    num_sequences: int = 1000,
    seq_len_range: tuple[int, int] = (10, 50),
    fraud_ratio: float = 0.1,
    num_features: int = 12,
) -> tuple[list[np.ndarray], list[int]]:
    """
    Generate synthetic transaction sequences for testing.

    Returns:
        sequences, labels
    """
    sequences = []
    labels = []

    for i in range(num_sequences):
        seq_len = np.random.randint(seq_len_range[0], seq_len_range[1] + 1)

        # Generate sequence
        seq = np.random.randn(seq_len, num_features).astype(np.float32)

        # Add temporal patterns
        for t in range(seq_len):
            seq[t, 2] = math.sin(2 * math.pi * (t % 24) / 24)  # Hour pattern
            seq[t, 3] = math.cos(2 * math.pi * (t % 24) / 24)

        # Determine if fraud
        is_fraud = np.random.random() < fraud_ratio

        if is_fraud:
            # Add fraud patterns
            fraud_start = np.random.randint(0, max(1, seq_len - 3))
            # Sudden spike in amount
            seq[fraud_start:, 0] *= 5
            # Unusual timing
            seq[fraud_start:, 2:4] = 0.5
            # High velocity
            seq[fraud_start:, 10] = 0.9

        sequences.append(seq)
        labels.append(1 if is_fraud else 0)

    return sequences, labels

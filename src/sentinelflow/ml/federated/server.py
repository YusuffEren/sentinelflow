# =============================================================================
# SentinelFlow - Federated Learning Server
# =============================================================================
"""
Federated Learning server for aggregating model updates.

The server:
- Maintains the global model
- Distributes parameters to clients
- Aggregates client updates using FedAvg
- Tracks training progress

This enables privacy-preserving collaborative learning where
multiple banks can train a shared fraud detection model without
sharing their transaction data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import numpy as np
from loguru import logger

try:
    import torch
    import torch.nn as nn

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

try:
    import flwr as fl
    from flwr.server import ServerConfig
    from flwr.server.strategy import FedAvg

    HAS_FLOWER = True
except ImportError:
    HAS_FLOWER = False


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class AggregationResult:
    """Result of a federated aggregation round."""

    round_number: int
    num_clients: int
    total_samples: int
    avg_loss: float
    avg_metrics: dict[str, float] = field(default_factory=dict)


@dataclass
class FederatedHistory:
    """Training history across federated rounds."""

    rounds: list[AggregationResult] = field(default_factory=list)

    def add_round(self, result: AggregationResult) -> None:
        self.rounds.append(result)

    @property
    def losses(self) -> list[float]:
        return [r.avg_loss for r in self.rounds]

    @property
    def accuracies(self) -> list[float]:
        return [r.avg_metrics.get("accuracy", 0.0) for r in self.rounds]


# =============================================================================
# Global Model (same architecture as client)
# =============================================================================

if HAS_TORCH:

    class GlobalFraudModel(nn.Module):
        """Global fraud detection model for federated aggregation."""

        def __init__(self, input_dim: int = 21, hidden_dim: int = 64):
            super().__init__()

            self.network = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim),
                nn.Dropout(0.3),
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim // 2),
                nn.Dropout(0.2),
                nn.Linear(hidden_dim // 2, 1),
                nn.Sigmoid(),
            )

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            return self.network(x)


# =============================================================================
# Federated Server
# =============================================================================


class FederatedServer:
    """
    Federated Learning aggregation server.

    Coordinates multiple financial institution clients in collaborative
    fraud model training. Implements FedAvg for model aggregation.

    Example:
        >>> server = FederatedServer()
        >>> server.register_client("bank_a", client_a)
        >>> server.register_client("bank_b", client_b)
        >>> history = server.train(rounds=10)
    """

    def __init__(
        self,
        input_dim: int = 21,
        hidden_dim: int = 64,
        aggregation_strategy: str = "fedavg",
    ):
        """
        Initialize federated server.

        Args:
            input_dim: Number of input features
            hidden_dim: Hidden layer size
            aggregation_strategy: Aggregation method ("fedavg", "fedprox")
        """
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.aggregation_strategy = aggregation_strategy

        self._global_model: nn.Module | None = None
        self._clients: dict[str, Any] = {}
        self._history = FederatedHistory()
        self._current_round = 0
        self._device = (
            torch.device("cuda" if torch.cuda.is_available() else "cpu") if HAS_TORCH else None
        )

        self._init_global_model()

        logger.info(f"FederatedServer initialized (strategy={aggregation_strategy})")

    def _init_global_model(self) -> None:
        """Initialize the global model."""
        if not HAS_TORCH:
            return

        self._global_model = GlobalFraudModel(self.input_dim, self.hidden_dim)
        self._global_model = self._global_model.to(self._device)

    def get_global_parameters(self) -> list[np.ndarray]:
        """Get current global model parameters."""
        if self._global_model is None:
            return []

        return [p.cpu().detach().numpy() for p in self._global_model.parameters()]

    def set_global_parameters(self, parameters: list[np.ndarray]) -> None:
        """Set global model parameters."""
        if self._global_model is None:
            return

        for param, new_value in zip(self._global_model.parameters(), parameters):
            param.data = torch.from_numpy(new_value).to(self._device)

    def register_client(self, client_id: str, client: Any) -> None:
        """
        Register a client for federated training.

        Args:
            client_id: Unique client identifier
            client: FederatedClient instance
        """
        self._clients[client_id] = client
        logger.info(f"Registered client: {client_id} ({client.institution_name})")

    def aggregate_fedavg(
        self,
        client_updates: list[tuple[list[np.ndarray], int]],
    ) -> list[np.ndarray]:
        """
        Aggregate client updates using Federated Averaging.

        FedAvg: weighted average of parameters based on number of samples.

        Args:
            client_updates: List of (parameters, num_samples) tuples

        Returns:
            Aggregated parameters
        """
        if not client_updates:
            return self.get_global_parameters()

        total_samples = sum(n for _, n in client_updates)

        # Weighted average
        aggregated = []
        for i in range(len(client_updates[0][0])):
            weighted_sum = sum(params[i] * (n / total_samples) for params, n in client_updates)
            aggregated.append(weighted_sum)

        return aggregated

    def train_round(self) -> AggregationResult:
        """
        Execute one round of federated training.

        1. Distribute global parameters to all clients
        2. Each client trains locally
        3. Aggregate client updates
        4. Update global model

        Returns:
            AggregationResult with round metrics
        """
        self._current_round += 1

        if not self._clients:
            logger.warning("No clients registered")
            return AggregationResult(self._current_round, 0, 0, 0.0)

        global_params = self.get_global_parameters()

        # Collect client updates
        client_updates = []
        total_loss = 0.0
        all_metrics: dict[str, list[float]] = {}
        total_samples = 0

        for client_id, client in self._clients.items():
            # Send global parameters
            client.set_parameters(global_params)

            # Client trains locally
            result = client.train()

            # Collect updates
            updated_params = client.get_parameters()
            client_updates.append((updated_params, result.num_samples))

            total_loss += result.loss * result.num_samples
            total_samples += result.num_samples

            for metric, value in result.metrics.items():
                if metric not in all_metrics:
                    all_metrics[metric] = []
                all_metrics[metric].append(value)

        # Aggregate updates
        aggregated_params = self.aggregate_fedavg(client_updates)
        self.set_global_parameters(aggregated_params)

        # Compute average metrics
        avg_loss = total_loss / max(total_samples, 1)
        avg_metrics = {k: np.mean(v) for k, v in all_metrics.items()}

        result = AggregationResult(
            round_number=self._current_round,
            num_clients=len(self._clients),
            total_samples=total_samples,
            avg_loss=avg_loss,
            avg_metrics=avg_metrics,
        )

        self._history.add_round(result)

        logger.info(
            f"Round {self._current_round}: "
            f"loss={avg_loss:.4f}, "
            f"acc={avg_metrics.get('accuracy', 0.0):.4f}, "
            f"clients={len(self._clients)}, "
            f"samples={total_samples}"
        )

        return result

    def train(self, rounds: int = 10) -> FederatedHistory:
        """
        Run multiple rounds of federated training.

        Args:
            rounds: Number of federated rounds

        Returns:
            FederatedHistory with all round results
        """
        logger.info(f"Starting federated training for {rounds} rounds...")

        for round_num in range(rounds):
            self.train_round()

        logger.info(
            f"Federated training complete. "
            f"Final loss: {self._history.losses[-1]:.4f}, "
            f"Final acc: {self._history.accuracies[-1]:.4f}"
        )

        return self._history

    def evaluate_global(
        self,
        X_test: np.ndarray,
        y_test: np.ndarray,
    ) -> dict[str, float]:
        """
        Evaluate global model on test data.

        Args:
            X_test: Test features
            y_test: Test labels

        Returns:
            Dictionary of metrics
        """
        if not HAS_TORCH or self._global_model is None:
            return {}

        self._global_model.eval()

        X_tensor = torch.FloatTensor(X_test.astype(np.float32)).to(self._device)

        with torch.no_grad():
            predictions = self._global_model(X_tensor).cpu().numpy().flatten()

        pred_labels = (predictions >= 0.5).astype(int)
        accuracy = (pred_labels == y_test).mean()

        tp = ((pred_labels == 1) & (y_test == 1)).sum()
        fp = ((pred_labels == 1) & (y_test == 0)).sum()
        fn = ((pred_labels == 0) & (y_test == 1)).sum()
        tn = ((pred_labels == 0) & (y_test == 0)).sum()

        precision = tp / (tp + fp + 1e-6)
        recall = tp / (tp + fn + 1e-6)

        # AUC
        try:
            from sklearn.metrics import roc_auc_score

            auc = roc_auc_score(y_test, predictions)
        except:
            auc = 0.5

        return {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
            "f1": 2 * precision * recall / (precision + recall + 1e-6),
            "auc": float(auc),
            "confusion_matrix": {
                "tp": int(tp),
                "fp": int(fp),
                "fn": int(fn),
                "tn": int(tn),
            },
        }

    def save_global_model(self, path: str = "models/federated_global.pt") -> None:
        """Save the global model to disk."""
        if not HAS_TORCH or self._global_model is None:
            return

        from pathlib import Path

        Path(path).parent.mkdir(parents=True, exist_ok=True)

        torch.save(
            {
                "model_state_dict": self._global_model.state_dict(),
                "input_dim": self.input_dim,
                "hidden_dim": self.hidden_dim,
                "num_rounds": self._current_round,
            },
            path,
        )

        logger.info(f"Global model saved to {path}")

    def load_global_model(self, path: str) -> None:
        """Load global model from disk."""
        if not HAS_TORCH:
            return

        import os

        if not os.path.exists(path):
            logger.warning(f"Model file not found: {path}")
            return

        checkpoint = torch.load(path, map_location=self._device, weights_only=False)

        self.input_dim = checkpoint.get("input_dim", 21)
        self.hidden_dim = checkpoint.get("hidden_dim", 64)
        self._current_round = checkpoint.get("num_rounds", 0)

        self._init_global_model()
        self._global_model.load_state_dict(checkpoint["model_state_dict"])

        logger.info(f"Global model loaded from {path}")

    @property
    def num_clients(self) -> int:
        return len(self._clients)

    @property
    def history(self) -> FederatedHistory:
        return self._history

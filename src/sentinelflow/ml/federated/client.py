# =============================================================================
# SentinelFlow - Federated Learning Client
# =============================================================================
"""
Federated Learning client representing a financial institution.

Each client:
- Holds private transaction data
- Trains locally on its data
- Sends only model updates (not data) to the server

Privacy guarantees:
- Raw transaction data never leaves the institution
- Only model gradients/weights are shared
- Differential privacy can be added for extra protection
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional
import numpy as np
from loguru import logger

try:
    import torch
    import torch.nn as nn
    from torch.optim import Adam
    from torch.utils.data import DataLoader, TensorDataset
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

try:
    import flwr as fl
    from flwr.client import Client, NumPyClient
    HAS_FLOWER = True
except ImportError:
    HAS_FLOWER = False
    logger.warning("Flower not available, federated learning disabled")


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class ClientConfig:
    """Configuration for a federated client."""
    
    client_id: str
    institution_name: str
    data_size: int = 0
    epochs_per_round: int = 3
    batch_size: int = 32
    learning_rate: float = 0.001


@dataclass
class TrainingResult:
    """Result of local training round."""
    
    client_id: str
    loss: float
    num_samples: int
    metrics: dict[str, float] = field(default_factory=dict)


# =============================================================================
# Local Model (same architecture as server)
# =============================================================================

if HAS_TORCH:
    
    class FraudDetectionNet(nn.Module):
        """Simple fraud detection network for federated learning."""
        
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
# Federated Client
# =============================================================================

class FederatedClient:
    """
    Federated Learning client for a financial institution.
    
    This client:
    1. Holds local transaction data
    2. Receives global model parameters from server
    3. Trains locally for a few epochs
    4. Sends updated parameters back to server
    
    Example:
        >>> client = FederatedClient("bank_a", "Banka A")
        >>> client.set_data(X_train, y_train)
        >>> client.set_parameters(global_params)
        >>> updated_params, metrics = client.train()
    """
    
    def __init__(
        self,
        client_id: str,
        institution_name: str,
        input_dim: int = 21,
        hidden_dim: int = 64,
        epochs_per_round: int = 3,
        batch_size: int = 32,
        learning_rate: float = 0.001,
    ):
        """
        Initialize federated client.
        
        Args:
            client_id: Unique client identifier
            institution_name: Human-readable institution name
            input_dim: Number of input features
            hidden_dim: Hidden layer size
            epochs_per_round: Local epochs per federated round
            batch_size: Training batch size
            learning_rate: Local learning rate
        """
        self.client_id = client_id
        self.institution_name = institution_name
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.epochs_per_round = epochs_per_round
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        
        self._model: nn.Module | None = None
        self._X_train: np.ndarray | None = None
        self._y_train: np.ndarray | None = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu") if HAS_TORCH else None
        
        self._init_model()
        
        logger.info(f"FederatedClient '{institution_name}' ({client_id}) initialized")
    
    def _init_model(self) -> None:
        """Initialize the local model."""
        if not HAS_TORCH:
            return
        
        self._model = FraudDetectionNet(self.input_dim, self.hidden_dim)
        self._model = self._model.to(self._device)
    
    def set_data(self, X: np.ndarray, y: np.ndarray) -> None:
        """
        Set the local training data.
        
        Args:
            X: Feature matrix (n_samples, n_features)
            y: Labels (n_samples,)
        """
        self._X_train = X.astype(np.float32)
        self._y_train = y.astype(np.float32)
        logger.info(
            f"Client '{self.institution_name}' received {len(X)} samples "
            f"({sum(y)} fraud, {len(y) - sum(y)} normal)"
        )
    
    def get_parameters(self) -> list[np.ndarray]:
        """Get model parameters as numpy arrays."""
        if self._model is None:
            return []
        
        return [p.cpu().detach().numpy() for p in self._model.parameters()]
    
    def set_parameters(self, parameters: list[np.ndarray]) -> None:
        """Set model parameters from numpy arrays."""
        if self._model is None:
            return
        
        for param, new_value in zip(self._model.parameters(), parameters):
            param.data = torch.from_numpy(new_value).to(self._device)
    
    def train(self) -> TrainingResult:
        """
        Train the model locally on client data.
        
        Returns:
            TrainingResult with loss and metrics
        """
        if not HAS_TORCH or self._model is None:
            return TrainingResult(self.client_id, 0.0, 0)
        
        if self._X_train is None or self._y_train is None:
            logger.warning(f"Client '{self.institution_name}' has no data")
            return TrainingResult(self.client_id, 0.0, 0)
        
        # Create data loader
        X_tensor = torch.FloatTensor(self._X_train).to(self._device)
        y_tensor = torch.FloatTensor(self._y_train).unsqueeze(1).to(self._device)
        
        dataset = TensorDataset(X_tensor, y_tensor)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)
        
        # Training setup
        optimizer = Adam(self._model.parameters(), lr=self.learning_rate)
        
        # Class weighting
        pos_count = sum(self._y_train)
        neg_count = len(self._y_train) - pos_count
        pos_weight = torch.tensor([neg_count / (pos_count + 1e-6)]).to(self._device)
        criterion = nn.BCELoss()
        
        # Train for specified epochs
        self._model.train()
        total_loss = 0.0
        num_batches = 0
        
        for epoch in range(self.epochs_per_round):
            epoch_loss = 0.0
            for batch_X, batch_y in loader:
                optimizer.zero_grad()
                predictions = self._model(batch_X)
                loss = criterion(predictions, batch_y)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
                num_batches += 1
            
            total_loss += epoch_loss
        
        avg_loss = total_loss / max(num_batches, 1)
        
        # Calculate metrics
        self._model.eval()
        with torch.no_grad():
            predictions = self._model(X_tensor).cpu().numpy().flatten()
            
            # Accuracy
            pred_labels = (predictions >= 0.5).astype(int)
            accuracy = (pred_labels == self._y_train).mean()
            
            # Precision, Recall
            tp = ((pred_labels == 1) & (self._y_train == 1)).sum()
            fp = ((pred_labels == 1) & (self._y_train == 0)).sum()
            fn = ((pred_labels == 0) & (self._y_train == 1)).sum()
            
            precision = tp / (tp + fp + 1e-6)
            recall = tp / (tp + fn + 1e-6)
        
        result = TrainingResult(
            client_id=self.client_id,
            loss=avg_loss,
            num_samples=len(self._X_train),
            metrics={
                "accuracy": float(accuracy),
                "precision": float(precision),
                "recall": float(recall),
                "f1": 2 * precision * recall / (precision + recall + 1e-6),
            },
        )
        
        logger.debug(
            f"Client '{self.institution_name}' trained: "
            f"loss={avg_loss:.4f}, acc={accuracy:.4f}"
        )
        
        return result
    
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> dict[str, float]:
        """
        Evaluate model on test data.
        
        Args:
            X_test: Test features
            y_test: Test labels
        
        Returns:
            Dictionary of metrics
        """
        if not HAS_TORCH or self._model is None:
            return {}
        
        self._model.eval()
        
        X_tensor = torch.FloatTensor(X_test.astype(np.float32)).to(self._device)
        
        with torch.no_grad():
            predictions = self._model(X_tensor).cpu().numpy().flatten()
        
        pred_labels = (predictions >= 0.5).astype(int)
        accuracy = (pred_labels == y_test).mean()
        
        tp = ((pred_labels == 1) & (y_test == 1)).sum()
        fp = ((pred_labels == 1) & (y_test == 0)).sum()
        fn = ((pred_labels == 0) & (y_test == 1)).sum()
        
        precision = tp / (tp + fp + 1e-6)
        recall = tp / (tp + fn + 1e-6)
        
        return {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
            "f1": 2 * precision * recall / (precision + recall + 1e-6),
        }
    
    @property
    def num_samples(self) -> int:
        """Number of training samples."""
        return len(self._X_train) if self._X_train is not None else 0


# =============================================================================
# Flower NumPy Client (for integration with Flower framework)
# =============================================================================

if HAS_FLOWER and HAS_TORCH:
    
    class FlowerFraudClient(NumPyClient):
        """
        Flower-compatible client for federated fraud detection.
        
        This wraps FederatedClient for use with the Flower framework.
        """
        
        def __init__(self, federated_client: FederatedClient):
            self.client = federated_client
        
        def get_parameters(self, config: dict) -> list[np.ndarray]:
            return self.client.get_parameters()
        
        def fit(
            self, parameters: list[np.ndarray], config: dict
        ) -> tuple[list[np.ndarray], int, dict]:
            # Set global parameters
            self.client.set_parameters(parameters)
            
            # Train locally
            result = self.client.train()
            
            # Return updated parameters
            return (
                self.client.get_parameters(),
                result.num_samples,
                result.metrics,
            )
        
        def evaluate(
            self, parameters: list[np.ndarray], config: dict
        ) -> tuple[float, int, dict]:
            self.client.set_parameters(parameters)
            
            if self.client._X_train is not None and self.client._y_train is not None:
                metrics = self.client.evaluate(
                    self.client._X_train, 
                    self.client._y_train
                )
                loss = 1.0 - metrics.get("accuracy", 0.0)
                return loss, self.client.num_samples, metrics
            
            return 0.0, 0, {}

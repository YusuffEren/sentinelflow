# =============================================================================
# SentinelFlow - Graph Neural Network Model for Fraud Detection
# =============================================================================
"""
Graph Neural Network (GNN) based fraud detection using PyTorch Geometric.

This module provides GNN-based anomaly detection that leverages the
graph structure of financial transactions to detect:
- Money laundering rings
- Mule account networks
- Suspicious transaction patterns

Architecture:
    - GraphSAGE/GAT layers for node embedding
    - Node-level fraud classification
    - Edge-level anomaly detection
    - Integration with Neo4j graph database

Usage:
    from sentinelflow.ml.gnn_model import GNNFraudModel

    model = GNNFraudModel()
    model.load_from_neo4j(graph_engine)
    model.train()
    predictions = model.predict_node_risk(iban="TR123...")
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import pickle

import numpy as np
from loguru import logger

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.optim import Adam
    from torch.optim.lr_scheduler import ReduceLROnPlateau

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    logger.warning("PyTorch not available, GNN model disabled")

try:
    from torch_geometric.data import Data
    from torch_geometric.nn import SAGEConv, GATConv, BatchNorm
    from torch_geometric.utils import to_undirected, add_self_loops

    HAS_TORCH_GEOMETRIC = True
except ImportError:
    HAS_TORCH_GEOMETRIC = False
    logger.warning("PyTorch Geometric not available, GNN model disabled")


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class GraphData:
    """Container for graph data extracted from Neo4j."""

    node_features: np.ndarray  # Shape: (num_nodes, num_features)
    edge_index: np.ndarray  # Shape: (2, num_edges)
    edge_attr: np.ndarray  # Shape: (num_edges, edge_features)
    node_labels: np.ndarray  # Shape: (num_nodes,) - 0: normal, 1: fraud
    iban_to_idx: dict[str, int] = field(default_factory=dict)
    idx_to_iban: dict[int, str] = field(default_factory=dict)

    @property
    def num_nodes(self) -> int:
        return len(self.node_features)

    @property
    def num_edges(self) -> int:
        return self.edge_index.shape[1]

    @property
    def num_node_features(self) -> int:
        return self.node_features.shape[1] if len(self.node_features) > 0 else 0


@dataclass
class GNNPrediction:
    """Result of GNN prediction for a node."""

    iban: str
    risk_score: float  # 0-1, higher = more fraud risk
    embedding: np.ndarray  # Node embedding vector
    is_fraud: bool
    confidence: float
    neighbors_at_risk: int  # Number of suspicious neighbors

    def to_dict(self) -> dict[str, Any]:
        return {
            "iban": self.iban,
            "risk_score": round(self.risk_score, 4),
            "is_fraud": self.is_fraud,
            "confidence": round(self.confidence, 4),
            "neighbors_at_risk": self.neighbors_at_risk,
        }


# =============================================================================
# GNN Network Architecture
# =============================================================================

if HAS_TORCH and HAS_TORCH_GEOMETRIC:

    class GraphSAGENetwork(nn.Module):
        """
        GraphSAGE-based network for node classification.

        Uses neighbor sampling and aggregation to learn node embeddings
        that capture the structural context of each account in the
        transaction graph.
        """

        def __init__(
            self,
            in_channels: int,
            hidden_channels: int = 64,
            out_channels: int = 32,
            num_layers: int = 3,
            dropout: float = 0.3,
        ):
            super().__init__()

            self.num_layers = num_layers
            self.dropout = dropout

            # Input projection
            self.input_proj = nn.Linear(in_channels, hidden_channels)

            # GraphSAGE convolution layers
            self.convs = nn.ModuleList()
            self.bns = nn.ModuleList()

            for i in range(num_layers):
                in_ch = hidden_channels
                out_ch = hidden_channels if i < num_layers - 1 else out_channels
                self.convs.append(SAGEConv(in_ch, out_ch, aggr="mean"))
                self.bns.append(BatchNorm(out_ch))

            # Classification head
            self.classifier = nn.Sequential(
                nn.Linear(out_channels, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

        def forward(
            self, x: torch.Tensor, edge_index: torch.Tensor
        ) -> tuple[torch.Tensor, torch.Tensor]:
            """
            Forward pass returning both embeddings and predictions.

            Args:
                x: Node features (num_nodes, in_channels)
                edge_index: Graph connectivity (2, num_edges)

            Returns:
                embeddings: Node embeddings (num_nodes, out_channels)
                predictions: Fraud probabilities (num_nodes, 1)
            """
            # Project input features
            x = self.input_proj(x)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)

            # Apply GNN layers
            for i, (conv, bn) in enumerate(zip(self.convs, self.bns)):
                x = conv(x, edge_index)
                x = bn(x)
                if i < self.num_layers - 1:
                    x = F.relu(x)
                    x = F.dropout(x, p=self.dropout, training=self.training)

            embeddings = x
            predictions = self.classifier(x)

            return embeddings, predictions

        def get_embeddings(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
            """Get only the node embeddings without classification."""
            with torch.no_grad():
                embeddings, _ = self.forward(x, edge_index)
            return embeddings

    class GATNetwork(nn.Module):
        """
        Graph Attention Network for fraud detection.

        Uses attention mechanisms to weight neighbor contributions,
        allowing the model to focus on the most relevant connections.
        """

        def __init__(
            self,
            in_channels: int,
            hidden_channels: int = 64,
            out_channels: int = 32,
            num_heads: int = 4,
            num_layers: int = 2,
            dropout: float = 0.3,
        ):
            super().__init__()

            self.num_layers = num_layers
            self.dropout = dropout

            # Input projection
            self.input_proj = nn.Linear(in_channels, hidden_channels)

            # GAT layers
            self.convs = nn.ModuleList()
            self.bns = nn.ModuleList()

            for i in range(num_layers):
                if i == 0:
                    in_ch = hidden_channels
                else:
                    in_ch = hidden_channels * num_heads

                out_ch = hidden_channels if i < num_layers - 1 else out_channels
                heads = num_heads if i < num_layers - 1 else 1
                concat = i < num_layers - 1

                self.convs.append(
                    GATConv(in_ch, out_ch, heads=heads, concat=concat, dropout=dropout)
                )
                self.bns.append(BatchNorm(out_ch * heads if concat else out_ch))

            # Classification head
            self.classifier = nn.Sequential(
                nn.Linear(out_channels, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
                nn.Sigmoid(),
            )

        def forward(
            self, x: torch.Tensor, edge_index: torch.Tensor
        ) -> tuple[torch.Tensor, torch.Tensor]:
            """Forward pass."""
            x = self.input_proj(x)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)

            for i, (conv, bn) in enumerate(zip(self.convs, self.bns)):
                x = conv(x, edge_index)
                x = bn(x)
                if i < self.num_layers - 1:
                    x = F.elu(x)
                    x = F.dropout(x, p=self.dropout, training=self.training)

            embeddings = x
            predictions = self.classifier(x)

            return embeddings, predictions


# =============================================================================
# GNN Fraud Model
# =============================================================================


class GNNFraudModel:
    """
    Graph Neural Network based fraud detection model.

    This model learns node embeddings from the transaction graph and
    uses them to predict fraud risk for each account. It can be used
    standalone or as part of the ensemble.

    Features:
        - Learns from graph structure (transaction patterns)
        - Node-level fraud classification
        - Exports embeddings for downstream tasks
        - Integration with Neo4j

    Example:
        >>> model = GNNFraudModel()
        >>> model.build_graph_from_neo4j(graph_engine)
        >>> model.train(epochs=100)
        >>> risk = model.predict_node_risk("TR123...")
    """

    def __init__(
        self,
        model_type: str = "sage",  # "sage" or "gat"
        hidden_channels: int = 64,
        out_channels: int = 32,
        num_layers: int = 3,
        dropout: float = 0.3,
        learning_rate: float = 0.001,
        weight_decay: float = 1e-5,
        threshold: float = 0.5,
        model_path: str | None = None,
    ):
        """
        Initialize GNN model.

        Args:
            model_type: "sage" for GraphSAGE, "gat" for GAT
            hidden_channels: Hidden layer size
            out_channels: Embedding dimension
            num_layers: Number of GNN layers
            dropout: Dropout rate
            learning_rate: Learning rate for training
            weight_decay: L2 regularization
            threshold: Classification threshold
            model_path: Path to load pre-trained model
        """
        self._model_type = model_type
        self._hidden_channels = hidden_channels
        self._out_channels = out_channels
        self._num_layers = num_layers
        self._dropout = dropout
        self._learning_rate = learning_rate
        self._weight_decay = weight_decay
        self._threshold = threshold

        self._network: nn.Module | None = None
        self._graph_data: GraphData | None = None
        self._torch_data: Any = None  # PyG Data object
        self._is_fitted = False
        self._device = (
            torch.device("cuda" if torch.cuda.is_available() else "cpu") if HAS_TORCH else None
        )

        if model_path and os.path.exists(model_path):
            self.load(model_path)

        logger.info(
            f"GNNFraudModel initialized (type={model_type}, "
            f"hidden={hidden_channels}, layers={num_layers}, "
            f"device={self._device})"
        )

    def _build_network(self, in_channels: int) -> None:
        """Build the GNN network architecture."""
        if not HAS_TORCH or not HAS_TORCH_GEOMETRIC:
            logger.error("PyTorch Geometric required for GNN model")
            return

        if self._model_type == "sage":
            self._network = GraphSAGENetwork(
                in_channels=in_channels,
                hidden_channels=self._hidden_channels,
                out_channels=self._out_channels,
                num_layers=self._num_layers,
                dropout=self._dropout,
            )
        elif self._model_type == "gat":
            self._network = GATNetwork(
                in_channels=in_channels,
                hidden_channels=self._hidden_channels,
                out_channels=self._out_channels,
                num_layers=self._num_layers,
                dropout=self._dropout,
            )
        else:
            raise ValueError(f"Unknown model type: {self._model_type}")

        self._network = self._network.to(self._device)
        logger.info(f"Built {self._model_type.upper()} network with {in_channels} input features")

    def build_graph_from_neo4j(self, graph_engine: Any) -> None:
        """
        Build PyG graph data from Neo4j database.

        Args:
            graph_engine: GraphEngine instance connected to Neo4j
        """
        if not HAS_TORCH or not HAS_TORCH_GEOMETRIC:
            return

        logger.info("Building graph from Neo4j...")

        # Query all nodes and relationships
        with graph_engine._session() as session:
            # Get all users with features
            node_query = """
            MATCH (u:User)
            OPTIONAL MATCH (u)-[s:SENT]->()
            WITH u, 
                 COUNT(s) as out_degree,
                 COALESCE(SUM(s.amount), 0) as total_sent,
                 COALESCE(AVG(s.amount), 0) as avg_sent
            OPTIONAL MATCH ()-[r:SENT]->(u)
            WITH u, out_degree, total_sent, avg_sent,
                 COUNT(r) as in_degree,
                 COALESCE(SUM(r.amount), 0) as total_received,
                 COALESCE(AVG(r.amount), 0) as avg_received
            RETURN 
                u.iban as iban,
                u.name as name,
                u.city as city,
                out_degree,
                in_degree,
                total_sent,
                total_received,
                avg_sent,
                avg_received
            """

            node_result = session.run(node_query)
            nodes = list(node_result)

            # Get all edges
            edge_query = """
            MATCH (s:User)-[r:SENT]->(t:User)
            RETURN 
                s.iban as source,
                t.iban as target,
                r.amount as amount,
                r.fraud_type as fraud_type
            """

            edge_result = session.run(edge_query)
            edges = list(edge_result)

        if not nodes:
            logger.warning("No nodes found in Neo4j graph")
            return

        # Build mappings
        iban_to_idx = {node["iban"]: i for i, node in enumerate(nodes)}
        idx_to_iban = {i: node["iban"] for i, node in enumerate(nodes)}

        # Build node features
        # Features: [out_degree, in_degree, total_sent, total_received, avg_sent, avg_received, degree_ratio]
        node_features = []
        node_labels = []

        for node in nodes:
            out_deg = float(node["out_degree"])
            in_deg = float(node["in_degree"])
            total_sent = float(node["total_sent"])
            total_received = float(node["total_received"])
            avg_sent = float(node["avg_sent"])
            avg_received = float(node["avg_received"])

            # Derived features
            total_degree = out_deg + in_deg
            degree_ratio = out_deg / (in_deg + 1e-6)
            flow_ratio = total_sent / (total_received + 1e-6)

            features = [
                out_deg,
                in_deg,
                np.log1p(total_sent),
                np.log1p(total_received),
                np.log1p(avg_sent),
                np.log1p(avg_received),
                total_degree,
                degree_ratio,
                flow_ratio,
            ]
            node_features.append(features)

            # Label: 1 if involved in any fraud, 0 otherwise
            # This would need actual fraud labels from the database
            node_labels.append(0)  # Default to 0, will be updated during training

        # Build edge index and attributes
        edge_sources = []
        edge_targets = []
        edge_attrs = []

        for edge in edges:
            src_iban = edge["source"]
            tgt_iban = edge["target"]

            if src_iban in iban_to_idx and tgt_iban in iban_to_idx:
                edge_sources.append(iban_to_idx[src_iban])
                edge_targets.append(iban_to_idx[tgt_iban])

                amount = float(edge["amount"]) if edge["amount"] else 0.0
                is_fraud_edge = 1.0 if edge["fraud_type"] and edge["fraud_type"] != "none" else 0.0

                edge_attrs.append([np.log1p(amount), is_fraud_edge])

                # Update node labels if fraud edge
                if is_fraud_edge > 0:
                    node_labels[iban_to_idx[src_iban]] = 1
                    node_labels[iban_to_idx[tgt_iban]] = 1

        # Convert to numpy arrays
        node_features_np = np.array(node_features, dtype=np.float32)
        edge_index_np = np.array([edge_sources, edge_targets], dtype=np.int64)
        edge_attr_np = (
            np.array(edge_attrs, dtype=np.float32)
            if edge_attrs
            else np.zeros((0, 2), dtype=np.float32)
        )
        node_labels_np = np.array(node_labels, dtype=np.int64)

        self._graph_data = GraphData(
            node_features=node_features_np,
            edge_index=edge_index_np,
            edge_attr=edge_attr_np,
            node_labels=node_labels_np,
            iban_to_idx=iban_to_idx,
            idx_to_iban=idx_to_iban,
        )

        # Create PyG Data object
        self._torch_data = Data(
            x=torch.FloatTensor(node_features_np).to(self._device),
            edge_index=torch.LongTensor(edge_index_np).to(self._device),
            edge_attr=torch.FloatTensor(edge_attr_np).to(self._device),
            y=torch.LongTensor(node_labels_np).to(self._device),
        )

        # Build network
        self._build_network(in_channels=node_features_np.shape[1])

        logger.info(
            f"Graph built: {self._graph_data.num_nodes} nodes, "
            f"{self._graph_data.num_edges} edges, "
            f"{sum(node_labels)} fraud nodes"
        )

    def build_graph_from_data(
        self,
        node_features: np.ndarray,
        edge_index: np.ndarray,
        node_labels: np.ndarray,
        iban_list: list[str],
    ) -> None:
        """
        Build graph from pre-processed numpy arrays.

        Args:
            node_features: Node feature matrix (N, F)
            edge_index: Edge connectivity (2, E)
            node_labels: Node labels (N,)
            iban_list: List of IBANs in order
        """
        if not HAS_TORCH or not HAS_TORCH_GEOMETRIC:
            return

        iban_to_idx = {iban: i for i, iban in enumerate(iban_list)}
        idx_to_iban = {i: iban for i, iban in enumerate(iban_list)}

        self._graph_data = GraphData(
            node_features=node_features.astype(np.float32),
            edge_index=edge_index.astype(np.int64),
            edge_attr=np.zeros((edge_index.shape[1], 1), dtype=np.float32),
            node_labels=node_labels.astype(np.int64),
            iban_to_idx=iban_to_idx,
            idx_to_iban=idx_to_iban,
        )

        self._torch_data = Data(
            x=torch.FloatTensor(node_features).to(self._device),
            edge_index=torch.LongTensor(edge_index).to(self._device),
            y=torch.LongTensor(node_labels).to(self._device),
        )

        self._build_network(in_channels=node_features.shape[1])

        logger.info(f"Graph built from data: {len(iban_list)} nodes")

    def train(
        self,
        epochs: int = 100,
        patience: int = 10,
        val_ratio: float = 0.2,
    ) -> dict[str, list[float]]:
        """
        Train the GNN model.

        Args:
            epochs: Number of training epochs
            patience: Early stopping patience
            val_ratio: Validation set ratio

        Returns:
            Dictionary with training history
        """
        if not HAS_TORCH or not HAS_TORCH_GEOMETRIC:
            return {"error": ["PyTorch Geometric not available"]}

        if self._network is None or self._torch_data is None:
            logger.error("Build graph before training")
            return {"error": ["Graph not built"]}

        logger.info(f"Starting GNN training for {epochs} epochs...")

        # Create train/val masks
        num_nodes = self._torch_data.x.shape[0]
        perm = torch.randperm(num_nodes)
        val_size = int(num_nodes * val_ratio)

        val_mask = torch.zeros(num_nodes, dtype=torch.bool)
        val_mask[perm[:val_size]] = True
        train_mask = ~val_mask

        # Setup optimizer
        optimizer = Adam(
            self._network.parameters(),
            lr=self._learning_rate,
            weight_decay=self._weight_decay,
        )
        scheduler = ReduceLROnPlateau(optimizer, mode="min", patience=5, factor=0.5)

        # Class weights for imbalanced data
        pos_count = self._torch_data.y.sum().item()
        neg_count = num_nodes - pos_count
        pos_weight = torch.tensor([neg_count / (pos_count + 1e-6)]).to(self._device)
        criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

        # Training history
        history = {"train_loss": [], "val_loss": [], "val_auc": []}
        best_val_loss = float("inf")
        patience_counter = 0

        for epoch in range(epochs):
            # Training
            self._network.train()
            optimizer.zero_grad()

            embeddings, predictions = self._network(self._torch_data.x, self._torch_data.edge_index)

            # Compute loss only on training nodes
            train_pred = predictions[train_mask].squeeze()
            train_labels = self._torch_data.y[train_mask].float()
            train_loss = criterion(train_pred, train_labels)

            train_loss.backward()
            optimizer.step()

            # Validation
            self._network.eval()
            with torch.no_grad():
                _, val_predictions = self._network(self._torch_data.x, self._torch_data.edge_index)
                val_pred = val_predictions[val_mask].squeeze()
                val_labels = self._torch_data.y[val_mask].float()
                val_loss = criterion(val_pred, val_labels)

                # AUC calculation
                try:
                    from sklearn.metrics import roc_auc_score

                    val_pred_np = torch.sigmoid(val_pred).cpu().numpy()
                    val_labels_np = val_labels.cpu().numpy()
                    if len(np.unique(val_labels_np)) > 1:
                        val_auc = roc_auc_score(val_labels_np, val_pred_np)
                    else:
                        val_auc = 0.5
                except:
                    val_auc = 0.5

            history["train_loss"].append(train_loss.item())
            history["val_loss"].append(val_loss.item())
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
                    f"Train Loss={train_loss.item():.4f}, "
                    f"Val Loss={val_loss.item():.4f}, "
                    f"Val AUC={val_auc:.4f}"
                )

        self._is_fitted = True
        logger.info(f"Training complete. Best Val Loss: {best_val_loss:.4f}")

        return history

    def predict_all(self) -> dict[str, GNNPrediction]:
        """
        Predict fraud risk for all nodes in the graph.

        Returns:
            Dictionary mapping IBAN to prediction
        """
        if not self._is_fitted or self._network is None or self._torch_data is None:
            return {}

        self._network.eval()
        predictions: dict[str, GNNPrediction] = {}

        with torch.no_grad():
            embeddings, raw_predictions = self._network(
                self._torch_data.x, self._torch_data.edge_index
            )

            probs = raw_predictions.squeeze().cpu().numpy()
            embeddings_np = embeddings.cpu().numpy()

            for idx, iban in self._graph_data.idx_to_iban.items():
                risk_score = float(probs[idx]) if len(probs.shape) > 0 else float(probs)

                predictions[iban] = GNNPrediction(
                    iban=iban,
                    risk_score=risk_score,
                    embedding=embeddings_np[idx],
                    is_fraud=risk_score >= self._threshold,
                    confidence=abs(risk_score - 0.5) * 2,
                    neighbors_at_risk=self._count_risky_neighbors(idx, probs),
                )

        return predictions

    def predict_node_risk(self, iban: str) -> GNNPrediction | None:
        """
        Predict fraud risk for a specific node.

        Args:
            iban: IBAN to predict

        Returns:
            GNNPrediction or None if IBAN not in graph
        """
        all_predictions = self.predict_all()
        return all_predictions.get(iban)

    def get_embeddings(self) -> dict[str, np.ndarray]:
        """
        Get node embeddings for all nodes.

        Returns:
            Dictionary mapping IBAN to embedding vector
        """
        if not self._is_fitted or self._network is None:
            return {}

        self._network.eval()
        embeddings_dict: dict[str, np.ndarray] = {}

        with torch.no_grad():
            embeddings = self._network.get_embeddings(
                self._torch_data.x, self._torch_data.edge_index
            )
            embeddings_np = embeddings.cpu().numpy()

            for idx, iban in self._graph_data.idx_to_iban.items():
                embeddings_dict[iban] = embeddings_np[idx]

        return embeddings_dict

    def _count_risky_neighbors(self, node_idx: int, all_probs: np.ndarray) -> int:
        """Count how many neighbors have high risk scores."""
        if self._torch_data is None:
            return 0

        edge_index = self._torch_data.edge_index.cpu().numpy()

        # Find all neighbors
        neighbors = set()
        for i in range(edge_index.shape[1]):
            if edge_index[0, i] == node_idx:
                neighbors.add(edge_index[1, i])
            if edge_index[1, i] == node_idx:
                neighbors.add(edge_index[0, i])

        # Count risky neighbors
        risky_count = sum(1 for n in neighbors if all_probs[n] >= self._threshold)
        return risky_count

    def save(self, path: str | None = None) -> None:
        """Save the trained model to disk."""
        if not self._is_fitted or self._network is None:
            logger.warning("Model not trained, nothing to save")
            return

        path = path or "models/gnn_fraud_model.pt"
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        checkpoint = {
            "model_state_dict": self._network.state_dict(),
            "model_type": self._model_type,
            "hidden_channels": self._hidden_channels,
            "out_channels": self._out_channels,
            "num_layers": self._num_layers,
            "dropout": self._dropout,
            "threshold": self._threshold,
            "graph_data": (
                {
                    "iban_to_idx": self._graph_data.iban_to_idx,
                    "idx_to_iban": self._graph_data.idx_to_iban,
                    "num_node_features": self._graph_data.num_node_features,
                }
                if self._graph_data
                else None
            ),
        }

        torch.save(checkpoint, path)
        logger.info(f"GNN model saved to {path}")

    def load(self, path: str) -> None:
        """Load a trained model from disk."""
        if not HAS_TORCH:
            return

        try:
            checkpoint = torch.load(path, map_location=self._device, weights_only=False)

            self._model_type = checkpoint.get("model_type", "sage")
            self._hidden_channels = checkpoint.get("hidden_channels", 64)
            self._out_channels = checkpoint.get("out_channels", 32)
            self._num_layers = checkpoint.get("num_layers", 3)
            self._dropout = checkpoint.get("dropout", 0.3)
            self._threshold = checkpoint.get("threshold", 0.5)

            graph_info = checkpoint.get("graph_data", {})
            num_features = graph_info.get("num_node_features", 9)

            self._build_network(in_channels=num_features)
            self._network.load_state_dict(checkpoint["model_state_dict"])
            self._network.eval()
            self._is_fitted = True

            logger.info(f"GNN model loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load GNN model: {e}")

    @property
    def is_ready(self) -> bool:
        """Whether the model is trained and ready for predictions."""
        return self._is_fitted

    @property
    def name(self) -> str:
        """Model name for ensemble."""
        return f"GNN-{self._model_type.upper()}"

    def predict_single(self, features: np.ndarray) -> float:
        """
        Interface for ensemble compatibility.

        Note: For GNN, this requires the node to be in the graph.
        This method is a fallback that returns 0.0.
        """
        # GNN requires graph structure, this is just for interface compatibility
        return 0.0


# =============================================================================
# Utility Functions
# =============================================================================


def create_synthetic_graph(
    num_nodes: int = 1000,
    num_edges: int = 5000,
    fraud_ratio: float = 0.05,
    num_features: int = 9,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str]]:
    """
    Create a synthetic transaction graph for testing.

    Returns:
        node_features, edge_index, node_labels, iban_list
    """
    # Generate random IBANs
    iban_list = [f"TR{i:024d}" for i in range(num_nodes)]

    # Generate random features
    node_features = np.random.randn(num_nodes, num_features).astype(np.float32)

    # Generate random edges
    sources = np.random.randint(0, num_nodes, num_edges)
    targets = np.random.randint(0, num_nodes, num_edges)
    edge_index = np.stack([sources, targets])

    # Generate labels
    num_fraud = int(num_nodes * fraud_ratio)
    node_labels = np.zeros(num_nodes, dtype=np.int64)
    fraud_indices = np.random.choice(num_nodes, num_fraud, replace=False)
    node_labels[fraud_indices] = 1

    return node_features, edge_index, node_labels, iban_list

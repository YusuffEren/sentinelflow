# =============================================================================
# SentinelFlow - Federated Learning Simulator
# =============================================================================
"""
Simulates federated learning across multiple financial institutions.

This module provides a complete simulation environment for demonstrating
how multiple banks can collaboratively train a fraud detection model
without sharing their transaction data.

Features:
- Simulates N banks with different data distributions
- Non-IID data partitioning (realistic scenario)
- Privacy metrics tracking
- Comparison with centralized training

Usage:
    from sentinelflow.ml.federated import FederatedSimulator

    sim = FederatedSimulator(num_clients=5, institution_names=["Banka A", ...])
    results = sim.run_simulation(rounds=20)
    sim.plot_results()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import numpy as np
from loguru import logger

from sentinelflow.ml.federated.server import FederatedServer, FederatedHistory
from sentinelflow.ml.federated.client import FederatedClient


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class SimulationConfig:
    """Configuration for federated simulation."""

    num_clients: int = 5
    samples_per_client: int = 2000
    fraud_ratio: float = 0.05
    non_iid_alpha: float = 0.5  # Dirichlet concentration (lower = more non-IID)
    test_ratio: float = 0.2
    num_features: int = 21
    hidden_dim: int = 64
    epochs_per_round: int = 3
    batch_size: int = 32
    learning_rate: float = 0.001


@dataclass
class SimulationResult:
    """Results from federated simulation."""

    federated_history: FederatedHistory
    final_metrics: dict[str, float]
    centralized_metrics: dict[str, float] | None = None
    client_metrics: dict[str, dict[str, float]] = field(default_factory=dict)
    privacy_analysis: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> str:
        """Generate a summary string."""
        lines = [
            "=" * 60,
            "FEDERATED LEARNING SIMULATION RESULTS",
            "=" * 60,
            f"\nFederated Learning:",
            f"  Final Accuracy: {self.final_metrics.get('accuracy', 0):.4f}",
            f"  Final AUC: {self.final_metrics.get('auc', 0):.4f}",
            f"  Final F1: {self.final_metrics.get('f1', 0):.4f}",
        ]

        if self.centralized_metrics:
            lines.extend(
                [
                    f"\nCentralized Training (baseline):",
                    f"  Accuracy: {self.centralized_metrics.get('accuracy', 0):.4f}",
                    f"  AUC: {self.centralized_metrics.get('auc', 0):.4f}",
                    f"  F1: {self.centralized_metrics.get('f1', 0):.4f}",
                ]
            )

            # Performance comparison
            fed_acc = self.final_metrics.get("accuracy", 0)
            cent_acc = self.centralized_metrics.get("accuracy", 0)
            diff = (fed_acc - cent_acc) / cent_acc * 100 if cent_acc > 0 else 0

            lines.append(f"\nFederated vs Centralized: {diff:+.2f}% accuracy difference")

        lines.extend(
            [
                f"\nPrivacy Guarantees:",
                f"  Raw data shared: {self.privacy_analysis.get('data_shared', 'None')}",
                f"  Parameters exchanged: {self.privacy_analysis.get('params_exchanged', 0):,}",
                "=" * 60,
            ]
        )

        return "\n".join(lines)


# =============================================================================
# Turkish Bank Names for Demo
# =============================================================================

TURKISH_BANKS = [
    "Ziraat Bankası",
    "İş Bankası",
    "Garanti BBVA",
    "Akbank",
    "Yapı Kredi",
    "Halkbank",
    "VakıfBank",
    "QNB Finansbank",
    "Denizbank",
    "TEB",
]


# =============================================================================
# Federated Simulator
# =============================================================================


class FederatedSimulator:
    """
    Complete federated learning simulation environment.

    Simulates multiple Turkish banks collaboratively training a
    fraud detection model. Each bank has its own local data with
    potentially different distributions (non-IID setting).

    Example:
        >>> sim = FederatedSimulator(num_clients=5)
        >>> results = sim.run_simulation(rounds=20)
        >>> print(results.summary())
    """

    def __init__(
        self,
        num_clients: int = 5,
        institution_names: list[str] | None = None,
        config: SimulationConfig | None = None,
    ):
        """
        Initialize the simulator.

        Args:
            num_clients: Number of participating institutions
            institution_names: Custom institution names (defaults to Turkish banks)
            config: Simulation configuration
        """
        self.config = config or SimulationConfig(num_clients=num_clients)
        self.num_clients = self.config.num_clients

        if institution_names:
            self.institution_names = institution_names[:num_clients]
        else:
            self.institution_names = TURKISH_BANKS[:num_clients]

        self._server: FederatedServer | None = None
        self._clients: list[FederatedClient] = []
        self._X_test: np.ndarray | None = None
        self._y_test: np.ndarray | None = None
        self._all_X: np.ndarray | None = None
        self._all_y: np.ndarray | None = None

        logger.info(
            f"FederatedSimulator initialized with {num_clients} clients: "
            f"{', '.join(self.institution_names)}"
        )

    def _generate_synthetic_data(self) -> tuple[np.ndarray, np.ndarray]:
        """Generate synthetic transaction features and labels."""
        n_total = self.config.samples_per_client * self.num_clients
        n_fraud = int(n_total * self.config.fraud_ratio)
        n_normal = n_total - n_fraud

        # Generate normal transactions
        X_normal = np.random.randn(n_normal, self.config.num_features).astype(np.float32)
        # Normalize amounts to realistic range
        X_normal[:, 0] = np.abs(X_normal[:, 0]) * 5000 / 1_000_000  # amount_normalized
        X_normal[:, 1] = np.log1p(np.abs(X_normal[:, 0]) * 1_000_000) / 15  # amount_log

        # Generate fraud transactions (different distribution)
        X_fraud = np.random.randn(n_fraud, self.config.num_features).astype(np.float32)
        X_fraud[:, 0] = np.abs(X_fraud[:, 0]) * 50000 / 1_000_000  # Higher amounts
        X_fraud[:, 1] = np.log1p(np.abs(X_fraud[:, 0]) * 1_000_000) / 15
        X_fraud[:, 8] = 1.0  # is_weekend more common
        X_fraud[:, 9] = 1.0  # is_night more common
        X_fraud[:, 10] = np.random.uniform(0.5, 1.0, n_fraud)  # High velocity

        # Combine
        X = np.vstack([X_normal, X_fraud])
        y = np.concatenate([np.zeros(n_normal), np.ones(n_fraud)])

        # Shuffle
        perm = np.random.permutation(len(X))
        X = X[perm]
        y = y[perm]

        return X, y

    def _partition_data_iid(
        self,
        X: np.ndarray,
        y: np.ndarray,
    ) -> list[tuple[np.ndarray, np.ndarray]]:
        """Partition data IID (equal random split)."""
        n_per_client = len(X) // self.num_clients
        partitions = []

        indices = np.random.permutation(len(X))

        for i in range(self.num_clients):
            start = i * n_per_client
            end = start + n_per_client
            client_indices = indices[start:end]
            partitions.append((X[client_indices], y[client_indices]))

        return partitions

    def _partition_data_non_iid(
        self,
        X: np.ndarray,
        y: np.ndarray,
    ) -> list[tuple[np.ndarray, np.ndarray]]:
        """
        Partition data non-IID using Dirichlet distribution.

        This creates heterogeneous data distributions across clients,
        which is more realistic for banks with different customer bases.
        """
        # Sort by label to create skewed distributions
        fraud_indices = np.where(y == 1)[0]
        normal_indices = np.where(y == 0)[0]

        # Dirichlet distribution for fraud samples
        alpha = self.config.non_iid_alpha
        fraud_proportions = np.random.dirichlet([alpha] * self.num_clients)
        normal_proportions = np.random.dirichlet([alpha] * self.num_clients)

        # Assign samples to clients
        partitions: list[tuple[list, list]] = [([], []) for _ in range(self.num_clients)]

        # Distribute fraud samples
        np.random.shuffle(fraud_indices)
        fraud_splits = (fraud_proportions * len(fraud_indices)).astype(int)
        fraud_splits[-1] = len(fraud_indices) - fraud_splits[:-1].sum()  # Ensure all assigned

        start = 0
        for i, count in enumerate(fraud_splits):
            end = start + count
            for idx in fraud_indices[start:end]:
                partitions[i][0].append(X[idx])
                partitions[i][1].append(y[idx])
            start = end

        # Distribute normal samples
        np.random.shuffle(normal_indices)
        normal_splits = (normal_proportions * len(normal_indices)).astype(int)
        normal_splits[-1] = len(normal_indices) - normal_splits[:-1].sum()

        start = 0
        for i, count in enumerate(normal_splits):
            end = start + count
            for idx in normal_indices[start:end]:
                partitions[i][0].append(X[idx])
                partitions[i][1].append(y[idx])
            start = end

        # Convert to numpy arrays
        result = []
        for X_list, y_list in partitions:
            X_arr = np.array(X_list, dtype=np.float32)
            y_arr = np.array(y_list, dtype=np.float32)

            # Shuffle within client
            perm = np.random.permutation(len(X_arr))
            result.append((X_arr[perm], y_arr[perm]))

        return result

    def setup(self, non_iid: bool = True) -> None:
        """
        Set up the simulation environment.

        Args:
            non_iid: Whether to use non-IID data partitioning
        """
        logger.info("Setting up federated simulation...")

        # Generate data
        X, y = self._generate_synthetic_data()
        self._all_X = X
        self._all_y = y

        # Split train/test
        n_test = int(len(X) * self.config.test_ratio)
        test_indices = np.random.choice(len(X), n_test, replace=False)
        train_mask = np.ones(len(X), dtype=bool)
        train_mask[test_indices] = False

        self._X_test = X[test_indices]
        self._y_test = y[test_indices]
        X_train = X[train_mask]
        y_train = y[train_mask]

        # Partition training data
        if non_iid:
            partitions = self._partition_data_non_iid(X_train, y_train)
        else:
            partitions = self._partition_data_iid(X_train, y_train)

        # Initialize server
        self._server = FederatedServer(
            input_dim=self.config.num_features,
            hidden_dim=self.config.hidden_dim,
        )

        # Initialize clients
        self._clients = []
        for i, (name, (X_client, y_client)) in enumerate(zip(self.institution_names, partitions)):
            client = FederatedClient(
                client_id=f"client_{i}",
                institution_name=name,
                input_dim=self.config.num_features,
                hidden_dim=self.config.hidden_dim,
                epochs_per_round=self.config.epochs_per_round,
                batch_size=self.config.batch_size,
                learning_rate=self.config.learning_rate,
            )
            client.set_data(X_client, y_client)
            self._clients.append(client)
            self._server.register_client(f"client_{i}", client)

            fraud_rate = y_client.sum() / len(y_client) * 100
            logger.info(f"  {name}: {len(X_client)} samples ({fraud_rate:.1f}% fraud)")

        logger.info(f"Setup complete. Test set: {len(self._X_test)} samples")

    def run_simulation(
        self,
        rounds: int = 20,
        non_iid: bool = True,
        compare_centralized: bool = True,
    ) -> SimulationResult:
        """
        Run the federated learning simulation.

        Args:
            rounds: Number of federated rounds
            non_iid: Whether to use non-IID data partitioning
            compare_centralized: Whether to train centralized baseline

        Returns:
            SimulationResult with all metrics
        """
        # Setup if not already done
        if self._server is None:
            self.setup(non_iid=non_iid)

        logger.info(f"\n{'='*60}")
        logger.info("STARTING FEDERATED LEARNING SIMULATION")
        logger.info(f"{'='*60}\n")

        # Run federated training
        history = self._server.train(rounds=rounds)

        # Evaluate final model
        final_metrics = self._server.evaluate_global(self._X_test, self._y_test)

        logger.info(f"\nFinal federated model metrics:")
        logger.info(f"  Accuracy: {final_metrics.get('accuracy', 0):.4f}")
        logger.info(f"  AUC: {final_metrics.get('auc', 0):.4f}")
        logger.info(f"  Precision: {final_metrics.get('precision', 0):.4f}")
        logger.info(f"  Recall: {final_metrics.get('recall', 0):.4f}")

        # Centralized baseline
        centralized_metrics = None
        if compare_centralized:
            centralized_metrics = self._train_centralized_baseline()

        # Per-client metrics
        client_metrics = {}
        for client in self._clients:
            metrics = client.evaluate(self._X_test, self._y_test)
            client_metrics[client.institution_name] = metrics

        # Privacy analysis
        privacy_analysis = self._analyze_privacy()

        result = SimulationResult(
            federated_history=history,
            final_metrics=final_metrics,
            centralized_metrics=centralized_metrics,
            client_metrics=client_metrics,
            privacy_analysis=privacy_analysis,
        )

        logger.info(f"\n{result.summary()}")

        return result

    def _train_centralized_baseline(self) -> dict[str, float]:
        """Train a centralized model for comparison."""
        logger.info("\nTraining centralized baseline...")

        try:
            import torch
            import torch.nn as nn
            from torch.optim import Adam
            from torch.utils.data import DataLoader, TensorDataset

            # Use all training data
            train_mask = np.ones(len(self._all_X), dtype=bool)
            test_indices = np.random.choice(len(self._all_X), len(self._X_test), replace=False)
            train_mask[test_indices] = False

            X_train = self._all_X[train_mask]
            y_train = self._all_y[train_mask]

            # Create model
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model = nn.Sequential(
                nn.Linear(self.config.num_features, self.config.hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(self.config.hidden_dim),
                nn.Dropout(0.3),
                nn.Linear(self.config.hidden_dim, self.config.hidden_dim // 2),
                nn.ReLU(),
                nn.BatchNorm1d(self.config.hidden_dim // 2),
                nn.Dropout(0.2),
                nn.Linear(self.config.hidden_dim // 2, 1),
                nn.Sigmoid(),
            ).to(device)

            # Training
            X_tensor = torch.FloatTensor(X_train).to(device)
            y_tensor = torch.FloatTensor(y_train).unsqueeze(1).to(device)

            dataset = TensorDataset(X_tensor, y_tensor)
            loader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

            optimizer = Adam(model.parameters(), lr=self.config.learning_rate)
            criterion = nn.BCELoss()

            # Train for same total epochs as federated
            total_epochs = self.config.epochs_per_round * 10  # Approximate

            model.train()
            for epoch in range(total_epochs):
                for batch_X, batch_y in loader:
                    optimizer.zero_grad()
                    loss = criterion(model(batch_X), batch_y)
                    loss.backward()
                    optimizer.step()

            # Evaluate
            model.eval()
            X_test_tensor = torch.FloatTensor(self._X_test).to(device)

            with torch.no_grad():
                predictions = model(X_test_tensor).cpu().numpy().flatten()

            pred_labels = (predictions >= 0.5).astype(int)
            accuracy = (pred_labels == self._y_test).mean()

            tp = ((pred_labels == 1) & (self._y_test == 1)).sum()
            fp = ((pred_labels == 1) & (self._y_test == 0)).sum()
            fn = ((pred_labels == 0) & (self._y_test == 1)).sum()

            precision = tp / (tp + fp + 1e-6)
            recall = tp / (tp + fn + 1e-6)

            from sklearn.metrics import roc_auc_score

            auc = roc_auc_score(self._y_test, predictions)

            return {
                "accuracy": float(accuracy),
                "precision": float(precision),
                "recall": float(recall),
                "f1": 2 * precision * recall / (precision + recall + 1e-6),
                "auc": float(auc),
            }

        except Exception as e:
            logger.error(f"Centralized training failed: {e}")
            return {}

    def _analyze_privacy(self) -> dict[str, Any]:
        """Analyze privacy guarantees of the federated setup."""
        # Count parameters exchanged
        if self._server:
            params = self._server.get_global_parameters()
            total_params = sum(p.size for p in params)
        else:
            total_params = 0

        # Total data that would be shared in centralized
        total_samples = sum(c.num_samples for c in self._clients)

        return {
            "data_shared": "None (raw data stays local)",
            "params_exchanged": total_params,
            "total_local_samples": total_samples,
            "privacy_model": "Horizontal Federated Learning",
            "aggregation": "FedAvg (weighted averaging)",
            "potential_risks": [
                "Model inversion attacks (mitigated by averaging)",
                "Membership inference (mitigated by local training)",
            ],
            "recommendations": [
                "Add differential privacy for stronger guarantees",
                "Use secure aggregation for parameter protection",
            ],
        }

    def plot_results(self, result: SimulationResult) -> None:
        """
        Plot simulation results.

        Args:
            result: SimulationResult from run_simulation
        """
        try:
            import matplotlib.pyplot as plt

            fig, axes = plt.subplots(1, 3, figsize=(15, 4))

            # Loss curve
            ax1 = axes[0]
            ax1.plot(result.federated_history.losses, "b-", linewidth=2)
            ax1.set_xlabel("Round")
            ax1.set_ylabel("Loss")
            ax1.set_title("Federated Training Loss")
            ax1.grid(True, alpha=0.3)

            # Accuracy curve
            ax2 = axes[1]
            ax2.plot(result.federated_history.accuracies, "g-", linewidth=2)
            ax2.set_xlabel("Round")
            ax2.set_ylabel("Accuracy")
            ax2.set_title("Federated Training Accuracy")
            ax2.grid(True, alpha=0.3)

            # Per-client metrics
            ax3 = axes[2]
            clients = list(result.client_metrics.keys())
            accuracies = [result.client_metrics[c].get("accuracy", 0) for c in clients]

            bars = ax3.bar(range(len(clients)), accuracies)
            ax3.set_xticks(range(len(clients)))
            ax3.set_xticklabels(clients, rotation=45, ha="right")
            ax3.set_ylabel("Accuracy")
            ax3.set_title("Per-Client Model Accuracy")
            ax3.axhline(
                y=result.final_metrics.get("accuracy", 0),
                color="r",
                linestyle="--",
                label="Global Model",
            )
            ax3.legend()
            ax3.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig("federated_simulation_results.png", dpi=150)
            plt.show()

            logger.info("Results plotted and saved to federated_simulation_results.png")

        except ImportError:
            logger.warning("matplotlib not available for plotting")
        except Exception as e:
            logger.error(f"Plotting failed: {e}")


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """Run federated simulation from command line."""
    import argparse

    parser = argparse.ArgumentParser(description="SentinelFlow Federated Learning Simulator")
    parser.add_argument("--clients", type=int, default=5, help="Number of clients")
    parser.add_argument("--rounds", type=int, default=20, help="Federated rounds")
    parser.add_argument("--samples", type=int, default=2000, help="Samples per client")
    parser.add_argument("--fraud-ratio", type=float, default=0.05, help="Fraud ratio")
    parser.add_argument("--non-iid", action="store_true", help="Use non-IID partitioning")
    parser.add_argument("--plot", action="store_true", help="Plot results")

    args = parser.parse_args()

    config = SimulationConfig(
        num_clients=args.clients,
        samples_per_client=args.samples,
        fraud_ratio=args.fraud_ratio,
    )

    sim = FederatedSimulator(num_clients=args.clients, config=config)
    results = sim.run_simulation(rounds=args.rounds, non_iid=args.non_iid)

    if args.plot:
        sim.plot_results(results)


if __name__ == "__main__":
    main()

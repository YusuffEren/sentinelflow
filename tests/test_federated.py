# SentinelFlow - Federated Learning Tests

import numpy as np


class TestFederatedClient:
    """Tests for FederatedClient."""

    def test_client_initialization(self):
        """Test client initializes correctly."""
        from sentinelflow.ml.federated import FederatedClient

        client = FederatedClient(
            client_id="test_client",
            institution_name="Test Bank",
        )
        assert client is not None
        assert client.client_id == "test_client"

    def test_client_set_data(self, ml_feature_batch, ml_labels):
        """Test setting client data."""
        from sentinelflow.ml.federated import FederatedClient

        client = FederatedClient(
            client_id="test_client",
            institution_name="Test Bank",
        )

        client.set_data(ml_feature_batch, ml_labels.astype(np.float32))

        assert client.num_samples == 100

    def test_client_train(self, ml_feature_batch, ml_labels):
        """Test local training."""
        from sentinelflow.ml.federated import FederatedClient

        client = FederatedClient(
            client_id="test_client",
            institution_name="Test Bank",
            epochs_per_round=1,  # Quick test
        )

        client.set_data(ml_feature_batch, ml_labels.astype(np.float32))
        result = client.train()

        assert result is not None
        assert result.num_samples == 100
        assert "accuracy" in result.metrics


class TestFederatedServer:
    """Tests for FederatedServer."""

    def test_server_initialization(self):
        """Test server initializes correctly."""
        from sentinelflow.ml.federated import FederatedServer

        server = FederatedServer()
        assert server is not None

    def test_server_register_client(self, ml_feature_batch, ml_labels):
        """Test registering clients."""
        from sentinelflow.ml.federated import FederatedClient, FederatedServer

        server = FederatedServer()
        client = FederatedClient("c1", "Bank A")
        client.set_data(ml_feature_batch, ml_labels.astype(np.float32))

        server.register_client("c1", client)

        assert server.num_clients == 1

    def test_server_train_round(self, ml_feature_batch, ml_labels):
        """Test single training round."""
        from sentinelflow.ml.federated import FederatedClient, FederatedServer

        server = FederatedServer()

        # Register two clients with different data splits
        for i, name in enumerate(["Bank A", "Bank B"]):
            client = FederatedClient(f"c{i}", name, epochs_per_round=1)
            start = i * 50
            end = start + 50
            client.set_data(ml_feature_batch[start:end], ml_labels[start:end].astype(np.float32))
            server.register_client(f"c{i}", client)

        result = server.train_round()

        assert result is not None
        assert result.num_clients == 2


class TestFederatedSimulator:
    """Tests for FederatedSimulator."""

    def test_simulator_initialization(self):
        """Test simulator initializes correctly."""
        from sentinelflow.ml.federated import FederatedSimulator

        sim = FederatedSimulator(num_clients=3)
        assert sim is not None
        assert sim.num_clients == 3

    def test_simulator_setup(self):
        """Test simulator setup."""
        from sentinelflow.ml.federated import FederatedSimulator

        sim = FederatedSimulator(num_clients=3)

        sim.setup(non_iid=False)

        assert sim._server is not None
        assert len(sim._clients) == 3

    def test_simulator_run_short(self):
        """Test short simulation run."""
        from sentinelflow.ml.federated import FederatedSimulator
        from sentinelflow.ml.federated.simulator import SimulationConfig

        config = SimulationConfig(
            num_clients=2,
            samples_per_client=100,
            epochs_per_round=1,
        )

        sim = FederatedSimulator(num_clients=2, config=config)
        result = sim.run_simulation(rounds=2, compare_centralized=False)

        assert result is not None
        assert len(result.federated_history.rounds) == 2

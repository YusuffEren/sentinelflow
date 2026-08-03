# =============================================================================
# SentinelFlow - Federated Learning Module
# =============================================================================
"""
Federated Learning for privacy-preserving fraud detection.

This module demonstrates how multiple financial institutions can
collaboratively train a fraud detection model without sharing
raw transaction data.

Components:
    - FederatedServer: Central aggregation server
    - FederatedClient: Bank/institution client
    - FederatedSimulator: Multi-bank simulation

Usage:
    from sentinelflow.ml.federated import FederatedSimulator

    sim = FederatedSimulator(num_clients=5)
    sim.run_simulation(rounds=10)
"""

from sentinelflow.ml.federated.client import FederatedClient
from sentinelflow.ml.federated.server import FederatedServer
from sentinelflow.ml.federated.simulator import FederatedSimulator

__all__ = [
    "FederatedServer",
    "FederatedClient",
    "FederatedSimulator",
]

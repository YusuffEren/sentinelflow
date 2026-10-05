# =============================================================================
# SentinelFlow - Neo4j Graph Engine
# =============================================================================
"""
Neo4j graph database integration for fraud detection.

This package provides the GraphEngine class which handles:
- Connection management with connection pooling
- Schema setup (constraints and indexes)
- Transaction ingestion with MERGE queries
- Circular fraud ring detection using Cypher path queries

Usage:
    from sentinelflow.processor.graph_engine import GraphEngine

    engine = GraphEngine()
    engine.setup_constraints()

    # Add a transaction
    engine.add_transaction({
        "sender_iban": "TR123...",
        "receiver_iban": "TR456...",
        "amount": 5000.0,
        "timestamp": "2026-01-16T12:00:00"
    })

    # Check for fraud rings
    rings = engine.detect_fraud_rings(sender_iban="TR123...")
"""

from .engine import GraphEngine
from .types import FraudRing, TransactionData

__all__ = ["FraudRing", "GraphEngine", "TransactionData"]

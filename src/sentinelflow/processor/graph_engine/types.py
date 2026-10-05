# =============================================================================
# SentinelFlow - Graph Engine Types
# =============================================================================
"""TypedDict definitions for graph engine inputs and results."""

from __future__ import annotations

from typing import TypedDict


class TransactionData(TypedDict, total=False):
    """Type definition for transaction data input."""

    transaction_id: str
    sender_iban: str
    sender_name: str
    sender_city: str
    receiver_iban: str
    receiver_name: str
    receiver_city: str
    amount: float
    timestamp: str
    description: str
    fraud_type: str
    ring_id: str | None


class FraudRing(TypedDict):
    """Detected fraud ring result."""

    ring_id: str
    path: list[str]  # List of IBANs in the ring
    total_amount: float
    transaction_count: int
    detected_at: str

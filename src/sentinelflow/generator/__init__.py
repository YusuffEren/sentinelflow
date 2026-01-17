# =============================================================================
# SentinelFlow - Generator Module
# =============================================================================
"""
Synthetic Turkish banking data generator with fraud pattern injection.

This module provides:
- Realistic transaction generation with Turkish localization
- Fraud pattern injection (circular rings, impossible travel, blacklist keywords)
- Kafka producer integration for real-time streaming
"""

from sentinelflow.generator.models import (
    Account,
    City,
    FraudAlert,
    FraudType,
    Transaction,
    TransactionStatus,
)
from sentinelflow.generator.patterns import (
    BlacklistKeywordGenerator,
    CircularRingGenerator,
    FraudPatternMixer,
    ImpossibleTravelGenerator,
    NormalTransactionGenerator,
    TURKISH_CITIES,
)

__all__ = [
    # Models
    "Account",
    "City",
    "FraudAlert",
    "FraudType",
    "Transaction",
    "TransactionStatus",
    # Generators
    "BlacklistKeywordGenerator",
    "CircularRingGenerator",
    "FraudPatternMixer",
    "ImpossibleTravelGenerator",
    "NormalTransactionGenerator",
    "TURKISH_CITIES",
]

# =============================================================================
# SentinelFlow - Fraud Detector Service (The Brain)
# =============================================================================
"""
The core fraud detection service that processes transactions in real-time.

This is the "Brain" of SentinelFlow, orchestrating multiple fraud detection
engines:

1. **Graph Analysis (Neo4j)**: Detects circular transaction rings (money laundering)
   Example: A → B → C → A pattern where money flows in a circle

2. **Impossible Travel (Redis)**: Detects physically impossible travel speeds
   Example: Transaction in İstanbul, then 10 minutes later in Berlin

3. **NLP Blacklist**: Detects suspicious keywords in transaction descriptions
   Example: "bahis", "kumar", "crypto" in the description field

Architecture:
    [Kafka: transactions] → [Detector Service] → [Kafka: alerts]
                                    ↓
                            [Neo4j + Redis]

Usage:
    # Run the detector service
    python -m sentinelflow.processor.detector

    # Or with options
    python -m sentinelflow.processor.detector --consumer-group my-group --verbose
"""

from .checks import DetectionChecksMixin
from .cli import main, parse_args
from .publishing import AlertPublishingMixin
from .runtime import RuntimeMixin
from .service import FraudDetectorService
from .types import BLACKLIST_KEYWORDS, DetectorStats, FraudAlert, FraudType

__all__ = [
    "BLACKLIST_KEYWORDS",
    "AlertPublishingMixin",
    "DetectionChecksMixin",
    "DetectorStats",
    "FraudAlert",
    "FraudDetectorService",
    "FraudType",
    "RuntimeMixin",
    "main",
    "parse_args",
]

# =============================================================================
# SentinelFlow - Real-Time Financial Fraud Detection System
# =============================================================================
"""
SentinelFlow: Enterprise-Grade Real-Time Financial Fraud Detection System.

This package provides a complete solution for detecting complex fraud patterns
including Money Laundering Rings, Mule Accounts, and Impossible Travel anomalies.

Modules:
    - config: Configuration management
    - generator: Synthetic data generation
    - ingestor: Kafka producers
    - detectors: Fraud detection engines
    - database: Database connectors
    - dashboard: Streamlit visualization
"""

__version__ = "1.0.0"
__author__ = "Teknofest Team"
__email__ = "team@sentinelflow.dev"

from typing import Final

# Package constants
PACKAGE_NAME: Final[str] = "sentinelflow"

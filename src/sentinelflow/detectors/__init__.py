# =============================================================================
# SentinelFlow - Detection CLI Entry Points
# =============================================================================
"""
Command-line entry points for SentinelFlow detection services.

Usage:
    python -m sentinelflow.detectors.run          # Run detector service
    python -m sentinelflow.detectors.graph        # Run graph engine
    python -m sentinelfflow.detectors.geo         # Run Redis geo detection
"""

from sentinelflow.detectors.cli import main

__all__ = ["main"]

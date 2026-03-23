# =============================================================================
# SentinelFlow - Monitoring Module
# =============================================================================
"""
Production-grade observability and monitoring.

Provides:
- Prometheus metrics endpoint
- Grafana dashboard templates
- Structured JSON logging
- OpenTelemetry tracing
- Alert rules

Components:
    - MetricsCollector: Prometheus metrics
    - TracingManager: Distributed tracing
    - StructuredLogger: JSON logging
    - AlertManager: Alert rules and notifications
"""

from sentinelflow.monitoring.logging import StructuredLogger
from sentinelflow.monitoring.metrics import MetricsCollector, metrics
from sentinelflow.monitoring.tracing import TracingManager

__all__ = [
    "MetricsCollector",
    "metrics",
    "TracingManager",
    "StructuredLogger",
]

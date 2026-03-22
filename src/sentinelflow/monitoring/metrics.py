# =============================================================================
# SentinelFlow - Prometheus Metrics
# =============================================================================
"""
Prometheus metrics collection for SentinelFlow.

Exposes metrics for:
- Transaction processing
- Fraud detection rates
- ML model performance
- System health
- API latencies

Metrics are exposed at /metrics endpoint for Prometheus scraping.
"""

from __future__ import annotations

import time
from functools import wraps
from typing import Callable

from loguru import logger

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        Info,
        Summary,
        generate_latest,
    )

    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    logger.warning("prometheus_client not available")


# =============================================================================
# Metrics Registry
# =============================================================================

if HAS_PROMETHEUS:
    # Use default registry
    REGISTRY = CollectorRegistry(auto_describe=True)

    # =========================================================================
    # Transaction Metrics
    # =========================================================================

    TRANSACTIONS_TOTAL = Counter(
        "sentinelflow_transactions_total",
        "Total number of transactions processed",
        ["status", "fraud_type"],
        registry=REGISTRY,
    )

    TRANSACTION_AMOUNT = Histogram(
        "sentinelflow_transaction_amount_try",
        "Transaction amounts in TRY",
        buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
        registry=REGISTRY,
    )

    TRANSACTION_LATENCY = Histogram(
        "sentinelflow_transaction_latency_seconds",
        "Transaction processing latency",
        ["stage"],
        buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        registry=REGISTRY,
    )

    # =========================================================================
    # Fraud Detection Metrics
    # =========================================================================

    FRAUD_ALERTS_TOTAL = Counter(
        "sentinelflow_fraud_alerts_total",
        "Total fraud alerts generated",
        ["fraud_type", "severity"],
        registry=REGISTRY,
    )

    FRAUD_DETECTION_RATE = Gauge(
        "sentinelflow_fraud_detection_rate",
        "Current fraud detection rate",
        registry=REGISTRY,
    )

    FRAUD_FALSE_POSITIVE_RATE = Gauge(
        "sentinelflow_fraud_false_positive_rate",
        "Estimated false positive rate",
        registry=REGISTRY,
    )

    CIRCULAR_RINGS_DETECTED = Counter(
        "sentinelflow_circular_rings_total",
        "Total circular fraud rings detected",
        registry=REGISTRY,
    )

    IMPOSSIBLE_TRAVEL_DETECTED = Counter(
        "sentinelflow_impossible_travel_total",
        "Total impossible travel alerts",
        registry=REGISTRY,
    )

    # =========================================================================
    # ML Model Metrics
    # =========================================================================

    ML_PREDICTION_LATENCY = Histogram(
        "sentinelflow_ml_prediction_latency_seconds",
        "ML model prediction latency",
        ["model"],
        buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
        registry=REGISTRY,
    )

    ML_MODEL_SCORE = Summary(
        "sentinelflow_ml_model_score",
        "ML model prediction scores",
        ["model"],
        registry=REGISTRY,
    )

    ENSEMBLE_SCORE_HISTOGRAM = Histogram(
        "sentinelflow_ensemble_score",
        "Ensemble model score distribution",
        buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        registry=REGISTRY,
    )

    ML_MODEL_STATUS = Gauge(
        "sentinelflow_ml_model_status",
        "ML model status (1=ready, 0=not ready)",
        ["model"],
        registry=REGISTRY,
    )

    # =========================================================================
    # Compliance Metrics
    # =========================================================================

    COMPLIANCE_CHECKS_TOTAL = Counter(
        "sentinelflow_compliance_checks_total",
        "Total compliance checks performed",
        ["result"],
        registry=REGISTRY,
    )

    COMPLIANCE_VIOLATIONS = Counter(
        "sentinelflow_compliance_violations_total",
        "Total compliance violations",
        ["rule", "severity"],
        registry=REGISTRY,
    )

    STR_REPORTS_TOTAL = Counter(
        "sentinelflow_str_reports_total",
        "Total STR reports generated",
        ["status"],
        registry=REGISTRY,
    )

    # =========================================================================
    # System Metrics
    # =========================================================================

    ACTIVE_CONNECTIONS = Gauge(
        "sentinelflow_active_connections",
        "Active connections",
        ["service"],
        registry=REGISTRY,
    )

    KAFKA_MESSAGES_PROCESSED = Counter(
        "sentinelflow_kafka_messages_total",
        "Kafka messages processed",
        ["topic", "status"],
        registry=REGISTRY,
    )

    KAFKA_LAG = Gauge(
        "sentinelflow_kafka_consumer_lag",
        "Kafka consumer lag",
        ["topic", "partition"],
        registry=REGISTRY,
    )

    NEO4J_QUERIES = Counter(
        "sentinelflow_neo4j_queries_total",
        "Neo4j queries executed",
        ["query_type"],
        registry=REGISTRY,
    )

    REDIS_OPERATIONS = Counter(
        "sentinelflow_redis_operations_total",
        "Redis operations",
        ["operation"],
        registry=REGISTRY,
    )

    # =========================================================================
    # API Metrics
    # =========================================================================

    API_REQUESTS = Counter(
        "sentinelflow_api_requests_total",
        "API requests",
        ["method", "endpoint", "status_code"],
        registry=REGISTRY,
    )

    API_LATENCY = Histogram(
        "sentinelflow_api_latency_seconds",
        "API request latency",
        ["method", "endpoint"],
        buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        registry=REGISTRY,
    )

    WEBSOCKET_CONNECTIONS = Gauge(
        "sentinelflow_websocket_connections",
        "Active WebSocket connections",
        registry=REGISTRY,
    )

    # =========================================================================
    # App Info
    # =========================================================================

    APP_INFO = Info(
        "sentinelflow",
        "SentinelFlow application info",
        registry=REGISTRY,
    )


# =============================================================================
# Metrics Collector Class
# =============================================================================


class MetricsCollector:
    """
    Centralized metrics collection and exposure.

    Example:
        >>> metrics = MetricsCollector()
        >>> metrics.record_transaction("success", "none", 5000.0)
        >>> metrics.record_fraud_alert("circular_ring", "HIGH")
        >>>
        >>> # Get Prometheus metrics
        >>> prometheus_data = metrics.get_prometheus_metrics()
    """

    def __init__(self):
        """Initialize metrics collector."""
        self._initialized = HAS_PROMETHEUS

        if self._initialized:
            # Set app info
            APP_INFO.info(
                {
                    "version": "2.0.0",
                    "name": "SentinelFlow",
                    "description": "Real-Time Financial Fraud Detection",
                }
            )

        logger.info(f"MetricsCollector initialized (prometheus={self._initialized})")

    # =========================================================================
    # Transaction Metrics
    # =========================================================================

    def record_transaction(
        self,
        status: str,
        fraud_type: str,
        amount: float,
    ) -> None:
        """Record a processed transaction."""
        if not self._initialized:
            return

        TRANSACTIONS_TOTAL.labels(status=status, fraud_type=fraud_type).inc()
        TRANSACTION_AMOUNT.observe(amount)

    def record_transaction_latency(self, stage: str, latency_seconds: float) -> None:
        """Record transaction processing latency."""
        if not self._initialized:
            return

        TRANSACTION_LATENCY.labels(stage=stage).observe(latency_seconds)

    # =========================================================================
    # Fraud Detection Metrics
    # =========================================================================

    def record_fraud_alert(self, fraud_type: str, severity: str) -> None:
        """Record a fraud alert."""
        if not self._initialized:
            return

        FRAUD_ALERTS_TOTAL.labels(fraud_type=fraud_type, severity=severity).inc()

        if fraud_type == "circular_ring":
            CIRCULAR_RINGS_DETECTED.inc()
        elif fraud_type == "impossible_travel":
            IMPOSSIBLE_TRAVEL_DETECTED.inc()

    def set_fraud_detection_rate(self, rate: float) -> None:
        """Set current fraud detection rate."""
        if not self._initialized:
            return

        FRAUD_DETECTION_RATE.set(rate)

    def set_false_positive_rate(self, rate: float) -> None:
        """Set false positive rate."""
        if not self._initialized:
            return

        FRAUD_FALSE_POSITIVE_RATE.set(rate)

    # =========================================================================
    # ML Model Metrics
    # =========================================================================

    def record_ml_prediction(
        self,
        model: str,
        score: float,
        latency_seconds: float,
    ) -> None:
        """Record ML model prediction."""
        if not self._initialized:
            return

        ML_PREDICTION_LATENCY.labels(model=model).observe(latency_seconds)
        ML_MODEL_SCORE.labels(model=model).observe(score)

    def record_ensemble_score(self, score: float) -> None:
        """Record ensemble model score."""
        if not self._initialized:
            return

        ENSEMBLE_SCORE_HISTOGRAM.observe(score)

    def set_model_status(self, model: str, ready: bool) -> None:
        """Set ML model status."""
        if not self._initialized:
            return

        ML_MODEL_STATUS.labels(model=model).set(1 if ready else 0)

    # =========================================================================
    # Compliance Metrics
    # =========================================================================

    def record_compliance_check(self, compliant: bool) -> None:
        """Record compliance check result."""
        if not self._initialized:
            return

        result = "pass" if compliant else "fail"
        COMPLIANCE_CHECKS_TOTAL.labels(result=result).inc()

    def record_compliance_violation(self, rule: str, severity: str) -> None:
        """Record compliance violation."""
        if not self._initialized:
            return

        COMPLIANCE_VIOLATIONS.labels(rule=rule, severity=severity).inc()

    def record_str_report(self, status: str) -> None:
        """Record STR report generation."""
        if not self._initialized:
            return

        STR_REPORTS_TOTAL.labels(status=status).inc()

    # =========================================================================
    # System Metrics
    # =========================================================================

    def set_active_connections(self, service: str, count: int) -> None:
        """Set active connection count."""
        if not self._initialized:
            return

        ACTIVE_CONNECTIONS.labels(service=service).set(count)

    def record_kafka_message(self, topic: str, success: bool) -> None:
        """Record Kafka message processing."""
        if not self._initialized:
            return

        status = "success" if success else "error"
        KAFKA_MESSAGES_PROCESSED.labels(topic=topic, status=status).inc()

    def set_kafka_lag(self, topic: str, partition: int, lag: int) -> None:
        """Set Kafka consumer lag."""
        if not self._initialized:
            return

        KAFKA_LAG.labels(topic=topic, partition=str(partition)).set(lag)

    def record_neo4j_query(self, query_type: str) -> None:
        """Record Neo4j query."""
        if not self._initialized:
            return

        NEO4J_QUERIES.labels(query_type=query_type).inc()

    def record_redis_operation(self, operation: str) -> None:
        """Record Redis operation."""
        if not self._initialized:
            return

        REDIS_OPERATIONS.labels(operation=operation).inc()

    # =========================================================================
    # API Metrics
    # =========================================================================

    def record_api_request(
        self,
        method: str,
        endpoint: str,
        status_code: int,
        latency_seconds: float,
    ) -> None:
        """Record API request."""
        if not self._initialized:
            return

        API_REQUESTS.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code),
        ).inc()

        API_LATENCY.labels(method=method, endpoint=endpoint).observe(latency_seconds)

    def set_websocket_connections(self, count: int) -> None:
        """Set WebSocket connection count."""
        if not self._initialized:
            return

        WEBSOCKET_CONNECTIONS.set(count)

    # =========================================================================
    # Export
    # =========================================================================

    def get_prometheus_metrics(self) -> bytes:
        """Get Prometheus metrics in exposition format."""
        if not self._initialized:
            return b"# prometheus_client not installed\n"

        return generate_latest(REGISTRY)

    def get_content_type(self) -> str:
        """Get Prometheus content type."""
        if not self._initialized:
            return "text/plain"

        return CONTENT_TYPE_LATEST


# =============================================================================
# Decorators
# =============================================================================


def track_latency(stage: str):
    """Decorator to track function latency."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                latency = time.time() - start
                if HAS_PROMETHEUS:
                    TRANSACTION_LATENCY.labels(stage=stage).observe(latency)

        return wrapper

    return decorator


def track_api_request(method: str, endpoint: str):
    """Decorator to track API request metrics."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            status_code = 200
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception:
                status_code = 500
                raise
            finally:
                latency = time.time() - start
                if HAS_PROMETHEUS:
                    API_REQUESTS.labels(
                        method=method,
                        endpoint=endpoint,
                        status_code=str(status_code),
                    ).inc()
                    API_LATENCY.labels(method=method, endpoint=endpoint).observe(latency)

        return wrapper

    return decorator


# =============================================================================
# Global Instance
# =============================================================================

metrics = MetricsCollector()

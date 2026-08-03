# =============================================================================
# SentinelFlow - Monitoring Module Tests
# =============================================================================
"""
Tests for monitoring: MetricsCollector, StructuredLogger, TracingManager.

Run with: pytest tests/test_monitoring.py -v
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_initialization(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        assert mc is not None

    def test_record_transaction(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_transaction("completed", "none", 5000.0)

    def test_record_fraud_alert(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_fraud_alert("circular_ring", "high")

    def test_record_api_request(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_api_request("GET", "/api/v1/health", 200, 0.045)

    def test_record_ml_prediction(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_ml_prediction("IsolationForest", 0.87, 0.003)

    def test_set_model_status(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.set_model_status("XGBoost", True)

    def test_record_compliance(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_compliance_check(True)
        mc.record_compliance_violation("AML-001", "high")

    def test_record_kafka(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.record_kafka_message("transactions", True)
        mc.set_kafka_lag("transactions", 0, 100)

    def test_set_connections(self):
        from sentinelflow.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.set_active_connections("websocket", 5)


class TestStructuredLogger:
    """Tests for StructuredLogger."""

    def test_initialization(self):
        from sentinelflow.monitoring import StructuredLogger

        sl = StructuredLogger(service_name="test")
        assert sl is not None

    def test_log_basic(self):
        from sentinelflow.monitoring import StructuredLogger

        sl = StructuredLogger(service_name="test")
        sl.info("Test message")

    def test_log_levels(self):
        from sentinelflow.monitoring import StructuredLogger

        sl = StructuredLogger(service_name="test")
        sl.debug("Debug")
        sl.info("Info")
        sl.warning("Warning")
        sl.error("Error")

    def test_log_with_correlation(self):
        from sentinelflow.monitoring import StructuredLogger

        sl = StructuredLogger(service_name="test")
        sl.set_correlation_id("corr-123")
        sl.info("With correlation")
        sl.set_correlation_id("")  # reset

    def test_log_exception(self):
        from sentinelflow.monitoring import StructuredLogger

        sl = StructuredLogger(service_name="test")
        try:
            1 / 0
        except ZeroDivisionError:
            sl.exception("Division error")


class TestTracingManager:
    """Tests for TracingManager."""

    def test_initialization(self):
        from sentinelflow.monitoring import TracingManager

        tm = TracingManager(service_name="test")
        assert tm is not None

    def test_span_context_manager(self):
        from sentinelflow.monitoring import TracingManager

        tm = TracingManager(service_name="test")
        with tm.span("test-operation") as span:
            span.set_attribute("key", "value")

    def test_trace_decorator(self):
        from sentinelflow.monitoring import TracingManager

        tm = TracingManager(service_name="test")

        @tm.trace("decorated-func")
        def my_func():
            return 42

        assert my_func() == 42

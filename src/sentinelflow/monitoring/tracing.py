# =============================================================================
# SentinelFlow - Distributed Tracing
# =============================================================================
"""
OpenTelemetry distributed tracing for SentinelFlow.

Provides end-to-end tracing across:
- API requests
- Kafka message processing
- ML model inference
- Database queries
- External service calls

Exports traces to Jaeger, Zipkin, or other OTLP-compatible backends.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Generator, Optional

from loguru import logger

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    HAS_OPENTELEMETRY = True
except ImportError:
    HAS_OPENTELEMETRY = False
    logger.warning("OpenTelemetry not available")

try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    HAS_OTLP = True
except ImportError:
    HAS_OTLP = False

try:
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter

    HAS_JAEGER = True
except ImportError:
    HAS_JAEGER = False


# =============================================================================
# Tracing Manager
# =============================================================================


class TracingManager:
    """
    Centralized distributed tracing management.

    Example:
        >>> tracing = TracingManager(service_name="sentinelflow-api")
        >>> tracing.setup()
        >>>
        >>> with tracing.span("process_transaction") as span:
        ...     span.set_attribute("transaction_id", "TX-001")
        ...     # Process transaction
    """

    def __init__(
        self,
        service_name: str = "sentinelflow",
        service_version: str = "2.0.0",
        exporter_type: str = "console",  # "console", "otlp", "jaeger"
        endpoint: str | None = None,
    ):
        """
        Initialize tracing manager.

        Args:
            service_name: Service name for traces
            service_version: Service version
            exporter_type: Type of exporter to use
            endpoint: Exporter endpoint (for otlp/jaeger)
        """
        self._service_name = service_name
        self._service_version = service_version
        self._exporter_type = exporter_type
        self._endpoint = endpoint or os.getenv("OTEL_EXPORTER_ENDPOINT", "localhost:4317")

        self._tracer: Any = None
        self._provider: Any = None
        self._initialized = False

        logger.info(f"TracingManager created (service={service_name}, exporter={exporter_type})")

    def setup(self) -> None:
        """Setup tracing with configured exporter."""
        if not HAS_OPENTELEMETRY:
            logger.warning("OpenTelemetry not available, tracing disabled")
            return

        # Create resource
        resource = Resource.create(
            {
                SERVICE_NAME: self._service_name,
                "service.version": self._service_version,
                "deployment.environment": os.getenv("ENVIRONMENT", "development"),
            }
        )

        # Create provider
        self._provider = TracerProvider(resource=resource)

        # Create exporter
        exporter = self._create_exporter()
        if exporter:
            self._provider.add_span_processor(BatchSpanProcessor(exporter))

        # Set global provider
        trace.set_tracer_provider(self._provider)

        # Get tracer
        self._tracer = trace.get_tracer(
            self._service_name,
            self._service_version,
        )

        self._initialized = True
        logger.info(f"Tracing setup complete (exporter={self._exporter_type})")

    def _create_exporter(self) -> Any:
        """Create the configured exporter."""
        if self._exporter_type == "console":
            return ConsoleSpanExporter()

        elif self._exporter_type == "otlp" and HAS_OTLP:
            return OTLPSpanExporter(endpoint=self._endpoint, insecure=True)

        elif self._exporter_type == "jaeger" and HAS_JAEGER:
            return JaegerExporter(
                agent_host_name=self._endpoint.split(":")[0],
                agent_port=int(self._endpoint.split(":")[1]) if ":" in self._endpoint else 6831,
            )

        else:
            logger.warning(f"Exporter '{self._exporter_type}' not available, using console")
            return ConsoleSpanExporter()

    @contextmanager
    def span(
        self,
        name: str,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[Any, None, None]:
        """
        Create a tracing span context manager.

        Args:
            name: Span name
            kind: Span kind (client, server, internal, etc.)
            attributes: Initial span attributes

        Yields:
            Span object
        """
        if not self._initialized or self._tracer is None:
            yield DummySpan()
            return

        with self._tracer.start_as_current_span(
            name,
            kind=kind,
            attributes=attributes,
        ) as span:
            try:
                yield span
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    def trace(
        self,
        name: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Callable:
        """
        Decorator to trace a function.

        Args:
            name: Span name (defaults to function name)
            attributes: Span attributes
        """

        def decorator(func: Callable) -> Callable:
            span_name = name or func.__name__

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                with self.span(span_name, attributes=attributes) as span:
                    span.set_attribute("function", func.__name__)
                    return func(*args, **kwargs)

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                with self.span(span_name, attributes=attributes) as span:
                    span.set_attribute("function", func.__name__)
                    return await func(*args, **kwargs)

            import asyncio

            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper

        return decorator

    def get_current_span(self) -> Any:
        """Get the current active span."""
        if not self._initialized:
            return DummySpan()

        return trace.get_current_span()

    def inject_context(self, carrier: dict[str, str]) -> None:
        """Inject trace context into carrier (for propagation)."""
        if not self._initialized:
            return

        propagator = TraceContextTextMapPropagator()
        propagator.inject(carrier)

    def extract_context(self, carrier: dict[str, str]) -> Any:
        """Extract trace context from carrier."""
        if not self._initialized:
            return None

        propagator = TraceContextTextMapPropagator()
        return propagator.extract(carrier)

    def shutdown(self) -> None:
        """Shutdown tracing provider."""
        if self._provider:
            self._provider.shutdown()
        self._initialized = False


class DummySpan:
    """Dummy span for when tracing is disabled."""

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def add_event(self, name: str, attributes: dict | None = None) -> None:
        pass

    def set_status(self, status: Any) -> None:
        pass

    def record_exception(self, exception: Exception) -> None:
        pass


# =============================================================================
# Span Attributes
# =============================================================================


class SpanAttributes:
    """Common span attribute names."""

    # Transaction
    TRANSACTION_ID = "transaction.id"
    TRANSACTION_AMOUNT = "transaction.amount"
    TRANSACTION_SENDER = "transaction.sender_iban"
    TRANSACTION_RECEIVER = "transaction.receiver_iban"

    # Fraud Detection
    FRAUD_TYPE = "fraud.type"
    FRAUD_SCORE = "fraud.score"
    FRAUD_IS_FRAUD = "fraud.is_fraud"

    # ML
    ML_MODEL = "ml.model"
    ML_SCORE = "ml.score"
    ML_LATENCY = "ml.latency_ms"

    # Database
    DB_SYSTEM = "db.system"
    DB_OPERATION = "db.operation"
    DB_STATEMENT = "db.statement"

    # Messaging
    MESSAGING_SYSTEM = "messaging.system"
    MESSAGING_DESTINATION = "messaging.destination"
    MESSAGING_OPERATION = "messaging.operation"

    # Compliance
    COMPLIANCE_CHECK = "compliance.check"
    COMPLIANCE_RESULT = "compliance.result"


# =============================================================================
# Global Instance
# =============================================================================

_tracing: TracingManager | None = None


def get_tracing() -> TracingManager:
    """Get or create global tracing manager."""
    global _tracing
    if _tracing is None:
        _tracing = TracingManager()
    return _tracing


def setup_tracing(
    service_name: str = "sentinelflow",
    exporter_type: str = "console",
    endpoint: str | None = None,
) -> TracingManager:
    """Setup global tracing manager."""
    global _tracing
    _tracing = TracingManager(
        service_name=service_name,
        exporter_type=exporter_type,
        endpoint=endpoint,
    )
    _tracing.setup()
    return _tracing

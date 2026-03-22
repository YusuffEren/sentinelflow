# =============================================================================
# SentinelFlow - Structured Logging
# =============================================================================
"""
Structured JSON logging for SentinelFlow.

Provides production-grade logging with:
- JSON format for log aggregation
- Correlation IDs for request tracing
- Log levels and filtering
- Sensitive data redaction
- Integration with ELK/Loki/CloudWatch

Example log format:
{
    "timestamp": "2026-01-16T12:00:00.000Z",
    "level": "INFO",
    "message": "Transaction processed",
    "service": "sentinelflow-api",
    "correlation_id": "abc123",
    "transaction_id": "TX-001",
    "duration_ms": 45,
    "extra": {...}
}
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from loguru import logger

# =============================================================================
# Context Variables
# =============================================================================

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")
request_id_var: ContextVar[str] = ContextVar("request_id", default="")


# =============================================================================
# Sensitive Data Patterns
# =============================================================================

SENSITIVE_KEYS = {
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "credit_card",
    "card_number",
    "cvv",
    "ssn",
    "tc_kimlik",
}

IBAN_MASK_LENGTH = 8  # Show last 8 characters of IBAN


# =============================================================================
# JSON Log Formatter
# =============================================================================


def json_formatter(record: dict) -> str:
    """Format log record as JSON."""
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "logger": record["name"],
        "module": record["module"],
        "function": record["function"],
        "line": record["line"],
    }

    # Add service info
    log_entry["service"] = os.getenv("SERVICE_NAME", "sentinelflow")
    log_entry["environment"] = os.getenv("ENVIRONMENT", "development")

    # Add correlation/request IDs if available
    if correlation_id_var.get():
        log_entry["correlation_id"] = correlation_id_var.get()
    if request_id_var.get():
        log_entry["request_id"] = request_id_var.get()

    # Add extra data
    if record["extra"]:
        # Redact sensitive data
        extra = redact_sensitive(record["extra"])
        log_entry["extra"] = extra

    # Add exception info if present
    if record["exception"]:
        log_entry["exception"] = {
            "type": record["exception"].type.__name__ if record["exception"].type else None,
            "value": str(record["exception"].value) if record["exception"].value else None,
            "traceback": record["exception"].traceback if record["exception"].traceback else None,
        }

    return json.dumps(log_entry, ensure_ascii=False, default=str) + "\n"


def redact_sensitive(data: Any, depth: int = 0) -> Any:
    """Redact sensitive information from data."""
    if depth > 10:  # Prevent infinite recursion
        return data

    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            key_lower = key.lower()

            # Check for sensitive keys
            if any(s in key_lower for s in SENSITIVE_KEYS):
                result[key] = "[REDACTED]"
            elif "iban" in key_lower and isinstance(value, str) and len(value) > IBAN_MASK_LENGTH:
                # Mask IBAN except last characters
                result[key] = "*" * (len(value) - IBAN_MASK_LENGTH) + value[-IBAN_MASK_LENGTH:]
            else:
                result[key] = redact_sensitive(value, depth + 1)
        return result

    elif isinstance(data, list):
        return [redact_sensitive(item, depth + 1) for item in data]

    else:
        return data


# =============================================================================
# Structured Logger
# =============================================================================


class StructuredLogger:
    """
    Structured logging manager for SentinelFlow.

    Example:
        >>> slog = StructuredLogger(service_name="sentinelflow-api")
        >>> slog.setup()
        >>> slog.info("Transaction processed", transaction_id="TX-001", amount=5000)
    """

    def __init__(
        self,
        service_name: str = "sentinelflow",
        log_level: str = "INFO",
        json_output: bool = True,
        log_file: str | None = None,
        rotation: str = "100 MB",
        retention: str = "30 days",
    ):
        """
        Initialize structured logger.

        Args:
            service_name: Service name for logs
            log_level: Minimum log level
            json_output: Use JSON format
            log_file: Optional log file path
            rotation: Log rotation size
            retention: Log retention period
        """
        self._service_name = service_name
        self._log_level = log_level
        self._json_output = json_output
        self._log_file = log_file
        self._rotation = rotation
        self._retention = retention
        self._configured = False

    def setup(self) -> None:
        """Configure logging."""
        # Remove default handler
        logger.remove()

        # Console handler
        if self._json_output:
            logger.add(
                sys.stdout,
                format=json_formatter,
                level=self._log_level,
                colorize=False,
            )
        else:
            logger.add(
                sys.stdout,
                format=(
                    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                    "<level>{level: <8}</level> | "
                    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                    "<level>{message}</level>"
                ),
                level=self._log_level,
                colorize=True,
            )

        # File handler
        if self._log_file:
            log_path = Path(self._log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            logger.add(
                str(log_path),
                format=json_formatter,
                level=self._log_level,
                rotation=self._rotation,
                retention=self._retention,
                compression="gz",
            )

        self._configured = True
        logger.info(
            "Structured logging configured",
            service=self._service_name,
            level=self._log_level,
            json=self._json_output,
        )

    def set_correlation_id(self, correlation_id: str | None = None) -> str:
        """Set correlation ID for current context."""
        cid = correlation_id or str(uuid.uuid4())
        correlation_id_var.set(cid)
        return cid

    def set_request_id(self, request_id: str | None = None) -> str:
        """Set request ID for current context."""
        rid = request_id or str(uuid.uuid4())
        request_id_var.set(rid)
        return rid

    def get_correlation_id(self) -> str:
        """Get current correlation ID."""
        return correlation_id_var.get()

    def with_context(self, **context) -> Any:
        """Create a logger with bound context."""
        return logger.bind(**context)

    # =========================================================================
    # Log Methods
    # =========================================================================

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        logger.critical(message, **kwargs)

    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        logger.exception(message, **kwargs)

    # =========================================================================
    # Domain-Specific Logging
    # =========================================================================

    def log_transaction(
        self,
        transaction_id: str,
        status: str,
        amount: float,
        duration_ms: float | None = None,
        **extra,
    ) -> None:
        """Log transaction processing."""
        logger.info(
            f"Transaction {status}",
            event_type="transaction",
            transaction_id=transaction_id,
            status=status,
            amount=amount,
            duration_ms=duration_ms,
            **extra,
        )

    def log_fraud_alert(
        self,
        alert_id: str,
        fraud_type: str,
        severity: str,
        transaction_id: str | None = None,
        confidence: float | None = None,
        **extra,
    ) -> None:
        """Log fraud alert."""
        logger.warning(
            f"Fraud alert: {fraud_type}",
            event_type="fraud_alert",
            alert_id=alert_id,
            fraud_type=fraud_type,
            severity=severity,
            transaction_id=transaction_id,
            confidence=confidence,
            **extra,
        )

    def log_ml_prediction(
        self,
        model: str,
        score: float,
        latency_ms: float,
        transaction_id: str | None = None,
        **extra,
    ) -> None:
        """Log ML prediction."""
        logger.debug(
            f"ML prediction: {model}",
            event_type="ml_prediction",
            model=model,
            score=score,
            latency_ms=latency_ms,
            transaction_id=transaction_id,
            **extra,
        )

    def log_compliance(
        self,
        transaction_id: str,
        compliant: bool,
        violations: list[str] | None = None,
        **extra,
    ) -> None:
        """Log compliance check."""
        level = logger.info if compliant else logger.warning
        level(
            f"Compliance check: {'pass' if compliant else 'fail'}",
            event_type="compliance_check",
            transaction_id=transaction_id,
            compliant=compliant,
            violations=violations or [],
            **extra,
        )

    def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        client_ip: str | None = None,
        **extra,
    ) -> None:
        """Log API request."""
        level = logger.info if status_code < 400 else logger.warning
        level(
            f"{method} {path} -> {status_code}",
            event_type="api_request",
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            client_ip=client_ip,
            **extra,
        )


# =============================================================================
# Decorators
# =============================================================================


def log_function(
    log_args: bool = True,
    log_result: bool = False,
    level: str = "DEBUG",
):
    """Decorator to log function entry and exit."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__name__}"

            # Log entry
            if log_args:
                logger.log(level, f"Entering {func_name}", args=args, kwargs=kwargs)
            else:
                logger.log(level, f"Entering {func_name}")

            try:
                result = func(*args, **kwargs)

                # Log exit
                if log_result:
                    logger.log(level, f"Exiting {func_name}", result=result)
                else:
                    logger.log(level, f"Exiting {func_name}")

                return result
            except Exception as e:
                logger.error(f"Error in {func_name}: {e}")
                raise

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__name__}"
            logger.log(level, f"Entering {func_name}")

            try:
                result = await func(*args, **kwargs)
                logger.log(level, f"Exiting {func_name}")
                return result
            except Exception as e:
                logger.error(f"Error in {func_name}: {e}")
                raise

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# =============================================================================
# Global Instance
# =============================================================================

slog = StructuredLogger()

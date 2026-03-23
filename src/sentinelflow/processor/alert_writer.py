# =============================================================================
# SentinelFlow - Alert Writer Service
# =============================================================================
"""
Centralized alert persistence service.

Writes alerts to:
1. PostgreSQL (primary store)
2. Kafka (event streaming)
3. WebSocket broadcast (optional)

Features:
- Idempotent writes (no duplicates)
- Async and sync interfaces
- Metrics collection
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger

from sentinelflow.contracts import (
    Alert,
    AlertCreate,
    FraudType,
    Severity,
    EventType,
)
from sentinelflow.contracts.alert import Evidence


class AlertWriter:
    """
    Service for persisting alerts to database and Kafka.

    Usage:
        writer = AlertWriter()
        writer.init_postgres()  # Call once at startup

        # In detector loop:
        alert = writer.write(alert_data)
    """

    def __init__(
        self,
        *,
        enable_postgres: bool = True,
        enable_kafka: bool = True,
        kafka_topic: str = "alerts",
        kafka_servers: str | None = None,
    ):
        """
        Initialize alert writer.

        Args:
            enable_postgres: Enable PostgreSQL writes
            enable_kafka: Enable Kafka publishing
            kafka_topic: Kafka topic for alerts
            kafka_servers: Kafka bootstrap servers
        """
        self._enable_postgres = enable_postgres
        self._enable_kafka = enable_kafka
        self._kafka_topic = kafka_topic
        self._kafka_servers = kafka_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )

        self._db_session_factory = None
        self._kafka_producer = None

        # Metrics
        self._alerts_written = 0
        self._alerts_deduplicated = 0
        self._errors = 0

        # Callbacks for WebSocket broadcast
        self._broadcast_callbacks: list[Callable[[Alert], None]] = []

    # =========================================================================
    # Initialization
    # =========================================================================

    def init_postgres(self) -> bool:
        """Initialize PostgreSQL connection and ensure tables exist."""
        if not self._enable_postgres:
            return True

        try:
            from sentinelflow.database.postgres import get_session_factory, init_db

            # Initialize tables if needed
            init_db(drop_all=False)

            # Get session factory
            self._db_session_factory = get_session_factory()

            logger.info("AlertWriter: PostgreSQL initialized")
            return True

        except Exception as e:
            logger.error(f"AlertWriter: PostgreSQL init failed: {e}")
            self._enable_postgres = False
            return False

    def init_kafka(self) -> bool:
        """Initialize Kafka producer."""
        if not self._enable_kafka:
            return True

        try:
            from confluent_kafka import Producer

            config = {
                "bootstrap.servers": self._kafka_servers,
                "client.id": "sentinelflow-alert-writer",
                "acks": "all",
                "retries": 3,
                "linger.ms": 5,
                "compression.type": "snappy",
            }

            self._kafka_producer = Producer(config)

            logger.info(f"AlertWriter: Kafka producer ready for {self._kafka_topic}")
            return True

        except Exception as e:
            logger.error(f"AlertWriter: Kafka init failed: {e}")
            self._enable_kafka = False
            return False

    def add_broadcast_callback(self, callback: Callable[[Alert], None]) -> None:
        """Add callback for WebSocket broadcast."""
        self._broadcast_callbacks.append(callback)

    # =========================================================================
    # Write Operations
    # =========================================================================

    def write(
        self,
        alert_data: AlertCreate | dict[str, Any],
        *,
        tx_data: dict[str, Any] | None = None,
    ) -> Alert | None:
        """
        Write alert to PostgreSQL and Kafka.

        Args:
            alert_data: Alert data to persist
            tx_data: Original transaction data (for context)

        Returns:
            Created Alert or None if deduplicated/failed
        """
        start_time = time.perf_counter()

        # Convert to AlertCreate if dict
        if isinstance(alert_data, dict):
            alert_data = AlertCreate(**alert_data)

        alert: Alert | None = None

        # Step 1: Write to PostgreSQL (idempotent)
        if self._enable_postgres and self._db_session_factory:
            try:
                alert = self._write_to_postgres(alert_data)
                if alert is None:
                    self._alerts_deduplicated += 1
                    return None
            except Exception as e:
                logger.error(f"AlertWriter: Postgres write failed: {e}")
                self._errors += 1
        else:
            # Create alert without DB (for testing/degraded mode)
            alert = Alert.from_create(alert_data)

        # Step 2: Publish to Kafka
        if self._enable_kafka and self._kafka_producer and alert:
            try:
                self._publish_to_kafka(alert)
            except Exception as e:
                logger.error(f"AlertWriter: Kafka publish failed: {e}")
                self._errors += 1

        # Step 3: Broadcast to WebSocket callbacks
        if alert:
            for callback in self._broadcast_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error(f"AlertWriter: Broadcast callback failed: {e}")

        elapsed = (time.perf_counter() - start_time) * 1000

        if alert:
            self._alerts_written += 1
            logger.info(
                f"Alert written: {alert.alert_id} | {alert.fraud_type} | "
                f"{alert.severity} | {elapsed:.1f}ms"
            )

        return alert

    def _write_to_postgres(self, alert_data: AlertCreate) -> Alert | None:
        """Write alert to PostgreSQL (idempotent)."""
        from sentinelflow.repository import AlertRepository

        session = self._db_session_factory()
        try:
            repo = AlertRepository(session)
            alert = repo.create(alert_data)
            session.commit()
            return alert
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _publish_to_kafka(self, alert: Alert) -> None:
        """Publish alert to Kafka."""
        from sentinelflow.contracts.alert import AlertKafkaMessage

        message = AlertKafkaMessage(alert=alert)
        value = json.dumps(message.to_kafka_dict()).encode("utf-8")
        key = alert.alert_id.encode("utf-8")

        self._kafka_producer.produce(
            topic=self._kafka_topic,
            key=key,
            value=value,
        )
        self._kafka_producer.poll(0)

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def write_batch(
        self,
        alerts: list[AlertCreate | dict[str, Any]],
    ) -> list[Alert]:
        """Write multiple alerts."""
        results = []
        for alert_data in alerts:
            alert = self.write(alert_data)
            if alert:
                results.append(alert)
        return results

    # =========================================================================
    # Metrics
    # =========================================================================

    @property
    def stats(self) -> dict[str, int]:
        """Get writer statistics."""
        return {
            "alerts_written": self._alerts_written,
            "alerts_deduplicated": self._alerts_deduplicated,
            "errors": self._errors,
        }

    # =========================================================================
    # Cleanup
    # =========================================================================

    def close(self) -> None:
        """Close connections."""
        if self._kafka_producer:
            self._kafka_producer.flush(timeout=10)
            self._kafka_producer = None

        logger.info("AlertWriter: Closed")


# =============================================================================
# Helper Functions
# =============================================================================


def create_alert_from_detection(
    fraud_type: FraudType | str,
    severity: Severity | str,
    confidence: float,
    tx_data: dict[str, Any],
    description: str,
    evidence: list[Evidence] | None = None,
    related_transactions: list[str] | None = None,
) -> AlertCreate:
    """
    Create AlertCreate from detection result.

    Convenience function for detector engines.
    """
    ft = fraud_type if isinstance(fraud_type, FraudType) else FraudType(fraud_type)
    sev = severity if isinstance(severity, Severity) else Severity(severity)

    return AlertCreate(
        fraud_type=ft,
        severity=sev,
        confidence=confidence,
        transaction_id=tx_data.get("transaction_id", ""),
        sender_iban=tx_data.get("sender_iban", ""),
        sender_name=tx_data.get("sender_name", ""),
        sender_city=tx_data.get("sender_city", ""),
        receiver_iban=tx_data.get("receiver_iban", ""),
        receiver_name=tx_data.get("receiver_name", ""),
        receiver_city=tx_data.get("receiver_city", ""),
        amount=float(tx_data.get("amount", 0)),
        currency=tx_data.get("currency", "TRY"),
        title=f"{ft.value.replace('_', ' ').title()} Detected",
        description=description,
        evidence=evidence or [],
        related_transactions=related_transactions or [],
    )

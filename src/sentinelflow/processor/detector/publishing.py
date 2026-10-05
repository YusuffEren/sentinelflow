# =============================================================================
# SentinelFlow - Alert Publishing & Display
# =============================================================================
"""Alert persistence (PostgreSQL), Kafka publishing and console display."""

from __future__ import annotations

import json

from confluent_kafka import KafkaException
from loguru import logger
from rich.panel import Panel

from .types import FraudAlert


class AlertPublishingMixin:
    """Publishes alerts to Kafka/PostgreSQL and renders them on the console."""

    def _publish_alert(self, alert: FraudAlert) -> None:
        """
        Publish a fraud alert to Kafka and persist to PostgreSQL.

        Args:
            alert: FraudAlert to publish
        """
        # Step 1: Persist to PostgreSQL (if enabled)
        if self._alert_writer:
            try:
                from sentinelflow.processor.alert_writer import create_alert_from_detection

                # Convert FraudAlert to AlertCreate
                alert_create = create_alert_from_detection(
                    fraud_type=(
                        alert.fraud_type.value
                        if hasattr(alert.fraud_type, "value")
                        else alert.fraud_type
                    ),
                    severity=alert.severity,
                    confidence=alert.confidence,
                    tx_data={
                        "transaction_id": alert.transaction_id,
                        "sender_iban": alert.sender_iban,
                        "sender_name": alert.sender_name,
                        "receiver_iban": alert.receiver_iban,
                        "receiver_name": alert.receiver_name,
                        "amount": alert.amount,
                    },
                    description=alert.description,
                )

                persisted = self._alert_writer.write(alert_create)
                if persisted:
                    alert.alert_id = persisted.alert_id  # Use DB-generated ID
                    logger.debug(f"Alert persisted to PostgreSQL: {alert.alert_id}")

            except Exception as e:
                logger.error(f"Failed to persist alert to PostgreSQL: {e}")
                self.stats.errors += 1

        # Step 2: Publish to Kafka
        if self._producer is None:
            logger.warning("Kafka producer not available")
            return

        try:
            value = json.dumps(alert.to_dict()).encode("utf-8")
            key = alert.alert_id.encode("utf-8")

            self._producer.produce(
                topic=self.topic_out,
                key=key,
                value=value,
            )
            self._producer.poll(0)

            logger.debug(f"Alert published to Kafka: {alert.alert_id}")

        except KafkaException as e:
            logger.error(f"Failed to publish alert to Kafka: {e}")
            self.stats.errors += 1

    def _print_alert(self, alert: FraudAlert) -> None:
        """Print a formatted fraud alert to console (RED warning!)."""
        severity_colors = {
            "low": "yellow",
            "medium": "orange1",
            "high": "red",
            "critical": "red bold",
        }
        color = severity_colors.get(alert.severity, "red")

        # Build alert panel
        content = f"""
[{color}][!] FRAUD DETECTED![/{color}]

[bold]Alert ID:[/bold] {alert.alert_id}
[bold]Type:[/bold] {alert.fraud_type.value.upper().replace('_', ' ')}
[bold]Severity:[/bold] [{color}]{alert.severity.upper()}[/{color}]
[bold]Confidence:[/bold] {alert.confidence * 100:.0f}%

[bold]Transaction:[/bold] {alert.transaction_id[:12]}...
[bold]Sender:[/bold] {alert.sender_name} ({alert.sender_iban[:12]}...)
[bold]Receiver:[/bold] {alert.receiver_name} ({alert.receiver_iban[:12]}...)
[bold]Amount:[/bold] {alert.amount:,.2f} TRY

[bold]Description:[/bold]
{alert.description}
"""

        self.console.print(
            Panel(
                content.strip(),
                title="[red bold][!] FRAUD ALERT [!][/red bold]",
                border_style="red",
            )
        )

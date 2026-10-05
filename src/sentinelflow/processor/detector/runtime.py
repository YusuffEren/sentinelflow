# =============================================================================
# SentinelFlow - Detector Runtime Loop
# =============================================================================
"""Kafka consumption loops, transaction processing and the live dashboard."""

from __future__ import annotations

import json
import signal
import time
from typing import Any

from confluent_kafka import KafkaError
from loguru import logger
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from .types import FraudAlert


class RuntimeMixin:
    """Main processing loop and real-time statistics dashboard."""

    def _process_transaction(self, tx_data: dict) -> None:
        """
        Process a single transaction through all fraud detection engines.

        This is the core processing logic that:
        1. Runs graph analysis (Neo4j) for circular rings
        2. Runs geo analysis (Redis) for impossible travel
        3. Runs NLP analysis for blacklisted keywords
        4. Publishes any detected fraud as alerts

        Args:
            tx_data: Transaction data dictionary
        """
        self.stats.transactions_processed += 1
        alerts: list[FraudAlert] = []

        # =====================================================================
        # ENGINE 1: Graph Analysis (Neo4j) - Circular Rings
        # =====================================================================
        ring_alert = self._check_circular_ring(tx_data)
        if ring_alert:
            alerts.append(ring_alert)

        # =====================================================================
        # ENGINE 2: Geo Analysis (Redis) - Impossible Travel
        # =====================================================================
        travel_alert = self._check_impossible_travel(tx_data)
        if travel_alert:
            alerts.append(travel_alert)

        # =====================================================================
        # ENGINE 3: NLP Analysis - Blacklist Keywords
        # =====================================================================
        blacklist_alert = self._check_blacklist_keywords(tx_data)
        if blacklist_alert:
            alerts.append(blacklist_alert)

        # =====================================================================
        # ENGINE 4: ML Ensemble Detection (IsolationForest+XGBoost+AutoEncoder)
        # =====================================================================
        ml_alert = self._check_ml_ensemble(tx_data)
        if ml_alert:
            alerts.append(ml_alert)

        # =====================================================================
        # Publish all detected fraud alerts
        # =====================================================================
        for alert in alerts:
            self.stats.fraud_detected += 1
            self._publish_alert(alert)
            self._print_alert(alert)

    def _create_stats_table(self) -> Table:
        """Create a rich table with detector statistics."""
        table = Table(title="[*] SentinelFlow Fraud Detector", expand=True)

        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green", justify="right")

        table.add_row("[>] Transactions Processed", f"{self.stats.transactions_processed:,}")
        table.add_row("[!] Fraud Detected", f"[red]{self.stats.fraud_detected:,}[/red]")
        table.add_row("   +-- Circular Rings", f"{self.stats.circular_rings:,}")
        table.add_row("   +-- Impossible Travel", f"{self.stats.impossible_travel:,}")
        table.add_row("   +-- Blacklist Hits", f"{self.stats.blacklist_hits:,}")
        table.add_row("   +-- AI Anomalies", f"[magenta]{self.stats.ai_anomalies:,}[/magenta]")
        table.add_row(
            "   +-- ML Ensemble",
            f"[bright_magenta]{self.stats.ml_ensemble_hits:,}[/bright_magenta]",
        )
        table.add_row("[x] Errors", f"{self.stats.errors:,}")
        table.add_row("[T] Uptime", f"{self.stats.uptime_seconds:.0f}s")
        table.add_row("[ML] Feature Engine", f"{self._feature_engine.accounts_tracked:,} accounts")
        table.add_row(
            "[ML] Ensemble Models",
            f"{self._ensemble.num_ready_models}/{self._ensemble.num_models} ready",
        )

        # PostgreSQL status
        pg_status = (
            "[green]connected[/green]" if self._alert_writer else "[yellow]disabled[/yellow]"
        )
        table.add_row("[DB] PostgreSQL", pg_status)

        fraud_rate = self.stats.fraud_rate * 100
        rate_color = "green" if fraud_rate < 5 else "yellow" if fraud_rate < 10 else "red"
        table.add_row("[~] Fraud Rate", f"[{rate_color}]{fraud_rate:.2f}%[/{rate_color}]")

        return table

    def start(self, show_dashboard: bool = True) -> None:
        """
        Start the fraud detector service.

        This runs the main processing loop that:
        1. Consumes transactions from Kafka
        2. Runs fraud detection
        3. Publishes alerts

        Args:
            show_dashboard: Whether to show real-time stats dashboard
        """
        self._running = True

        # Handle graceful shutdown
        def signal_handler(sig: int, frame: Any) -> None:
            logger.info("Shutdown signal received...")
            self._running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Initialize connections
        self.console.print(
            Panel.fit(
                "[bold blue]SentinelFlow[/bold blue]\n"
                "[dim]Real-Time Fraud Detection System[/dim]\n"
                "[yellow]Fraud Detector Service[/yellow]",
                border_style="blue",
            )
        )

        logger.info("Initializing connections...")
        self._init_kafka_consumer()
        self._init_kafka_producer()
        self._init_graph_engine()
        self._init_redis_client()
        self._init_alert_writer()

        logger.info("Fraud Detector Service started!")
        logger.info(f"Consuming from: {self.topic_in}")
        logger.info(f"Publishing alerts to: {self.topic_out}")

        # Main processing loop
        try:
            if show_dashboard:
                self._run_with_dashboard()
            else:
                self._run_without_dashboard()
        except Exception as e:
            logger.exception(f"Fatal error in detector: {e}")
        finally:
            self._close_connections()
            self.console.print("\n[green][+] Detector service stopped gracefully[/green]")

    def _run_with_dashboard(self) -> None:
        """Run with real-time stats dashboard."""
        with Live(console=self.console, refresh_per_second=1) as live:
            while self._running:
                # Update dashboard
                live.update(self._create_stats_table())

                # Poll for messages
                msg = self._consumer.poll(timeout=0.1)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        self.stats.errors += 1
                        continue

                # Parse and process message
                try:
                    tx_data = json.loads(msg.value().decode("utf-8"))
                    self._process_transaction(tx_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    self.stats.errors += 1
                except Exception as e:
                    logger.error(f"Processing error: {e}")
                    self.stats.errors += 1

    def _run_without_dashboard(self) -> None:
        """Run without dashboard (log-based output)."""
        last_log_time = time.time()

        while self._running:
            # Poll for messages
            msg = self._consumer.poll(timeout=0.1)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    self.stats.errors += 1
                    continue

            # Parse and process message
            try:
                tx_data = json.loads(msg.value().decode("utf-8"))
                self._process_transaction(tx_data)

                # Log stats periodically
                if time.time() - last_log_time > 10:
                    logger.info(
                        f"Stats: {self.stats.transactions_processed} processed, "
                        f"{self.stats.fraud_detected} fraud detected"
                    )
                    last_log_time = time.time()

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                self.stats.errors += 1
            except Exception as e:
                logger.error(f"Processing error: {e}")
                self.stats.errors += 1

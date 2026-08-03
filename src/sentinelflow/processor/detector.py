# =============================================================================
# SentinelFlow - Fraud Detector Service (The Brain)
# =============================================================================
"""
The core fraud detection service that processes transactions in real-time.

This is the "Brain" of SentinelFlow, orchestrating multiple fraud detection
engines:

1. **Graph Analysis (Neo4j)**: Detects circular transaction rings (money laundering)
   Example: A → B → C → A pattern where money flows in a circle

2. **Impossible Travel (Redis)**: Detects physically impossible travel speeds
   Example: Transaction in İstanbul, then 10 minutes later in Berlin

3. **NLP Blacklist**: Detects suspicious keywords in transaction descriptions
   Example: "bahis", "kumar", "crypto" in the description field

Architecture:
    [Kafka: transactions] → [Detector Service] → [Kafka: alerts]
                                    ↓
                            [Neo4j + Redis]

Usage:
    # Run the detector service
    python -m sentinelflow.processor.detector

    # Or with options
    python -m sentinelflow.processor.detector --consumer-group my-group --verbose
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

import numpy as np

# ML Pipeline imports
from sentinelflow.ml.feature_engine import TransactionFeatureEngine
from sentinelflow.ml.models import IsolationForestModel, XGBoostFraudModel, AutoEncoderModel
from sentinelflow.ml.ensemble import EnsembleVoter
from sentinelflow.ml.explainer import FraudExplainer

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from loguru import logger
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from sentinelflow.config import get_settings
from sentinelflow.processor.graph_engine import GraphEngine
from sentinelflow.processor.redis_geo import (
    RedisGeoClient,
    get_city_coordinates,
    CITY_COORDINATES,
)


# =============================================================================
# Constants & Enums
# =============================================================================


class FraudType(str, Enum):
    """Types of fraud detected by the system."""

    CIRCULAR_RING = "circular_ring"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    BLACKLIST_KEYWORD = "blacklist_keyword"
    MULE_ACCOUNT = "mule_account"
    AI_DETECTED_ANOMALY = "ai_detected_anomaly"
    ML_ENSEMBLE = "ml_ensemble"


# Blacklisted keywords for NLP check (Turkish + English)
BLACKLIST_KEYWORDS: list[str] = [
    # Gambling / Betting
    "bahis",
    "casino",
    "kumar",
    "poker",
    "rulet",
    "slot",
    "bet365",
    "betting",
    # Cryptocurrency (suspicious transfers)
    "kripto",
    "bitcoin",
    "btc",
    "ethereum",
    "usdt",
    "binance",
    "crypto",
    # Offshore / Anonymous
    "offshore",
    "anonim",
    "anonymous",
    "gizli",
    "secret",
    # Urgency patterns (social engineering)
    "acil",
    "urgent",
    "hemen",
    "immediately",
]


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FraudAlert:
    """Represents a detected fraud case."""

    alert_id: str = field(default_factory=lambda: f"ALERT-{uuid4().hex[:12].upper()}")
    fraud_type: FraudType = FraudType.CIRCULAR_RING
    severity: str = "high"  # low, medium, high, critical
    confidence: float = 0.9

    # Transaction details
    transaction_id: str = ""
    sender_iban: str = ""
    sender_name: str = ""
    receiver_iban: str = ""
    receiver_name: str = ""
    amount: float = 0.0

    # Fraud details
    description: str = ""
    evidence: dict = field(default_factory=dict)
    related_transactions: list[str] = field(default_factory=list)

    # Metadata
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    detector_version: str = "1.0.0"

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return {
            "alert_id": self.alert_id,
            "fraud_type": (
                self.fraud_type.value if isinstance(self.fraud_type, FraudType) else self.fraud_type
            ),
            "severity": self.severity,
            "confidence": self.confidence,
            "transaction_id": self.transaction_id,
            "sender_iban": self.sender_iban,
            "sender_name": self.sender_name,
            "receiver_iban": self.receiver_iban,
            "receiver_name": self.receiver_name,
            "amount": self.amount,
            "description": self.description,
            "evidence": self.evidence,
            "related_transactions": self.related_transactions,
            "detected_at": self.detected_at,
            "detector_version": self.detector_version,
        }


@dataclass
class DetectorStats:
    """Statistics for the detector service."""

    transactions_processed: int = 0
    fraud_detected: int = 0
    circular_rings: int = 0
    impossible_travel: int = 0
    blacklist_hits: int = 0
    ai_anomalies: int = 0
    ml_ensemble_hits: int = 0
    errors: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()

    @property
    def fraud_rate(self) -> float:
        if self.transactions_processed == 0:
            return 0.0
        return self.fraud_detected / self.transactions_processed


# =============================================================================
# Fraud Detector Service
# =============================================================================


class FraudDetectorService:
    """
    The main fraud detection service.

    Consumes transactions from Kafka, runs multiple fraud detection engines,
    and publishes alerts to a separate Kafka topic.

    Fraud Detection Engines:
    1. Graph Analysis (Neo4j) - Circular transaction rings
    2. Geo Analysis (Redis) - Impossible travel detection
    3. NLP Analysis - Blacklisted keyword detection

    Example:
        detector = FraudDetectorService()
        detector.start()  # Runs until interrupted
    """

    def __init__(
        self,
        kafka_servers: str | None = None,
        kafka_topic_in: str = "transactions",
        kafka_topic_out: str = "alerts",
        consumer_group: str = "sentinelflow-detectors",
    ) -> None:
        """
        Initialize the fraud detector service.

        Args:
            kafka_servers: Kafka bootstrap servers
            kafka_topic_in: Topic to consume transactions from
            kafka_topic_out: Topic to publish alerts to
            consumer_group: Kafka consumer group ID
        """
        self.settings = get_settings()

        # Kafka configuration
        self.kafka_servers = kafka_servers or self.settings.kafka.bootstrap_servers
        self.topic_in = kafka_topic_in
        self.topic_out = kafka_topic_out
        self.consumer_group = consumer_group

        # Statistics
        self.stats = DetectorStats()

        # Control flags
        self._running = False
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None

        # Detection engines (initialized lazily)
        self._graph_engine: GraphEngine | None = None
        self._redis_client: RedisGeoClient | None = None

        # ================================================================
        # ML Ensemble Pipeline (Upgraded from single IsolationForest)
        # ================================================================
        self._feature_engine = TransactionFeatureEngine(history_window_size=500)

        # Initialize models
        self._isolation_forest_model = IsolationForestModel(
            contamination=0.05,
            n_estimators=200,
            min_samples_to_train=100,
            retrain_interval=500,
        )
        self._xgboost_model = XGBoostFraudModel(
            model_path="models/xgboost_fraud.json",
            n_estimators=300,
            max_depth=6,
        )
        self._autoencoder_model = AutoEncoderModel(
            input_dim=21,
            encoding_dim=8,
            model_path="models/autoencoder.pt",
        )

        # Ensemble voter
        self._ensemble = EnsembleVoter(threshold=0.65)
        self._ensemble.add_model(self._isolation_forest_model, weight=0.3)
        self._ensemble.add_model(self._xgboost_model, weight=0.5)
        self._ensemble.add_model(self._autoencoder_model, weight=0.2)

        # Explainability
        self._explainer = FraudExplainer(
            feature_names=TransactionFeatureEngine.get_feature_names(),
            top_n=5,
        )

        # Legacy compatibility: keep amount buffer for basic check
        self._amount_buffer: deque[float] = deque(maxlen=1000)
        self._anomaly_amount_threshold: float = 50000.0

        # Console for rich output
        self.console = Console()

        # Alert writer for PostgreSQL persistence
        self._alert_writer = None
        self._enable_postgres = True

        logger.info("FraudDetectorService initialized with ML Ensemble Pipeline")

    # =========================================================================
    # Connection Management
    # =========================================================================

    def _init_kafka_consumer(self) -> None:
        """Initialize Kafka consumer."""
        config = {
            "bootstrap.servers": self.kafka_servers,
            "group.id": self.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }

        self._consumer = Consumer(config)
        self._consumer.subscribe([self.topic_in])
        logger.info(f"Kafka consumer subscribed to: {self.topic_in}")

    def _init_kafka_producer(self) -> None:
        """Initialize Kafka producer for alerts."""
        config = {
            "bootstrap.servers": self.kafka_servers,
            "client.id": "sentinelflow-detector",
            "acks": "all",
            "retries": 3,
            "linger.ms": 5,
            "compression.type": "snappy",
        }

        self._producer = Producer(config)
        logger.info(f"Kafka producer ready for: {self.topic_out}")

    def _init_graph_engine(self) -> None:
        """Initialize Neo4j graph engine."""
        try:
            self._graph_engine = GraphEngine()
            self._graph_engine.setup_constraints()
            logger.info("Neo4j GraphEngine connected")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            logger.warning("Graph-based fraud detection will be disabled!")
            self._graph_engine = None

    def _init_redis_client(self) -> None:
        """Initialize Redis geo client."""
        try:
            self._redis_client = RedisGeoClient()
            logger.info("Redis GeoClient connected")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.warning("Impossible travel detection will be disabled!")
            self._redis_client = None

    def _init_alert_writer(self) -> None:
        """Initialize alert writer for PostgreSQL persistence."""
        if not self._enable_postgres:
            logger.info("PostgreSQL persistence disabled")
            return

        try:
            from sentinelflow.processor.alert_writer import AlertWriter

            self._alert_writer = AlertWriter(
                enable_postgres=True,
                enable_kafka=False,  # We handle Kafka separately
                kafka_topic=self.topic_out,
                kafka_servers=self.kafka_servers,
            )

            if self._alert_writer.init_postgres():
                logger.info("Alert writer initialized with PostgreSQL")
            else:
                logger.warning(
                    "Alert writer PostgreSQL init failed - continuing without persistence"
                )
                self._alert_writer = None

        except Exception as e:
            logger.error(f"Failed to initialize alert writer: {e}")
            logger.warning("Alert persistence will be disabled!")
            self._alert_writer = None

    def _close_connections(self) -> None:
        """Close all connections gracefully."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None

        if self._producer:
            self._producer.flush(timeout=10)
            self._producer = None

        if self._graph_engine:
            self._graph_engine.close()
            self._graph_engine = None

        if self._redis_client:
            self._redis_client.close()
            self._redis_client = None

        logger.info("All connections closed")

    # =========================================================================
    # Fraud Detection Engines
    # =========================================================================

    def _check_circular_ring(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 1: Check for circular transaction rings using Neo4j.

        This detects patterns like: A → B → C → A
        Where money flows in a circle, potentially indicating money laundering.

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if ring detected, None otherwise
        """
        if self._graph_engine is None:
            return None

        try:
            # Step 1: Add transaction to graph
            self._graph_engine.add_transaction(tx_data)

            # Step 2: Check for rings starting from this sender
            rings = self._graph_engine.detect_fraud_rings(
                sender_iban=tx_data.get("sender_iban"),
                min_hops=3,
                max_hops=5,
            )

            if rings:
                ring = rings[0]  # Take the first detected ring

                self.stats.circular_rings += 1

                return FraudAlert(
                    fraud_type=FraudType.CIRCULAR_RING,
                    severity="critical",
                    confidence=0.95,
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=tx_data.get("sender_iban", ""),
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=tx_data.get("amount", 0),
                    description=f"Circular transaction ring detected: {' → '.join(ring['path'][:4])}...",
                    evidence={
                        "ring_id": ring["ring_id"],
                        "ring_path": ring["path"],
                        "total_amount": ring["total_amount"],
                        "transaction_count": ring["transaction_count"],
                    },
                )

        except Exception as e:
            logger.error(f"Graph analysis error: {e}")
            self.stats.errors += 1

        return None

    def _check_impossible_travel(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 2: Check for impossible travel using Redis.

        Detects when a user makes transactions from locations that are
        physically impossible to travel between in the given time.

        Example: İstanbul at 12:00, Berlin at 12:10 (1,500 km in 10 min = 9,000 km/h!)

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if impossible travel detected, None otherwise
        """
        if self._redis_client is None:
            return None

        try:
            sender_iban = tx_data.get("sender_iban", "")
            sender_city = tx_data.get("sender_city", "")

            # Get coordinates for the city
            coords = get_city_coordinates(sender_city)
            if coords is None:
                # Unknown city, cannot check travel
                logger.debug(f"Unknown city: {sender_city}")
                return None

            latitude, longitude = coords

            # Parse timestamp
            timestamp_str = tx_data.get("timestamp", "")
            try:
                if "T" in timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                else:
                    timestamp = datetime.now(timezone.utc)
            except ValueError:
                timestamp = datetime.now(timezone.utc)

            # Check for impossible travel
            is_impossible, details = self._redis_client.check_impossible_travel(
                iban=sender_iban,
                new_city=sender_city,
                new_latitude=latitude,
                new_longitude=longitude,
                new_timestamp=timestamp,
                max_speed_kmh=self.settings.fraud.max_travel_speed_kmh,
            )

            # Update location for future checks
            self._redis_client.update_user_location(
                iban=sender_iban,
                city=sender_city,
                latitude=latitude,
                longitude=longitude,
                timestamp=timestamp,
                transaction_id=tx_data.get("transaction_id"),
            )

            if is_impossible and details:
                self.stats.impossible_travel += 1

                return FraudAlert(
                    fraud_type=FraudType.IMPOSSIBLE_TRAVEL,
                    severity="high",
                    confidence=0.90,
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=sender_iban,
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=tx_data.get("amount", 0),
                    description=(
                        f"Impossible travel detected: {details['from_city']} → {details['to_city']} "
                        f"({details['distance_km']} km in {details['time_elapsed_minutes']} min = "
                        f"{details['required_speed_kmh']} km/h)"
                    ),
                    evidence=details,
                )

        except Exception as e:
            logger.error(f"Geo analysis error: {e}")
            self.stats.errors += 1

        return None

    def _check_blacklist_keywords(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 3: Check for blacklisted keywords in transaction descriptions.

        Flags transactions with suspicious terms like:
        - Gambling: "bahis", "kumar", "casino"
        - Crypto: "bitcoin", "kripto", "usdt"
        - Anonymity: "offshore", "anonim", "gizli"

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if blacklist hit detected, None otherwise
        """
        description = tx_data.get("description", "").lower()

        if not description:
            return None

        # Check for any blacklisted keyword
        found_keywords = [kw for kw in BLACKLIST_KEYWORDS if kw in description]

        if found_keywords:
            self.stats.blacklist_hits += 1

            # Determine severity based on keyword type
            critical_keywords = ["casino", "kumar", "offshore", "anonymous"]
            is_critical = any(kw in critical_keywords for kw in found_keywords)

            return FraudAlert(
                fraud_type=FraudType.BLACKLIST_KEYWORD,
                severity="critical" if is_critical else "medium",
                confidence=0.85,
                transaction_id=tx_data.get("transaction_id", ""),
                sender_iban=tx_data.get("sender_iban", ""),
                sender_name=tx_data.get("sender_name", ""),
                receiver_iban=tx_data.get("receiver_iban", ""),
                receiver_name=tx_data.get("receiver_name", ""),
                amount=tx_data.get("amount", 0),
                description=f"Suspicious keywords detected in description: {', '.join(found_keywords)}",
                evidence={
                    "keywords_found": found_keywords,
                    "original_description": tx_data.get("description", ""),
                },
            )

        return None

    def _check_ml_ensemble(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 4: ML Ensemble Fraud Detection.

        Uses multiple ML models (IsolationForest, XGBoost, AutoEncoder) with
        weighted voting to detect anomalous transactions. Provides SHAP-based
        explanations for flagged transactions.

        Pipeline:
        1. Feature Engineering → 21 features from raw transaction
        2. Multi-Model Prediction → Ensemble weighted vote
        3. Explainability → SHAP/heuristic top reasons

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if ensemble flags fraud, None otherwise
        """
        try:
            # Step 1: Extract features
            features_dict = self._feature_engine.extract(tx_data)
            features_vector = np.array(
                [
                    features_dict.get(name, 0.0)
                    for name in TransactionFeatureEngine.get_feature_names()
                ],
                dtype=np.float64,
            )

            # Step 2: Feed to IsolationForest for online learning
            self._isolation_forest_model.add_sample_and_maybe_retrain(features_vector)

            # Step 3: Ensemble prediction
            prediction = self._ensemble.predict(features_vector)

            # Also keep legacy amount buffer for backward compat
            amount = float(tx_data.get("amount", 0.0))
            self._amount_buffer.append(amount)

            if prediction.is_fraud:
                self.stats.ml_ensemble_hits += 1
                self.stats.ai_anomalies += 1

                # Step 4: Generate explanation
                explanation = self._explainer.explain(
                    features=features_vector,
                    feature_values=features_dict,
                )

                # Determine severity based on score
                if prediction.final_score >= 0.85:
                    severity = "critical"
                elif prediction.final_score >= 0.75:
                    severity = "high"
                else:
                    severity = "medium"

                return FraudAlert(
                    fraud_type=FraudType.ML_ENSEMBLE,
                    severity=severity,
                    confidence=min(0.99, prediction.final_score),
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=tx_data.get("sender_iban", ""),
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=amount,
                    description=(
                        f"ML Ensemble detected fraud (score: {prediction.final_score:.2f}): "
                        f"{explanation.summary()}"
                    ),
                    evidence={
                        **prediction.to_dict(),
                        "xai_explanation": explanation.to_dict(),
                        "features": {k: round(v, 4) for k, v in features_dict.items()},
                    },
                )

        except Exception as e:
            logger.error(f"ML ensemble error: {e}")
            self.stats.errors += 1

        return None

    # =========================================================================
    # Alert Publishing
    # =========================================================================

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
                from sentinelflow.contracts import FraudType, Severity

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

    # =========================================================================
    # Main Processing Loop
    # =========================================================================

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


# =============================================================================
# CLI Interface
# =============================================================================


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="SentinelFlow Fraud Detector Service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--kafka-servers",
        "-k",
        type=str,
        default=None,
        help="Kafka bootstrap servers",
    )

    parser.add_argument(
        "--topic-in",
        "-i",
        type=str,
        default="transactions",
        help="Input topic for transactions",
    )

    parser.add_argument(
        "--topic-out",
        "-o",
        type=str,
        default="alerts",
        help="Output topic for fraud alerts",
    )

    parser.add_argument(
        "--consumer-group",
        "-g",
        type=str,
        default="sentinelflow-detectors",
        help="Kafka consumer group ID",
    )

    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable real-time dashboard",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    settings = get_settings()

    # Configure logging
    log_level = "DEBUG" if args.verbose else settings.log_level
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )

    # Initialize and start detector
    kafka_servers = args.kafka_servers or settings.kafka.bootstrap_servers

    detector = FraudDetectorService(
        kafka_servers=kafka_servers,
        kafka_topic_in=args.topic_in,
        kafka_topic_out=args.topic_out,
        consumer_group=args.consumer_group,
    )

    detector.start(show_dashboard=not args.no_dashboard)


if __name__ == "__main__":
    main()

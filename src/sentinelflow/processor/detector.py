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
    [Kafka: transactions] → [Detector Service] → [Kafka: fraud_alerts]
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
from datetime import datetime
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

import numpy as np
from sklearn.ensemble import IsolationForest

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


# Blacklisted keywords for NLP check (Turkish + English)
BLACKLIST_KEYWORDS: list[str] = [
    # Gambling / Betting
    "bahis", "casino", "kumar", "poker", "rulet", "slot", "bet365", "betting",
    # Cryptocurrency (suspicious transfers)
    "kripto", "bitcoin", "btc", "ethereum", "usdt", "binance", "crypto",
    # Offshore / Anonymous
    "offshore", "anonim", "anonymous", "gizli", "secret",
    # Urgency patterns (social engineering)
    "acil", "urgent", "hemen", "immediately",
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
    detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    detector_version: str = "1.0.0"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return {
            "alert_id": self.alert_id,
            "fraud_type": self.fraud_type.value if isinstance(self.fraud_type, FraudType) else self.fraud_type,
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
    errors: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def uptime_seconds(self) -> float:
        return (datetime.utcnow() - self.start_time).total_seconds()
    
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
        kafka_topic_out: str = "fraud_alerts",
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
        
        # ML-based anomaly detection (Upgrade 1: Unsupervised Anomaly Detection)
        self._amount_buffer: deque[float] = deque(maxlen=1000)  # Sliding window of last 1000 amounts
        self._isolation_forest: IsolationForest | None = None
        self._ml_min_samples: int = 100  # Minimum samples before training ML model
        self._anomaly_amount_threshold: float = 50000.0  # Amount threshold for AI anomaly (TL)
        
        # Console for rich output
        self.console = Console()
        
        logger.info("FraudDetectorService initialized")
    
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
                    timestamp = datetime.utcnow()
            except ValueError:
                timestamp = datetime.utcnow()
            
            # Check for impossible travel
            is_impossible, details = self._redis_client.check_impossible_travel(
                iban=sender_iban,
                new_city=sender_city,
                new_latitude=latitude,
                new_longitude=longitude,
                new_timestamp=timestamp,
                max_speed_kmh=900.0,  # Commercial jet speed
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
    
    def _check_ai_anomaly(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 4: AI-based Anomaly Detection using Isolation Forest.
        
        This uses unsupervised machine learning to detect unusual transaction
        amounts that deviate significantly from the historical distribution.
        
        How it works:
        1. Maintains a sliding window of the last 1000 transaction amounts
        2. Trains an IsolationForest model on this buffer
        3. Predicts if the current transaction is an anomaly (-1) or normal (1)
        4. Flags as fraud if anomaly AND amount > 50,000 TL threshold
        
        Args:
            tx_data: Transaction data dictionary
        
        Returns:
            FraudAlert if AI detects anomaly, None otherwise
        """
        try:
            amount = tx_data.get("amount", 0.0)
            
            # Step 1: Add the new transaction amount to the sliding window buffer
            self._amount_buffer.append(amount)
            
            # Step 2: Check if we have enough data to train the model
            if len(self._amount_buffer) < self._ml_min_samples:
                logger.debug(f"ML buffer collecting: {len(self._amount_buffer)}/{self._ml_min_samples}")
                return None
            
            # Step 3: Initialize and train IsolationForest on the buffer
            # Reshape buffer data for sklearn (needs 2D array)
            buffer_array = np.array(list(self._amount_buffer)).reshape(-1, 1)
            
            # Create and fit the model
            self._isolation_forest = IsolationForest(
                contamination=0.05,  # Expect ~5% anomalies
                random_state=42,
                n_estimators=100,
                max_samples='auto',
                n_jobs=-1,  # Use all CPU cores
            )
            self._isolation_forest.fit(buffer_array)
            
            # Step 4: Predict if current transaction is anomaly
            current_amount = np.array([[amount]])
            prediction = self._isolation_forest.predict(current_amount)[0]
            anomaly_score = self._isolation_forest.decision_function(current_amount)[0]
            
            # Step 5: Flag as fraud if anomaly (-1) AND amount exceeds threshold
            if prediction == -1 and amount > self._anomaly_amount_threshold:
                self.stats.ai_anomalies += 1
                
                return FraudAlert(
                    fraud_type=FraudType.AI_DETECTED_ANOMALY,
                    severity="high",
                    confidence=min(0.95, abs(anomaly_score)),  # Use anomaly score as confidence
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=tx_data.get("sender_iban", ""),
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=amount,
                    description=(
                        f"AI-detected anomaly: Amount {amount:,.2f} TL is statistically unusual "
                        f"(Isolation Forest score: {anomaly_score:.4f})"
                    ),
                    evidence={
                        "anomaly_score": float(anomaly_score),
                        "prediction": int(prediction),
                        "buffer_size": len(self._amount_buffer),
                        "buffer_mean": float(np.mean(buffer_array)),
                        "buffer_std": float(np.std(buffer_array)),
                        "amount_threshold": self._anomaly_amount_threshold,
                    },
                )
                
        except Exception as e:
            logger.error(f"ML anomaly detection error: {e}")
            self.stats.errors += 1
        
        return None
    
    # =========================================================================
    # Alert Publishing
    # =========================================================================
    
    def _publish_alert(self, alert: FraudAlert) -> None:
        """
        Publish a fraud alert to Kafka.
        
        Args:
            alert: FraudAlert to publish
        """
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
            
            logger.debug(f"Alert published: {alert.alert_id}")
            
        except KafkaException as e:
            logger.error(f"Failed to publish alert: {e}")
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
        
        self.console.print(Panel(
            content.strip(),
            title="[red bold][!] FRAUD ALERT [!][/red bold]",
            border_style="red",
        ))
    
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
        # ENGINE 4: AI Anomaly Detection (Isolation Forest)
        # =====================================================================
        ai_alert = self._check_ai_anomaly(tx_data)
        if ai_alert:
            alerts.append(ai_alert)
        
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
        table.add_row("[x] Errors", f"{self.stats.errors:,}")
        table.add_row("[T] Uptime", f"{self.stats.uptime_seconds:.0f}s")
        table.add_row("[ML] Buffer Size", f"{len(self._amount_buffer):,}/1000")
        
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
        self.console.print(Panel.fit(
            "[bold blue]SentinelFlow[/bold blue]\n"
            "[dim]Real-Time Fraud Detection System[/dim]\n"
            "[yellow]Fraud Detector Service[/yellow]",
            border_style="blue",
        ))
        
        logger.info("Initializing connections...")
        self._init_kafka_consumer()
        self._init_kafka_producer()
        self._init_graph_engine()
        self._init_redis_client()
        
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
        "--kafka-servers", "-k",
        type=str,
        default=None,
        help="Kafka bootstrap servers",
    )
    
    parser.add_argument(
        "--topic-in", "-i",
        type=str,
        default="transactions",
        help="Input topic for transactions",
    )
    
    parser.add_argument(
        "--topic-out", "-o",
        type=str,
        default="fraud_alerts",
        help="Output topic for fraud alerts",
    )
    
    parser.add_argument(
        "--consumer-group", "-g",
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
        "--verbose", "-v",
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

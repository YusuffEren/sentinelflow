# =============================================================================
# SentinelFlow - Fraud Detector Service (The Brain)
# =============================================================================
"""
The core fraud detection service that processes transactions in real-time.

Consumes transactions from Kafka, orchestrates the detection engines from the
sibling modules (graph, geo, blacklist, ML ensemble) and publishes alerts.

Architecture:
    [Kafka: transactions] → [Detector Service] → [Kafka: alerts]
                                    ↓
                            [Neo4j + Redis + PostgreSQL]
"""

from __future__ import annotations

from collections import deque

from confluent_kafka import Consumer, Producer
from loguru import logger
from rich.console import Console

from sentinelflow.config import get_settings
from sentinelflow.ml.ensemble import EnsembleVoter
from sentinelflow.ml.explainer import FraudExplainer

# ML Pipeline imports
from sentinelflow.ml.feature_engine import TransactionFeatureEngine
from sentinelflow.ml.models import AutoEncoderModel, IsolationForestModel, XGBoostFraudModel
from sentinelflow.processor.graph_engine import GraphEngine
from sentinelflow.processor.redis_geo import RedisGeoClient

from .checks import DetectionChecksMixin
from .publishing import AlertPublishingMixin
from .runtime import RuntimeMixin
from .types import DetectorStats


class FraudDetectorService(RuntimeMixin, AlertPublishingMixin, DetectionChecksMixin):
    """
    The main fraud detection service.

    Consumes transactions from Kafka, runs multiple fraud detection engines,
    and publishes alerts to a separate Kafka topic.

    Fraud Detection Engines:
    1. Graph Analysis (Neo4j) - Circular transaction rings
    2. Geo Analysis (Redis) - Impossible travel detection
    3. NLP Analysis - Blacklisted keyword detection
    4. ML Ensemble (IsolationForest + XGBoost + AutoEncoder)

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

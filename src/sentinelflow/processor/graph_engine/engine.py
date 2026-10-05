# =============================================================================
# SentinelFlow - Neo4j Graph Engine
# =============================================================================
"""GraphEngine: connection management, ingestion and ring detection."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger("sentinelflow.graph_engine")

try:
    from neo4j import Driver, GraphDatabase, Session
    from neo4j.exceptions import Neo4jError, ServiceUnavailable

    HAS_NEO4J = True
except ImportError:
    Driver = GraphDatabase = Session = Any  # type: ignore
    Neo4jError = ServiceUnavailable = Exception  # type: ignore
    HAS_NEO4J = False

from sentinelflow.config import get_settings

from .cypher import (
    ADD_TRANSACTION_QUERY,
    ADD_TRANSACTIONS_BATCH_QUERY,
    CLEAR_ALL_QUERY,
    CONSTRAINT_STATEMENTS,
    GRAPH_STATS_QUERY,
    INDEX_STATEMENTS,
    USER_TRANSACTIONS_QUERY,
    all_rings_query,
    fraud_rings_query,
    rings_without_apoc_query,
)
from .types import FraudRing, TransactionData


class GraphEngine:
    """
    Neo4j Graph Engine for fraud detection.

    Manages connections to Neo4j and provides methods for:
    - Transaction ingestion into the graph
    - Circular fraud ring detection
    - Path analysis for money laundering patterns

    Attributes:
        driver: Neo4j driver instance
        database: Target database name

    Example:
        >>> engine = GraphEngine()
        >>> engine.setup_constraints()
        >>> engine.add_transaction({
        ...     "sender_iban": "TR001",
        ...     "receiver_iban": "TR002",
        ...     "amount": 1000.0,
        ...     "timestamp": datetime.now().isoformat()
        ... })
        >>> rings = engine.detect_fraud_rings(sender_iban="TR001")
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str = "neo4j",
    ) -> None:
        """
        Initialize GraphEngine with Neo4j connection.

        Args:
            uri: Neo4j Bolt URI (default: from env/config)
            user: Neo4j username (default: from env/config)
            password: Neo4j password (default: from env/config)
            database: Target database name

        Raises:
            ServiceUnavailable: If Neo4j is not reachable
        """
        settings = get_settings()

        # Read from environment first, then config, then defaults
        self._uri = uri or os.getenv("NEO4J_URI") or settings.neo4j.uri
        self._user = user or os.getenv("NEO4J_USER") or settings.neo4j.user
        self._password = password or os.getenv("NEO4J_PASSWORD") or settings.neo4j.password
        self.database = database

        if not self._password:
            raise ValueError(
                "Neo4j password is not configured. Set the NEO4J_PASSWORD "
                "environment variable (see .env.example)."
            )

        # Initialize driver with connection pooling
        self._driver: Driver | None = None
        self._connect()

        logger.info(f"GraphEngine initialized: {self._uri}")

    def _connect(self) -> None:
        """Establish connection to Neo4j."""
        if not HAS_NEO4J:
            raise RuntimeError(
                "The 'neo4j' driver package is not installed. "
                "Install project dependencies first (pip install -e .)."
            )
        try:
            self._driver = GraphDatabase.driver(
                self._uri,
                auth=(self._user, self._password),
                max_connection_lifetime=3600,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60,
            )
            # Verify connectivity
            self._driver.verify_connectivity()
            logger.debug("Neo4j connection verified")
        except ServiceUnavailable as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        """Get a session context manager."""
        if self._driver is None:
            self._connect()

        session = self._driver.session(database=self.database)
        try:
            yield session
        finally:
            session.close()

    def close(self) -> None:
        """Close the driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.debug("Neo4j connection closed")

    def query(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read-only Cypher sorgusu çalıştırır ve sonuçları dict listesi olarak döner.

        Güvenli bağlantı yönetimi için _session() context manager'ını kullanır;
        böylece session her zaman kapatılır ve connection pool sızmaz.

        Args:
            cypher: Cypher sorgusu (çağıranın read-only olduğunu garanti etmesi beklenir)
            params: Sorgu parametreleri — Cypher injection önlemek için her zaman
                parametrize edilmiş sorgu kullanılmalı

        Returns:
            Her kayıt bir dict olarak liste halinde döner; boş sonuçta []
        """
        with self._session() as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]

    def __enter__(self) -> GraphEngine:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # Schema Setup
    # =========================================================================

    def setup_constraints(self) -> None:
        """
        Create necessary constraints and indexes for performance.

        Creates:
        - Unique constraint on User.iban
        - Index on SENT relationship properties
        """
        with self._session() as session:
            for constraint in CONSTRAINT_STATEMENTS:
                try:
                    session.run(constraint.strip())
                    logger.debug("Constraint created/verified")
                except Neo4jError as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Constraint error: {e}")

            for index in INDEX_STATEMENTS:
                try:
                    session.run(index.strip())
                    logger.debug("Index created/verified")
                except Neo4jError as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Index error: {e}")

        logger.info("Schema constraints and indexes configured")

    # =========================================================================
    # Transaction Ingestion
    # =========================================================================

    def add_transaction(self, transaction_data: TransactionData) -> str:
        """
        Add a transaction to the graph.

        Creates or merges User nodes for sender and receiver,
        then creates a SENT relationship with transaction properties.

        Args:
            transaction_data: Dict containing transaction details

        Returns:
            The relationship ID of the created SENT edge

        Example:
            >>> engine.add_transaction({
            ...     "sender_iban": "TR12345",
            ...     "sender_name": "Ahmet Yılmaz",
            ...     "sender_city": "İstanbul",
            ...     "receiver_iban": "TR67890",
            ...     "receiver_name": "Mehmet Kaya",
            ...     "receiver_city": "Ankara",
            ...     "amount": 5000.0,
            ...     "timestamp": "2026-01-16T12:00:00"
            ... })
        """
        params = {
            "sender_iban": transaction_data.get("sender_iban"),
            "sender_name": transaction_data.get("sender_name", "Unknown"),
            "sender_city": transaction_data.get("sender_city", "Unknown"),
            "receiver_iban": transaction_data.get("receiver_iban"),
            "receiver_name": transaction_data.get("receiver_name", "Unknown"),
            "receiver_city": transaction_data.get("receiver_city", "Unknown"),
            "transaction_id": transaction_data.get("transaction_id", ""),
            "amount": float(transaction_data.get("amount", 0)),
            "timestamp": transaction_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "description": transaction_data.get("description", ""),
            "fraud_type": transaction_data.get("fraud_type", "none"),
            "ring_id": transaction_data.get("ring_id"),
        }

        with self._session() as session:
            result = session.run(ADD_TRANSACTION_QUERY, params)
            record = result.single()
            relationship_id = record["relationship_id"] if record else "unknown"

        logger.debug(
            f"Transaction added: {params['sender_iban'][:8]}... -> "
            f"{params['receiver_iban'][:8]}... ({params['amount']} TRY)"
        )

        return relationship_id

    def add_transactions_batch(self, transactions: list[TransactionData]) -> int:
        """
        Add multiple transactions in a single batch for performance.

        Args:
            transactions: List of transaction data dicts

        Returns:
            Number of transactions successfully added
        """
        # Prepare batch parameters
        tx_params = [
            {
                "sender_iban": tx.get("sender_iban"),
                "sender_name": tx.get("sender_name", "Unknown"),
                "sender_city": tx.get("sender_city", "Unknown"),
                "receiver_iban": tx.get("receiver_iban"),
                "receiver_name": tx.get("receiver_name", "Unknown"),
                "receiver_city": tx.get("receiver_city", "Unknown"),
                "transaction_id": tx.get("transaction_id", ""),
                "amount": float(tx.get("amount", 0)),
                "timestamp": tx.get("timestamp", datetime.now(timezone.utc).isoformat()),
                "description": tx.get("description", ""),
                "fraud_type": tx.get("fraud_type", "none"),
                "ring_id": tx.get("ring_id"),
            }
            for tx in transactions
        ]

        with self._session() as session:
            result = session.run(ADD_TRANSACTIONS_BATCH_QUERY, {"transactions": tx_params})
            record = result.single()
            count = record["created"] if record else 0

        logger.info(f"Batch added: {count} transactions")
        return count

    # =========================================================================
    # Fraud Ring Detection (CRITICAL)
    # =========================================================================

    def detect_fraud_rings(
        self,
        sender_iban: str | None = None,
        receiver_iban: str | None = None,
        min_hops: int = 3,
        max_hops: int = 5,
        time_window_hours: int = 168,  # 7 days default
    ) -> list[FraudRing]:
        """
        Detect circular fraud rings starting from a given IBAN.

        Looks for closed-loop paths where money flows in a circle:
        A -> B -> C -> A (3 hops)
        A -> B -> C -> D -> A (4 hops)
        etc.

        Args:
            sender_iban: Starting IBAN to check (uses sender)
            receiver_iban: Alternative starting IBAN (uses receiver)
            min_hops: Minimum ring size (default: 3)
            max_hops: Maximum ring size (default: 5)
            time_window_hours: Only consider transactions within this window

        Returns:
            List of detected FraudRing objects with paths and amounts

        Example:
            >>> rings = engine.detect_fraud_rings(sender_iban="TR123")
            >>> for ring in rings:
            ...     print(f"Ring found: {' -> '.join(ring['path'])}")
        """
        # Use either sender or receiver IBAN as starting point
        start_iban = sender_iban or receiver_iban
        if not start_iban:
            logger.warning("No IBAN provided for fraud ring detection")
            return []

        query = fraud_rings_query(min_hops, max_hops)

        params = {
            "start_iban": start_iban,
            "time_window_hours": time_window_hours,
        }

        detected_rings: list[FraudRing] = []

        with self._session() as session:
            result = session.run(query, params)

            for record in result:
                ibans = record["ibans"]
                ring_id = f"RING-{abs(hash(tuple(ibans))) % 100000:05d}"

                fraud_ring: FraudRing = {
                    "ring_id": ring_id,
                    "path": ibans,
                    "total_amount": record["total_amount"],
                    "transaction_count": record["ring_size"],
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
                detected_rings.append(fraud_ring)

                logger.warning(
                    f"🚨 FRAUD RING DETECTED: {' -> '.join(ibans[:4])}... "
                    f"({record['ring_size']} hops, {record['total_amount']:,.2f} TRY)"
                )

        return detected_rings

    def detect_all_rings(
        self,
        min_hops: int = 3,
        max_hops: int = 5,
        min_amount: float = 1000.0,
        limit: int = 50,
    ) -> list[FraudRing]:
        """
        Scan the entire graph for fraud rings (batch detection).

        Use this for periodic full scans rather than real-time detection.

        Args:
            min_hops: Minimum ring size
            max_hops: Maximum ring size
            min_amount: Minimum total amount in ring
            limit: Maximum rings to return

        Returns:
            List of all detected fraud rings
        """
        query = all_rings_query(min_hops, max_hops)

        params = {
            "min_amount": min_amount,
            "limit": limit,
        }

        detected_rings: list[FraudRing] = []

        try:
            with self._session() as session:
                result = session.run(query, params)

                for record in result:
                    ibans = record["ibans"]
                    ring_id = f"RING-{abs(hash(tuple(ibans))) % 100000:05d}"

                    fraud_ring: FraudRing = {
                        "ring_id": ring_id,
                        "path": ibans,
                        "total_amount": record["total_amount"],
                        "transaction_count": record["ring_size"],
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    }
                    detected_rings.append(fraud_ring)
        except Neo4jError as e:
            # APOC might not be available
            if "apoc" in str(e).lower():
                logger.warning("APOC not available, using simplified query")
                return self._detect_rings_without_apoc(min_hops, max_hops, min_amount, limit)
            raise

        logger.info(f"Full scan complete: {len(detected_rings)} rings detected")
        return detected_rings

    def _detect_rings_without_apoc(
        self,
        min_hops: int,
        max_hops: int,
        min_amount: float,
        limit: int,
    ) -> list[FraudRing]:
        """Fallback ring detection without APOC."""
        query = rings_without_apoc_query(min_hops, max_hops)

        detected_rings: list[FraudRing] = []

        with self._session() as session:
            result = session.run(query, {"min_amount": min_amount, "limit": limit})

            for record in result:
                ibans = record["ibans"]
                ring_id = f"RING-{abs(hash(tuple(ibans))) % 100000:05d}"

                fraud_ring: FraudRing = {
                    "ring_id": ring_id,
                    "path": ibans,
                    "total_amount": record["total_amount"],
                    "transaction_count": record["ring_size"],
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
                detected_rings.append(fraud_ring)

        return detected_rings

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_user_transactions(self, iban: str, limit: int = 50) -> list[dict]:
        """Get all transactions for a specific user."""
        with self._session() as session:
            result = session.run(USER_TRANSACTIONS_QUERY, {"iban": iban, "limit": limit})
            return [dict(record) for record in result]

    def get_graph_stats(self) -> dict:
        """Get basic statistics about the graph."""
        with self._session() as session:
            result = session.run(GRAPH_STATS_QUERY)
            record = result.single()

            if record:
                return {
                    "user_count": record["user_count"],
                    "transaction_count": record["transaction_count"],
                    "total_volume": record["total_volume"],
                }
            return {"user_count": 0, "transaction_count": 0, "total_volume": 0}

    def clear_all(self) -> None:
        """
        Clear all data from the graph. USE WITH CAUTION!

        This is primarily for testing purposes.
        """
        with self._session() as session:
            session.run(CLEAR_ALL_QUERY)

        logger.warning("All graph data cleared!")

# =============================================================================
# SentinelFlow - Neo4j Graph Engine
# =============================================================================
"""
Neo4j graph database integration for fraud detection.

This module provides the GraphEngine class which handles:
- Connection management with connection pooling
- Schema setup (constraints and indexes)
- Transaction ingestion with MERGE queries
- Circular fraud ring detection using Cypher path queries

Usage:
    from sentinelflow.processor.graph_engine import GraphEngine
    
    engine = GraphEngine()
    engine.setup_constraints()
    
    # Add a transaction
    engine.add_transaction({
        "sender_iban": "TR123...",
        "receiver_iban": "TR456...",
        "amount": 5000.0,
        "timestamp": "2026-01-16T12:00:00"
    })
    
    # Check for fraud rings
    rings = engine.detect_fraud_rings(sender_iban="TR123...")
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, TypedDict

from loguru import logger
from neo4j import GraphDatabase, Driver, Session, ManagedTransaction
from neo4j.exceptions import Neo4jError, ServiceUnavailable

from sentinelflow.config import get_settings


# =============================================================================
# Type Definitions
# =============================================================================

class TransactionData(TypedDict, total=False):
    """Type definition for transaction data input."""
    
    transaction_id: str
    sender_iban: str
    sender_name: str
    sender_city: str
    receiver_iban: str
    receiver_name: str
    receiver_city: str
    amount: float
    timestamp: str
    description: str
    fraud_type: str
    ring_id: str | None


class FraudRing(TypedDict):
    """Detected fraud ring result."""
    
    ring_id: str
    path: list[str]  # List of IBANs in the ring
    total_amount: float
    transaction_count: int
    detected_at: str


# =============================================================================
# Graph Engine Class
# =============================================================================

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
        
        # Initialize driver with connection pooling
        self._driver: Driver | None = None
        self._connect()
        
        logger.info(f"GraphEngine initialized: {self._uri}")
    
    def _connect(self) -> None:
        """Establish connection to Neo4j."""
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
    
    def __enter__(self) -> "GraphEngine":
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
        constraints = [
            # Unique IBAN constraint
            """
            CREATE CONSTRAINT user_iban_unique IF NOT EXISTS
            FOR (u:User) REQUIRE u.iban IS UNIQUE
            """,
            # Node key for User
            """
            CREATE CONSTRAINT user_iban_not_null IF NOT EXISTS
            FOR (u:User) REQUIRE u.iban IS NOT NULL
            """,
        ]
        
        indexes = [
            # Index on User name for search
            """
            CREATE INDEX user_name_index IF NOT EXISTS
            FOR (u:User) ON (u.name)
            """,
            # Index on User city
            """
            CREATE INDEX user_city_index IF NOT EXISTS
            FOR (u:User) ON (u.city)
            """,
        ]
        
        with self._session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint.strip())
                    logger.debug(f"Constraint created/verified")
                except Neo4jError as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Constraint error: {e}")
            
            for index in indexes:
                try:
                    session.run(index.strip())
                    logger.debug(f"Index created/verified")
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
        query = """
        // MERGE sender node
        MERGE (sender:User {iban: $sender_iban})
        ON CREATE SET 
            sender.name = $sender_name,
            sender.city = $sender_city,
            sender.created_at = datetime()
        ON MATCH SET
            sender.name = COALESCE($sender_name, sender.name),
            sender.city = COALESCE($sender_city, sender.city)
        
        // MERGE receiver node
        MERGE (receiver:User {iban: $receiver_iban})
        ON CREATE SET 
            receiver.name = $receiver_name,
            receiver.city = $receiver_city,
            receiver.created_at = datetime()
        ON MATCH SET
            receiver.name = COALESCE($receiver_name, receiver.name),
            receiver.city = COALESCE($receiver_city, receiver.city)
        
        // Create the SENT relationship
        CREATE (sender)-[r:SENT {
            transaction_id: $transaction_id,
            amount: $amount,
            timestamp: datetime($timestamp),
            description: $description,
            fraud_type: $fraud_type,
            ring_id: $ring_id,
            created_at: datetime()
        }]->(receiver)
        
        RETURN elementId(r) as relationship_id
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
            "timestamp": transaction_data.get("timestamp", datetime.utcnow().isoformat()),
            "description": transaction_data.get("description", ""),
            "fraud_type": transaction_data.get("fraud_type", "none"),
            "ring_id": transaction_data.get("ring_id"),
        }
        
        with self._session() as session:
            result = session.run(query, params)
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
        query = """
        UNWIND $transactions AS tx
        
        MERGE (sender:User {iban: tx.sender_iban})
        ON CREATE SET sender.name = tx.sender_name, sender.city = tx.sender_city
        
        MERGE (receiver:User {iban: tx.receiver_iban})
        ON CREATE SET receiver.name = tx.receiver_name, receiver.city = tx.receiver_city
        
        CREATE (sender)-[:SENT {
            transaction_id: tx.transaction_id,
            amount: tx.amount,
            timestamp: datetime(tx.timestamp),
            description: tx.description,
            fraud_type: tx.fraud_type,
            ring_id: tx.ring_id
        }]->(receiver)
        
        RETURN count(*) AS created
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
                "timestamp": tx.get("timestamp", datetime.utcnow().isoformat()),
                "description": tx.get("description", ""),
                "fraud_type": tx.get("fraud_type", "none"),
                "ring_id": tx.get("ring_id"),
            }
            for tx in transactions
        ]
        
        with self._session() as session:
            result = session.run(query, {"transactions": tx_params})
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
        
        # Cypher query to find circular paths
        # This is the CRITICAL fraud detection query
        query = f"""
        // Find the starting user
        MATCH (start:User {{iban: $start_iban}})
        
        // Look for circular paths of length min_hops to max_hops
        MATCH path = (start)-[rels:SENT*{min_hops}..{max_hops}]->(start)
        
        // Filter by time window
        WHERE ALL(r IN rels WHERE 
            r.timestamp > datetime() - duration({{hours: $time_window_hours}})
        )
        
        // Extract path details
        WITH path, rels,
             [node IN nodes(path) | node.iban] AS ibans,
             [node IN nodes(path) | node.name] AS names,
             REDUCE(total = 0.0, r IN rels | total + r.amount) AS total_amount,
             length(path) AS ring_size
        
        // Return unique rings (avoid duplicates from different starting points)
        RETURN DISTINCT
            ibans,
            names,
            total_amount,
            ring_size,
            [r IN rels | r.transaction_id] AS transaction_ids,
            [r IN rels | r.timestamp] AS timestamps
        
        ORDER BY total_amount DESC
        LIMIT 100
        """
        
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
                    "detected_at": datetime.utcnow().isoformat(),
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
        query = f"""
        // Find all circular paths in the graph
        MATCH path = (a:User)-[rels:SENT*{min_hops}..{max_hops}]->(a)
        
        // Calculate totals
        WITH path, rels, a,
             [node IN nodes(path) | node.iban] AS ibans,
             REDUCE(total = 0.0, r IN rels | total + r.amount) AS total_amount,
             length(path) AS ring_size
        
        // Filter by minimum amount
        WHERE total_amount >= $min_amount
        
        // Get unique rings (use sorted IBANs as identifier)
        WITH ibans, total_amount, ring_size,
             apoc.coll.sort(ibans) AS sorted_ibans
        
        RETURN DISTINCT
            sorted_ibans AS ibans,
            total_amount,
            ring_size
        
        ORDER BY total_amount DESC
        LIMIT $limit
        """
        
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
                        "detected_at": datetime.utcnow().isoformat(),
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
        query = f"""
        MATCH path = (a:User)-[rels:SENT*{min_hops}..{max_hops}]->(a)
        WITH path, rels, a,
             [node IN nodes(path) | node.iban] AS ibans,
             REDUCE(total = 0.0, r IN rels | total + r.amount) AS total_amount,
             length(path) AS ring_size
        WHERE total_amount >= $min_amount
        RETURN DISTINCT ibans, total_amount, ring_size
        ORDER BY total_amount DESC
        LIMIT $limit
        """
        
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
                    "detected_at": datetime.utcnow().isoformat(),
                }
                detected_rings.append(fraud_ring)
        
        return detected_rings
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_user_transactions(self, iban: str, limit: int = 50) -> list[dict]:
        """Get all transactions for a specific user."""
        query = """
        MATCH (u:User {iban: $iban})-[s:SENT]->(other:User)
        RETURN 
            u.iban AS sender_iban,
            u.name AS sender_name,
            other.iban AS receiver_iban,
            other.name AS receiver_name,
            s.amount AS amount,
            s.timestamp AS timestamp,
            s.transaction_id AS transaction_id
        ORDER BY s.timestamp DESC
        LIMIT $limit
        
        UNION
        
        MATCH (other:User)-[s:SENT]->(u:User {iban: $iban})
        RETURN 
            other.iban AS sender_iban,
            other.name AS sender_name,
            u.iban AS receiver_iban,
            u.name AS receiver_name,
            s.amount AS amount,
            s.timestamp AS timestamp,
            s.transaction_id AS transaction_id
        ORDER BY s.timestamp DESC
        LIMIT $limit
        """
        
        with self._session() as session:
            result = session.run(query, {"iban": iban, "limit": limit})
            return [dict(record) for record in result]
    
    def get_graph_stats(self) -> dict:
        """Get basic statistics about the graph."""
        query = """
        MATCH (u:User)
        WITH count(u) AS user_count
        MATCH ()-[s:SENT]->()
        RETURN 
            user_count,
            count(s) AS transaction_count,
            sum(s.amount) AS total_volume
        """
        
        with self._session() as session:
            result = session.run(query)
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
        query = "MATCH (n) DETACH DELETE n"
        
        with self._session() as session:
            session.run(query)
        
        logger.warning("All graph data cleared!")

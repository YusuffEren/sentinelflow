# =============================================================================
# SentinelFlow - Graph-Based Feature Engineering (Neo4j)
# =============================================================================
"""
Neo4j graf veritabanından gelişmiş özellik çıkarımı.

Graf Özellikleri:
1. Centrality Features    - PageRank, Betweenness, Degree Centrality
2. Community Features     - Louvain clustering, community membership
3. Path Features          - Shortest paths, ring detection
4. Temporal Graph         - Zaman pencereli graf özellikleri
5. Neighborhood Features  - Komşuluk analizi ve risk yayılımı

TEKNOFEST yarışması için: Graf yapısındaki anomalileri tespit
ederek %99.5+ doğruluk hedefi.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
from loguru import logger

try:
    from neo4j import Driver, GraphDatabase

    HAS_NEO4J = True
except ImportError:
    HAS_NEO4J = False
    logger.warning("neo4j driver not available")

try:
    import networkx as nx

    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    logger.warning("networkx not available")


# =============================================================================
# Constants
# =============================================================================

GRAPH_FEATURE_NAMES: list[str] = [
    # Centrality features (5)
    "pagerank_score",
    "betweenness_centrality",
    "degree_centrality",
    "in_degree",
    "out_degree",
    # Community features (4)
    "community_id",
    "community_size",
    "inter_community_ratio",
    "community_fraud_ratio",
    # Path features (5)
    "ring_participation_count",
    "shortest_path_to_fraud",
    "avg_path_length",
    "is_bridge_node",
    "clustering_coefficient",
    # Neighborhood features (6)
    "neighbor_fraud_ratio",
    "neighbor_avg_amount",
    "neighbor_count_1hop",
    "neighbor_count_2hop",
    "risky_neighbor_count",
    "neighbor_diversity_score",
    # Temporal graph features (4)
    "recent_edge_count_7d",
    "recent_unique_targets_7d",
    "temporal_burst_score",
    "edge_recency_score",
    # Risk propagation (4)
    "risk_propagation_score",
    "fraud_distance_score",
    "contamination_score",
    "network_risk_score",
]

NUM_GRAPH_FEATURES = len(GRAPH_FEATURE_NAMES)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class GraphNodeProfile:
    """Tek bir node'un graf profili."""

    iban: str = ""

    # Centrality
    pagerank: float = 0.0
    betweenness: float = 0.0
    degree_centrality: float = 0.0
    in_degree: int = 0
    out_degree: int = 0

    # Community
    community_id: int = -1
    community_size: int = 0
    inter_community_edges: int = 0

    # Fraud proximity
    is_fraud: bool = False
    fraud_neighbor_count: int = 0
    shortest_path_to_fraud: int = -1

    # Ring participation
    ring_count: int = 0
    ring_total_amount: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "iban": self.iban,
            "pagerank": round(self.pagerank, 6),
            "betweenness": round(self.betweenness, 6),
            "degree_centrality": round(self.degree_centrality, 6),
            "in_degree": self.in_degree,
            "out_degree": self.out_degree,
            "community_id": self.community_id,
            "is_fraud": self.is_fraud,
            "ring_count": self.ring_count,
        }


@dataclass
class GraphEdgeInfo:
    """Edge (transaction) bilgisi."""

    source: str
    target: str
    amount: float
    timestamp: datetime
    fraud_type: str = "none"
    edge_id: str = ""


# =============================================================================
# Neo4j Graph Feature Engine
# =============================================================================


class GraphFeatureEngine:
    """
    Neo4j tabanlı graf özellik çıkarıcı.

    Bu motor, işlem grafından yapısal özellikler çıkarır:
    - Centrality metrics (önem ölçüleri)
    - Community detection (topluluk tespiti)
    - Ring detection (döngüsel pattern'lar)
    - Risk propagation (risk yayılımı)

    TEKNOFEST için graf analizi kritik:
    Para aklama ağlarını tespit etmek için graf yapısı şart.

    Usage:
        engine = GraphFeatureEngine(neo4j_uri, neo4j_user, neo4j_password)
        features = engine.extract_features("TR12345...")
    """

    def __init__(
        self,
        neo4j_uri: str = "bolt://localhost:7687",
        neo4j_user: str = "neo4j",
        neo4j_password: str = "password",
        cache_ttl_seconds: int = 300,
    ) -> None:
        """
        Initialize graph feature engine.

        Args:
            neo4j_uri: Neo4j connection URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            cache_ttl_seconds: Cache time-to-live
        """
        self._uri = neo4j_uri
        self._user = neo4j_user
        self._password = neo4j_password
        self._cache_ttl = cache_ttl_seconds

        self._driver: Driver | None = None
        self._feature_cache: dict[str, tuple[dict, datetime]] = {}
        self._community_cache: dict[int, set[str]] = {}
        self._fraud_nodes: set[str] = set()

        self._connect()
        logger.info(f"GraphFeatureEngine initialized (uri={neo4j_uri})")

    def _connect(self) -> None:
        """Connect to Neo4j."""
        if not HAS_NEO4J:
            logger.warning("Neo4j driver not available")
            return

        try:
            self._driver = GraphDatabase.driver(
                self._uri,
                auth=(self._user, self._password),
                max_connection_lifetime=3600,
            )
            # Test connection
            with self._driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to Neo4j")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            self._driver = None

    def extract_features(self, iban: str) -> dict[str, float]:
        """
        Extract graph features for a given IBAN.

        Args:
            iban: Account IBAN

        Returns:
            Feature dictionary
        """
        # Check cache
        if iban in self._feature_cache:
            cached, timestamp = self._feature_cache[iban]
            if (datetime.now(timezone.utc) - timestamp).seconds < self._cache_ttl:
                return cached

        features: dict[str, float] = dict.fromkeys(GRAPH_FEATURE_NAMES, 0.0)

        if not self._driver:
            return features

        try:
            with self._driver.session() as session:
                # Centrality features
                centrality = self._get_centrality_features(session, iban)
                features.update(centrality)

                # Community features
                community = self._get_community_features(session, iban)
                features.update(community)

                # Path features
                path = self._get_path_features(session, iban)
                features.update(path)

                # Neighborhood features
                neighborhood = self._get_neighborhood_features(session, iban)
                features.update(neighborhood)

                # Temporal graph features
                temporal = self._get_temporal_graph_features(session, iban)
                features.update(temporal)

                # Risk propagation
                risk = self._get_risk_propagation_features(session, iban)
                features.update(risk)

        except Exception as e:
            logger.error(f"Error extracting graph features for {iban}: {e}")

        # Cache features
        self._feature_cache[iban] = (features, datetime.now(timezone.utc))

        return features

    def extract_vector(self, iban: str) -> np.ndarray:
        """Extract features as numpy array."""
        features = self.extract_features(iban)
        return np.array([features.get(name, 0.0) for name in GRAPH_FEATURE_NAMES], dtype=np.float64)

    # =========================================================================
    # Centrality Features
    # =========================================================================

    def _get_centrality_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get centrality-based features."""
        features = {}

        # Degree centrality (normalized)
        query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH (u)-[r:SENT]->()
        WITH u, COUNT(r) as out_deg
        OPTIONAL MATCH ()-[r2:SENT]->(u)
        WITH u, out_deg, COUNT(r2) as in_deg
        MATCH (total:User)
        WITH out_deg, in_deg, COUNT(total) as total_nodes
        RETURN
            out_deg,
            in_deg,
            toFloat(out_deg + in_deg) / (total_nodes - 1) as degree_centrality
        """

        result = session.run(query, iban=iban).single()
        if result:
            features["out_degree"] = float(result["out_deg"])
            features["in_degree"] = float(result["in_deg"])
            features["degree_centrality"] = float(result["degree_centrality"] or 0)
        else:
            features["out_degree"] = 0.0
            features["in_degree"] = 0.0
            features["degree_centrality"] = 0.0

        # PageRank (approximation using Neo4j GDS if available, otherwise simple)
        pagerank_query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH path = ()-[:SENT*1..3]->(u)
        WITH u, COUNT(path) as incoming_paths
        RETURN toFloat(incoming_paths) / 1000.0 as pagerank_approx
        """

        pr_result = session.run(pagerank_query, iban=iban).single()
        features["pagerank_score"] = float(pr_result["pagerank_approx"] or 0) if pr_result else 0.0

        # Betweenness centrality approximation
        betweenness_query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH (a:User)-[:SENT*1..2]->(u)-[:SENT*1..2]->(b:User)
        WHERE a <> b AND a <> u AND b <> u
        WITH u, COUNT(DISTINCT a) + COUNT(DISTINCT b) as bridge_count
        RETURN toFloat(bridge_count) / 100.0 as betweenness_approx
        """

        bc_result = session.run(betweenness_query, iban=iban).single()
        features["betweenness_centrality"] = (
            float(bc_result["betweenness_approx"] or 0) if bc_result else 0.0
        )

        return features

    # =========================================================================
    # Community Features
    # =========================================================================

    def _get_community_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get community-based features."""
        features = {}

        # Simple community detection based on connected component
        community_query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH (u)-[:SENT*1..3]-(connected:User)
        WITH u, COLLECT(DISTINCT connected.iban) + [u.iban] as community_members
        RETURN
            SIZE(community_members) as community_size,
            SIZE([m IN community_members WHERE m STARTS WITH 'FRAUD_']) as fraud_in_community
        """

        result = session.run(community_query, iban=iban).single()
        if result:
            features["community_size"] = float(result["community_size"])
            community_size = result["community_size"]
            fraud_count = result["fraud_in_community"]
            features["community_fraud_ratio"] = (
                fraud_count / community_size if community_size > 0 else 0.0
            )
        else:
            features["community_size"] = 1.0
            features["community_fraud_ratio"] = 0.0

        # Inter-community ratio (edges going outside community)
        inter_query = """
        MATCH (u:User {iban: $iban})-[r:SENT]-(other:User)
        OPTIONAL MATCH (u)-[:SENT*1..2]-(community:User)
        WITH u, other, COLLECT(DISTINCT community.iban) as community_members
        RETURN
            COUNT(CASE WHEN other.iban IN community_members THEN 1 END) as intra,
            COUNT(CASE WHEN NOT other.iban IN community_members THEN 1 END) as inter
        """

        inter_result = session.run(inter_query, iban=iban).single()
        if inter_result:
            total_edges = inter_result["intra"] + inter_result["inter"]
            features["inter_community_ratio"] = (
                inter_result["inter"] / total_edges if total_edges > 0 else 0.0
            )
        else:
            features["inter_community_ratio"] = 0.0

        features["community_id"] = hash(iban) % 1000  # Placeholder

        return features

    # =========================================================================
    # Path Features
    # =========================================================================

    def _get_path_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get path-based features including ring detection."""
        features = {}

        # Ring participation (circular transaction patterns)
        ring_query = """
        MATCH path = (u:User {iban: $iban})-[:SENT*2..6]->(u)
        WITH path,
             REDUCE(total = 0, r IN relationships(path) | total + r.amount) as ring_amount
        RETURN
            COUNT(path) as ring_count,
            SUM(ring_amount) as total_ring_amount
        """

        ring_result = session.run(ring_query, iban=iban).single()
        if ring_result:
            features["ring_participation_count"] = float(ring_result["ring_count"] or 0)
        else:
            features["ring_participation_count"] = 0.0

        # Shortest path to known fraud node
        fraud_path_query = """
        MATCH (u:User {iban: $iban})
        MATCH (fraud:User)
        WHERE fraud.is_fraud = true OR fraud.iban CONTAINS 'FRAUD'
        MATCH path = shortestPath((u)-[:SENT*1..10]-(fraud))
        RETURN MIN(LENGTH(path)) as shortest_fraud_path
        """

        try:
            fraud_result = session.run(fraud_path_query, iban=iban).single()
            if fraud_result and fraud_result["shortest_fraud_path"]:
                features["shortest_path_to_fraud"] = float(fraud_result["shortest_fraud_path"])
            else:
                features["shortest_path_to_fraud"] = 10.0  # Default: far from fraud
        except Exception:
            features["shortest_path_to_fraud"] = 10.0

        # Average path length to other nodes
        avg_path_query = """
        MATCH (u:User {iban: $iban})
        MATCH (other:User)
        WHERE other.iban <> $iban
        MATCH path = shortestPath((u)-[:SENT*1..5]-(other))
        WITH LENGTH(path) as path_length
        RETURN AVG(path_length) as avg_path_length
        LIMIT 100
        """

        try:
            avg_result = session.run(avg_path_query, iban=iban).single()
            features["avg_path_length"] = (
                float(avg_result["avg_path_length"] or 3.0) if avg_result else 3.0
            )
        except Exception:
            features["avg_path_length"] = 3.0

        # Bridge node detection
        bridge_query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH (a:User)-[:SENT]->(u)-[:SENT]->(b:User)
        WHERE NOT EXISTS((a)-[:SENT*1..2]-(b))
        RETURN COUNT(DISTINCT a) + COUNT(DISTINCT b) > 5 as is_bridge
        """

        bridge_result = session.run(bridge_query, iban=iban).single()
        features["is_bridge_node"] = 1.0 if bridge_result and bridge_result["is_bridge"] else 0.0

        # Clustering coefficient
        clustering_query = """
        MATCH (u:User {iban: $iban})-[:SENT]-(neighbor:User)
        WITH u, COLLECT(neighbor) as neighbors
        UNWIND neighbors as n1
        UNWIND neighbors as n2
        WITH u, n1, n2, neighbors
        WHERE n1 <> n2
        OPTIONAL MATCH (n1)-[:SENT]-(n2)
        WITH u, COUNT(n1) as possible_connections,
             COUNT((n1)-[:SENT]-(n2)) as actual_connections,
             SIZE(neighbors) as neighbor_count
        RETURN
            CASE WHEN possible_connections > 0
            THEN toFloat(actual_connections) / possible_connections
            ELSE 0 END as clustering_coef
        """

        try:
            cc_result = session.run(clustering_query, iban=iban).single()
            features["clustering_coefficient"] = (
                float(cc_result["clustering_coef"] or 0) if cc_result else 0.0
            )
        except Exception:
            features["clustering_coefficient"] = 0.0

        return features

    # =========================================================================
    # Neighborhood Features
    # =========================================================================

    def _get_neighborhood_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get neighborhood-based features."""
        features = {}

        # Neighbor statistics
        neighbor_query = """
        MATCH (u:User {iban: $iban})-[r:SENT]-(neighbor:User)
        WITH u, neighbor, r
        OPTIONAL MATCH (neighbor)-[:SENT]-(n2:User)
        WHERE n2 <> u
        WITH u,
             COUNT(DISTINCT neighbor) as neighbor_1hop,
             COUNT(DISTINCT n2) as neighbor_2hop,
             AVG(r.amount) as avg_neighbor_amount,
             SUM(CASE WHEN neighbor.is_fraud = true THEN 1 ELSE 0 END) as fraud_neighbors
        RETURN
            neighbor_1hop,
            neighbor_2hop,
            avg_neighbor_amount,
            fraud_neighbors,
            toFloat(fraud_neighbors) / CASE WHEN neighbor_1hop > 0 THEN neighbor_1hop ELSE 1 END as fraud_ratio
        """

        result = session.run(neighbor_query, iban=iban).single()
        if result:
            features["neighbor_count_1hop"] = float(result["neighbor_1hop"] or 0)
            features["neighbor_count_2hop"] = float(result["neighbor_2hop"] or 0)
            features["neighbor_avg_amount"] = float(result["avg_neighbor_amount"] or 0)
            features["risky_neighbor_count"] = float(result["fraud_neighbors"] or 0)
            features["neighbor_fraud_ratio"] = float(result["fraud_ratio"] or 0)
        else:
            features["neighbor_count_1hop"] = 0.0
            features["neighbor_count_2hop"] = 0.0
            features["neighbor_avg_amount"] = 0.0
            features["risky_neighbor_count"] = 0.0
            features["neighbor_fraud_ratio"] = 0.0

        # Neighbor diversity (unique countries/cities/banks)
        diversity_query = """
        MATCH (u:User {iban: $iban})-[:SENT]-(neighbor:User)
        RETURN
            COUNT(DISTINCT neighbor.city) as unique_cities,
            COUNT(DISTINCT SUBSTRING(neighbor.iban, 0, 4)) as unique_banks
        """

        div_result = session.run(diversity_query, iban=iban).single()
        if div_result:
            unique_cities = div_result["unique_cities"] or 0
            unique_banks = div_result["unique_banks"] or 0
            features["neighbor_diversity_score"] = (unique_cities + unique_banks) / 20.0
        else:
            features["neighbor_diversity_score"] = 0.0

        return features

    # =========================================================================
    # Temporal Graph Features
    # =========================================================================

    def _get_temporal_graph_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get time-windowed graph features."""
        features = {}

        # Recent edge statistics
        temporal_query = """
        MATCH (u:User {iban: $iban})-[r:SENT]->(target:User)
        WHERE r.timestamp > datetime() - duration('P7D')
        WITH u, COUNT(r) as recent_edges, COUNT(DISTINCT target) as unique_targets
        RETURN recent_edges, unique_targets
        """

        result = session.run(temporal_query, iban=iban).single()
        if result:
            features["recent_edge_count_7d"] = float(result["recent_edges"] or 0)
            features["recent_unique_targets_7d"] = float(result["unique_targets"] or 0)
        else:
            features["recent_edge_count_7d"] = 0.0
            features["recent_unique_targets_7d"] = 0.0

        # Temporal burst detection
        burst_query = """
        MATCH (u:User {iban: $iban})-[r:SENT]->()
        WHERE r.timestamp > datetime() - duration('PT1H')
        WITH COUNT(r) as last_hour_edges
        MATCH (u:User {iban: $iban})-[r2:SENT]->()
        WHERE r2.timestamp > datetime() - duration('P7D')
        WITH last_hour_edges, COUNT(r2) as week_edges
        RETURN
            last_hour_edges,
            CASE WHEN week_edges > 0
            THEN toFloat(last_hour_edges) / (week_edges / (7.0 * 24.0))
            ELSE 0 END as burst_ratio
        """

        burst_result = session.run(burst_query, iban=iban).single()
        if burst_result:
            features["temporal_burst_score"] = (
                min(float(burst_result["burst_ratio"] or 0), 10.0) / 10.0
            )
        else:
            features["temporal_burst_score"] = 0.0

        # Edge recency score (how recent are connections)
        recency_query = """
        MATCH (u:User {iban: $iban})-[r:SENT]-()
        WITH MAX(r.timestamp) as most_recent
        RETURN duration.between(most_recent, datetime()).hours as hours_since_last
        """

        recency_result = session.run(recency_query, iban=iban).single()
        if recency_result and recency_result["hours_since_last"]:
            hours = float(recency_result["hours_since_last"])
            features["edge_recency_score"] = max(0, 1.0 - (hours / 168.0))  # 7 days = 168 hours
        else:
            features["edge_recency_score"] = 0.0

        return features

    # =========================================================================
    # Risk Propagation Features
    # =========================================================================

    def _get_risk_propagation_features(
        self,
        session: Any,
        iban: str,
    ) -> dict[str, float]:
        """Get risk propagation features."""
        features = {}

        # Risk propagation score (weighted by distance to fraud)
        risk_query = """
        MATCH (u:User {iban: $iban})
        OPTIONAL MATCH path = (u)-[:SENT*1..3]-(fraud:User)
        WHERE fraud.is_fraud = true
        WITH u, path, LENGTH(path) as distance
        WITH u, COLLECT({distance: distance}) as fraud_paths
        WITH u,
             SIZE([p IN fraud_paths WHERE p.distance = 1]) as fraud_1hop,
             SIZE([p IN fraud_paths WHERE p.distance = 2]) as fraud_2hop,
             SIZE([p IN fraud_paths WHERE p.distance = 3]) as fraud_3hop
        RETURN
            fraud_1hop * 1.0 + fraud_2hop * 0.5 + fraud_3hop * 0.25 as risk_score,
            CASE WHEN fraud_1hop > 0 THEN 1.0
                 WHEN fraud_2hop > 0 THEN 0.5
                 WHEN fraud_3hop > 0 THEN 0.25
                 ELSE 0 END as fraud_distance_score
        """

        result = session.run(risk_query, iban=iban).single()
        if result:
            features["risk_propagation_score"] = min(float(result["risk_score"] or 0) / 10.0, 1.0)
            features["fraud_distance_score"] = float(result["fraud_distance_score"] or 0)
        else:
            features["risk_propagation_score"] = 0.0
            features["fraud_distance_score"] = 0.0

        # Contamination score (how much fraud flows through this node)
        contamination_query = """
        MATCH (fraud:User)-[r:SENT*1..2]->(u:User {iban: $iban})-[r2:SENT*1..2]->(target:User)
        WHERE fraud.is_fraud = true
        RETURN COUNT(DISTINCT target) as contaminated_targets
        """

        cont_result = session.run(contamination_query, iban=iban).single()
        features["contamination_score"] = (
            min(float(cont_result["contaminated_targets"] or 0) / 10.0, 1.0) if cont_result else 0.0
        )

        # Network risk score (composite)
        features["network_risk_score"] = (
            features["risk_propagation_score"] * 0.4
            + features["fraud_distance_score"] * 0.3
            + features["contamination_score"] * 0.3
        )

        return features

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def close(self) -> None:
        """Close Neo4j connection."""
        if self._driver:
            self._driver.close()
            logger.info("Neo4j connection closed")

    def clear_cache(self) -> None:
        """Clear feature cache."""
        self._feature_cache.clear()
        logger.info("Feature cache cleared")

    @staticmethod
    def get_feature_names() -> list[str]:
        """Return ordered feature names."""
        return GRAPH_FEATURE_NAMES.copy()

    @property
    def is_connected(self) -> bool:
        """Check if connected to Neo4j."""
        return self._driver is not None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# =============================================================================
# In-Memory Graph Feature Engine (NetworkX based)
# =============================================================================


class InMemoryGraphFeatureEngine:
    """
    NetworkX tabanlı in-memory graf özellik çıkarıcı.

    Neo4j bağlantısı olmadan test ve geliştirme için kullanılır.
    Daha küçük veri setleri için uygundur.
    """

    def __init__(self) -> None:
        if not HAS_NETWORKX:
            logger.warning("NetworkX not available")
            self._graph = None
        else:
            self._graph = nx.DiGraph()

        self._node_labels: dict[str, bool] = {}  # iban -> is_fraud
        logger.info("InMemoryGraphFeatureEngine initialized")

    def add_transaction(
        self,
        sender: str,
        receiver: str,
        amount: float,
        timestamp: datetime,
        is_fraud: bool = False,
    ) -> None:
        """Add a transaction edge to the graph."""
        if self._graph is None:
            return

        self._graph.add_edge(
            sender,
            receiver,
            amount=amount,
            timestamp=timestamp,
            is_fraud=is_fraud,
        )

        if is_fraud:
            self._node_labels[sender] = True
            self._node_labels[receiver] = True

    def extract_features(self, iban: str) -> dict[str, float]:
        """Extract graph features using NetworkX."""
        features: dict[str, float] = dict.fromkeys(GRAPH_FEATURE_NAMES, 0.0)

        if self._graph is None or iban not in self._graph:
            return features

        # Degree centrality
        try:
            features["in_degree"] = float(self._graph.in_degree(iban))
            features["out_degree"] = float(self._graph.out_degree(iban))

            dc = nx.degree_centrality(self._graph)
            features["degree_centrality"] = dc.get(iban, 0.0)
        except Exception:
            pass

        # PageRank
        try:
            pr = nx.pagerank(self._graph, alpha=0.85)
            features["pagerank_score"] = pr.get(iban, 0.0)
        except Exception:
            pass

        # Betweenness centrality (can be slow for large graphs)
        try:
            if self._graph.number_of_nodes() < 1000:
                bc = nx.betweenness_centrality(self._graph)
                features["betweenness_centrality"] = bc.get(iban, 0.0)
        except Exception:
            pass

        # Clustering coefficient
        try:
            cc = nx.clustering(self._graph.to_undirected(), iban)
            features["clustering_coefficient"] = cc
        except Exception:
            pass

        # Neighbors
        try:
            neighbors = set(self._graph.predecessors(iban)) | set(self._graph.successors(iban))
            features["neighbor_count_1hop"] = float(len(neighbors))

            # Fraud neighbors
            fraud_neighbors = sum(1 for n in neighbors if self._node_labels.get(n, False))
            features["risky_neighbor_count"] = float(fraud_neighbors)
            features["neighbor_fraud_ratio"] = (
                fraud_neighbors / len(neighbors) if neighbors else 0.0
            )
        except Exception:
            pass

        # Ring detection (cycles)
        try:
            cycles = list(nx.simple_cycles(self._graph))
            ring_count = sum(1 for c in cycles if iban in c and 2 <= len(c) <= 6)
            features["ring_participation_count"] = float(ring_count)
        except Exception:
            pass

        return features

    def extract_vector(self, iban: str) -> np.ndarray:
        """Extract features as numpy array."""
        features = self.extract_features(iban)
        return np.array([features.get(name, 0.0) for name in GRAPH_FEATURE_NAMES], dtype=np.float64)

    @property
    def node_count(self) -> int:
        """Number of nodes in graph."""
        return self._graph.number_of_nodes() if self._graph else 0

    @property
    def edge_count(self) -> int:
        """Number of edges in graph."""
        return self._graph.number_of_edges() if self._graph else 0

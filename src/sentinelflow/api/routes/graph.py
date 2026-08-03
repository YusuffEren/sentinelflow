# =============================================================================
# SentinelFlow - Graph API Routes (Neo4j Integration)
# =============================================================================
"""
Graph visualization and fraud ring detection API endpoints.

Endpoints:
- GET /api/v1/graph/nodes - Get transaction network nodes
- GET /api/v1/graph/edges - Get transaction network edges
- GET /api/v1/graph/rings - Get detected fraud rings
- GET /api/v1/graph/account/{iban} - Get account transaction network
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel, Field

router = APIRouter(prefix="/graph", tags=["Graph"])


# =============================================================================
# Schemas
# =============================================================================


class GraphNode(BaseModel):
    """Node in the transaction graph."""

    id: str
    label: str
    group: int = Field(default=0, description="0=normal, 1=fraud, 2=suspicious")
    amount_total: float = 0.0
    tx_count: int = 0
    city: str | None = None
    is_fraud: bool = False


class GraphEdge(BaseModel):
    """Edge (transaction) in the graph."""

    source: str
    target: str
    amount: float
    timestamp: str
    color: str | None = None
    is_fraud: bool = False


class GraphData(BaseModel):
    """Complete graph data for visualization."""

    nodes: list[GraphNode]
    links: list[GraphEdge]
    metadata: dict[str, Any] = Field(default_factory=dict)


class FraudRing(BaseModel):
    """Detected fraud ring."""

    ring_id: str
    accounts: list[str]
    total_amount: float
    transaction_count: int
    detected_at: str
    severity: str = "high"


# =============================================================================
# Neo4j Connection (lazy loaded)
# =============================================================================


_graph_engine = None


def get_graph_engine():
    """Get or create graph engine connection."""
    global _graph_engine

    if _graph_engine is None:
        try:
            from sentinelflow.processor.graph_engine import GraphEngine

            _graph_engine = GraphEngine()
            logger.info("GraphEngine connected for API")
        except Exception as e:
            logger.warning(f"GraphEngine not available: {e}")
            return None

    return _graph_engine


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/data", response_model=GraphData)
async def get_graph_data(
    limit: int = Query(default=100, ge=1, le=1000),
    hours: int = Query(default=24, ge=1, le=168),
    include_fraud_only: bool = Query(default=False),
) -> GraphData:
    """
    Get transaction network graph data for visualization.

    Returns nodes (accounts) and links (transactions) for force-directed graph.
    """
    engine = get_graph_engine()

    if engine is None:
        return _generate_mock_graph_data(limit)

    try:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        query = """
        MATCH (s:User)-[t:SENT]->(r:User)
        WHERE t.timestamp > $since
        RETURN s.iban AS sender, r.iban AS receiver,
               s.name AS sender_name, r.name AS receiver_name,
               s.city AS sender_city, r.city AS receiver_city,
               t.amount AS amount, t.timestamp AS timestamp,
               t.fraud_type AS fraud_type
        ORDER BY t.timestamp DESC
        LIMIT $limit
        """

        records = engine.query(
            cypher=query,
            params={"since": since.isoformat(), "limit": limit},
        )

        nodes_map: dict[str, GraphNode] = {}
        edges: list[GraphEdge] = []

        for record in records:
            sender_id = record["sender"][:12] + "..."
            receiver_id = record["receiver"][:12] + "..."

            if sender_id not in nodes_map:
                nodes_map[sender_id] = GraphNode(
                    id=sender_id,
                    label=record["sender_name"] or sender_id,
                    group=2,
                    city=record["sender_city"],
                )

            if receiver_id not in nodes_map:
                nodes_map[receiver_id] = GraphNode(
                    id=receiver_id,
                    label=record["receiver_name"] or receiver_id,
                    group=0,
                    city=record["receiver_city"],
                )

            nodes_map[sender_id].tx_count += 1
            nodes_map[sender_id].amount_total += record["amount"]

            # fraud_type "none"/""/None değilse fraud olarak işaretle
            is_fraud = record.get("fraud_type") not in (None, "none", "")

            if is_fraud:
                nodes_map[sender_id].is_fraud = True
                nodes_map[sender_id].group = 1
                nodes_map[receiver_id].is_fraud = True
                nodes_map[receiver_id].group = 1

            edges.append(
                GraphEdge(
                    source=sender_id,
                    target=receiver_id,
                    amount=record["amount"],
                    timestamp=str(record["timestamp"]),
                    color="#ef4444" if is_fraud else "#334155",
                    is_fraud=is_fraud,
                )
            )

        if include_fraud_only:
            fraud_nodes = {n.id for n in nodes_map.values() if n.is_fraud}
            edges = [e for e in edges if e.source in fraud_nodes or e.target in fraud_nodes]
            nodes_map = {k: v for k, v in nodes_map.items() if k in fraud_nodes}

        return GraphData(
            nodes=list(nodes_map.values()),
            links=edges,
            metadata={
                "total_nodes": len(nodes_map),
                "total_edges": len(edges),
                "fraud_nodes": sum(1 for n in nodes_map.values() if n.is_fraud),
                "time_range_hours": hours,
            },
        )

    except Exception as e:
        logger.error(f"Graph query failed: {e}")
        return _generate_mock_graph_data(limit)


@router.get("/rings", response_model=list[FraudRing])
async def get_fraud_rings(
    min_depth: int = Query(default=3, ge=2, le=10),
    max_depth: int = Query(default=6, ge=3, le=10),
    limit: int = Query(default=10, ge=1, le=50),
) -> list[FraudRing]:
    """
    Get detected fraud rings (circular transaction patterns).
    """
    engine = get_graph_engine()

    if engine is None:
        return _generate_mock_rings(limit)

    try:
        rings = engine.detect_all_rings(min_hops=min_depth, max_hops=max_depth, limit=limit)

        return [
            FraudRing(
                ring_id=ring["ring_id"],
                accounts=ring["path"],
                total_amount=ring["total_amount"],
                transaction_count=ring["transaction_count"],
                detected_at=datetime.now(timezone.utc).isoformat(),
                severity="critical" if ring["total_amount"] > 100000 else "high",
            )
            for ring in rings[:limit]
        ]

    except Exception as e:
        logger.error(f"Ring detection failed: {e}")
        return _generate_mock_rings(limit)


@router.get("/account/{iban}")
async def get_account_network(
    iban: str,
    depth: int = Query(default=2, ge=1, le=4),
) -> GraphData:
    """
    Get transaction network centered on a specific account.
    """
    engine = get_graph_engine()

    if engine is None:
        raise HTTPException(status_code=503, detail="Graph database not available")

    try:
        # SAFETY: `depth` FastAPI Query(ge=1, le=4) ile int olarak doğrulanmıştır.
        # Neo4j değişken desen derinliğini parametrize etmeyi desteklemediği için f-string gereklidir.
        query = f"""
        MATCH path = (start:User {{iban: $iban}})-[:SENT*1..{depth}]-(connected)
        UNWIND relationships(path) AS rel
        WITH DISTINCT startNode(rel) AS s, endNode(rel) AS r, rel AS t
        RETURN s.iban AS sender, r.iban AS receiver,
               s.name AS sender_name, r.name AS receiver_name,
               t.amount AS amount, t.timestamp AS timestamp
        LIMIT 200
        """

        records = engine.query(cypher=query, params={"iban": iban})

        if not records:
            raise HTTPException(status_code=404, detail=f"Account {iban} not found")

        nodes_map: dict[str, GraphNode] = {}
        edges: list[GraphEdge] = []

        for record in records:
            sender_id = record["sender"][:12] + "..."
            receiver_id = record["receiver"][:12] + "..."

            is_center = record["sender"] == iban or record["receiver"] == iban

            if sender_id not in nodes_map:
                nodes_map[sender_id] = GraphNode(
                    id=sender_id,
                    label=record["sender_name"] or sender_id,
                    group=2 if record["sender"] == iban else 0,
                )

            if receiver_id not in nodes_map:
                nodes_map[receiver_id] = GraphNode(
                    id=receiver_id,
                    label=record["receiver_name"] or receiver_id,
                    group=2 if record["receiver"] == iban else 0,
                )

            edges.append(
                GraphEdge(
                    source=sender_id,
                    target=receiver_id,
                    amount=record["amount"],
                    timestamp=str(record["timestamp"]),
                    color="#3b82f6" if is_center else "#334155",
                )
            )

        return GraphData(
            nodes=list(nodes_map.values()),
            links=edges,
            metadata={
                "center_account": iban,
                "depth": depth,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Account network query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Mock Data Generators (for demo/testing)
# =============================================================================


def _generate_mock_graph_data(limit: int = 100) -> GraphData:
    """Generate mock graph data when Neo4j is not available."""
    import random

    nodes = []
    edges = []

    n_accounts = min(limit // 2, 50)

    for i in range(n_accounts):
        is_fraud = random.random() < 0.1
        nodes.append(
            GraphNode(
                id=f"ACC{i:04d}",
                label=f"Account {i}",
                group=1 if is_fraud else (2 if random.random() < 0.3 else 0),
                amount_total=random.uniform(1000, 100000),
                tx_count=random.randint(1, 20),
                is_fraud=is_fraud,
            )
        )

    for i in range(min(limit, 100)):
        source_idx = random.randint(0, n_accounts - 1)
        target_idx = random.randint(0, n_accounts - 1)

        if source_idx == target_idx:
            continue

        is_fraud = nodes[source_idx].is_fraud or nodes[target_idx].is_fraud

        edges.append(
            GraphEdge(
                source=f"ACC{source_idx:04d}",
                target=f"ACC{target_idx:04d}",
                amount=random.uniform(100, 50000),
                timestamp=datetime.now(timezone.utc).isoformat(),
                color="#ef4444" if is_fraud else "#334155",
                is_fraud=is_fraud,
            )
        )

    return GraphData(
        nodes=nodes,
        links=edges,
        metadata={
            "is_mock": True,
            "message": "Neo4j not connected, showing mock data",
        },
    )


def _generate_mock_rings(limit: int = 10) -> list[FraudRing]:
    """Generate mock fraud rings."""
    import random

    rings = []

    for i in range(limit):
        ring_size = random.randint(3, 6)
        accounts = [f"ACC{random.randint(0, 99):04d}" for _ in range(ring_size)]
        accounts.append(accounts[0])

        rings.append(
            FraudRing(
                ring_id=f"RING-{i:04d}",
                accounts=accounts,
                total_amount=random.uniform(50000, 500000),
                transaction_count=ring_size,
                detected_at=datetime.now(timezone.utc).isoformat(),
                severity=random.choice(["high", "critical"]),
            )
        )

    return rings

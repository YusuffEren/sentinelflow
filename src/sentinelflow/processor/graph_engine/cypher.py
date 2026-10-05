# =============================================================================
# SentinelFlow - Graph Engine Cypher Statements
# =============================================================================
"""
Cypher templates used by the GraphEngine.

Keeping the (often multi-line) queries in one place makes the engine methods
readable and the queries easy to review. Parameters are ALWAYS passed
separately to avoid Cypher injection; only structural values (hop counts)
are interpolated by the builder functions below.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Schema Setup
# -----------------------------------------------------------------------------

CONSTRAINT_STATEMENTS = [
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

INDEX_STATEMENTS = [
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

# -----------------------------------------------------------------------------
# Transaction Ingestion
# -----------------------------------------------------------------------------

ADD_TRANSACTION_QUERY = """
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

ADD_TRANSACTIONS_BATCH_QUERY = """
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

# -----------------------------------------------------------------------------
# Fraud Ring Detection
# -----------------------------------------------------------------------------


def fraud_rings_query(min_hops: int, max_hops: int) -> str:
    """Circular-path query from a starting IBAN (the critical detection query)."""
    return f"""
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


def all_rings_query(min_hops: int, max_hops: int) -> str:
    """Full-graph ring scan (uses apoc.coll.sort for de-duplication)."""
    return f"""
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


def rings_without_apoc_query(min_hops: int, max_hops: int) -> str:
    """Fallback full-graph ring scan for clusters without APOC installed."""
    return f"""
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


# -----------------------------------------------------------------------------
# Utility Queries
# -----------------------------------------------------------------------------

USER_TRANSACTIONS_QUERY = """
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

GRAPH_STATS_QUERY = """
MATCH (u:User)
WITH count(u) AS user_count
MATCH ()-[s:SENT]->()
RETURN
    user_count,
    count(s) AS transaction_count,
    sum(s.amount) AS total_volume
"""

CLEAR_ALL_QUERY = "MATCH (n) DETACH DELETE n"

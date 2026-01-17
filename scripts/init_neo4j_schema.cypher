// =============================================================================
// Neo4j Schema Initialization - SentinelFlow
// =============================================================================
// Run this after Neo4j is up to create indexes and constraints
// Execute via: cat init_neo4j_schema.cypher | cypher-shell -u neo4j -p sentinelflow_secret_2024

// -----------------------------------------------------------------------------
// Constraints - Ensure data integrity
// -----------------------------------------------------------------------------

// Unique account IDs
CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (a:Account) REQUIRE a.account_id IS UNIQUE;

// Unique transaction IDs
CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS
FOR (t:Transaction) REQUIRE t.transaction_id IS UNIQUE;

// Unique customer IDs
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;

// -----------------------------------------------------------------------------
// Indexes - Optimize query performance
// -----------------------------------------------------------------------------

// Index on account IBAN for fast lookups
CREATE INDEX account_iban_index IF NOT EXISTS
FOR (a:Account) ON (a.iban);

// Index on transaction timestamp for time-range queries
CREATE INDEX transaction_timestamp_index IF NOT EXISTS
FOR ()-[t:TRANSACTION]-() ON (t.timestamp);

// Index on transaction amount for threshold queries
CREATE INDEX transaction_amount_index IF NOT EXISTS
FOR ()-[t:TRANSACTION]-() ON (t.amount);

// Index on customer name for search
CREATE INDEX customer_name_index IF NOT EXISTS
FOR (c:Customer) ON (c.name);

// Composite index for fraud detection queries
CREATE INDEX transaction_fraud_index IF NOT EXISTS
FOR ()-[t:TRANSACTION]-() ON (t.is_flagged, t.fraud_type);

// -----------------------------------------------------------------------------
// Sample Query Templates (for reference)
// -----------------------------------------------------------------------------

// Detect circular transactions (depth 3-6)
// MATCH path = (a:Account)-[:TRANSACTION*3..6]->(a)
// WHERE ALL(r IN relationships(path) WHERE r.timestamp > datetime() - duration('P7D'))
// RETURN path, length(path) as cycle_length
// ORDER BY cycle_length DESC
// LIMIT 100;

// Find high-value transaction chains
// MATCH path = (a:Account)-[t:TRANSACTION*2..4]->(b:Account)
// WHERE ALL(r IN relationships(path) WHERE r.amount > 10000)
// RETURN a, b, path;

// Get account transaction summary
// MATCH (a:Account {account_id: $account_id})-[t:TRANSACTION]-(other:Account)
// RETURN a, collect(t) as transactions, collect(other) as connected_accounts;

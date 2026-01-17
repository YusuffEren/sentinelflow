#!/bin/bash
# =============================================================================
# Kafka Topic Initialization Script
# =============================================================================
# Run this after Kafka is up to create required topics

KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"

echo "🚀 Creating Kafka topics..."

# Create transactions topic (high throughput)
docker exec sentinelflow-kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic transactions \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

# Create alerts topic
docker exec sentinelflow-kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic alerts \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --if-not-exists

# Create fraud-detected topic for flagged transactions
docker exec sentinelflow-kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic fraud-detected \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo "✅ Kafka topics created successfully!"

# List all topics
echo ""
echo "📋 Current topics:"
docker exec sentinelflow-kafka kafka-topics --list --bootstrap-server kafka:29092

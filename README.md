# SentinelFlow

**Real-Time Financial Fraud Detection System**

A cloud-native, microservices-based fraud detection platform designed for detecting complex financial fraud patterns including Money Laundering Rings, Mule Accounts, Impossible Travel, and AI-detected anomalies.


---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Fraud Detection Engines](#fraud-detection-engines)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Performance](#performance)
- [License](#license)

---

## Architecture

```
+-----------------------------------------------------------------------------+
|                         SentinelFlow Architecture                           |
+-----------------------------------------------------------------------------+
|                                                                             |
|   +-------------+     +-------------+     +--------------------------+      |
|   |  Generator  |---->|    Kafka    |---->|     Fraud Detectors      |      |
|   | (Synthetic  |     | (Streaming) |     |  +--------------------+  |      |
|   |   Data)     |     |             |     |  | Circular (Neo4j)   |  |      |
|   +-------------+     +-------------+     |  | Travel (Redis Geo) |  |      |
|                             |             |  | NLP (Blacklist)    |  |      |
|                             |             |  | AI (IsolationForest)|  |      |
|                             v             |  +--------------------+  |      |
|                       +----------+        +-----------+--------------+      |
|                       | Kafka UI |                    |                     |
|                       +----------+                    v                     |
|                                           +----------------------+          |
|   +-------------+     +-------------+     |     Dashboard        |          |
|   |    Neo4j    |<--->|    Redis    |<--->|    (Streamlit)       |          |
|   |   (Graph)   |     | (Cache/Geo) |     |  +----------------+  |          |
|   +-------------+     +-------------+     |  | Network Graph  |  |          |
|         |                   |             |  | Alert Feed     |  |          |
|         v                   v             |  | Statistics     |  |          |
|   +-------------+     +-------------+     |  +----------------+  |          |
|   |Neo4j Browser|     |Redis Cmdr   |     +----------------------+          |
|   +-------------+     +-------------+                                       |
|                                                                             |
+-----------------------------------------------------------------------------+
```

---

## Features

### Fraud Detection Engines

| Engine | Technology | Detection Pattern |
|--------|------------|-------------------|
| Circular Ring Detection | Neo4j Graph Database | A -> B -> C -> A money flow patterns |
| Impossible Travel | Redis Geo-Spatial | Transactions from impossible locations |
| NLP Blacklist | Keyword Matching | Suspicious terms in descriptions |
| AI Anomaly Detection | Isolation Forest (sklearn) | Statistically unusual transaction amounts |

### System Capabilities

- Real-time streaming with Apache Kafka
- Sub-100ms detection latency
- Horizontal scalability
- SOC-style monitoring dashboard
- Configurable detection thresholds
- Multi-language support (Turkish/English)

---

## Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Message Streaming | Apache Kafka | 7.5.0 |
| Graph Database | Neo4j | 5.15.0 |
| Cache / Geo-Spatial | Redis | 7.2 |
| ML Framework | scikit-learn | 1.4+ |
| Dashboard | Streamlit | 1.30+ |
| Language | Python | 3.9+ |
| Container Runtime | Docker | 20.10+ |

---

## Prerequisites

- Docker and Docker Compose v2.0+
- Python 3.9 or higher
- 8GB RAM minimum (16GB recommended)
- Git

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/sentinelflow.git
cd sentinelflow
```

### 2. Start Infrastructure Services

```bash
docker-compose up -d
```

Verify all services are running:

```bash
docker-compose ps
```

Expected output:

| Container | Port | Status |
|-----------|------|--------|
| sentinelflow-kafka | 9092 | Running |
| sentinelflow-zookeeper | 2181 | Running |
| sentinelflow-neo4j | 7474, 7687 | Running |
| sentinelflow-redis | 6379 | Running |
| sentinelflow-kafka-ui | 8080 | Running |
| sentinelflow-redis-commander | 8081 | Running |

### 3. Install Python Dependencies

```bash
# Create virtual environment (optional but recommended)
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/macOS

# Install the package
pip install -e ".[dev]"
```

### 4. Configure Environment

```bash
cp .env.example .env
# Edit .env file with your configurations
```

---

## Usage

### Start Transaction Generator

Generates synthetic financial transactions with configurable fraud patterns:

```bash
sentinelflow-generate
```

Options:
- `--batch-size`: Number of transactions per batch (default: 100)
- `--delay`: Delay between batches in seconds (default: 1.0)
- `--fraud-ratio`: Percentage of fraudulent transactions (default: 0.05)

### Start Fraud Detector

Processes transactions and detects fraud in real-time:

```bash
python -m sentinelflow.processor.detector
```

### Start Dashboard

Launches the SOC-style monitoring dashboard:

```bash
streamlit run src/sentinelflow/dashboard/app.py
```

Access at: http://localhost:8501

---

## Fraud Detection Engines

### 1. Circular Ring Detection (Neo4j)

Detects money laundering patterns where funds flow in a circle.

```cypher
MATCH path = (a:Account)-[:TRANSACTION*3..6]->(a)
WHERE ALL(r IN relationships(path) WHERE r.timestamp > datetime() - duration('P7D'))
RETURN path
```

**Example Pattern:**
```
Account_A --5000 TL--> Account_B --4500 TL--> Account_C --4000 TL--> Account_A
```

**Severity:** Critical

### 2. Impossible Travel Detection (Redis)

Flags transactions from geographically impossible locations within short time windows.

**Detection Logic:**
- Calculates distance between consecutive transaction locations
- Computes required travel speed
- Flags if speed exceeds 900 km/h (commercial jet speed)

**Example Alert:**
```
Istanbul (12:00) -> Berlin (12:10)
Distance: 1,500 km | Time: 10 min | Required Speed: 9,000 km/h
Status: IMPOSSIBLE
```

**Severity:** High

### 3. NLP Blacklist Detection

Scans transaction descriptions for suspicious keywords:

| Category | Keywords |
|----------|----------|
| Gambling | bahis, casino, kumar, poker, bet365 |
| Cryptocurrency | kripto, bitcoin, btc, ethereum, usdt |
| Anonymity | offshore, anonim, gizli, anonymous |
| Urgency | acil, urgent, hemen, immediately |

**Severity:** Medium to Critical

### 4. AI Anomaly Detection (Isolation Forest)

Uses unsupervised machine learning to detect statistically unusual transactions.

**Algorithm:**
- Maintains sliding window of last 1,000 transaction amounts
- Trains IsolationForest with 5% contamination rate
- Flags transactions where: `prediction == -1 AND amount > 50,000 TL`

**Detection Parameters:**
| Parameter | Value |
|-----------|-------|
| Buffer Size | 1,000 transactions |
| Contamination | 5% |
| Amount Threshold | 50,000 TL |
| Min Training Samples | 100 |

**Severity:** High

---

## Project Structure

```
sentinelflow/
├── docker-compose.yml          # Infrastructure orchestration
├── pyproject.toml              # Python dependencies and tooling
├── .env.example                # Environment configuration template
├── README.md                   # This file
│
├── src/
│   └── sentinelflow/
│       ├── __init__.py
│       ├── config/             # Configuration management
│       │   ├── __init__.py
│       │   └── settings.py
│       │
│       ├── generator/          # Synthetic data generation
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── main.py
│       │   ├── models.py
│       │   └── patterns.py
│       │
│       ├── processor/          # Fraud detection engines
│       │   ├── __init__.py
│       │   ├── detector.py     # Main detector service
│       │   ├── graph_engine.py # Neo4j integration
│       │   └── redis_geo.py    # Redis geo-spatial
│       │
│       ├── ingestor/           # Kafka producers
│       │   ├── __init__.py
│       │   └── producer.py
│       │
│       └── dashboard/          # Streamlit visualization
│           ├── __init__.py
│           └── app.py
│
├── scripts/                    # Utility scripts
│   ├── init_kafka_topics.sh
│   └── init_neo4j_schema.cypher
│
└── tests/                      # Test suite
    └── __init__.py
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | localhost:9092 |
| `KAFKA_TOPIC_TRANSACTIONS` | Input topic for transactions | transactions |
| `KAFKA_TOPIC_ALERTS` | Output topic for fraud alerts | alerts |
| `NEO4J_URI` | Neo4j connection URI | bolt://localhost:7687 |
| `NEO4J_USER` | Neo4j username | neo4j |
| `NEO4J_PASSWORD` | Neo4j password | sentinelflow_secret_2024 |
| `REDIS_HOST` | Redis server host | localhost |
| `REDIS_PORT` | Redis server port | 6379 |
| `CIRCULAR_TRANSACTION_MIN_DEPTH` | Min ring depth | 3 |
| `CIRCULAR_TRANSACTION_MAX_DEPTH` | Max ring depth | 6 |
| `IMPOSSIBLE_TRAVEL_MAX_SPEED_KMH` | Max travel speed threshold | 900 |

---

## API Reference

### Service Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | Kafka cluster monitoring |
| Neo4j Browser | http://localhost:7474 | Graph database UI |
| Redis Commander | http://localhost:8081 | Redis data explorer |
| Dashboard | http://localhost:8501 | Fraud monitoring dashboard |

### Kafka Topics

| Topic | Purpose | Message Format |
|-------|---------|----------------|
| `transactions` | Incoming transaction stream | JSON |
| `fraud_alerts` | Detected fraud alerts | JSON |

### Transaction Message Schema

```json
{
  "transaction_id": "uuid",
  "sender_iban": "TR...",
  "sender_name": "string",
  "sender_city": "string",
  "receiver_iban": "TR...",
  "receiver_name": "string",
  "receiver_city": "string",
  "amount": 1000.00,
  "currency": "TRY",
  "description": "string",
  "timestamp": "2026-01-17T12:00:00Z"
}
```

### Fraud Alert Message Schema

```json
{
  "alert_id": "ALERT-XXXXXXXXXXXX",
  "fraud_type": "circular_ring|impossible_travel|blacklist_keyword|ai_detected_anomaly",
  "severity": "low|medium|high|critical",
  "confidence": 0.95,
  "transaction_id": "uuid",
  "sender_iban": "TR...",
  "receiver_iban": "TR...",
  "amount": 1000.00,
  "description": "string",
  "evidence": {},
  "detected_at": "2026-01-17T12:00:00Z"
}
```

---

## Performance

### Target Metrics

| Metric | Target | Actual |
|--------|--------|--------|
| Transaction Throughput | 10,000 tx/sec | - |
| Detection Latency | < 100ms | - |
| Graph Query Time | < 50ms | - |
| Dashboard Refresh | 1 second | - |

### Resource Requirements

| Component | CPU | Memory |
|-----------|-----|--------|
| Kafka | 2 cores | 2GB |
| Neo4j | 2 cores | 2GB |
| Redis | 1 core | 512MB |
| Detector Service | 2 cores | 1GB |
| Dashboard | 1 core | 512MB |

---

## Development

### Run Tests

```bash
pytest
```

### Code Formatting

```bash
black src/ tests/
ruff check src/ tests/
```

### Type Checking

```bash
mypy src/
```

---





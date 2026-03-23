#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Demo Transaction Producer
# =============================================================================
"""
Generates synthetic fraud and legitimate transactions for demo purposes.

Usage:
    python scripts/run_demo.py
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Any

from faker import Faker
from loguru import logger

try:
    from confluent_kafka import Producer

    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    logger.warning("confluent_kafka not available")

# Initialize Faker with Turkish locale
fake = Faker("tr_TR")
Faker.seed(42)

# Turkish cities for realistic demo
TURKISH_CITIES = [
    "Istanbul",
    "Ankara",
    "Izmir",
    "Bursa",
    "Antalya",
    "Adana",
    "Konya",
    "Gaziantep",
    "Mersin",
    "Diyarbakır",
    "Kayseri",
    "Eskişehir",
    "Samsun",
    "Trabzon",
    "Denizli",
]

# Suspicious keywords for blacklist detection
SUSPICIOUS_KEYWORDS = [
    "komisyon",
    "ödül",
    "casino",
    "bahis",
    "yurtdışı transfer",
    "acil",
    "kara para",
    "kaçak",
]


def generate_iban() -> str:
    """Generate random Turkish IBAN."""
    bank_code = random.choice(["0001", "0004", "0006", "0010", "0012", "0015"])
    account = "".join([str(random.randint(0, 9)) for _ in range(16)])
    return f"TR{random.randint(10, 99)}{bank_code}0{account}"


def generate_transaction(is_fraud: bool = False) -> dict[str, Any]:
    """Generate a synthetic transaction."""
    tx_id = f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"

    sender_city = random.choice(TURKISH_CITIES)
    receiver_city = random.choice(TURKISH_CITIES)

    # Base transaction
    tx = {
        "transaction_id": tx_id,
        "sender_iban": generate_iban(),
        "sender_name": fake.name(),
        "sender_city": sender_city,
        "receiver_iban": generate_iban(),
        "receiver_name": fake.name(),
        "receiver_city": receiver_city,
        "currency": "TRY",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "latitude": 36.0 + random.random() * 6,  # Turkey latitude range
        "longitude": 26.0 + random.random() * 18,  # Turkey longitude range
    }

    if is_fraud:
        # Generate suspicious transaction
        fraud_type = random.choice(["high_amount", "suspicious_desc", "odd_hour", "velocity"])

        if fraud_type == "high_amount":
            tx["amount"] = round(random.uniform(50000, 500000), 2)
            tx["description"] = "Yüksek tutarlı transfer"
        elif fraud_type == "suspicious_desc":
            tx["amount"] = round(random.uniform(1000, 20000), 2)
            tx["description"] = random.choice(SUSPICIOUS_KEYWORDS) + " ödemesi"
        elif fraud_type == "odd_hour":
            tx["amount"] = round(random.uniform(5000, 30000), 2)
            tx["description"] = "Gece transferi"
        else:  # velocity
            tx["amount"] = round(random.uniform(2000, 10000), 2)
            tx["description"] = "Hızlı işlem"
    else:
        # Normal transaction
        tx["amount"] = round(random.uniform(50, 5000), 2)
        tx["description"] = random.choice(
            [
                "Kira ödemesi",
                "Market alışverişi",
                "Fatura ödemesi",
                "Maaş transferi",
                "Arkadaşa borç",
                "Online alışveriş",
                "Elektrik faturası",
                "Su faturası",
            ]
        )

    return tx


def run_demo_producer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions",
    transactions_per_second: float = 5.0,
    fraud_rate: float = 0.05,
    duration_seconds: int = 300,
):
    """
    Run demo transaction producer.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic name
        transactions_per_second: Target TPS
        fraud_rate: Fraction of fraudulent transactions
        duration_seconds: How long to run (0 = forever)
    """
    if not HAS_KAFKA:
        logger.error("Kafka not available, cannot run demo")
        return

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "sentinelflow-demo",
        }
    )

    logger.info(
        f"Starting demo producer: {transactions_per_second} TPS, {fraud_rate*100:.1f}% fraud"
    )
    logger.info(f"Topic: {topic}, Duration: {duration_seconds}s")

    start_time = time.time()
    sent_count = 0
    fraud_count = 0

    try:
        while True:
            elapsed = time.time() - start_time

            if duration_seconds > 0 and elapsed >= duration_seconds:
                break

            # Determine if this should be a fraud transaction
            is_fraud = random.random() < fraud_rate
            tx = generate_transaction(is_fraud)

            # Send to Kafka
            try:
                value = json.dumps(tx).encode("utf-8")
                key = tx["transaction_id"].encode("utf-8")

                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                )
                producer.poll(0)

                sent_count += 1
                if is_fraud:
                    fraud_count += 1

                # Log progress every 50 transactions
                if sent_count % 50 == 0:
                    logger.info(
                        f"Sent {sent_count} transactions "
                        f"({fraud_count} fraud, {fraud_count/sent_count*100:.1f}%) "
                        f"| Rate: {sent_count/elapsed:.1f} TPS"
                    )

            except Exception as e:
                logger.error(f"Error sending transaction: {e}")

            # Sleep to maintain target TPS
            sleep_time = 1.0 / transactions_per_second
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Demo stopped by user")

    finally:
        producer.flush(timeout=10)
        elapsed = time.time() - start_time
        logger.success(
            f"Demo complete: {sent_count} transactions sent "
            f"({fraud_count} fraud) in {elapsed:.1f}s "
            f"({sent_count/elapsed:.1f} TPS)"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SentinelFlow Demo Producer")
    parser.add_argument("--tps", type=float, default=5.0, help="Transactions per second")
    parser.add_argument("--fraud-rate", type=float, default=0.05, help="Fraud rate (0-1)")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=forever)")
    parser.add_argument("--bootstrap", type=str, default="localhost:9092", help="Kafka servers")
    parser.add_argument("--topic", type=str, default="transactions", help="Kafka topic")

    args = parser.parse_args()

    run_demo_producer(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        transactions_per_second=args.tps,
        fraud_rate=args.fraud_rate,
        duration_seconds=args.duration,
    )

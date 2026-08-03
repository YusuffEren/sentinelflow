# =============================================================================
# SentinelFlow - Dataset Replay Producer
# =============================================================================
"""
Replays dataset transactions through Kafka for real-time simulation.

This allows replaying historical or synthetic fraud datasets through the
full SentinelFlow pipeline (Kafka → Detector → Alerts) for testing
and benchmarking.

Usage:
    producer = DatasetReplayProducer(bootstrap_servers="localhost:9092")
    producer.replay_dataframe(df, speed_multiplier=10.0)
"""

from __future__ import annotations

import json
import time
from typing import Any

import pandas as pd
from loguru import logger

try:
    from confluent_kafka import KafkaException, Producer

    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    logger.warning("confluent_kafka not available, replay producer disabled")


class DatasetReplayProducer:
    """
    Replays dataset transactions through Kafka topic.

    Can replay at configurable speed (e.g., 10x for benchmarking)
    or as fast as possible for stress testing.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
        client_id: str = "sentinelflow-replay",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: Any = None
        self._delivered = 0
        self._errors = 0

        if HAS_KAFKA:
            self._producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": client_id,
                    "acks": "all",
                    "linger.ms": 5,
                    "compression.type": "snappy",
                    "batch.size": 32768,
                }
            )

        logger.info(f"DatasetReplayProducer initialized (topic={topic})")

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Kafka delivery callback."""
        if err:
            self._errors += 1
            logger.error(f"Delivery failed: {err}")
        else:
            self._delivered += 1

    def replay_dataframe(
        self,
        df: pd.DataFrame,
        speed_multiplier: float = 1.0,
        batch_size: int = 100,
        max_rows: int | None = None,
    ) -> dict[str, int]:
        """
        Replay transactions from a DataFrame through Kafka.

        Args:
            df: DataFrame with transaction data
            speed_multiplier: Speed multiplier (10.0 = 10x faster)
            batch_size: Flush interval
            max_rows: Maximum rows to replay (None = all)

        Returns:
            Dict with replay statistics
        """
        if self._producer is None:
            logger.error("Kafka producer not available")
            return {"delivered": 0, "errors": 0, "total": 0}

        rows = df.head(max_rows) if max_rows else df
        total = len(rows)
        self._delivered = 0
        self._errors = 0

        logger.info(f"Replaying {total} transactions (speed: {speed_multiplier}x)")
        start_time = time.time()

        for i, (_, row) in enumerate(rows.iterrows()):
            # Convert row to transaction dict
            tx_data = self._row_to_transaction(row)

            try:
                value = json.dumps(tx_data).encode("utf-8")
                key = tx_data.get("transaction_id", f"replay-{i}").encode("utf-8")

                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )

                # Periodic flush
                if (i + 1) % batch_size == 0:
                    self._producer.flush(timeout=5)
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {i+1}/{total} ({(i+1)/total*100:.1f}%) "
                        f"Rate: {rate:.0f} tx/s"
                    )

                # Delay for realistic replay
                if speed_multiplier > 0 and speed_multiplier < 1000:
                    time.sleep(0.01 / speed_multiplier)

                self._producer.poll(0)

            except Exception as e:
                logger.error(f"Replay error at row {i}: {e}")
                self._errors += 1

        # Final flush
        self._producer.flush(timeout=30)

        elapsed = time.time() - start_time
        stats = {
            "delivered": self._delivered,
            "errors": self._errors,
            "total": total,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_tps": round(total / elapsed, 2) if elapsed > 0 else 0,
        }

        logger.info(
            f"Replay complete: {stats['delivered']}/{stats['total']} delivered, "
            f"{stats['errors']} errors, {stats['throughput_tps']} tx/s"
        )

        return stats

    def replay_transactions(
        self,
        transactions: list[dict[str, Any]],
        speed_multiplier: float = 1.0,
    ) -> dict[str, int]:
        """
        Replay a list of transaction dicts through Kafka.

        Args:
            transactions: List of transaction dictionaries
            speed_multiplier: Speed multiplier

        Returns:
            Dict with replay statistics
        """
        df = pd.DataFrame(transactions)
        return self.replay_dataframe(df, speed_multiplier)

    @staticmethod
    def _row_to_transaction(row: pd.Series) -> dict[str, Any]:
        """Convert DataFrame row to transaction dict."""
        tx: dict[str, Any] = {}

        # Map common column names
        col_map = {
            "transaction_id": ["transaction_id", "TransactionID", "txid"],
            "sender_iban": ["sender_iban", "nameOrig", "sender"],
            "sender_name": ["sender_name", "nameOrig"],
            "sender_city": ["sender_city", "origCity"],
            "receiver_iban": ["receiver_iban", "nameDest", "receiver"],
            "receiver_name": ["receiver_name", "nameDest"],
            "receiver_city": ["receiver_city", "destCity"],
            "amount": ["amount", "Amount", "newbalanceOrig"],
            "description": ["description", "desc", "type"],
            "timestamp": ["timestamp", "step", "time"],
        }

        for field, candidates in col_map.items():
            for col in candidates:
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    if field == "amount":
                        tx[field] = float(val)
                    else:
                        tx[field] = str(val)
                    break
            else:
                tx[field] = "" if field != "amount" else 0.0

        return tx

    def close(self) -> None:
        """Close the producer."""
        if self._producer:
            self._producer.flush(timeout=10)
            self._producer = None

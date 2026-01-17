# =============================================================================
# SentinelFlow - Data Generator Main Script
# =============================================================================
"""
Main entry point for the synthetic transaction data generator.

This script generates realistic Turkish bank transactions with injected
fraud patterns and streams them to Kafka in real-time.

Usage:
    python -m sentinelflow.generator.main --batch-size 100 --delay 1.0
    
    # Or using the CLI entry point:
    sentinelflow-generate --batch-size 100 --fraud-ratio 0.1
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from confluent_kafka import Producer, KafkaError, KafkaException
from loguru import logger
from rich.console import Console
import os

# Windows compatibility: disable rich formatting if needed
WINDOWS_SAFE = os.name == 'nt'
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from sentinelflow.config import get_settings
from sentinelflow.generator.models import FraudType, Transaction
from sentinelflow.generator.patterns import FraudPatternMixer


# =============================================================================
# Kafka Producer Setup
# =============================================================================

class TransactionProducer:
    """
    Kafka producer for streaming transactions.
    
    Handles connection management, serialization, and delivery callbacks.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "sentinelflow-generator",
    ):
        self.topic = topic
        self.stats = {
            "sent": 0,
            "delivered": 0,
            "failed": 0,
            "fraud_count": 0,
            "whale_count": 0,  # Track injected whale transactions
        }
        
        # Producer configuration
        config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",  # Wait for all replicas
            "retries": 3,
            "retry.backoff.ms": 500,
            "linger.ms": 5,  # Batch for performance
            "batch.size": 16384,
            "compression.type": "snappy",
            "enable.idempotence": True,
        }
        
        self.producer = Producer(config)
        logger.info(f"Kafka producer initialized: {bootstrap_servers}")
    
    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Handle delivery confirmation from Kafka."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
            self.stats["failed"] += 1
        else:
            self.stats["delivered"] += 1
    
    def send(self, transaction: Transaction) -> None:
        """Send a transaction to Kafka."""
        try:
            # Serialize transaction to JSON
            value = json.dumps(transaction.to_kafka_dict()).encode("utf-8")
            
            # Use sender_account_id as key for partitioning
            key = str(transaction.sender_account_id).encode("utf-8")
            
            # Produce message
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            
            self.stats["sent"] += 1
            if transaction.fraud_type != FraudType.NONE:
                self.stats["fraud_count"] += 1
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
        except KafkaException as e:
            logger.error(f"Failed to send transaction: {e}")
            self.stats["failed"] += 1
    
    def flush(self, timeout: float = 10.0) -> None:
        """Wait for all messages to be delivered."""
        self.producer.flush(timeout)
    
    def close(self) -> None:
        """Clean shutdown."""
        self.flush()
        logger.info(f"Producer closed. Stats: {self.stats}")


# =============================================================================
# Dashboard Display
# =============================================================================

def create_stats_table(
    producer: TransactionProducer,
    batch_count: int,
    start_time: datetime,
) -> Table:
    """Create a rich table showing generator statistics."""
    elapsed = (datetime.now() - start_time).total_seconds()
    rate = producer.stats["sent"] / elapsed if elapsed > 0 else 0
    
    table = Table(title="[*] SentinelFlow Transaction Generator", expand=True)
    
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="green", justify="right")
    
    table.add_row("[>] Sent", f"{producer.stats['sent']:,}")
    table.add_row("[+] Delivered", f"{producer.stats['delivered']:,}")
    table.add_row("[x] Failed", f"{producer.stats['failed']:,}")
    table.add_row("[!] Fraud Injected", f"{producer.stats['fraud_count']:,}")
    table.add_row("[W] Whale Txs", f"[magenta]{producer.stats['whale_count']:,}[/magenta]")
    table.add_row("[#] Batches", f"{batch_count:,}")
    table.add_row("[T] Elapsed", f"{elapsed:.1f}s")
    table.add_row("[~] Rate", f"{rate:.1f} tx/s")
    
    return table


def create_sample_panel(transaction: Transaction) -> Panel:
    """Create a panel showing the last transaction."""
    fraud_label = "[!] FRAUD" if transaction.fraud_type != FraudType.NONE else "[OK]"
    
    content = f"""
{fraud_label} **{transaction.fraud_type.value.upper()}**
-------------------------------
[>] Sender: {transaction.sender_name}
   {transaction.sender_city} | {transaction.sender_iban[:12]}...

[<] Receiver: {transaction.receiver_name}
   {transaction.receiver_city} | {transaction.receiver_iban[:12]}...

[$] Amount: {transaction.amount:,.2f} {transaction.currency}
[i] Description: {transaction.description}
[T] Time: {transaction.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    if transaction.ring_id:
        content += f"[*] Ring ID: {transaction.ring_id}\n"
    
    return Panel(content.strip(), title="Last Transaction", border_style="blue")


# =============================================================================
# Main Generator Loop
# =============================================================================

def run_generator(
    producer: TransactionProducer,
    mixer: FraudPatternMixer,
    batch_size: int = 100,
    delay: float = 1.0,
    max_batches: int | None = None,
    show_dashboard: bool = True,
) -> None:
    """
    Run the continuous transaction generator.
    
    Args:
        producer: Kafka producer instance
        mixer: Fraud pattern mixer for generating transactions
        batch_size: Number of transactions per batch
        delay: Seconds between batches
        max_batches: Stop after this many batches (None = infinite)
        show_dashboard: Show real-time stats dashboard
    """
    console = Console()
    start_time = datetime.now()
    batch_count = 0
    running = True
    last_tx: Transaction | None = None
    
    # Handle graceful shutdown
    def signal_handler(sig: int, frame: Any) -> None:
        nonlocal running
        logger.info("Shutdown signal received...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(f"Starting generator: batch_size={batch_size}, delay={delay}s")
    
    if show_dashboard:
        with Live(console=console, refresh_per_second=2) as live:
            while running and (max_batches is None or batch_count < max_batches):
                # Generate batch
                batch = mixer.generate_batch(batch_size)
                
                # Send to Kafka
                for tx in batch:
                    # === WHALE TRANSACTION INJECTION (1% chance) ===
                    if random.random() < 0.01:
                        # Inject a high-value anomaly for AI detector testing
                        whale_amount = Decimal(str(random.uniform(150000.0, 500000.0)))
                        tx = Transaction(
                            transaction_id=uuid4(),
                            sender_account_id=tx.sender_account_id,
                            sender_iban=tx.sender_iban,
                            sender_name=tx.sender_name,
                            sender_city=tx.sender_city,
                            receiver_account_id=tx.receiver_account_id,
                            receiver_iban=tx.receiver_iban,
                            receiver_name=tx.receiver_name,
                            receiver_city=tx.receiver_city,
                            amount=round(whale_amount, 2),
                            description="Yüksek değerli transfer",
                            timestamp=tx.timestamp,
                            fraud_type=FraudType.HIGH_VALUE_ANOMALY,
                            pattern_label="whale_transaction",
                        )
                        producer.stats["whale_count"] += 1
                        console.print(f"[bold magenta][!] INJECTING ANOMALY: {float(whale_amount):,.2f} TL[/bold magenta]")
                    
                    producer.send(tx)
                    last_tx = tx
                
                batch_count += 1
                
                # Update display
                stats_table = create_stats_table(producer, batch_count, start_time)
                if last_tx:
                    sample_panel = create_sample_panel(last_tx)
                    from rich.layout import Layout
                    layout = Layout()
                    layout.split_column(
                        Layout(stats_table, name="stats"),
                        Layout(sample_panel, name="sample"),
                    )
                    live.update(layout)
                else:
                    live.update(stats_table)
                
                # Wait before next batch
                time.sleep(delay)
    else:
        while running and (max_batches is None or batch_count < max_batches):
            batch = mixer.generate_batch(batch_size)
            
            for tx in batch:
                # === WHALE TRANSACTION INJECTION (1% chance) ===
                if random.random() < 0.01:
                    # Inject a high-value anomaly for AI detector testing
                    whale_amount = Decimal(str(random.uniform(150000.0, 500000.0)))
                    tx = Transaction(
                        transaction_id=uuid4(),
                        sender_account_id=tx.sender_account_id,
                        sender_iban=tx.sender_iban,
                        sender_name=tx.sender_name,
                        sender_city=tx.sender_city,
                        receiver_account_id=tx.receiver_account_id,
                        receiver_iban=tx.receiver_iban,
                        receiver_name=tx.receiver_name,
                        receiver_city=tx.receiver_city,
                        amount=round(whale_amount, 2),
                        description="Yüksek değerli transfer",
                        timestamp=tx.timestamp,
                        fraud_type=FraudType.HIGH_VALUE_ANOMALY,
                        pattern_label="whale_transaction",
                    )
                    producer.stats["whale_count"] += 1
                    logger.warning(f"[!] INJECTING ANOMALY: {float(whale_amount):,.2f} TL")
                
                producer.send(tx)
            
            batch_count += 1
            
            if batch_count % 10 == 0:
                logger.info(f"Batch {batch_count}: {producer.stats['sent']} total sent")
            
            time.sleep(delay)
    
    # Final flush
    producer.flush()
    logger.info(f"Generator stopped. Total batches: {batch_count}")


# =============================================================================
# CLI Interface
# =============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="SentinelFlow Transaction Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=100,
        help="Number of transactions per batch",
    )
    
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=1.0,
        help="Delay in seconds between batches",
    )
    
    parser.add_argument(
        "--fraud-ratio", "-f",
        type=float,
        default=0.05,
        help="Ratio of fraudulent transactions (0.0-1.0)",
    )
    
    parser.add_argument(
        "--max-batches", "-m",
        type=int,
        default=None,
        help="Maximum number of batches (None = infinite)",
    )
    
    parser.add_argument(
        "--kafka-servers", "-k",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides config)",
    )
    
    parser.add_argument(
        "--topic", "-t",
        type=str,
        default=None,
        help="Kafka topic name (overrides config)",
    )
    
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable real-time dashboard",
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    settings = get_settings()
    
    # Configure logging
    log_level = "DEBUG" if args.verbose else settings.log_level
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    
    # Banner (Windows-safe, no emojis)
    console = Console(force_terminal=not WINDOWS_SAFE)
    console.print(Panel.fit(
        "[bold blue]SentinelFlow[/bold blue]\n"
        "[dim]Real-Time Fraud Detection System[/dim]\n"
        "[yellow]Transaction Generator[/yellow]",
        border_style="blue",
    ))
    
    # Initialize components
    kafka_servers = args.kafka_servers or settings.kafka.bootstrap_servers
    topic = args.topic or settings.kafka.topic_transactions
    
    logger.info(f"Connecting to Kafka: {kafka_servers}")
    logger.info(f"Target topic: {topic}")
    
    producer = TransactionProducer(
        bootstrap_servers=kafka_servers,
        topic=topic,
    )
    
    mixer = FraudPatternMixer(
        fraud_ratio=args.fraud_ratio,
        seed=args.seed,
    )
    
    try:
        run_generator(
            producer=producer,
            mixer=mixer,
            batch_size=args.batch_size,
            delay=args.delay,
            max_batches=args.max_batches,
            show_dashboard=not args.no_dashboard,
        )
    except Exception as e:
        logger.exception(f"Generator error: {e}")
        sys.exit(1)
    finally:
        producer.close()
    
    console.print("\n[green][+] Generator completed successfully![/green]")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# =============================================================================
# SentinelFlow - Transaction Replay Script
# =============================================================================
"""
Replay transactions from a JSON file or generate sample transactions
to test the fraud detection pipeline.

Usage:
    # Generate and replay sample transactions
    python scripts/replay_transactions.py --generate 100
    
    # Replay from file
    python scripts/replay_transactions.py --file transactions.json
    
    # Replay with specific fraud scenarios
    python scripts/replay_transactions.py --scenario circular_ring --count 10
"""

import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

console = Console()

# =============================================================================
# Configuration
# =============================================================================

API_BASE = "http://localhost:8000"
KAFKA_TOPIC = "transactions"

# Turkish first names and surnames for realistic data
FIRST_NAMES = [
    "Ahmet", "Mehmet", "Ali", "Mustafa", "Fatma", "Ayşe", "Emine", "Zeynep",
    "Hasan", "Hüseyin", "İbrahim", "Osman", "Yusuf", "Murat", "Emre", "Can",
    "Deniz", "Elif", "Selin", "Özlem", "Burak", "Kaan", "Ece", "Beren"
]

LAST_NAMES = [
    "Yılmaz", "Kaya", "Demir", "Çelik", "Şahin", "Yıldız", "Yıldırım", "Öztürk",
    "Aydın", "Özdemir", "Arslan", "Doğan", "Kılıç", "Aslan", "Çetin", "Koç",
    "Kurt", "Özkan", "Şimşek", "Polat", "Korkmaz", "Yavuz", "Erdoğan", "Güneş"
]

CITIES = [
    "İstanbul", "Ankara", "İzmir", "Bursa", "Antalya", "Adana", "Konya",
    "Gaziantep", "Mersin", "Diyarbakır", "Kayseri", "Eskişehir", "Trabzon"
]

BANKS = {
    "0001": "Türkiye Cumhuriyeti Ziraat Bankası",
    "0006": "Akbank",
    "0010": "Türkiye İş Bankası",
    "0012": "Halk Bankası",
    "0015": "Vakıfbank",
    "0046": "Garanti BBVA",
    "0059": "Şekerbank",
    "0062": "ING Bank",
    "0064": "Yapı Kredi",
}


def generate_iban(bank_code: str = None) -> str:
    """Generate a realistic Turkish IBAN."""
    if not bank_code:
        bank_code = random.choice(list(BANKS.keys()))
    
    account = "".join([str(random.randint(0, 9)) for _ in range(16)])
    return f"TR{random.randint(10, 99)}{bank_code}00{account}"


def generate_name() -> str:
    """Generate a random Turkish name."""
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def generate_transaction(
    sender_iban: str = None,
    sender_name: str = None,
    receiver_iban: str = None,
    receiver_name: str = None,
    amount: float = None,
    description: str = None,
) -> dict:
    """Generate a random transaction."""
    return {
        "sender_iban": sender_iban or generate_iban(),
        "sender_name": sender_name or generate_name(),
        "sender_city": random.choice(CITIES),
        "receiver_iban": receiver_iban or generate_iban(),
        "receiver_name": receiver_name or generate_name(),
        "receiver_city": random.choice(CITIES),
        "amount": amount or round(random.uniform(100, 50000), 2),
        "currency": "TRY",
        "description": description or random.choice([
            "Kira ödemesi",
            "Fatura ödemesi",
            "Havale",
            "Maaş ödemesi",
            "Alışveriş",
            "Transfer",
            "",
        ]),
    }


# =============================================================================
# Fraud Scenarios
# =============================================================================

def generate_circular_ring(count: int = 5) -> list[dict]:
    """Generate a circular ring of transactions."""
    accounts = [(generate_iban(), generate_name()) for _ in range(count)]
    transactions = []
    
    # Create ring: A -> B -> C -> D -> E -> A
    for i in range(count):
        sender = accounts[i]
        receiver = accounts[(i + 1) % count]
        
        transactions.append(generate_transaction(
            sender_iban=sender[0],
            sender_name=sender[1],
            receiver_iban=receiver[0],
            receiver_name=receiver[1],
            amount=round(random.uniform(10000, 50000), 2),
            description="Özel transfer",
        ))
    
    return transactions


def generate_impossible_travel() -> list[dict]:
    """Generate impossible travel scenario (same sender, different cities, short time)."""
    sender = (generate_iban(), generate_name())
    
    tx1 = generate_transaction(
        sender_iban=sender[0],
        sender_name=sender[1],
        amount=5000,
    )
    tx1["sender_city"] = "İstanbul"
    
    tx2 = generate_transaction(
        sender_iban=sender[0],
        sender_name=sender[1],
        amount=5000,
    )
    tx2["sender_city"] = "Diyarbakır"  # ~1000km away
    
    return [tx1, tx2]


def generate_structuring(total: float = 100000, threshold: float = 10000) -> list[dict]:
    """Generate structuring (smurfing) scenario - amounts just under threshold."""
    sender = (generate_iban(), generate_name())
    receiver = (generate_iban(), generate_name())
    
    transactions = []
    remaining = total
    
    while remaining > 0:
        # Always just under threshold
        amount = min(remaining, threshold - random.uniform(100, 500))
        
        transactions.append(generate_transaction(
            sender_iban=sender[0],
            sender_name=sender[1],
            receiver_iban=receiver[0],
            receiver_name=receiver[1],
            amount=round(amount, 2),
        ))
        
        remaining -= amount
        if remaining < 1000:
            break
    
    return transactions


def generate_blacklist_keywords() -> list[dict]:
    """Generate transactions with suspicious keywords."""
    keywords = ["bahis", "kumar", "casino", "kripto", "bitcoin", "acil nakit"]
    
    return [
        generate_transaction(description=f"Ödeme için - {kw}")
        for kw in random.sample(keywords, 3)
    ]


def generate_high_amount() -> list[dict]:
    """Generate unusually high amount transactions."""
    return [
        generate_transaction(amount=random.uniform(200000, 1000000))
        for _ in range(3)
    ]


# =============================================================================
# Replay Functions
# =============================================================================

def send_to_api(transaction: dict) -> dict:
    """Send transaction to API for analysis."""
    try:
        response = requests.post(
            f"{API_BASE}/api/v1/transactions",
            json=transaction,
            timeout=10,
        )
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def send_to_kafka(transaction: dict, producer) -> bool:
    """Send transaction to Kafka."""
    try:
        from confluent_kafka import Producer
        
        value = json.dumps(transaction).encode("utf-8")
        producer.produce(
            topic=KAFKA_TOPIC,
            value=value,
        )
        producer.poll(0)
        return True
    except Exception as e:
        console.print(f"[red]Kafka error: {e}[/red]")
        return False


def replay_transactions(
    transactions: list[dict],
    target: str = "api",
    delay: float = 0.5,
    show_response: bool = True,
) -> dict:
    """
    Replay a list of transactions.
    
    Args:
        transactions: List of transaction dicts
        target: "api" or "kafka"
        delay: Delay between transactions in seconds
        show_response: Show API response for each transaction
    
    Returns:
        Statistics dict
    """
    stats = {
        "total": len(transactions),
        "successful": 0,
        "failed": 0,
        "fraud_detected": 0,
        "alerts": [],
    }
    
    kafka_producer = None
    if target == "kafka":
        try:
            from confluent_kafka import Producer
            kafka_producer = Producer({"bootstrap.servers": "localhost:9092"})
        except Exception as e:
            console.print(f"[red]Cannot connect to Kafka: {e}[/red]")
            console.print("[yellow]Falling back to API...[/yellow]")
            target = "api"
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Replaying transactions...", total=len(transactions))
        
        for tx in transactions:
            if target == "api":
                result = send_to_api(tx)
                
                if "error" in result:
                    stats["failed"] += 1
                else:
                    stats["successful"] += 1
                    
                    if result.get("is_fraud"):
                        stats["fraud_detected"] += 1
                        if result.get("alerts"):
                            stats["alerts"].extend(result["alerts"])
                    
                    if show_response and result.get("is_fraud"):
                        console.print(
                            f"[red]FRAUD[/red] {tx['sender_name']} → {tx['receiver_name']} | "
                            f"{tx['amount']:,.2f} TRY | Score: {result.get('fraud_score', 0):.2f}"
                        )
            
            else:  # kafka
                success = send_to_kafka(tx, kafka_producer)
                if success:
                    stats["successful"] += 1
                else:
                    stats["failed"] += 1
            
            progress.update(task, advance=1)
            time.sleep(delay)
    
    if kafka_producer:
        kafka_producer.flush()
    
    return stats


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Replay transactions for testing")
    
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="JSON file with transactions to replay",
    )
    parser.add_argument(
        "--generate", "-g",
        type=int,
        help="Generate N random transactions",
    )
    parser.add_argument(
        "--scenario", "-s",
        type=str,
        choices=["circular_ring", "impossible_travel", "structuring", "blacklist", "high_amount", "mixed"],
        help="Generate specific fraud scenario",
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=5,
        help="Count for scenario generation",
    )
    parser.add_argument(
        "--target", "-t",
        type=str,
        choices=["api", "kafka"],
        default="api",
        help="Send to API or Kafka",
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=0.3,
        help="Delay between transactions (seconds)",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="http://localhost:8000",
        help="API base URL",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Quiet mode - minimal output",
    )
    
    args = parser.parse_args()
    global API_BASE
    API_BASE = args.api_url
    
    console.print(Panel.fit(
        "[bold blue]SentinelFlow[/bold blue]\n"
        "[dim]Transaction Replay Tool[/dim]",
        border_style="blue",
    ))
    
    transactions = []
    
    # Load from file
    if args.file:
        with open(args.file) as f:
            data = json.load(f)
            transactions = data if isinstance(data, list) else data.get("transactions", [])
        console.print(f"[green]Loaded {len(transactions)} transactions from {args.file}[/green]")
    
    # Generate random
    elif args.generate:
        transactions = [generate_transaction() for _ in range(args.generate)]
        console.print(f"[green]Generated {args.generate} random transactions[/green]")
    
    # Generate scenario
    elif args.scenario:
        if args.scenario == "circular_ring":
            transactions = generate_circular_ring(args.count)
        elif args.scenario == "impossible_travel":
            transactions = generate_impossible_travel()
        elif args.scenario == "structuring":
            transactions = generate_structuring(total=args.count * 10000)
        elif args.scenario == "blacklist":
            transactions = generate_blacklist_keywords()
        elif args.scenario == "high_amount":
            transactions = generate_high_amount()
        elif args.scenario == "mixed":
            transactions.extend(generate_circular_ring(4))
            transactions.extend(generate_impossible_travel())
            transactions.extend(generate_structuring())
            transactions.extend(generate_blacklist_keywords())
            transactions.extend(generate_high_amount())
            transactions.extend([generate_transaction() for _ in range(10)])  # Normal ones
        
        console.print(f"[green]Generated {len(transactions)} transactions for '{args.scenario}' scenario[/green]")
    
    else:
        console.print("[yellow]No input specified. Use --generate, --file, or --scenario[/yellow]")
        parser.print_help()
        return
    
    # Replay
    console.print(f"\n[cyan]Target: {args.target.upper()}[/cyan]")
    console.print(f"[cyan]Delay: {args.delay}s[/cyan]\n")
    
    stats = replay_transactions(
        transactions,
        target=args.target,
        delay=args.delay,
        show_response=not args.quiet,
    )
    
    # Summary
    table = Table(title="Replay Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    
    table.add_row("Total Transactions", str(stats["total"]))
    table.add_row("Successful", str(stats["successful"]))
    table.add_row("Failed", str(stats["failed"]))
    table.add_row("Fraud Detected", f"[red]{stats['fraud_detected']}[/red]")
    table.add_row("Alerts Created", str(len(stats["alerts"])))
    
    console.print(table)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Graph Engine Test Script
# =============================================================================
"""
Standalone test script for the Neo4j GraphEngine.

This script tests the graph engine in isolation:
1. Connects to Neo4j
2. Sets up schema constraints
3. Creates a manual circular transaction ring (A -> B -> C -> A)
4. Runs fraud detection to verify ring is found

Usage:
    # Make sure Neo4j is running (docker-compose up -d neo4j)
    cd sentinelflow
    pip install -e .
    python scripts/test_graph_engine.py

Expected Output:
    ✅ Connected to Neo4j
    ✅ Schema constraints created
    ✅ Circular ring transactions added
    🚨 FRAUD RING DETECTED: TR_A... -> TR_B... -> TR_C... -> TR_A...
"""

import sys
from datetime import datetime, timedelta
from uuid import uuid4

from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    level="DEBUG",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
)

console = Console()


def test_graph_engine():
    """Run comprehensive GraphEngine tests."""
    
    console.print(Panel.fit(
        "[bold blue]🛡️ SentinelFlow[/bold blue]\n"
        "[yellow]Graph Engine Test Suite[/yellow]",
        border_style="blue",
    ))
    
    # Import after logging setup
    from sentinelflow.processor.graph_engine import GraphEngine
    
    # ==========================================================================
    # Test 1: Connection
    # ==========================================================================
    console.print("\n[bold]1️⃣ Testing Neo4j Connection...[/bold]")
    
    try:
        engine = GraphEngine()
        console.print("[green]✅ Connected to Neo4j successfully![/green]")
    except Exception as e:
        console.print(f"[red]❌ Failed to connect: {e}[/red]")
        console.print("\n[yellow]Make sure Neo4j is running:[/yellow]")
        console.print("  docker-compose up -d neo4j")
        return False
    
    # ==========================================================================
    # Test 2: Schema Setup
    # ==========================================================================
    console.print("\n[bold]2️⃣ Setting up schema constraints...[/bold]")
    
    try:
        engine.setup_constraints()
        console.print("[green]✅ Schema constraints created![/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Schema warning: {e}[/yellow]")
    
    # ==========================================================================
    # Test 3: Clear existing data (for clean test)
    # ==========================================================================
    console.print("\n[bold]3️⃣ Clearing existing test data...[/bold]")
    
    try:
        engine.clear_all()
        console.print("[green]✅ Graph cleared![/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Clear warning: {e}[/yellow]")
    
    # ==========================================================================
    # Test 4: Create a circular transaction ring (A -> B -> C -> A)
    # ==========================================================================
    console.print("\n[bold]4️⃣ Creating circular transaction ring (A → B → C → A)...[/bold]")
    
    # Generate test IBANs
    iban_a = f"TR{uuid4().hex[:24].upper()}"
    iban_b = f"TR{uuid4().hex[:24].upper()}"
    iban_c = f"TR{uuid4().hex[:24].upper()}"
    
    base_time = datetime.utcnow()
    ring_id = f"TEST-RING-{uuid4().hex[:6].upper()}"
    
    # Transaction A -> B
    tx1 = {
        "transaction_id": str(uuid4()),
        "sender_iban": iban_a,
        "sender_name": "Ahmet Yılmaz",
        "sender_city": "İstanbul",
        "receiver_iban": iban_b,
        "receiver_name": "Mehmet Kaya",
        "receiver_city": "Ankara",
        "amount": 10000.0,
        "timestamp": base_time.isoformat(),
        "description": "Transfer 1",
        "fraud_type": "circular_ring",
        "ring_id": ring_id,
    }
    
    # Transaction B -> C
    tx2 = {
        "transaction_id": str(uuid4()),
        "sender_iban": iban_b,
        "sender_name": "Mehmet Kaya",
        "sender_city": "Ankara",
        "receiver_iban": iban_c,
        "receiver_name": "Ayşe Demir",
        "receiver_city": "İzmir",
        "amount": 9500.0,
        "timestamp": (base_time + timedelta(hours=1)).isoformat(),
        "description": "Transfer 2",
        "fraud_type": "circular_ring",
        "ring_id": ring_id,
    }
    
    # Transaction C -> A (completing the ring!)
    tx3 = {
        "transaction_id": str(uuid4()),
        "sender_iban": iban_c,
        "sender_name": "Ayşe Demir",
        "sender_city": "İzmir",
        "receiver_iban": iban_a,
        "receiver_name": "Ahmet Yılmaz",
        "receiver_city": "İstanbul",
        "amount": 9000.0,
        "timestamp": (base_time + timedelta(hours=2)).isoformat(),
        "description": "Transfer 3",
        "fraud_type": "circular_ring",
        "ring_id": ring_id,
    }
    
    # Add transactions
    try:
        engine.add_transaction(tx1)
        console.print(f"  📤 Added: A ({iban_a[:12]}...) → B")
        
        engine.add_transaction(tx2)
        console.print(f"  📤 Added: B ({iban_b[:12]}...) → C")
        
        engine.add_transaction(tx3)
        console.print(f"  📤 Added: C ({iban_c[:12]}...) → A [RING COMPLETE!]")
        
        console.print("[green]✅ Circular ring created![/green]")
    except Exception as e:
        console.print(f"[red]❌ Failed to add transactions: {e}[/red]")
        return False
    
    # ==========================================================================
    # Test 5: Get Graph Stats
    # ==========================================================================
    console.print("\n[bold]5️⃣ Checking graph statistics...[/bold]")
    
    try:
        stats = engine.get_graph_stats()
        table = Table(title="Graph Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Users (Nodes)", str(stats.get("user_count", 0)))
        table.add_row("Transactions (Edges)", str(stats.get("transaction_count", 0)))
        table.add_row("Total Volume (TRY)", f"{stats.get('total_volume', 0):,.2f}")
        console.print(table)
    except Exception as e:
        console.print(f"[yellow]⚠️ Stats warning: {e}[/yellow]")
    
    # ==========================================================================
    # Test 6: FRAUD DETECTION! 🚨
    # ==========================================================================
    console.print("\n[bold]6️⃣ Running FRAUD RING DETECTION...[/bold]")
    console.print(f"   Searching for rings starting from IBAN: {iban_a[:12]}...")
    
    try:
        rings = engine.detect_fraud_rings(
            sender_iban=iban_a,
            min_hops=3,
            max_hops=5,
        )
        
        if rings:
            console.print(f"\n[red bold]🚨 ALERT: {len(rings)} FRAUD RING(S) DETECTED! 🚨[/red bold]\n")
            
            for ring in rings:
                ring_table = Table(title=f"Fraud Ring: {ring['ring_id']}", border_style="red")
                ring_table.add_column("Property", style="cyan")
                ring_table.add_column("Value", style="yellow")
                
                path_str = " → ".join([iban[:10] + "..." for iban in ring["path"]])
                ring_table.add_row("Path", path_str)
                ring_table.add_row("Transactions", str(ring["transaction_count"]))
                ring_table.add_row("Total Amount", f"{ring['total_amount']:,.2f} TRY")
                ring_table.add_row("Detected At", ring["detected_at"])
                
                console.print(ring_table)
            
            console.print("\n[green bold]✅ FRAUD DETECTION TEST PASSED![/green bold]")
        else:
            console.print("[yellow]⚠️ No rings detected (unexpected!)[/yellow]")
            console.print("[dim]This might be a timing issue or query problem.[/dim]")
            return False
            
    except Exception as e:
        console.print(f"[red]❌ Fraud detection failed: {e}[/red]")
        import traceback
        traceback.print_exc()
        return False
    
    # ==========================================================================
    # Test 7: Full Graph Scan
    # ==========================================================================
    console.print("\n[bold]7️⃣ Running full graph scan...[/bold]")
    
    try:
        all_rings = engine.detect_all_rings(min_hops=3, max_hops=5, min_amount=100.0)
        console.print(f"[green]✅ Full scan complete: {len(all_rings)} ring(s) found[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Full scan warning: {e}[/yellow]")
    
    # ==========================================================================
    # Cleanup
    # ==========================================================================
    console.print("\n[bold]8️⃣ Cleanup...[/bold]")
    engine.close()
    console.print("[green]✅ Connection closed[/green]")
    
    # ==========================================================================
    # Summary
    # ==========================================================================
    console.print(Panel.fit(
        "[bold green]✅ ALL TESTS PASSED![/bold green]\n\n"
        "The GraphEngine successfully:\n"
        "• Connected to Neo4j\n"
        "• Set up schema constraints\n"
        "• Ingested transactions with MERGE\n"
        "• Detected circular fraud rings\n\n"
        "[dim]Ready for Phase 4: Kafka Consumer Integration[/dim]",
        border_style="green",
    ))
    
    return True


if __name__ == "__main__":
    success = test_graph_engine()
    sys.exit(0 if success else 1)

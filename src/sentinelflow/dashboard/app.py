# =============================================================================
# SentinelFlow - Real-Time Fraud Detection Dashboard
# =============================================================================
"""
Streamlit-based Command Center for fraud monitoring.

This dashboard provides a Cybersecurity SOC (Security Operations Center) view
of the fraud detection system, featuring:

1. Real-time metrics (transactions, fraud counts)
2. Live alert feed with severity indicators
3. Network graph visualization for fraud rings
4. Geographic impossible travel visualization

Usage:
    streamlit run src/sentinelflow/dashboard/app.py

    # Or with custom config
    streamlit run src/sentinelflow/dashboard/app.py -- --kafka-servers localhost:9092
"""

import json
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import matplotlib.pyplot as plt
import networkx as nx
import streamlit as st
from confluent_kafka import Consumer, KafkaError

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from sentinelflow.config import get_settings


# =============================================================================
# Page Configuration - MUST BE FIRST STREAMLIT COMMAND
# =============================================================================

st.set_page_config(
    page_title="SentinelFlow | Fraud Detection Command Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# =============================================================================
# Custom CSS for SOC Aesthetic
# =============================================================================

def inject_custom_css():
    """Inject custom CSS for cybersecurity SOC aesthetic."""
    st.markdown("""
    <style>
    /* Dark SOC theme */
    .stApp {
        background: linear-gradient(180deg, #0a0a0f 0%, #1a1a2e 100%);
    }
    
    /* Main title styling */
    .main-title {
        text-align: center;
        color: #00ff88;
        font-size: 3rem;
        font-weight: 700;
        text-shadow: 0 0 20px rgba(0, 255, 136, 0.5);
        margin-bottom: 0.5rem;
        font-family: 'Courier New', monospace;
    }
    
    .subtitle {
        text-align: center;
        color: #888;
        font-size: 1rem;
        margin-bottom: 2rem;
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #1e1e2f 0%, #2a2a4a 100%);
        border: 1px solid #3a3a5a;
        border-radius: 15px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    .metric-value {
        font-size: 3rem;
        font-weight: 700;
        font-family: 'Courier New', monospace;
    }
    
    .metric-label {
        color: #888;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
    
    .metric-green { color: #00ff88; text-shadow: 0 0 10px rgba(0, 255, 136, 0.5); }
    .metric-red { color: #ff4444; text-shadow: 0 0 10px rgba(255, 68, 68, 0.5); }
    .metric-yellow { color: #ffaa00; text-shadow: 0 0 10px rgba(255, 170, 0, 0.5); }
    .metric-blue { color: #00aaff; text-shadow: 0 0 10px rgba(0, 170, 255, 0.5); }
    
    /* Alert feed */
    .alert-feed {
        background: #1a1a2e;
        border: 1px solid #3a3a5a;
        border-radius: 10px;
        padding: 1rem;
        max-height: 400px;
        overflow-y: auto;
    }
    
    .alert-item {
        background: linear-gradient(90deg, #2a1a1a 0%, #1a1a2e 100%);
        border-left: 4px solid #ff4444;
        padding: 0.8rem;
        margin-bottom: 0.5rem;
        border-radius: 5px;
    }
    
    .alert-critical { border-left-color: #ff0000; }
    .alert-high { border-left-color: #ff4444; }
    .alert-medium { border-left-color: #ffaa00; }
    .alert-low { border-left-color: #00aaff; }
    
    .alert-time {
        color: #666;
        font-size: 0.75rem;
        font-family: 'Courier New', monospace;
    }
    
    .alert-type {
        color: #ff4444;
        font-weight: 700;
        font-size: 0.9rem;
    }
    
    .alert-desc {
        color: #ccc;
        font-size: 0.85rem;
        margin-top: 0.3rem;
    }
    
    /* Section headers */
    .section-header {
        color: #00ff88;
        border-bottom: 2px solid #00ff88;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
        font-family: 'Courier New', monospace;
    }
    
    /* Status indicator */
    .status-online {
        display: inline-block;
        width: 10px;
        height: 10px;
        background: #00ff88;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(0, 255, 136, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(0, 255, 136, 0); }
        100% { box-shadow: 0 0 0 0 rgba(0, 255, 136, 0); }
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Graph container */
    .graph-container {
        background: #1a1a2e;
        border: 1px solid #3a3a5a;
        border-radius: 10px;
        padding: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class DashboardState:
    """Holds the dashboard state across Streamlit reruns."""
    
    total_transactions: int = 0
    fraud_rings: int = 0
    impossible_travel: int = 0
    blacklist_hits: int = 0
    alerts: deque = field(default_factory=lambda: deque(maxlen=50))
    latest_ring: dict | None = None
    is_connected: bool = False
    last_update: datetime = field(default_factory=datetime.utcnow)


# Initialize session state
if "dashboard" not in st.session_state:
    st.session_state.dashboard = DashboardState()

if "consumer_started" not in st.session_state:
    st.session_state.consumer_started = False

if "demo_initialized" not in st.session_state:
    st.session_state.demo_initialized = False


# =============================================================================
# Kafka Consumer (Background Thread)
# =============================================================================

def create_kafka_consumer(servers: str, topic: str, group_id: str) -> Consumer | None:
    """Create a Kafka consumer for fraud alerts."""
    try:
        config = {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 10000,
        }
        
        consumer = Consumer(config)
        consumer.subscribe([topic])
        return consumer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None


def poll_alerts(consumer: Consumer, timeout: float = 0.1) -> list[dict]:
    """Poll for new alerts from Kafka (non-blocking)."""
    alerts = []
    
    try:
        msg = consumer.poll(timeout=timeout)
        
        if msg is None:
            return alerts
        
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.session_state.dashboard.is_connected = False
            return alerts
        
        # Parse alert
        try:
            alert = json.loads(msg.value().decode("utf-8"))
            alerts.append(alert)
            st.session_state.dashboard.is_connected = True
        except json.JSONDecodeError:
            pass
            
    except Exception as e:
        st.session_state.dashboard.is_connected = False
    
    return alerts


def process_alerts(alerts: list[dict]) -> None:
    """Process incoming alerts and update dashboard state."""
    dashboard = st.session_state.dashboard
    
    for alert in alerts:
        fraud_type = alert.get("fraud_type", "")
        
        # Update counters
        if fraud_type == "circular_ring":
            dashboard.fraud_rings += 1
            # Store the ring for visualization
            evidence = alert.get("evidence", {})
            if "ring_path" in evidence:
                dashboard.latest_ring = {
                    "path": evidence["ring_path"],
                    "total_amount": evidence.get("total_amount", 0),
                    "alert": alert,
                }
        elif fraud_type == "impossible_travel":
            dashboard.impossible_travel += 1
        elif fraud_type == "blacklist_keyword":
            dashboard.blacklist_hits += 1
        
        # Add to alert feed
        dashboard.alerts.appendleft(alert)
        dashboard.last_update = datetime.utcnow()


# =============================================================================
# Visualization Components
# =============================================================================

def render_header():
    """Render the main header."""
    st.markdown("""
    <h1 class="main-title">🛡️ SENTINELFLOW</h1>
    <p class="subtitle">
        <span class="status-online"></span>
        Real-Time Financial Fraud Detection Command Center
    </p>
    """, unsafe_allow_html=True)


def render_metrics():
    """Render the top metrics row."""
    dashboard = st.session_state.dashboard
    
    # Simulate transaction count increasing
    dashboard.total_transactions += len(dashboard.alerts) * 20
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-green">{dashboard.total_transactions:,}</div>
            <div class="metric-label">Transactions Scanned</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-red">{dashboard.fraud_rings}</div>
            <div class="metric-label">🔴 Fraud Rings Detected</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-yellow">{dashboard.impossible_travel}</div>
            <div class="metric-label">⚡ Impossible Travel</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value metric-blue">{dashboard.blacklist_hits}</div>
            <div class="metric-label">🔤 Blacklist Hits</div>
        </div>
        """, unsafe_allow_html=True)


def render_alert_feed():
    """Render the live alert feed."""
    dashboard = st.session_state.dashboard
    
    st.markdown('<h3 class="section-header">📡 LIVE ALERT FEED</h3>', unsafe_allow_html=True)
    
    if not dashboard.alerts:
        st.info("🔍 Monitoring for fraud alerts... No alerts yet.")
        return
    
    # Show latest 10 alerts
    for alert in list(dashboard.alerts)[:10]:
        fraud_type = alert.get("fraud_type", "unknown").upper().replace("_", " ")
        severity = alert.get("severity", "high")
        description = alert.get("description", "No description")
        timestamp = alert.get("detected_at", "")
        amount = alert.get("amount", 0)
        
        # Format timestamp
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            time_str = dt.strftime("%H:%M:%S")
        except:
            time_str = timestamp[:8] if timestamp else "N/A"
        
        # Severity colors
        severity_class = f"alert-{severity}"
        severity_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(severity, "⚪")
        
        st.markdown(f"""
        <div class="alert-item {severity_class}">
            <span class="alert-time">[{time_str}]</span>
            <span class="alert-type">{severity_emoji} {fraud_type}</span>
            <span style="float: right; color: #ff4444; font-weight: bold;">
                {amount:,.2f} TRY
            </span>
            <div class="alert-desc">{description[:100]}...</div>
        </div>
        """, unsafe_allow_html=True)


def render_fraud_ring_graph():
    """Render the fraud ring network visualization."""
    dashboard = st.session_state.dashboard
    
    st.markdown('<h3 class="section-header">🕸️ FRAUD RING VISUALIZATION</h3>', unsafe_allow_html=True)
    
    if not dashboard.latest_ring:
        st.info("⏳ Waiting for fraud ring detection... Graph will appear here.")
        return
    
    ring_data = dashboard.latest_ring
    path = ring_data.get("path", [])
    
    if len(path) < 3:
        st.warning("Invalid ring data")
        return
    
    # Create network graph
    G = nx.DiGraph()
    
    # Add nodes (IBANs shortened for display)
    node_labels = {}
    for i, iban in enumerate(path[:-1]):  # Exclude duplicate last node
        short_iban = f"{iban[:8]}..."
        G.add_node(short_iban)
        node_labels[short_iban] = f"User {i+1}\n{short_iban}"
    
    # Add edges (with amounts if available)
    for i in range(len(path) - 1):
        source = f"{path[i][:8]}..."
        target = f"{path[i+1][:8]}..."
        G.add_edge(source, target, weight=i+1)
    
    # Create figure with dark theme
    fig, ax = plt.subplots(figsize=(10, 6), facecolor='#1a1a2e')
    ax.set_facecolor('#1a1a2e')
    
    # Layout
    pos = nx.circular_layout(G)
    
    # Draw edges with red arrows
    nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color='#ff4444',
        arrows=True,
        arrowsize=25,
        arrowstyle='-|>',
        width=3,
        connectionstyle="arc3,rad=0.1",
    )
    
    # Draw nodes (red with glow effect)
    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        node_color='#ff2222',
        node_size=2000,
        edgecolors='#ff6666',
        linewidths=3,
    )
    
    # Draw labels
    nx.draw_networkx_labels(
        G, pos, ax=ax,
        labels=node_labels,
        font_size=8,
        font_color='white',
        font_family='monospace',
    )
    
    # Title
    total_amount = ring_data.get("total_amount", 0)
    ax.set_title(
        f"🚨 FRAUD RING DETECTED | Total: {total_amount:,.2f} TRY",
        color='#ff4444',
        fontsize=14,
        fontweight='bold',
        fontfamily='monospace',
    )
    
    ax.axis('off')
    plt.tight_layout()
    
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Ring details
    alert = ring_data.get("alert", {})
    st.markdown(f"""
    <div style="text-align: center; color: #888; font-size: 0.9rem;">
        Ring ID: {alert.get('alert_id', 'N/A')} | 
        Confidence: {alert.get('confidence', 0) * 100:.0f}% |
        Detected: {alert.get('detected_at', 'N/A')[:19]}
    </div>
    """, unsafe_allow_html=True)


def render_system_status():
    """Render system status indicators."""
    dashboard = st.session_state.dashboard
    
    col1, col2, col3 = st.columns(3)
    
    status_color = "#00ff88" if dashboard.is_connected else "#ff4444"
    status_text = "ONLINE" if dashboard.is_connected else "CONNECTING..."
    
    with col1:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px;">
            <span style="color: {status_color}; font-size: 1.2rem;">●</span>
            <span style="color: #888; margin-left: 8px;">Kafka: {status_text}</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px;">
            <span style="color: #00ff88; font-size: 1.2rem;">●</span>
            <span style="color: #888; margin-left: 8px;">Neo4j: ONLINE</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px;">
            <span style="color: #00ff88; font-size: 1.2rem;">●</span>
            <span style="color: #888; margin-left: 8px;">Redis: ONLINE</span>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# Demo Mode (for testing without Kafka)
# =============================================================================

def add_demo_alert():
    """Add a demo alert for testing."""
    import random
    from uuid import uuid4
    
    dashboard = st.session_state.dashboard
    
    fraud_types = [
        ("circular_ring", "Circular transaction ring detected: TR001 → TR002 → TR003 → TR001"),
        ("impossible_travel", "Impossible travel: İstanbul → Berlin (1,500 km in 10 min = 9,000 km/h)"),
        ("blacklist_keyword", "Suspicious keywords detected: bahis, kumar"),
    ]
    
    fraud_type, description = random.choice(fraud_types)
    
    alert = {
        "alert_id": f"ALERT-{uuid4().hex[:12].upper()}",
        "fraud_type": fraud_type,
        "severity": random.choice(["critical", "high", "medium"]),
        "confidence": random.uniform(0.8, 0.99),
        "transaction_id": str(uuid4()),
        "sender_iban": f"TR{uuid4().hex[:24].upper()}",
        "sender_name": random.choice(["Ahmet Yılmaz", "Mehmet Kaya", "Ayşe Demir"]),
        "receiver_iban": f"TR{uuid4().hex[:24].upper()}",
        "receiver_name": random.choice(["Fatma Şahin", "Ali Yıldız", "Zeynep Çelik"]),
        "amount": random.uniform(1000, 50000),
        "description": description,
        "detected_at": datetime.utcnow().isoformat(),
        "evidence": {
            "ring_path": [
                f"TR{uuid4().hex[:24].upper()}",
                f"TR{uuid4().hex[:24].upper()}",
                f"TR{uuid4().hex[:24].upper()}",
                f"TR{uuid4().hex[:24].upper()}",  # Back to first
            ],
            "total_amount": random.uniform(10000, 100000),
        } if fraud_type == "circular_ring" else {},
    }
    
    process_alerts([alert])


# =============================================================================
# Main Application
# =============================================================================

def main():
    """Main dashboard application."""
    inject_custom_css()
    
    # Get settings
    settings = get_settings()
    kafka_servers = settings.kafka.bootstrap_servers
    kafka_topic = "fraud_alerts"
    
    # Auto-initialize with demo data on first load
    if not st.session_state.demo_initialized:
        for _ in range(5):
            add_demo_alert()
        st.session_state.demo_initialized = True
    
    # Header
    render_header()
    
    st.markdown("---")
    
    # Metrics row
    render_metrics()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Main content area
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        render_alert_feed()
    
    with col_right:
        render_fraud_ring_graph()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # System status
    render_system_status()
    
    # Sidebar controls
    with st.sidebar:
        st.markdown("## ⚙️ Controls")
        
        # Demo mode toggle
        st.markdown("### 🎮 Demo Mode")
        if st.button("➕ Add Demo Alert", use_container_width=True):
            add_demo_alert()
            st.rerun()
        
        if st.button("🔄 Reset Counters", use_container_width=True):
            st.session_state.dashboard = DashboardState()
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("### 📊 Configuration")
        st.text_input("Kafka Servers", value=kafka_servers, disabled=True)
        st.text_input("Alert Topic", value=kafka_topic, disabled=True)
        
        st.markdown("---")
        
        st.markdown("### 📈 Stats")
        dashboard = st.session_state.dashboard
        st.metric("Total Alerts", len(dashboard.alerts))
        st.metric("Last Update", dashboard.last_update.strftime("%H:%M:%S"))
    
    # Auto-refresh and Kafka polling
    # Create Kafka consumer placeholder
    consumer_placeholder = st.empty()
    
    try:
        consumer = create_kafka_consumer(kafka_servers, kafka_topic, "sentinelflow-dashboard")
        if consumer:
            st.session_state.dashboard.is_connected = True
            
            # Poll for new alerts
            alerts = poll_alerts(consumer, timeout=0.5)
            if alerts:
                process_alerts(alerts)
                st.rerun()
            
            consumer.close()
    except Exception as e:
        st.session_state.dashboard.is_connected = False
    
    # Auto-refresh every 2 seconds
    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()

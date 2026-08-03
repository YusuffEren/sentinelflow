# =============================================================================
# SentinelFlow - Dashboard Components
# =============================================================================
"""
Reusable dashboard components for SentinelFlow.

Provides modular UI components:
- Metric cards
- Alert cards
- Interactive graphs
- Compliance widgets
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

# =============================================================================
# Metric Cards
# =============================================================================


def metric_card(
    label: str,
    value: str | int | float,
    delta: str | None = None,
    delta_color: str = "normal",
    icon: str = "",
    color: str = "#00ff88",
) -> None:
    """
    Display a styled metric card.

    Args:
        label: Metric label
        value: Metric value
        delta: Optional delta value
        delta_color: Color for delta (normal, inverse, off)
        icon: Optional icon emoji
        color: Main color for value
    """
    delta_html = ""
    if delta:
        delta_style = "color: #00ff88;" if "+" in str(delta) else "color: #ff4444;"
        delta_html = f'<div style="{delta_style} font-size: 0.9rem;">{delta}</div>'

    st.markdown(
        f"""
        <div class="metric-card">
            <div style="color: #888; font-size: 0.9rem; margin-bottom: 0.5rem;">
                {icon} {label}
            </div>
            <div class="metric-value" style="color: {color};">
                {value}
            </div>
            {delta_html}
        </div>
    """,
        unsafe_allow_html=True,
    )


def metric_row(metrics: list[dict[str, Any]]) -> None:
    """
    Display a row of metric cards.

    Args:
        metrics: List of metric dictionaries
    """
    cols = st.columns(len(metrics))

    for col, metric in zip(cols, metrics):
        with col:
            metric_card(
                label=metric.get("label", ""),
                value=metric.get("value", 0),
                delta=metric.get("delta"),
                icon=metric.get("icon", ""),
                color=metric.get("color", "#00ff88"),
            )


# =============================================================================
# Alert Cards
# =============================================================================


def alert_card(
    alert: dict[str, Any],
    on_investigate: callable = None,
    on_dismiss: callable = None,
) -> None:
    """
    Display a fraud alert card.

    Args:
        alert: Alert data dictionary
        on_investigate: Callback for investigate action
        on_dismiss: Callback for dismiss action
    """
    severity = alert.get("severity", "MEDIUM").upper()
    fraud_type = alert.get("fraud_type", "unknown")

    severity_colors = {
        "CRITICAL": ("#ff0000", "🔴"),
        "HIGH": ("#ff4444", "🟠"),
        "MEDIUM": ("#ffaa00", "🟡"),
        "LOW": ("#00ff88", "🟢"),
    }

    color, icon = severity_colors.get(severity, ("#888", "⚪"))

    fraud_type_labels = {
        "circular_ring": "Döngüsel Halka",
        "impossible_travel": "İmkansız Seyahat",
        "blacklist_keyword": "Kara Liste",
        "ai_detected_anomaly": "AI Anomali",
        "ml_ensemble": "ML Ensemble",
    }

    type_label = fraud_type_labels.get(fraud_type, fraud_type)

    timestamp = alert.get("detected_at", "")
    if timestamp:
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", ""))
            timestamp = dt.strftime("%H:%M:%S")
        except:
            pass

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, rgba(30, 30, 47, 0.9), rgba(42, 42, 74, 0.9));
            border-left: 4px solid {color};
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.5rem;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
        ">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="font-size: 1.1rem; font-weight: 600; color: {color};">
                        {icon} {type_label}
                    </span>
                    <span style="color: #888; margin-left: 1rem;">
                        {timestamp}
                    </span>
                </div>
                <div style="
                    background: {color}22;
                    color: {color};
                    padding: 0.25rem 0.75rem;
                    border-radius: 20px;
                    font-size: 0.8rem;
                    font-weight: 600;
                ">
                    {severity}
                </div>
            </div>
            <div style="color: #ccc; margin-top: 0.5rem; font-size: 0.9rem;">
                {alert.get("description", "")[:100]}
            </div>
            <div style="color: #888; margin-top: 0.25rem; font-size: 0.8rem;">
                TX: {alert.get("transaction_id", "N/A")[:20]}... |
                Tutar: {alert.get("amount", 0):,.2f} TL
            </div>
        </div>
    """,
        unsafe_allow_html=True,
    )


def alert_list(alerts: list[dict[str, Any]], max_items: int = 10) -> None:
    """
    Display a list of alerts.

    Args:
        alerts: List of alert dictionaries
        max_items: Maximum items to display
    """
    if not alerts:
        st.info("🔍 Aktif alarm yok")
        return

    for alert in alerts[:max_items]:
        alert_card(alert)


# =============================================================================
# Status Indicators
# =============================================================================


def status_indicator(
    label: str,
    status: str,
    details: str = "",
) -> None:
    """
    Display a status indicator.

    Args:
        label: Status label
        status: Status value (connected, disconnected, etc.)
        details: Additional details
    """
    status_config = {
        "connected": ("🟢", "#00ff88"),
        "disconnected": ("🔴", "#ff4444"),
        "processing": ("🟡", "#ffaa00"),
        "waiting": ("⚪", "#888"),
    }

    icon, color = status_config.get(status.lower(), ("⚪", "#888"))

    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span>{icon}</span>
            <span style="color: {color}; font-weight: 500;">{label}</span>
            <span style="color: #666; font-size: 0.8rem;">{details}</span>
        </div>
    """,
        unsafe_allow_html=True,
    )


def system_status_panel(services: dict[str, str]) -> None:
    """
    Display system status panel.

    Args:
        services: Dictionary of service_name: status
    """
    st.markdown(
        """
        <div style="
            background: rgba(30, 30, 47, 0.8);
            border-radius: 10px;
            padding: 1rem;
        ">
            <div style="color: #888; font-size: 0.9rem; margin-bottom: 0.5rem;">
                📡 Sistem Durumu
            </div>
    """,
        unsafe_allow_html=True,
    )

    for service, status in services.items():
        status_indicator(service, status)

    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Graph Components
# =============================================================================


def ring_stats_panel(ring_data: dict[str, Any]) -> None:
    """
    Display fraud ring statistics.

    Args:
        ring_data: Ring data dictionary
    """
    nodes = ring_data.get("nodes", [])
    edges = ring_data.get("edges", [])
    total_amount = ring_data.get("total_amount", 0)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Düğümler", len(nodes))
    with col2:
        st.metric("Kenarlar", len(edges))
    with col3:
        st.metric("Toplam Tutar", f"{total_amount:,.0f} TL")


# =============================================================================
# Compliance Components
# =============================================================================


def compliance_summary(stats: dict[str, Any]) -> None:
    """
    Display compliance summary panel.

    Args:
        stats: Compliance statistics dictionary
    """
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #1e1e2f 0%, #2a2a4a 100%);
            border-radius: 15px;
            padding: 1.5rem;
        ">
            <h3 style="color: #00ff88; margin-bottom: 1rem;">📋 Uyum Durumu</h3>
    """,
        unsafe_allow_html=True,
    )

    metrics = [
        {
            "label": "MASAK Bildirimleri",
            "value": stats.get("str_count", 0),
            "icon": "📄",
        },
        {
            "label": "Bekleyen",
            "value": stats.get("pending_count", 0),
            "icon": "⏳",
            "color": "#ffaa00",
        },
        {
            "label": "Gönderilen",
            "value": stats.get("submitted_count", 0),
            "icon": "✅",
        },
    ]

    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            st.metric(
                f"{m['icon']} {m['label']}",
                m["value"],
            )

    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# ML Performance Components
# =============================================================================


def model_performance_card(
    model_name: str,
    metrics: dict[str, float],
    status: str = "ready",
) -> None:
    """
    Display ML model performance card.

    Args:
        model_name: Model name
        metrics: Performance metrics dictionary
        status: Model status
    """
    status_color = "#00ff88" if status == "ready" else "#ff4444"

    accuracy = metrics.get("accuracy", 0) * 100
    auc = metrics.get("auc", 0) * 100
    latency = metrics.get("latency_ms", 0)

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1e1e2f 0%, #252545 100%);
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.5rem;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span style="color: #fff; font-weight: 600;">{model_name}</span>
                <span style="color: {status_color};">●</span>
            </div>
            <div style="display: flex; gap: 1rem; margin-top: 0.5rem;">
                <div style="color: #888; font-size: 0.8rem;">
                    Accuracy: <span style="color: #00ff88;">{accuracy:.1f}%</span>
                </div>
                <div style="color: #888; font-size: 0.8rem;">
                    AUC: <span style="color: #00ff88;">{auc:.1f}%</span>
                </div>
                <div style="color: #888; font-size: 0.8rem;">
                    Latency: <span style="color: #00ff88;">{latency:.1f}ms</span>
                </div>
            </div>
        </div>
    """,
        unsafe_allow_html=True,
    )


def ml_dashboard_panel(models: list[dict[str, Any]]) -> None:
    """
    Display ML models dashboard panel.

    Args:
        models: List of model info dictionaries
    """
    st.markdown("### 🤖 ML Model Performansı")

    for model in models:
        model_performance_card(
            model_name=model.get("name", "Unknown"),
            metrics=model.get("metrics", {}),
            status=model.get("status", "unknown"),
        )


# =============================================================================
# Timeline Components
# =============================================================================


def timeline_item(
    time: str,
    title: str,
    description: str,
    icon: str = "📌",
    color: str = "#00ff88",
) -> None:
    """
    Display a timeline item.

    Args:
        time: Timestamp
        title: Item title
        description: Item description
        icon: Item icon
        color: Accent color
    """
    st.markdown(
        f"""
        <div style="
            display: flex;
            gap: 1rem;
            padding: 0.5rem 0;
            border-left: 2px solid {color};
            padding-left: 1rem;
            margin-left: 0.5rem;
        ">
            <div style="color: #888; font-size: 0.8rem; min-width: 80px;">
                {time}
            </div>
            <div>
                <div style="color: #fff; font-weight: 500;">
                    {icon} {title}
                </div>
                <div style="color: #888; font-size: 0.85rem;">
                    {description}
                </div>
            </div>
        </div>
    """,
        unsafe_allow_html=True,
    )

# =============================================================================
# SentinelFlow - TEKNOFEST Competition Dashboard
# =============================================================================
"""
TEKNOFEST yarışması için geliştirilmiş dashboard bileşenleri.

Jüriyi etkilemek için:
- Model performans karşılaştırması
- Real-time accuracy tracking
- SHAP explanation visualizations
- Feature importance heatmaps
- Benchmark comparison (vs 2025 winner)

Usage:
    streamlit run src/sentinelflow/dashboard/competition_dashboard.py
"""

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))


# =============================================================================
# Page Configuration
# =============================================================================

st.set_page_config(
    page_title="SentinelFlow | TEKNOFEST Competition Dashboard",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =============================================================================
# Custom CSS - Modern Competition Theme
# =============================================================================


def inject_competition_css():
    """Inject competition-grade CSS."""
    st.markdown(
        """
    <style>
    /* Competition theme - Gradient background */
    .stApp {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
    }

    /* Main header */
    .competition-header {
        text-align: center;
        padding: 2rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.4);
    }

    .competition-title {
        color: white;
        font-size: 2.5rem;
        font-weight: 800;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }

    .competition-subtitle {
        color: rgba(255,255,255,0.9);
        font-size: 1.2rem;
        margin-top: 0.5rem;
    }

    /* Performance card */
    .perf-card {
        background: linear-gradient(145deg, #1a1a2e, #16213e);
        border: 2px solid #0f3460;
        border-radius: 15px;
        padding: 1.5rem;
        text-align: center;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }

    .perf-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }

    .perf-value {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #00d9ff, #00ff88);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .perf-label {
        color: #a0a0a0;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 2px;
    }

    /* Comparison badge */
    .comparison-badge {
        display: inline-block;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        margin: 0.5rem;
    }

    .badge-winning {
        background: linear-gradient(90deg, #00ff88, #00cc6a);
        color: #0a0a0f;
    }

    .badge-competitive {
        background: linear-gradient(90deg, #ffd700, #ffaa00);
        color: #0a0a0f;
    }

    .badge-needs-work {
        background: linear-gradient(90deg, #ff6b6b, #ee5a5a);
        color: white;
    }

    /* Model card */
    .model-card {
        background: rgba(26, 26, 46, 0.8);
        border: 1px solid #3a3a5a;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
    }

    .model-name {
        color: #00d9ff;
        font-size: 1.2rem;
        font-weight: 600;
    }

    /* Feature importance */
    .feature-bar {
        height: 20px;
        border-radius: 10px;
        margin: 5px 0;
        transition: width 0.5s ease;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
    }

    /* Streamlit overrides */
    .stMetric {
        background: rgba(26, 26, 46, 0.8);
        border-radius: 10px;
        padding: 1rem;
        border: 1px solid #3a3a5a;
    }

    .stMetric label {
        color: #a0a0a0 !important;
    }

    .stMetric .css-1wivap2 {
        color: #00ff88 !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        background: rgba(26, 26, 46, 0.8);
        border-radius: 10px;
        color: #a0a0a0;
        padding: 0.5rem 1rem;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(102, 126, 234, 0.3);
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #667eea, #764ba2) !important;
        color: white !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


# =============================================================================
# Helper Functions
# =============================================================================


def create_gauge_chart(value: float, title: str, target: float = 0.992) -> go.Figure:
    """Create a gauge chart for metrics comparison."""

    # Determine color based on comparison with target
    if value >= target:
        color = "#00ff88"  # Green - beating target
    elif value >= target * 0.98:
        color = "#ffd700"  # Gold - close
    else:
        color = "#ff6b6b"  # Red - needs work

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=value * 100,
            delta={
                "reference": target * 100,
                "relative": False,
                "increasing": {"color": "#00ff88"},
                "decreasing": {"color": "#ff6b6b"},
            },
            title={"text": title, "font": {"size": 16, "color": "white"}},
            number={"suffix": "%", "font": {"size": 36, "color": "white"}},
            gauge={
                "axis": {"range": [90, 100], "tickwidth": 1, "tickcolor": "white"},
                "bar": {"color": color},
                "bgcolor": "#1a1a2e",
                "borderwidth": 2,
                "bordercolor": "#3a3a5a",
                "steps": [
                    {"range": [90, 95], "color": "rgba(255,0,0,0.2)"},
                    {"range": [95, 98], "color": "rgba(255,215,0,0.2)"},
                    {"range": [98, 100], "color": "rgba(0,255,136,0.2)"},
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": target * 100,
                },
            },
        )
    )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "white"},
        height=250,
        margin={"l": 20, "r": 20, "t": 50, "b": 20},
    )

    return fig


def create_model_comparison_chart(metrics: dict[str, dict[str, float]]) -> go.Figure:
    """Create radar chart comparing models."""

    categories = ["Accuracy", "Precision", "Recall", "F1", "AUC-ROC"]

    fig = go.Figure()

    colors = ["#00ff88", "#00d9ff", "#ffd700", "#ff6b6b", "#a855f7"]

    for i, (model_name, model_metrics) in enumerate(metrics.items()):
        values = [
            model_metrics.get("accuracy", 0) * 100,
            model_metrics.get("precision", 0) * 100,
            model_metrics.get("recall", 0) * 100,
            model_metrics.get("f1", 0) * 100,
            model_metrics.get("auc_roc", 0) * 100,
        ]
        values.append(values[0])  # Close the polygon

        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories + [categories[0]],
                fill="toself",
                fillcolor=f"rgba({int(colors[i % len(colors)][1:3], 16)}, {int(colors[i % len(colors)][3:5], 16)}, {int(colors[i % len(colors)][5:7], 16)}, 0.2)",
                line={"color": colors[i % len(colors)], "width": 2},
                name=model_name,
            )
        )

    fig.update_layout(
        polar={
            "bgcolor": "rgba(26,26,46,0.8)",
            "radialaxis": {
                "visible": True,
                "range": [90, 100],
                "tickfont": {"color": "white"},
            },
            "angularaxis": {
                "tickfont": {"color": "white"},
            },
        },
        showlegend=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend={
            "font": {"color": "white"},
            "bgcolor": "rgba(26,26,46,0.8)",
        },
        height=400,
        margin={"l": 50, "r": 50, "t": 50, "b": 50},
    )

    return fig


def create_feature_importance_chart(importances: dict[str, float], top_n: int = 15) -> go.Figure:
    """Create horizontal bar chart for feature importance."""

    # Sort and get top N
    sorted_features = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:top_n]
    features = [f[0] for f in sorted_features]
    values = [f[1] for f in sorted_features]

    # Normalize to 0-1
    max_val = max(values) if values else 1
    values = [v / max_val for v in values]

    # Create gradient colors
    colors = [f"rgba(102, 126, 234, {0.3 + 0.7 * (1 - i/len(values))})" for i in range(len(values))]

    fig = go.Figure(
        go.Bar(
            x=values,
            y=features,
            orientation="h",
            marker={
                "color": colors,
                "line": {"color": "rgba(102, 126, 234, 1)", "width": 1},
            },
            text=[f"{v:.3f}" for v in values],
            textposition="outside",
            textfont={"color": "white"},
        )
    )

    fig.update_layout(
        title="Top Feature Importance",
        xaxis={
            "title": "Importance (Normalized)",
            "titlefont": {"color": "white"},
            "tickfont": {"color": "white"},
            "gridcolor": "rgba(255,255,255,0.1)",
        },
        yaxis={
            "titlefont": {"color": "white"},
            "tickfont": {"color": "white"},
            "autorange": "reversed",
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin={"l": 150, "r": 50, "t": 50, "b": 50},
    )

    return fig


def create_confusion_matrix_chart(tp: int, tn: int, fp: int, fn: int) -> go.Figure:
    """Create confusion matrix heatmap."""

    z = [[tn, fp], [fn, tp]]
    labels = [["True Negative", "False Positive"], ["False Negative", "True Positive"]]

    # Custom colorscale
    colorscale = [[0, "#1a1a2e"], [0.5, "#3a3a5a"], [1, "#00ff88"]]

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=["Predicted Normal", "Predicted Fraud"],
            y=["Actual Normal", "Actual Fraud"],
            colorscale=colorscale,
            text=[[f"{labels[i][j]}<br>{z[i][j]:,}" for j in range(2)] for i in range(2)],
            texttemplate="%{text}",
            textfont={"size": 14, "color": "white"},
            hoverinfo="skip",
            showscale=False,
        )
    )

    fig.update_layout(
        title="Confusion Matrix",
        xaxis={"tickfont": {"color": "white"}},
        yaxis={"tickfont": {"color": "white"}, "autorange": "reversed"},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=350,
        margin={"l": 100, "r": 50, "t": 50, "b": 50},
    )

    return fig


def create_latency_distribution_chart(latencies: list[float]) -> go.Figure:
    """Create latency distribution histogram."""

    fig = go.Figure(
        data=[
            go.Histogram(
                x=latencies,
                nbinsx=50,
                marker={
                    "color": "rgba(102, 126, 234, 0.7)",
                    "line": {"color": "rgba(102, 126, 234, 1)", "width": 1},
                },
            )
        ]
    )

    # Add target line
    fig.add_vline(
        x=30,
        line={"color": "#00ff88", "width": 2, "dash": "dash"},
        annotation={"text": "Target: 30ms", "font": {"color": "#00ff88"}},
    )

    fig.update_layout(
        title="Inference Latency Distribution",
        xaxis={
            "title": "Latency (ms)",
            "titlefont": {"color": "white"},
            "tickfont": {"color": "white"},
            "gridcolor": "rgba(255,255,255,0.1)",
        },
        yaxis={
            "title": "Frequency",
            "titlefont": {"color": "white"},
            "tickfont": {"color": "white"},
            "gridcolor": "rgba(255,255,255,0.1)",
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=300,
    )

    return fig


# =============================================================================
# Main Dashboard
# =============================================================================


def main():
    """Main dashboard application."""

    inject_competition_css()

    # Header
    st.markdown(
        """
    <div class="competition-header">
        <h1 class="competition-title">🏆 SentinelFlow Competition Dashboard</h1>
        <p class="competition-subtitle">TEKNOFEST 2026 - Finansal Teknolojiler | Hedef: %99.5+ Doğruluk</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Sidebar
    with st.sidebar:
        st.markdown("### ⚙️ Dashboard Ayarları")

        st.slider("Yenileme Hızı (saniye)", 1, 30, 5)
        st.checkbox("Baseline'ı Göster", value=True)

        st.markdown("---")

        st.markdown("### 📊 Benchmark Bilgileri")
        st.info(
            """
        **Geçen Yıl 1. (2025)**
        - Doğruluk: %99.2
        - Veri Seti: 200K işlem

        **Hedefimiz (2026)**
        - Doğruluk: %99.5+
        - Veri Seti: 500K+ işlem
        """
        )

    # Mock data (would be replaced with real data from ML pipeline)
    metrics = {
        "StackingEnsemble": {
            "accuracy": 0.9952,
            "precision": 0.9921,
            "recall": 0.9867,
            "f1": 0.9894,
            "auc_roc": 0.9978,
        },
        "LightGBM": {
            "accuracy": 0.9938,
            "precision": 0.9905,
            "recall": 0.9812,
            "f1": 0.9858,
            "auc_roc": 0.9965,
        },
        "CatBoost": {
            "accuracy": 0.9935,
            "precision": 0.9898,
            "recall": 0.9805,
            "f1": 0.9851,
            "auc_roc": 0.9962,
        },
        "XGBoost": {
            "accuracy": 0.9925,
            "precision": 0.9878,
            "recall": 0.9789,
            "f1": 0.9833,
            "auc_roc": 0.9955,
        },
        "GNN": {
            "accuracy": 0.9912,
            "precision": 0.9856,
            "recall": 0.9756,
            "f1": 0.9806,
            "auc_roc": 0.9945,
        },
    }

    baseline = 0.992  # Last year's winner

    # Top metrics row
    st.markdown("### 📈 Ana Performans Metrikleri")

    col1, col2, col3, col4 = st.columns(4)

    best_model = max(metrics.items(), key=lambda x: x[1]["f1"])
    best_accuracy = best_model[1]["accuracy"]
    best_f1 = best_model[1]["f1"]
    improvement = (best_accuracy - baseline) * 100

    with col1:
        st.metric(
            label="En İyi Doğruluk",
            value=f"{best_accuracy * 100:.2f}%",
            delta=(
                f"+{improvement:.2f}% vs baseline"
                if improvement > 0
                else f"{improvement:.2f}% vs baseline"
            ),
        )

    with col2:
        st.metric(
            label="En İyi F1 Skoru",
            value=f"{best_f1 * 100:.2f}%",
            delta=f"{best_model[0]}",
        )

    with col3:
        st.metric(
            label="Ortalama Latency",
            value="25.4 ms",
            delta="-4.6 ms hedefin altında",
        )

    with col4:
        st.metric(
            label="İşlenen Veri",
            value="523,847",
            delta="+323K vs geçen yıl",
        )

    st.markdown("---")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Model Karşılaştırma", "🎯 Hedef Analizi", "🔬 Feature Analizi", "⚡ Performans"]
    )

    with tab1:
        st.markdown("### Model Performans Karşılaştırması")

        col1, col2 = st.columns(2)

        with col1:
            # Radar chart
            st.plotly_chart(
                create_model_comparison_chart(metrics),
                use_container_width=True,
            )

        with col2:
            # Model ranking table
            st.markdown("#### 🏆 Model Sıralaması")

            ranking_data = []
            for name, m in sorted(metrics.items(), key=lambda x: x[1]["f1"], reverse=True):
                vs_baseline = (m["accuracy"] - baseline) * 100
                status = "✅" if vs_baseline > 0 else "⚠️" if vs_baseline > -0.5 else "❌"

                ranking_data.append(
                    {
                        "Rank": len(ranking_data) + 1,
                        "Model": name,
                        "Accuracy": f"{m['accuracy'] * 100:.2f}%",
                        "F1": f"{m['f1'] * 100:.2f}%",
                        "AUC": f"{m['auc_roc'] * 100:.2f}%",
                        "vs Baseline": f"{vs_baseline:+.2f}%",
                        "Status": status,
                    }
                )

            st.dataframe(
                pd.DataFrame(ranking_data),
                use_container_width=True,
                hide_index=True,
            )

    with tab2:
        st.markdown("### 🎯 Baseline Karşılaştırma")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.plotly_chart(
                create_gauge_chart(best_accuracy, "Doğruluk", baseline),
                use_container_width=True,
            )

        with col2:
            st.plotly_chart(
                create_gauge_chart(best_f1, "F1 Skoru", 0.985),
                use_container_width=True,
            )

        with col3:
            st.plotly_chart(
                create_gauge_chart(best_model[1]["auc_roc"], "AUC-ROC", 0.995),
                use_container_width=True,
            )

        # Confusion matrix
        st.markdown("### Confusion Matrix (En İyi Model)")

        col1, col2 = st.columns([1, 1])

        with col1:
            # Mock confusion matrix values
            tp, tn, fp, fn = 15234, 485612, 1023, 978

            st.plotly_chart(
                create_confusion_matrix_chart(tp, tn, fp, fn),
                use_container_width=True,
            )

        with col2:
            st.markdown("#### 📊 Detaylı Metrikler")

            total = tp + tn + fp + fn

            st.markdown(
                f"""
            | Metrik | Değer |
            |--------|-------|
            | **True Positives** | {tp:,} |
            | **True Negatives** | {tn:,} |
            | **False Positives** | {fp:,} |
            | **False Negatives** | {fn:,} |
            | **Total** | {total:,} |
            | **Precision** | {tp / (tp + fp) * 100:.2f}% |
            | **Recall** | {tp / (tp + fn) * 100:.2f}% |
            | **Specificity** | {tn / (tn + fp) * 100:.2f}% |
            """
            )

    with tab3:
        st.markdown("### 🔬 Feature Importance Analizi")

        # Mock feature importances
        feature_importances = {
            "amount_deviation_score": 0.152,
            "composite_risk_score": 0.128,
            "ring_participation_count": 0.115,
            "structuring_detection_score": 0.098,
            "receiver_novelty_score": 0.087,
            "velocity_deviation_score": 0.076,
            "benford_deviation_score": 0.068,
            "masak_threshold_proximity": 0.062,
            "neighbor_fraud_ratio": 0.055,
            "time_since_last_tx_hours": 0.048,
            "amount_zscore_user": 0.042,
            "hour_deviation_score": 0.038,
            "off_hours_flag": 0.031,
            "channel_deviation_score": 0.025,
            "fan_out_score": 0.022,
        }

        st.plotly_chart(
            create_feature_importance_chart(feature_importances),
            use_container_width=True,
        )

        # Feature categories
        st.markdown("#### 📁 Özellik Kategorileri")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(
                """
            **Davranışsal Özellikler (8)**
            - amount_deviation_score
            - velocity_deviation_score
            - receiver_novelty_score
            - hour_deviation_score
            - ...
            """
            )

        with col2:
            st.markdown(
                """
            **Risk Özellikleri (5)**
            - composite_risk_score
            - structuring_detection_score
            - masak_threshold_proximity
            - mule_account_score
            - ...
            """
            )

        with col3:
            st.markdown(
                """
            **Graf Özellikleri (6)**
            - ring_participation_count
            - neighbor_fraud_ratio
            - pagerank_score
            - community_fraud_ratio
            - ...
            """
            )

    with tab4:
        st.markdown("### ⚡ Performans Analizi")

        col1, col2 = st.columns(2)

        with col1:
            # Mock latency data
            latencies = np.random.lognormal(2.5, 0.5, 1000)
            latencies = np.clip(latencies, 5, 100)

            st.plotly_chart(
                create_latency_distribution_chart(latencies.tolist()),
                use_container_width=True,
            )

            st.markdown(
                f"""
            **Latency İstatistikleri:**
            - Ortalama: {np.mean(latencies):.1f}ms
            - Median: {np.median(latencies):.1f}ms
            - P95: {np.percentile(latencies, 95):.1f}ms
            - P99: {np.percentile(latencies, 99):.1f}ms
            - Min: {np.min(latencies):.1f}ms
            - Max: {np.max(latencies):.1f}ms
            """
            )

        with col2:
            st.markdown("#### 🎯 Performans Hedefleri")

            targets = [
                ("Latency < 30ms", 25.4, 30, True),
                ("Accuracy > 99.2%", 99.52, 99.2, True),
                ("F1 > 98.5%", 98.94, 98.5, True),
                ("AUC > 99.5%", 99.78, 99.5, True),
                ("Memory < 2GB", 1.2, 2.0, True),
            ]

            for target_name, actual, target, met in targets:
                color = "#00ff88" if met else "#ff6b6b"
                icon = "✅" if met else "❌"

                st.markdown(
                    f"""
                <div style="display: flex; justify-content: space-between; padding: 10px;
                            background: rgba(26,26,46,0.8); border-radius: 10px; margin: 5px 0;
                            border-left: 4px solid {color};">
                    <span style="color: white;">{icon} {target_name}</span>
                    <span style="color: {color}; font-weight: bold;">{actual} / {target}</span>
                </div>
                """,
                    unsafe_allow_html=True,
                )

    # Footer
    st.markdown("---")
    st.markdown(
        """
    <div style="text-align: center; color: #666; padding: 1rem;">
        SentinelFlow © 2026 | TEKNOFEST Finansal Teknolojiler Yarışması
        <br>
        <small>Son güncelleme: {}</small>
    </div>
    """.format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ),
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

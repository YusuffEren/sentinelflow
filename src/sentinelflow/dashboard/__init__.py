# =============================================================================
# SentinelFlow - Dashboard Package
# =============================================================================
"""
Streamlit-based dashboard for SentinelFlow fraud detection monitoring.

Components:
- app.py: Main dashboard application
- components.py: Reusable UI components
- i18n.py: Internationalization support
"""

from sentinelflow.dashboard.i18n import get_translations, t, I18n
from sentinelflow.dashboard.components import (
    metric_card,
    metric_row,
    alert_card,
    alert_list,
    status_indicator,
    system_status_panel,
    ring_stats_panel,
    compliance_summary,
    model_performance_card,
    ml_dashboard_panel,
    timeline_item,
)

__all__ = [
    # i18n
    "get_translations",
    "t",
    "I18n",
    # components
    "metric_card",
    "metric_row",
    "alert_card",
    "alert_list",
    "status_indicator",
    "system_status_panel",
    "ring_stats_panel",
    "compliance_summary",
    "model_performance_card",
    "ml_dashboard_panel",
    "timeline_item",
]

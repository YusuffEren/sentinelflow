# =============================================================================
# SentinelFlow - Dashboard Internationalization
# =============================================================================
"""
Multi-language support for SentinelFlow dashboard.

Supports:
- Turkish (tr)
- English (en)

Usage:
    from sentinelflow.dashboard.i18n import get_translations, t
    
    texts = get_translations("tr")
    print(texts["dashboard_title"])
"""

from __future__ import annotations

from typing import Dict, Any

# =============================================================================
# Translation Dictionaries
# =============================================================================

TRANSLATIONS: Dict[str, Dict[str, str]] = {
    "tr": {
        # Dashboard
        "dashboard_title": "SentinelFlow",
        "dashboard_subtitle": "Gerçek Zamanlı Finansal Dolandırıcılık Tespit Merkezi",
        "command_center": "Komuta Merkezi",
        "live_monitoring": "Canlı İzleme",
        
        # Metrics
        "total_transactions": "Toplam İşlem",
        "fraud_alerts": "Dolandırıcılık Alarmı",
        "fraud_rings": "Tespit Edilen Halkalar",
        "fraud_rate": "Dolandırıcılık Oranı",
        "avg_response_time": "Ort. Yanıt Süresi",
        "compliance_violations": "Uyum İhlalleri",
        
        # Alert Types
        "circular_ring": "Döngüsel Halka",
        "impossible_travel": "İmkansız Seyahat",
        "blacklist_keyword": "Kara Liste Anahtar Kelime",
        "ai_detected_anomaly": "AI Anomali Tespiti",
        "ml_ensemble": "ML Ensemble Tespiti",
        "mule_account": "Katır Hesap",
        
        # Severity
        "critical": "KRİTİK",
        "high": "YÜKSEK",
        "medium": "ORTA",
        "low": "DÜŞÜK",
        
        # Sections
        "live_alert_feed": "Canlı Alarm Akışı",
        "fraud_ring_visualization": "Dolandırıcılık Halkası Görselleştirme",
        "geographic_analysis": "Coğrafi Analiz",
        "ml_model_performance": "ML Model Performansı",
        "compliance_dashboard": "Uyum Panosu",
        "system_health": "Sistem Sağlığı",
        
        # Status
        "connected": "Bağlı",
        "disconnected": "Bağlantı Yok",
        "processing": "İşleniyor",
        "waiting": "Bekliyor",
        
        # Actions
        "refresh": "Yenile",
        "export": "Dışa Aktar",
        "filter": "Filtrele",
        "clear": "Temizle",
        "investigate": "İncele",
        "dismiss": "Reddet",
        "escalate": "Üst Kademeye İlet",
        
        # Time
        "last_updated": "Son Güncelleme",
        "today": "Bugün",
        "this_week": "Bu Hafta",
        "this_month": "Bu Ay",
        "seconds_ago": "saniye önce",
        "minutes_ago": "dakika önce",
        "hours_ago": "saat önce",
        
        # Messages
        "no_alerts": "Aktif alarm yok",
        "loading": "Yükleniyor...",
        "error_loading": "Yükleme hatası",
        "connection_lost": "Bağlantı koptu",
        "reconnecting": "Yeniden bağlanılıyor...",
        
        # Graph
        "nodes": "Düğümler",
        "edges": "Kenarlar",
        "ring_members": "Halka Üyeleri",
        "total_amount": "Toplam Tutar",
        "transaction_count": "İşlem Sayısı",
        
        # Compliance
        "masak_reports": "MASAK Bildirimleri",
        "pending_reports": "Bekleyen Raporlar",
        "submitted_reports": "Gönderilen Raporlar",
        "str_generated": "ŞİB Oluşturuldu",
        
        # Settings
        "settings": "Ayarlar",
        "language": "Dil",
        "theme": "Tema",
        "notifications": "Bildirimler",
        "sound_alerts": "Sesli Uyarılar",
        "auto_refresh": "Otomatik Yenileme",
        "refresh_interval": "Yenileme Aralığı",
    },
    
    "en": {
        # Dashboard
        "dashboard_title": "SentinelFlow",
        "dashboard_subtitle": "Real-Time Financial Fraud Detection Center",
        "command_center": "Command Center",
        "live_monitoring": "Live Monitoring",
        
        # Metrics
        "total_transactions": "Total Transactions",
        "fraud_alerts": "Fraud Alerts",
        "fraud_rings": "Detected Rings",
        "fraud_rate": "Fraud Rate",
        "avg_response_time": "Avg. Response Time",
        "compliance_violations": "Compliance Violations",
        
        # Alert Types
        "circular_ring": "Circular Ring",
        "impossible_travel": "Impossible Travel",
        "blacklist_keyword": "Blacklist Keyword",
        "ai_detected_anomaly": "AI Anomaly Detection",
        "ml_ensemble": "ML Ensemble Detection",
        "mule_account": "Mule Account",
        
        # Severity
        "critical": "CRITICAL",
        "high": "HIGH",
        "medium": "MEDIUM",
        "low": "LOW",
        
        # Sections
        "live_alert_feed": "Live Alert Feed",
        "fraud_ring_visualization": "Fraud Ring Visualization",
        "geographic_analysis": "Geographic Analysis",
        "ml_model_performance": "ML Model Performance",
        "compliance_dashboard": "Compliance Dashboard",
        "system_health": "System Health",
        
        # Status
        "connected": "Connected",
        "disconnected": "Disconnected",
        "processing": "Processing",
        "waiting": "Waiting",
        
        # Actions
        "refresh": "Refresh",
        "export": "Export",
        "filter": "Filter",
        "clear": "Clear",
        "investigate": "Investigate",
        "dismiss": "Dismiss",
        "escalate": "Escalate",
        
        # Time
        "last_updated": "Last Updated",
        "today": "Today",
        "this_week": "This Week",
        "this_month": "This Month",
        "seconds_ago": "seconds ago",
        "minutes_ago": "minutes ago",
        "hours_ago": "hours ago",
        
        # Messages
        "no_alerts": "No active alerts",
        "loading": "Loading...",
        "error_loading": "Error loading",
        "connection_lost": "Connection lost",
        "reconnecting": "Reconnecting...",
        
        # Graph
        "nodes": "Nodes",
        "edges": "Edges",
        "ring_members": "Ring Members",
        "total_amount": "Total Amount",
        "transaction_count": "Transaction Count",
        
        # Compliance
        "masak_reports": "MASAK Reports",
        "pending_reports": "Pending Reports",
        "submitted_reports": "Submitted Reports",
        "str_generated": "STR Generated",
        
        # Settings
        "settings": "Settings",
        "language": "Language",
        "theme": "Theme",
        "notifications": "Notifications",
        "sound_alerts": "Sound Alerts",
        "auto_refresh": "Auto Refresh",
        "refresh_interval": "Refresh Interval",
    },
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_translations(lang: str = "tr") -> Dict[str, str]:
    """
    Get translations for a language.
    
    Args:
        lang: Language code ("tr" or "en")
    
    Returns:
        Dictionary of translations
    """
    return TRANSLATIONS.get(lang, TRANSLATIONS["en"])


def t(key: str, lang: str = "tr", **kwargs) -> str:
    """
    Translate a key.
    
    Args:
        key: Translation key
        lang: Language code
        **kwargs: Format arguments
    
    Returns:
        Translated string
    """
    translations = get_translations(lang)
    text = translations.get(key, key)
    
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    
    return text


def get_available_languages() -> list[tuple[str, str]]:
    """Get list of available languages."""
    return [
        ("tr", "Türkçe"),
        ("en", "English"),
    ]


class I18n:
    """Internationalization helper class."""
    
    def __init__(self, default_lang: str = "tr"):
        self._lang = default_lang
        self._translations = get_translations(default_lang)
    
    def set_language(self, lang: str) -> None:
        """Set current language."""
        self._lang = lang
        self._translations = get_translations(lang)
    
    def get(self, key: str, **kwargs) -> str:
        """Get translation for key."""
        return t(key, self._lang, **kwargs)
    
    def __getitem__(self, key: str) -> str:
        """Allow dict-like access."""
        return self.get(key)
    
    @property
    def current_language(self) -> str:
        """Get current language code."""
        return self._lang

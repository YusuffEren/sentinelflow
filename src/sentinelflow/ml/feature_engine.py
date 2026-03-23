# =============================================================================
# SentinelFlow - Transaction Feature Engineering
# =============================================================================
"""
Extracts rich feature vectors from raw transaction data for ML models.

Feature Groups:
1. Amount Features     - Statistical properties of transaction amount
2. Temporal Features   - Time-based patterns
3. Velocity Features   - Transaction frequency and volume
4. Description Features - Text-based signals
5. Geographic Features - Spatial relationships

Usage:
    engine = TransactionFeatureEngine()
    features = engine.extract(tx_data)
    # features = {"amount_log": 8.52, "hour_of_day": 14, ...}
"""

from __future__ import annotations

import math
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np
from loguru import logger


# =============================================================================
# Constants
# =============================================================================

SUSPICIOUS_CHARS = re.compile(r"[!@#$%^&*(){}|<>~`]")

SUSPICIOUS_KEYWORDS: list[str] = [
    # Gambling / Betting
    "bahis",
    "casino",
    "kumar",
    "poker",
    "rulet",
    "slot",
    "bet365",
    "betting",
    # Cryptocurrency
    "kripto",
    "bitcoin",
    "btc",
    "ethereum",
    "usdt",
    "binance",
    "tether",
    # Anonymity / Evasion
    "offshore",
    "anonim",
    "gizli",
    "anonymous",
    "proxy",
    "vpn",
    # Urgency
    "acil",
    "urgent",
    "hemen",
    "immediately",
    # Money laundering
    "yıkama",
    "laundering",
    "temizleme",
    "aklama",
]

# City coordinates for distance calculation
CITY_COORDS: dict[str, tuple[float, float]] = {
    "İstanbul": (41.0082, 28.9784),
    "Istanbul": (41.0082, 28.9784),
    "Ankara": (39.9334, 32.8597),
    "İzmir": (38.4192, 27.1287),
    "Izmir": (38.4192, 27.1287),
    "Bursa": (40.1885, 29.0610),
    "Antalya": (36.8969, 30.7133),
    "Adana": (37.0000, 35.3213),
    "Konya": (37.8746, 32.4932),
    "Gaziantep": (37.0662, 37.3833),
    "Mersin": (36.8121, 34.6415),
    "Diyarbakır": (37.9144, 40.2306),
    "Kayseri": (38.7312, 35.4787),
    "Eskişehir": (39.7767, 30.5206),
    "Trabzon": (41.0027, 39.7168),
    "Samsun": (41.2867, 36.3300),
    "Denizli": (37.7765, 29.0864),
    "Berlin": (52.5200, 13.4050),
    "London": (51.5074, -0.1278),
    "Paris": (48.8566, 2.3522),
    "Dubai": (25.2048, 55.2708),
    "Moscow": (55.7558, 37.6173),
    "New York": (40.7128, -74.0060),
    "Tokyo": (35.6762, 139.6503),
}

# Feature names for ordered output
FEATURE_NAMES: list[str] = [
    # Amount features (5)
    "amount_raw",
    "amount_log",
    "amount_zscore",
    "amount_to_mean_ratio",
    "amount_percentile",
    # Temporal features (5)
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_night",
    "hour_sin",
    # Velocity features (4)
    "sender_tx_count_1h",
    "sender_tx_count_24h",
    "sender_amount_sum_1h",
    "sender_avg_amount",
    # Description features (4)
    "desc_length",
    "desc_word_count",
    "has_suspicious_chars",
    "keyword_score",
    # Geographic features (3)
    "city_distance_km",
    "sender_receiver_same_city",
    "is_international",
]

NUM_FEATURES = len(FEATURE_NAMES)


# =============================================================================
# Sliding Window Tracker
# =============================================================================


@dataclass
class AccountHistory:
    """Tracks per-account transaction history for velocity features."""

    timestamps: deque[datetime] = field(default_factory=lambda: deque(maxlen=500))
    amounts: deque[float] = field(default_factory=lambda: deque(maxlen=500))
    counterparts: set[str] = field(default_factory=set)


# =============================================================================
# Feature Engine
# =============================================================================


class TransactionFeatureEngine:
    """
    Extracts feature vectors from raw transaction data.

    Maintains per-account sliding windows for velocity and statistical features.
    Thread-safe for single-threaded Kafka consumer processing.

    Usage:
        engine = TransactionFeatureEngine()
        features_dict = engine.extract(tx_data)
        features_vector = engine.extract_vector(tx_data)
    """

    def __init__(self, history_window_size: int = 500) -> None:
        """
        Initialize the feature engine.

        Args:
            history_window_size: Max transactions to keep per account
        """
        self._account_histories: dict[str, AccountHistory] = defaultdict(AccountHistory)
        self._global_amounts: deque[float] = deque(maxlen=5000)
        self._history_window_size = history_window_size

        logger.info(f"TransactionFeatureEngine initialized (window={history_window_size})")

    def extract(self, tx_data: dict[str, Any]) -> dict[str, float]:
        """
        Extract all features from a transaction as a named dictionary.

        Args:
            tx_data: Raw transaction data from Kafka

        Returns:
            Dictionary mapping feature names to float values
        """
        features: dict[str, float] = {}

        amount = float(tx_data.get("amount", 0.0))
        sender_iban = tx_data.get("sender_iban", "")
        receiver_iban = tx_data.get("receiver_iban", "")
        description = tx_data.get("description", "")
        sender_city = tx_data.get("sender_city", "")
        receiver_city = tx_data.get("receiver_city", "")
        timestamp = self._parse_timestamp(tx_data.get("timestamp", ""))

        # =====================================================================
        # 1. Amount Features
        # =====================================================================
        features["amount_raw"] = amount
        features["amount_log"] = math.log1p(amount)  # log(1+x) to handle 0

        # Z-score against global history
        if len(self._global_amounts) >= 10:
            mean = float(np.mean(self._global_amounts))
            std = float(np.std(self._global_amounts))
            features["amount_zscore"] = (amount - mean) / std if std > 0 else 0.0
            features["amount_to_mean_ratio"] = amount / mean if mean > 0 else 1.0

            # Percentile rank
            sorted_amounts = sorted(self._global_amounts)
            rank = sum(1 for a in sorted_amounts if a <= amount)
            features["amount_percentile"] = rank / len(sorted_amounts)
        else:
            features["amount_zscore"] = 0.0
            features["amount_to_mean_ratio"] = 1.0
            features["amount_percentile"] = 0.5

        # =====================================================================
        # 2. Temporal Features
        # =====================================================================
        features["hour_of_day"] = float(timestamp.hour)
        features["day_of_week"] = float(timestamp.weekday())
        features["is_weekend"] = 1.0 if timestamp.weekday() >= 5 else 0.0
        features["is_night"] = 1.0 if timestamp.hour < 6 or timestamp.hour >= 23 else 0.0
        # Cyclical encoding: hour as sin for ML models
        features["hour_sin"] = math.sin(2 * math.pi * timestamp.hour / 24)

        # =====================================================================
        # 3. Velocity Features (per-sender)
        # =====================================================================
        history = self._account_histories[sender_iban]
        now = timestamp

        # Count transactions in last 1h and 24h
        one_hour_ago = now - timedelta(hours=1)
        twenty_four_hours_ago = now - timedelta(hours=24)

        tx_count_1h = sum(1 for t in history.timestamps if t >= one_hour_ago)
        tx_count_24h = sum(1 for t in history.timestamps if t >= twenty_four_hours_ago)

        # Amount sum in last 1h
        amount_sum_1h = sum(
            a for t, a in zip(history.timestamps, history.amounts) if t >= one_hour_ago
        )

        # Average historical amount for this sender
        avg_amount = float(np.mean(history.amounts)) if history.amounts else amount

        features["sender_tx_count_1h"] = float(tx_count_1h)
        features["sender_tx_count_24h"] = float(tx_count_24h)
        features["sender_amount_sum_1h"] = amount_sum_1h
        features["sender_avg_amount"] = avg_amount

        # =====================================================================
        # 4. Description Features
        # =====================================================================
        features["desc_length"] = float(len(description))
        features["desc_word_count"] = float(len(description.split())) if description else 0.0
        features["has_suspicious_chars"] = 1.0 if SUSPICIOUS_CHARS.search(description) else 0.0

        # Keyword score: number of suspicious keywords found
        desc_lower = description.lower()
        keyword_hits = sum(1 for kw in SUSPICIOUS_KEYWORDS if kw in desc_lower)
        features["keyword_score"] = float(keyword_hits)

        # =====================================================================
        # 5. Geographic Features
        # =====================================================================
        sender_coords = CITY_COORDS.get(sender_city)
        receiver_coords = CITY_COORDS.get(receiver_city)

        if sender_coords and receiver_coords:
            features["city_distance_km"] = self._haversine(
                sender_coords[0],
                sender_coords[1],
                receiver_coords[0],
                receiver_coords[1],
            )
        else:
            features["city_distance_km"] = 0.0

        features["sender_receiver_same_city"] = 1.0 if sender_city == receiver_city else 0.0

        turkish_cities = {
            "İstanbul",
            "Istanbul",
            "Ankara",
            "İzmir",
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
            "Trabzon",
            "Samsun",
            "Denizli",
        }
        is_sender_turkish = sender_city in turkish_cities
        is_receiver_turkish = receiver_city in turkish_cities
        features["is_international"] = 0.0 if (is_sender_turkish and is_receiver_turkish) else 1.0

        # =====================================================================
        # Update histories
        # =====================================================================
        self._global_amounts.append(amount)
        history.timestamps.append(timestamp)
        history.amounts.append(amount)
        history.counterparts.add(receiver_iban)

        return features

    def extract_vector(self, tx_data: dict[str, Any]) -> np.ndarray:
        """
        Extract features as a numpy array in canonical order.

        Args:
            tx_data: Raw transaction data

        Returns:
            1D numpy array of shape (NUM_FEATURES,)
        """
        features = self.extract(tx_data)
        return np.array([features.get(name, 0.0) for name in FEATURE_NAMES], dtype=np.float64)

    @staticmethod
    def get_feature_names() -> list[str]:
        """Return the ordered list of feature names."""
        return FEATURE_NAMES.copy()

    @staticmethod
    def _parse_timestamp(ts_str: str) -> datetime:
        """Parse ISO timestamp string, falling back to now."""
        if not ts_str:
            return datetime.utcnow()
        try:
            if "T" in ts_str:
                return datetime.fromisoformat(ts_str.replace("Z", "+00:00").replace("+00:00", ""))
            return datetime.utcnow()
        except (ValueError, TypeError):
            return datetime.utcnow()

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance in km between two points."""
        R = 6371.0  # Earth radius in km

        lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
        lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

        dlat = lat2_r - lat1_r
        dlon = lon2_r - lon1_r

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    @property
    def accounts_tracked(self) -> int:
        """Number of unique accounts being tracked."""
        return len(self._account_histories)

    @property
    def global_transaction_count(self) -> int:
        """Total transactions processed."""
        return len(self._global_amounts)

# =============================================================================
# SentinelFlow - Advanced Feature Engineering for TEKNOFEST Competition
# =============================================================================
"""
Gelişmiş özellik mühendisliği - TEKNOFEST birinciliği için tasarlandı.

Yeni Özellik Grupları:
1. Behavioral Features   - Davranışsal kalıplar ve anormallik tespiti
2. Benford's Law         - Sayısal anormallik analizi
3. Network Features      - Graf tabanlı ilişki özellikleri
4. Device/Channel        - Cihaz ve kanal anomalileri
5. Temporal Patterns     - Zaman serisi kalıpları
6. Risk Scoring          - Kompozit risk skorları

Bu özellikler, standart özelliklerin üzerine eklenerek
%99.5+ doğruluk hedefini yakalamalıdır.
"""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
from loguru import logger

try:
    from scipy.stats import entropy

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    logger.warning("scipy not available, some advanced features disabled")


# =============================================================================
# Constants
# =============================================================================

# Benford's Law expected distribution for first digit
BENFORD_EXPECTED = np.array([0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046])

# Risk thresholds (MASAK/BDDK regulations)
MASAK_THRESHOLD_TL = 75000.0  # STR reporting threshold
BDDK_LARGE_TX_TL = 50000.0  # Large transaction
SUSPICIOUS_ROUND_AMOUNTS = [1000, 5000, 10000, 25000, 50000, 75000, 100000]

# Turkish banking hours
TURKISH_BANKING_HOURS = (9, 17)  # 9 AM to 5 PM
TURKISH_TIMEZONE_OFFSET = 3  # UTC+3


class RiskLevel(str, Enum):
    """Risk seviyeleri."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class BehaviorProfile:
    """Kullanıcı davranış profili."""

    # Zaman kalıpları
    typical_hours: list[int] = field(default_factory=list)
    typical_days: list[int] = field(default_factory=list)
    hour_entropy: float = 0.0

    # Miktar kalıpları
    avg_amount: float = 0.0
    std_amount: float = 0.0
    median_amount: float = 0.0
    max_amount: float = 0.0
    typical_amount_range: tuple[float, float] = (0.0, 0.0)

    # Hız kalıpları
    avg_tx_per_day: float = 0.0
    avg_tx_per_week: float = 0.0
    burst_threshold: float = 0.0

    # Alıcı kalıpları
    frequent_receivers: set[str] = field(default_factory=set)
    unique_receiver_count: int = 0
    receiver_concentration: float = 0.0  # Herfindahl index

    # Cihaz/Kanal
    typical_channels: set[str] = field(default_factory=set)
    typical_devices: set[str] = field(default_factory=set)


@dataclass
class AccountActivity:
    """Hesap aktivite geçmişi (sliding window)."""

    # Zaman damgaları ve miktarlar
    timestamps: deque = field(default_factory=lambda: deque(maxlen=1000))
    amounts: deque = field(default_factory=lambda: deque(maxlen=1000))

    # Alıcılar
    receivers: deque = field(default_factory=lambda: deque(maxlen=1000))
    receiver_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Kanallar ve cihazlar
    channels: deque = field(default_factory=lambda: deque(maxlen=500))
    devices: deque = field(default_factory=lambda: deque(maxlen=500))

    # İşlem türleri
    transaction_types: deque = field(default_factory=lambda: deque(maxlen=500))

    # Önceki işlem bilgileri
    last_tx_timestamp: datetime | None = None
    last_tx_amount: float = 0.0
    last_tx_receiver: str = ""

    # Günlük/haftalık istatistikler
    daily_tx_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    daily_amounts: dict[str, float] = field(default_factory=lambda: defaultdict(float))


# =============================================================================
# ADVANCED FEATURE NAMES
# =============================================================================

ADVANCED_FEATURE_NAMES: list[str] = [
    # Behavioral deviation features (8)
    "amount_deviation_score",
    "hour_deviation_score",
    "velocity_deviation_score",
    "receiver_novelty_score",
    "channel_deviation_score",
    "behavior_anomaly_composite",
    "time_since_last_tx_hours",
    "amount_vs_last_tx_ratio",
    # Benford's Law features (3)
    "benford_deviation_score",
    "round_amount_score",
    "just_below_threshold_flag",
    # Network/Graph features (6)
    "unique_receivers_7d",
    "unique_receivers_30d",
    "receiver_concentration_score",
    "new_receiver_flag",
    "receiver_recency_score",
    "fan_out_score",
    # Temporal pattern features (6)
    "hour_entropy_deviation",
    "tx_velocity_1h",
    "tx_velocity_24h",
    "tx_velocity_7d",
    "burst_detection_score",
    "off_hours_flag",
    # Risk scoring features (5)
    "composite_risk_score",
    "masak_threshold_proximity",
    "structuring_detection_score",
    "rapid_movement_score",
    "mule_account_score",
    # Statistical features (4)
    "amount_percentile_user",
    "amount_zscore_user",
    "inter_arrival_time_zscore",
    "amount_volatility_7d",
]

NUM_ADVANCED_FEATURES = len(ADVANCED_FEATURE_NAMES)


# =============================================================================
# Advanced Feature Engine
# =============================================================================


class AdvancedFeatureEngine:
    """
    Gelişmiş özellik mühendisliği motoru.

    Bu motor, standart özelliklerin üzerine davranışsal, istatistiksel
    ve graf tabanlı özellikler ekleyerek fraud tespitini güçlendirir.

    TEKNOFEST için hedef: %99.5+ doğruluk

    Usage:
        engine = AdvancedFeatureEngine()
        features = engine.extract(tx_data, user_history)
    """

    def __init__(
        self,
        history_window_days: int = 30,
        behavior_profile_min_samples: int = 10,
    ) -> None:
        """
        Initialize advanced feature engine.

        Args:
            history_window_days: Geçmiş penceresi (gün)
            behavior_profile_min_samples: Profil için min işlem sayısı
        """
        self._history_window_days = history_window_days
        self._min_samples = behavior_profile_min_samples

        # Per-account activity tracking
        self._account_activities: dict[str, AccountActivity] = defaultdict(AccountActivity)

        # Global statistics for Benford analysis
        self._global_first_digits: list[int] = []
        self._global_amounts: deque = deque(maxlen=100000)

        logger.info(
            f"AdvancedFeatureEngine initialized "
            f"(window={history_window_days}d, min_samples={behavior_profile_min_samples})"
        )

    def extract(
        self,
        tx_data: dict[str, Any],
        include_base: bool = False,
    ) -> dict[str, float]:
        """
        Extract advanced features from transaction.

        Args:
            tx_data: Transaction data dictionary
            include_base: Whether to include base features

        Returns:
            Feature dictionary
        """
        features: dict[str, float] = {}

        # Parse transaction data
        amount = float(tx_data.get("amount", 0.0))
        sender_iban = tx_data.get("sender_iban", "")
        receiver_iban = tx_data.get("receiver_iban", "")
        timestamp = self._parse_timestamp(tx_data.get("timestamp", ""))
        channel = tx_data.get("channel", "unknown")
        device_id = tx_data.get("device_id", "")

        # Get account history
        activity = self._account_activities[sender_iban]

        # =====================================================================
        # 1. Behavioral Deviation Features
        # =====================================================================
        behavioral_features = self._extract_behavioral_features(
            amount, timestamp, receiver_iban, channel, activity
        )
        features.update(behavioral_features)

        # =====================================================================
        # 2. Benford's Law Features
        # =====================================================================
        benford_features = self._extract_benford_features(amount)
        features.update(benford_features)

        # =====================================================================
        # 3. Network/Graph Features
        # =====================================================================
        network_features = self._extract_network_features(receiver_iban, timestamp, activity)
        features.update(network_features)

        # =====================================================================
        # 4. Temporal Pattern Features
        # =====================================================================
        temporal_features = self._extract_temporal_features(timestamp, activity)
        features.update(temporal_features)

        # =====================================================================
        # 5. Risk Scoring Features
        # =====================================================================
        risk_features = self._extract_risk_features(amount, timestamp, receiver_iban, activity)
        features.update(risk_features)

        # =====================================================================
        # 6. Statistical Features
        # =====================================================================
        stat_features = self._extract_statistical_features(amount, timestamp, activity)
        features.update(stat_features)

        # =====================================================================
        # Update Activity History
        # =====================================================================
        self._update_activity(activity, amount, timestamp, receiver_iban, channel, device_id)

        # Update global statistics
        self._update_global_stats(amount)

        return features

    def extract_vector(self, tx_data: dict[str, Any]) -> np.ndarray:
        """Extract features as numpy array."""
        features = self.extract(tx_data)
        return np.array(
            [features.get(name, 0.0) for name in ADVANCED_FEATURE_NAMES], dtype=np.float64
        )

    # =========================================================================
    # Behavioral Features
    # =========================================================================

    def _extract_behavioral_features(
        self,
        amount: float,
        timestamp: datetime,
        receiver_iban: str,
        channel: str,
        activity: AccountActivity,
    ) -> dict[str, float]:
        """Davranışsal sapma özellikleri çıkar."""
        features = {}

        # Amount deviation
        if len(activity.amounts) >= self._min_samples:
            amounts = list(activity.amounts)
            mean_amt = np.mean(amounts)
            std_amt = np.std(amounts)
            if std_amt > 0:
                features["amount_deviation_score"] = abs(amount - mean_amt) / std_amt
            else:
                features["amount_deviation_score"] = 0.0
        else:
            features["amount_deviation_score"] = 0.0

        # Hour deviation
        if len(activity.timestamps) >= self._min_samples:
            typical_hours = [t.hour for t in activity.timestamps]
            hour_counts = Counter(typical_hours)
            most_common_hours = [h for h, _ in hour_counts.most_common(5)]
            current_hour = timestamp.hour
            features["hour_deviation_score"] = 0.0 if current_hour in most_common_hours else 1.0
        else:
            features["hour_deviation_score"] = 0.0

        # Velocity deviation (tx count in last hour vs average)
        if len(activity.timestamps) >= self._min_samples:
            one_hour_ago = timestamp - timedelta(hours=1)
            tx_count_1h = sum(1 for t in activity.timestamps if t >= one_hour_ago)

            # Calculate average hourly velocity
            if len(activity.timestamps) >= 2:
                time_span = (
                    max(activity.timestamps) - min(activity.timestamps)
                ).total_seconds() / 3600
                avg_hourly_velocity = len(activity.timestamps) / max(time_span, 1)
                if avg_hourly_velocity > 0:
                    features["velocity_deviation_score"] = tx_count_1h / avg_hourly_velocity
                else:
                    features["velocity_deviation_score"] = float(tx_count_1h)
            else:
                features["velocity_deviation_score"] = float(tx_count_1h)
        else:
            features["velocity_deviation_score"] = 0.0

        # Receiver novelty (have we seen this receiver before?)
        features["receiver_novelty_score"] = (
            0.0 if receiver_iban in activity.receiver_counts else 1.0
        )

        # Channel deviation
        typical_channels = set(activity.channels)
        features["channel_deviation_score"] = (
            0.0 if channel in typical_channels or len(typical_channels) == 0 else 1.0
        )

        # Composite anomaly score
        features["behavior_anomaly_composite"] = (
            features["amount_deviation_score"] * 0.3
            + features["hour_deviation_score"] * 0.2
            + features["velocity_deviation_score"] * 0.2
            + features["receiver_novelty_score"] * 0.2
            + features["channel_deviation_score"] * 0.1
        )

        # Time since last transaction
        if activity.last_tx_timestamp:
            time_diff = (timestamp - activity.last_tx_timestamp).total_seconds() / 3600
            features["time_since_last_tx_hours"] = min(time_diff, 720)  # Cap at 30 days
        else:
            features["time_since_last_tx_hours"] = 720.0  # First transaction

        # Amount vs last transaction ratio
        if activity.last_tx_amount > 0:
            features["amount_vs_last_tx_ratio"] = amount / activity.last_tx_amount
        else:
            features["amount_vs_last_tx_ratio"] = 1.0

        return features

    # =========================================================================
    # Benford's Law Features
    # =========================================================================

    def _extract_benford_features(self, amount: float) -> dict[str, float]:
        """Benford Yasası tabanlı özellikler çıkar."""
        features = {}

        # Extract first digit
        if amount > 0:
            first_digit = int(str(abs(amount)).lstrip("0").replace(".", "")[0])
            if 1 <= first_digit <= 9:
                self._global_first_digits.append(first_digit)

        # Calculate Benford deviation score
        if len(self._global_first_digits) >= 100:
            observed_counts = np.zeros(9)
            for d in self._global_first_digits[-1000:]:  # Last 1000 digits
                if 1 <= d <= 9:
                    observed_counts[d - 1] += 1

            total = observed_counts.sum()
            if total > 0:
                observed_dist = observed_counts / total
                # Chi-squared distance from expected Benford distribution
                chi_sq = np.sum(
                    (observed_dist - BENFORD_EXPECTED) ** 2 / (BENFORD_EXPECTED + 1e-10)
                )
                features["benford_deviation_score"] = min(chi_sq * 10, 1.0)
            else:
                features["benford_deviation_score"] = 0.0
        else:
            features["benford_deviation_score"] = 0.0

        # Round amount score (suspicious if too round)
        features["round_amount_score"] = 0.0
        for threshold in SUSPICIOUS_ROUND_AMOUNTS:
            if abs(amount - threshold) < 100:
                features["round_amount_score"] = 1.0
                break
            if amount > 1000 and amount % 1000 == 0:
                features["round_amount_score"] = 0.8
            elif amount > 100 and amount % 100 == 0:
                features["round_amount_score"] = 0.5

        # Just below MASAK threshold (structuring detection)
        threshold_proximity = MASAK_THRESHOLD_TL - amount
        if 0 < threshold_proximity < 5000:
            features["just_below_threshold_flag"] = 1.0 - (threshold_proximity / 5000)
        else:
            features["just_below_threshold_flag"] = 0.0

        return features

    # =========================================================================
    # Network/Graph Features
    # =========================================================================

    def _extract_network_features(
        self,
        receiver_iban: str,
        timestamp: datetime,
        activity: AccountActivity,
    ) -> dict[str, float]:
        """Graf tabanlı ağ özellikleri çıkar."""
        features = {}

        # Unique receivers in time windows
        seven_days_ago = timestamp - timedelta(days=7)
        thirty_days_ago = timestamp - timedelta(days=30)

        receivers_7d = set()
        receivers_30d = set()

        for t, r in zip(activity.timestamps, activity.receivers):
            if t >= seven_days_ago:
                receivers_7d.add(r)
            if t >= thirty_days_ago:
                receivers_30d.add(r)

        features["unique_receivers_7d"] = float(len(receivers_7d))
        features["unique_receivers_30d"] = float(len(receivers_30d))

        # Receiver concentration (Herfindahl-Hirschman Index)
        if activity.receiver_counts:
            total_txs = sum(activity.receiver_counts.values())
            if total_txs > 0:
                shares = [count / total_txs for count in activity.receiver_counts.values()]
                hhi = sum(s**2 for s in shares)
                features["receiver_concentration_score"] = hhi
            else:
                features["receiver_concentration_score"] = 1.0
        else:
            features["receiver_concentration_score"] = 1.0

        # New receiver flag
        features["new_receiver_flag"] = 0.0 if receiver_iban in activity.receiver_counts else 1.0

        # Receiver recency (when did we last send to this receiver?)
        features["receiver_recency_score"] = 1.0  # Default: never sent before
        for t, r in zip(reversed(list(activity.timestamps)), reversed(list(activity.receivers))):
            if r == receiver_iban:
                days_since = (timestamp - t).days
                features["receiver_recency_score"] = min(days_since / 30, 1.0)
                break

        # Fan-out score (many receivers in short time = money laundering)
        one_hour_ago = timestamp - timedelta(hours=1)
        recent_receivers = set()
        for t, r in zip(activity.timestamps, activity.receivers):
            if t >= one_hour_ago:
                recent_receivers.add(r)
        features["fan_out_score"] = min(len(recent_receivers) / 10, 1.0)

        return features

    # =========================================================================
    # Temporal Pattern Features
    # =========================================================================

    def _extract_temporal_features(
        self,
        timestamp: datetime,
        activity: AccountActivity,
    ) -> dict[str, float]:
        """Zaman serisi kalıp özellikleri çıkar."""
        features = {}

        # Hour entropy deviation
        if len(activity.timestamps) >= self._min_samples:
            hours = [t.hour for t in activity.timestamps]
            hour_counts = [hours.count(h) for h in range(24)]
            if sum(hour_counts) > 0:
                hour_dist = np.array(hour_counts) / sum(hour_counts)
                if HAS_SCIPY:
                    hour_entropy = entropy(hour_dist + 1e-10)
                    max_entropy = np.log(24)  # Uniform distribution
                    features["hour_entropy_deviation"] = 1.0 - (hour_entropy / max_entropy)
                else:
                    features["hour_entropy_deviation"] = 0.5
            else:
                features["hour_entropy_deviation"] = 0.0
        else:
            features["hour_entropy_deviation"] = 0.0

        # Transaction velocity
        one_hour_ago = timestamp - timedelta(hours=1)
        one_day_ago = timestamp - timedelta(hours=24)
        seven_days_ago = timestamp - timedelta(days=7)

        features["tx_velocity_1h"] = float(sum(1 for t in activity.timestamps if t >= one_hour_ago))
        features["tx_velocity_24h"] = float(sum(1 for t in activity.timestamps if t >= one_day_ago))
        features["tx_velocity_7d"] = float(
            sum(1 for t in activity.timestamps if t >= seven_days_ago)
        )

        # Burst detection (sudden spike in activity)
        if len(activity.timestamps) >= 10:
            # Compare last 1h to average hourly rate
            time_span_hours = (
                max(activity.timestamps) - min(activity.timestamps)
            ).total_seconds() / 3600
            if time_span_hours > 0:
                avg_hourly = len(activity.timestamps) / time_span_hours
                current_hourly = features["tx_velocity_1h"]
                if avg_hourly > 0:
                    features["burst_detection_score"] = min(current_hourly / avg_hourly / 3, 1.0)
                else:
                    features["burst_detection_score"] = 0.0
            else:
                features["burst_detection_score"] = 0.0
        else:
            features["burst_detection_score"] = 0.0

        # Off-hours flag (outside Turkish banking hours)
        local_hour = (timestamp.hour + TURKISH_TIMEZONE_OFFSET) % 24
        is_off_hours = (
            local_hour < TURKISH_BANKING_HOURS[0] or local_hour >= TURKISH_BANKING_HOURS[1]
        )
        is_weekend = timestamp.weekday() >= 5
        features["off_hours_flag"] = 1.0 if (is_off_hours or is_weekend) else 0.0

        return features

    # =========================================================================
    # Risk Scoring Features
    # =========================================================================

    def _extract_risk_features(
        self,
        amount: float,
        timestamp: datetime,
        receiver_iban: str,
        activity: AccountActivity,
    ) -> dict[str, float]:
        """Kompozit risk skorları çıkar."""
        features = {}

        # MASAK threshold proximity
        if amount >= MASAK_THRESHOLD_TL:
            features["masak_threshold_proximity"] = 1.0
        elif amount >= MASAK_THRESHOLD_TL * 0.9:
            features["masak_threshold_proximity"] = 0.8
        elif amount >= MASAK_THRESHOLD_TL * 0.8:
            features["masak_threshold_proximity"] = 0.5
        else:
            features["masak_threshold_proximity"] = amount / MASAK_THRESHOLD_TL

        # Structuring detection (multiple transactions just below threshold)
        one_day_ago = timestamp - timedelta(hours=24)
        recent_amounts = [
            a for t, a in zip(activity.timestamps, activity.amounts) if t >= one_day_ago
        ]

        near_threshold_count = sum(
            1 for a in recent_amounts if MASAK_THRESHOLD_TL * 0.8 <= a < MASAK_THRESHOLD_TL
        )
        features["structuring_detection_score"] = min(near_threshold_count / 3, 1.0)

        # Rapid movement score (fast money movement)
        if len(activity.timestamps) >= 2:
            inter_arrival_times = []
            sorted_ts = sorted(activity.timestamps)
            for i in range(1, min(10, len(sorted_ts))):
                delta = (sorted_ts[i] - sorted_ts[i - 1]).total_seconds() / 60
                inter_arrival_times.append(delta)

            if inter_arrival_times:
                avg_iat = np.mean(inter_arrival_times)
                features["rapid_movement_score"] = max(0, 1.0 - (avg_iat / 60))  # < 1h = suspicious
            else:
                features["rapid_movement_score"] = 0.0
        else:
            features["rapid_movement_score"] = 0.0

        # Mule account score (receives and immediately sends)
        # This would need incoming transaction data, approximating here
        if len(activity.timestamps) >= 5:
            # High velocity + high unique receivers = potential mule
            velocity = len(activity.timestamps) / max(
                (max(activity.timestamps) - min(activity.timestamps)).days, 1
            )
            unique_ratio = (
                len(set(activity.receivers)) / len(activity.receivers) if activity.receivers else 0
            )
            features["mule_account_score"] = min(velocity / 10 * unique_ratio, 1.0)
        else:
            features["mule_account_score"] = 0.0

        # Composite risk score
        features["composite_risk_score"] = (
            features["masak_threshold_proximity"] * 0.3
            + features["structuring_detection_score"] * 0.25
            + features["rapid_movement_score"] * 0.25
            + features["mule_account_score"] * 0.2
        )

        return features

    # =========================================================================
    # Statistical Features
    # =========================================================================

    def _extract_statistical_features(
        self,
        amount: float,
        timestamp: datetime,
        activity: AccountActivity,
    ) -> dict[str, float]:
        """İstatistiksel özellikler çıkar."""
        features = {}

        # Amount percentile within user's history
        if len(activity.amounts) >= self._min_samples:
            sorted_amounts = sorted(activity.amounts)
            rank = sum(1 for a in sorted_amounts if a <= amount)
            features["amount_percentile_user"] = rank / len(sorted_amounts)
        else:
            features["amount_percentile_user"] = 0.5

        # Amount z-score within user's history
        if len(activity.amounts) >= self._min_samples:
            mean_amt = np.mean(list(activity.amounts))
            std_amt = np.std(list(activity.amounts))
            if std_amt > 0:
                features["amount_zscore_user"] = (amount - mean_amt) / std_amt
            else:
                features["amount_zscore_user"] = 0.0
        else:
            features["amount_zscore_user"] = 0.0

        # Inter-arrival time z-score
        if len(activity.timestamps) >= 3:
            sorted_ts = sorted(activity.timestamps)
            inter_arrival_times = []
            for i in range(1, len(sorted_ts)):
                delta = (sorted_ts[i] - sorted_ts[i - 1]).total_seconds()
                inter_arrival_times.append(delta)

            if inter_arrival_times and activity.last_tx_timestamp:
                current_iat = (timestamp - activity.last_tx_timestamp).total_seconds()
                mean_iat = np.mean(inter_arrival_times)
                std_iat = np.std(inter_arrival_times)
                if std_iat > 0:
                    features["inter_arrival_time_zscore"] = (current_iat - mean_iat) / std_iat
                else:
                    features["inter_arrival_time_zscore"] = 0.0
            else:
                features["inter_arrival_time_zscore"] = 0.0
        else:
            features["inter_arrival_time_zscore"] = 0.0

        # Amount volatility in last 7 days
        seven_days_ago = timestamp - timedelta(days=7)
        recent_amounts = [
            a for t, a in zip(activity.timestamps, activity.amounts) if t >= seven_days_ago
        ]

        if len(recent_amounts) >= 3:
            features["amount_volatility_7d"] = np.std(recent_amounts) / (
                np.mean(recent_amounts) + 1e-6
            )
        else:
            features["amount_volatility_7d"] = 0.0

        return features

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _update_activity(
        self,
        activity: AccountActivity,
        amount: float,
        timestamp: datetime,
        receiver_iban: str,
        channel: str,
        device_id: str,
    ) -> None:
        """Update account activity history."""
        activity.timestamps.append(timestamp)
        activity.amounts.append(amount)
        activity.receivers.append(receiver_iban)
        activity.receiver_counts[receiver_iban] += 1
        activity.channels.append(channel)
        if device_id:
            activity.devices.append(device_id)

        activity.last_tx_timestamp = timestamp
        activity.last_tx_amount = amount
        activity.last_tx_receiver = receiver_iban

        # Update daily stats
        date_key = timestamp.strftime("%Y-%m-%d")
        activity.daily_tx_counts[date_key] += 1
        activity.daily_amounts[date_key] += amount

    def _update_global_stats(self, amount: float) -> None:
        """Update global statistics."""
        self._global_amounts.append(amount)

    @staticmethod
    def _parse_timestamp(ts_str: str) -> datetime:
        """Parse ISO timestamp string."""
        if not ts_str:
            return datetime.utcnow()
        try:
            if "T" in ts_str:
                return datetime.fromisoformat(ts_str.replace("Z", "+00:00").replace("+00:00", ""))
            return datetime.utcnow()
        except (ValueError, TypeError):
            return datetime.utcnow()

    @staticmethod
    def get_feature_names() -> list[str]:
        """Return ordered feature names."""
        return ADVANCED_FEATURE_NAMES.copy()

    @property
    def accounts_tracked(self) -> int:
        """Number of accounts being tracked."""
        return len(self._account_activities)


# =============================================================================
# Combined Feature Engine
# =============================================================================


class CombinedFeatureEngine:
    """
    Base + Advanced özellik çıkarıcı.

    TransactionFeatureEngine + AdvancedFeatureEngine birleşimi.
    TEKNOFEST için en yüksek performansı hedefler.

    Toplam özellik sayısı: 21 (base) + 32 (advanced) = 53 özellik
    """

    def __init__(self) -> None:
        from sentinelflow.ml.feature_engine import TransactionFeatureEngine

        self._base_engine = TransactionFeatureEngine()
        self._advanced_engine = AdvancedFeatureEngine()

        logger.info("CombinedFeatureEngine initialized (53 features)")

    def extract(self, tx_data: dict[str, Any]) -> dict[str, float]:
        """Extract all features."""
        base_features = self._base_engine.extract(tx_data)
        advanced_features = self._advanced_engine.extract(tx_data)

        # Combine
        combined = {**base_features, **advanced_features}
        return combined

    def extract_vector(self, tx_data: dict[str, Any]) -> np.ndarray:
        """Extract as numpy array."""
        base_vec = self._base_engine.extract_vector(tx_data)
        advanced_vec = self._advanced_engine.extract_vector(tx_data)
        return np.concatenate([base_vec, advanced_vec])

    @staticmethod
    def get_feature_names() -> list[str]:
        """Get all feature names."""
        from sentinelflow.ml.feature_engine import FEATURE_NAMES

        return FEATURE_NAMES + ADVANCED_FEATURE_NAMES

    @property
    def num_features(self) -> int:
        """Total number of features."""
        from sentinelflow.ml.feature_engine import NUM_FEATURES

        return NUM_FEATURES + NUM_ADVANCED_FEATURES

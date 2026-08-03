# =============================================================================
# SentinelFlow MLOps - Drift Detection
# =============================================================================
"""
Data and model drift detection for production monitoring.

Features:
- Statistical drift detection (KS test, Chi-square, PSI)
- Feature drift analysis
- Model performance drift
- Automatic alerting
- Drift visualization

Drift Types:
- Data Drift: Input data distribution changes
- Concept Drift: Relationship between input and output changes
- Model Drift: Model performance degradation

Usage:
    drift_detector = DriftDetector()

    # Check data drift
    report = drift_detector.detect_data_drift(
        reference_data=training_data,
        current_data=production_data,
    )

    if report.has_drift:
        print(f"Drift detected in features: {report.drifted_features}")

    # Monitor model drift
    model_report = drift_detector.detect_model_drift(
        model=model,
        reference_metrics={"f1": 0.95, "auc": 0.98},
        current_metrics={"f1": 0.88, "auc": 0.92},
    )
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from loguru import logger
from scipy import stats

# =============================================================================
# Enums
# =============================================================================


class DriftType(str, Enum):
    """Type of drift."""

    DATA = "data"
    FEATURE = "feature"
    MODEL = "model"
    CONCEPT = "concept"


class DriftSeverity(str, Enum):
    """Drift severity level."""

    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class FeatureDriftResult:
    """Drift result for a single feature."""

    feature_name: str = ""
    drift_detected: bool = False
    drift_score: float = 0.0
    p_value: float = 1.0
    test_statistic: float = 0.0
    test_method: str = ""
    severity: DriftSeverity = DriftSeverity.NONE

    # Distribution stats
    reference_mean: float = 0.0
    reference_std: float = 0.0
    current_mean: float = 0.0
    current_std: float = 0.0

    # PSI buckets
    psi_value: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_name": self.feature_name,
            "drift_detected": self.drift_detected,
            "drift_score": self.drift_score,
            "p_value": self.p_value,
            "test_statistic": self.test_statistic,
            "test_method": self.test_method,
            "severity": self.severity.value,
            "psi_value": self.psi_value,
        }


@dataclass
class DataDriftReport:
    """Complete data drift report."""

    report_id: str = ""
    timestamp: str = ""

    # Overall
    has_drift: bool = False
    overall_drift_score: float = 0.0
    severity: DriftSeverity = DriftSeverity.NONE

    # Feature-level
    total_features: int = 0
    drifted_features: list[str] = field(default_factory=list)
    feature_results: dict[str, FeatureDriftResult] = field(default_factory=dict)

    # Stats
    drift_percentage: float = 0.0

    # Thresholds used
    drift_threshold: float = 0.05

    # Recommendations
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "timestamp": self.timestamp,
            "has_drift": self.has_drift,
            "overall_drift_score": self.overall_drift_score,
            "severity": self.severity.value,
            "total_features": self.total_features,
            "drifted_features": self.drifted_features,
            "drift_percentage": self.drift_percentage,
            "feature_results": {k: v.to_dict() for k, v in self.feature_results.items()},
            "recommendations": self.recommendations,
        }

    def __str__(self) -> str:
        return (
            f"DataDriftReport(has_drift={self.has_drift}, "
            f"severity={self.severity.value}, "
            f"drifted={len(self.drifted_features)}/{self.total_features})"
        )


@dataclass
class ModelDriftReport:
    """Model performance drift report."""

    report_id: str = ""
    timestamp: str = ""

    # Overall
    has_drift: bool = False
    severity: DriftSeverity = DriftSeverity.NONE

    # Metrics comparison
    reference_metrics: dict[str, float] = field(default_factory=dict)
    current_metrics: dict[str, float] = field(default_factory=dict)
    metric_changes: dict[str, float] = field(default_factory=dict)

    # Drifted metrics
    drifted_metrics: list[str] = field(default_factory=list)

    # Thresholds
    drift_thresholds: dict[str, float] = field(default_factory=dict)

    # Recommendations
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "timestamp": self.timestamp,
            "has_drift": self.has_drift,
            "severity": self.severity.value,
            "reference_metrics": self.reference_metrics,
            "current_metrics": self.current_metrics,
            "metric_changes": self.metric_changes,
            "drifted_metrics": self.drifted_metrics,
            "recommendations": self.recommendations,
        }


# =============================================================================
# Drift Detector
# =============================================================================


class DriftDetector:
    """
    Production drift detection system.

    Provides:
    - Data drift detection using statistical tests
    - Feature-level drift analysis
    - Model performance monitoring
    - Automatic alerting

    Statistical Tests:
    - Kolmogorov-Smirnov (KS) test for numerical features
    - Chi-square test for categorical features
    - Population Stability Index (PSI)
    """

    def __init__(
        self,
        drift_threshold: float = 0.05,
        psi_threshold: float = 0.2,
        metric_threshold: float = 0.05,
        storage_path: str = "mlops/drift_reports",
    ) -> None:
        """
        Initialize drift detector.

        Args:
            drift_threshold: P-value threshold for statistical tests
            psi_threshold: PSI threshold (>0.2 indicates significant drift)
            metric_threshold: Relative change threshold for metrics
            storage_path: Path to store drift reports
        """
        self._drift_threshold = drift_threshold
        self._psi_threshold = psi_threshold
        self._metric_threshold = metric_threshold
        self._storage_path = Path(storage_path)

        # Alerting callbacks
        self._alert_callbacks: list[Callable[[DataDriftReport | ModelDriftReport], None]] = []

        # Initialize storage
        self._storage_path.mkdir(parents=True, exist_ok=True)

        logger.info("DriftDetector initialized")

    def add_alert_callback(
        self,
        callback: Callable[[DataDriftReport | ModelDriftReport], None],
    ) -> None:
        """Add a callback for drift alerts."""
        self._alert_callbacks.append(callback)

    def _trigger_alerts(self, report: DataDriftReport | ModelDriftReport) -> None:
        """Trigger alert callbacks."""
        for callback in self._alert_callbacks:
            try:
                callback(report)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

    def _calculate_psi(
        self,
        reference: np.ndarray,
        current: np.ndarray,
        n_bins: int = 10,
    ) -> float:
        """
        Calculate Population Stability Index (PSI).

        PSI = Σ (actual% - expected%) × ln(actual% / expected%)

        Interpretation:
        - PSI < 0.1: No significant change
        - 0.1 ≤ PSI < 0.2: Moderate change
        - PSI ≥ 0.2: Significant change
        """
        # Create bins from reference
        min_val = min(reference.min(), current.min())
        max_val = max(reference.max(), current.max())
        bins = np.linspace(min_val, max_val, n_bins + 1)

        # Calculate frequencies
        ref_counts, _ = np.histogram(reference, bins=bins)
        curr_counts, _ = np.histogram(current, bins=bins)

        # Convert to percentages (avoid division by zero)
        ref_pct = (ref_counts + 1) / (len(reference) + n_bins)
        curr_pct = (curr_counts + 1) / (len(current) + n_bins)

        # Calculate PSI
        psi = np.sum((curr_pct - ref_pct) * np.log(curr_pct / ref_pct))

        return float(psi)

    def _ks_test(
        self,
        reference: np.ndarray,
        current: np.ndarray,
    ) -> tuple[float, float]:
        """Kolmogorov-Smirnov test for numerical features."""
        statistic, p_value = stats.ks_2samp(reference, current)
        return float(statistic), float(p_value)

    def _chi_square_test(
        self,
        reference: np.ndarray,
        current: np.ndarray,
    ) -> tuple[float, float]:
        """Chi-square test for categorical features."""
        # Get all unique categories
        categories = np.unique(np.concatenate([reference, current]))

        # Count frequencies
        ref_counts = pd.Series(reference).value_counts()
        curr_counts = pd.Series(current).value_counts()

        # Align to same categories
        ref_freq = np.array([ref_counts.get(c, 0) for c in categories])
        curr_freq = np.array([curr_counts.get(c, 0) for c in categories])

        # Avoid zero frequencies
        ref_freq = ref_freq + 1
        curr_freq = curr_freq + 1

        # Chi-square test
        statistic, p_value = stats.chisquare(curr_freq, ref_freq)

        return float(statistic), float(p_value)

    def _determine_severity(
        self,
        drift_score: float,
        p_value: float,
        psi: float,
    ) -> DriftSeverity:
        """Determine drift severity."""
        if p_value > self._drift_threshold and psi < 0.1:
            return DriftSeverity.NONE
        elif psi < 0.1:
            return DriftSeverity.LOW
        elif psi < 0.2:
            return DriftSeverity.MEDIUM
        elif psi < 0.3:
            return DriftSeverity.HIGH
        else:
            return DriftSeverity.CRITICAL

    def detect_feature_drift(
        self,
        reference: np.ndarray,
        current: np.ndarray,
        feature_name: str,
        is_categorical: bool = False,
    ) -> FeatureDriftResult:
        """Detect drift for a single feature."""
        result = FeatureDriftResult(feature_name=feature_name)

        # Calculate statistics
        if not is_categorical:
            result.reference_mean = float(np.mean(reference))
            result.reference_std = float(np.std(reference))
            result.current_mean = float(np.mean(current))
            result.current_std = float(np.std(current))

            # KS test
            result.test_statistic, result.p_value = self._ks_test(reference, current)
            result.test_method = "ks_test"

            # PSI
            result.psi_value = self._calculate_psi(reference, current)
        else:
            # Chi-square test
            result.test_statistic, result.p_value = self._chi_square_test(reference, current)
            result.test_method = "chi_square"
            result.psi_value = 0.0

        # Determine drift
        result.drift_detected = result.p_value < self._drift_threshold
        result.drift_score = 1 - result.p_value
        result.severity = self._determine_severity(
            result.drift_score,
            result.p_value,
            result.psi_value,
        )

        return result

    def detect_data_drift(
        self,
        reference_data: pd.DataFrame,
        current_data: pd.DataFrame,
        categorical_features: list[str] | None = None,
        exclude_features: list[str] | None = None,
    ) -> DataDriftReport:
        """
        Detect data drift between reference and current datasets.

        Args:
            reference_data: Reference dataset (e.g., training data)
            current_data: Current dataset (e.g., production data)
            categorical_features: List of categorical feature names
            exclude_features: Features to exclude from analysis

        Returns:
            DataDriftReport with detailed results
        """
        report_id = f"data_drift_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        report = DataDriftReport(
            report_id=report_id,
            timestamp=datetime.now().isoformat(),
            drift_threshold=self._drift_threshold,
        )

        categorical_features = categorical_features or []
        exclude_features = exclude_features or []

        # Get common features
        common_features = list(
            set(reference_data.columns) & set(current_data.columns) - set(exclude_features)
        )

        report.total_features = len(common_features)

        # Analyze each feature
        for feature in common_features:
            ref_values = reference_data[feature].dropna().values
            curr_values = current_data[feature].dropna().values

            if len(ref_values) == 0 or len(curr_values) == 0:
                continue

            is_categorical = feature in categorical_features

            result = self.detect_feature_drift(
                ref_values,
                curr_values,
                feature,
                is_categorical,
            )

            report.feature_results[feature] = result

            if result.drift_detected:
                report.drifted_features.append(feature)

        # Overall analysis
        if report.drifted_features:
            report.has_drift = True
            report.drift_percentage = len(report.drifted_features) / report.total_features * 100

            # Calculate overall drift score
            scores = [r.drift_score for r in report.feature_results.values()]
            report.overall_drift_score = float(np.mean(scores))

            # Determine severity
            if report.drift_percentage > 50:
                report.severity = DriftSeverity.CRITICAL
            elif report.drift_percentage > 30:
                report.severity = DriftSeverity.HIGH
            elif report.drift_percentage > 15:
                report.severity = DriftSeverity.MEDIUM
            else:
                report.severity = DriftSeverity.LOW

            # Generate recommendations
            report.recommendations = self._generate_data_drift_recommendations(report)

        # Save report
        self._save_report(report)

        # Trigger alerts if needed
        if report.severity in [DriftSeverity.HIGH, DriftSeverity.CRITICAL]:
            self._trigger_alerts(report)

        logger.info(f"Data drift analysis complete: {report}")

        return report

    def detect_model_drift(
        self,
        reference_metrics: dict[str, float],
        current_metrics: dict[str, float],
        metric_thresholds: dict[str, float] | None = None,
    ) -> ModelDriftReport:
        """
        Detect model performance drift.

        Args:
            reference_metrics: Reference metrics (e.g., validation metrics)
            current_metrics: Current metrics (e.g., production metrics)
            metric_thresholds: Custom thresholds per metric

        Returns:
            ModelDriftReport with detailed results
        """
        report_id = f"model_drift_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        report = ModelDriftReport(
            report_id=report_id,
            timestamp=datetime.now().isoformat(),
            reference_metrics=reference_metrics,
            current_metrics=current_metrics,
            drift_thresholds=metric_thresholds or {},
        )

        # Compare metrics
        for metric, ref_value in reference_metrics.items():
            if metric not in current_metrics:
                continue

            curr_value = current_metrics[metric]

            # Calculate change
            if ref_value != 0:
                relative_change = (curr_value - ref_value) / abs(ref_value)
            else:
                relative_change = curr_value

            report.metric_changes[metric] = relative_change

            # Check threshold
            threshold = (
                metric_thresholds.get(metric, self._metric_threshold)
                if metric_thresholds
                else self._metric_threshold
            )

            # For metrics where higher is better (f1, auc, accuracy)
            # drift is detected when current is significantly lower
            if relative_change < -threshold:
                report.drifted_metrics.append(metric)

        # Overall analysis
        if report.drifted_metrics:
            report.has_drift = True

            # Determine severity based on metric changes
            max_decline = min(report.metric_changes.values())

            if max_decline < -0.15:
                report.severity = DriftSeverity.CRITICAL
            elif max_decline < -0.10:
                report.severity = DriftSeverity.HIGH
            elif max_decline < -0.05:
                report.severity = DriftSeverity.MEDIUM
            else:
                report.severity = DriftSeverity.LOW

            # Generate recommendations
            report.recommendations = self._generate_model_drift_recommendations(report)

        # Save report
        self._save_report(report)

        # Trigger alerts if needed
        if report.severity in [DriftSeverity.HIGH, DriftSeverity.CRITICAL]:
            self._trigger_alerts(report)

        logger.info(
            f"Model drift analysis complete: has_drift={report.has_drift}, severity={report.severity.value}"
        )

        return report

    def _generate_data_drift_recommendations(self, report: DataDriftReport) -> list[str]:
        """Generate recommendations for data drift."""
        recommendations = []

        if report.severity == DriftSeverity.CRITICAL:
            recommendations.append("🚨 CRITICAL: Immediate model retraining recommended")
            recommendations.append("Investigate data pipeline for potential issues")

        if report.drift_percentage > 30:
            recommendations.append("Consider retraining model with recent data")
            recommendations.append("Review feature engineering pipeline")

        for feature in report.drifted_features[:5]:
            result = report.feature_results[feature]
            if result.psi_value > 0.25:
                recommendations.append(
                    f"Feature '{feature}' has high PSI ({result.psi_value:.3f}). "
                    "Review data source."
                )

        if not recommendations:
            recommendations.append("No immediate action required")

        return recommendations

    def _generate_model_drift_recommendations(self, report: ModelDriftReport) -> list[str]:
        """Generate recommendations for model drift."""
        recommendations = []

        if report.severity == DriftSeverity.CRITICAL:
            recommendations.append("🚨 CRITICAL: Model performance severely degraded")
            recommendations.append("Consider immediate rollback to previous model version")
            recommendations.append("Investigate root cause before redeployment")
        elif report.severity == DriftSeverity.HIGH:
            recommendations.append("⚠️ Model performance significantly degraded")
            recommendations.append("Schedule model retraining")
            recommendations.append("Review recent data changes")

        for metric in report.drifted_metrics:
            change = report.metric_changes[metric]
            recommendations.append(
                f"Metric '{metric}' decreased by {abs(change)*100:.1f}%. "
                "Review model predictions."
            )

        if not recommendations:
            recommendations.append("Model performance is stable")

        return recommendations

    def _save_report(self, report: DataDriftReport | ModelDriftReport) -> None:
        """Save drift report to disk."""
        report_file = self._storage_path / f"{report.report_id}.json"

        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)

    def get_drift_history(
        self,
        drift_type: DriftType | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Get drift report history."""
        reports = []

        for report_file in sorted(self._storage_path.glob("*.json"), reverse=True)[:limit]:
            with open(report_file, encoding="utf-8") as f:
                report = json.load(f)

            if drift_type:
                if (
                    drift_type == DriftType.DATA
                    and "data_drift" in report_file.stem
                    or drift_type == DriftType.MODEL
                    and "model_drift" in report_file.stem
                ):
                    reports.append(report)
            else:
                reports.append(report)

        return reports

    def create_drift_monitor(
        self,
        reference_data: pd.DataFrame,
        check_interval: int = 3600,
    ) -> DriftMonitor:
        """Create a continuous drift monitor."""
        return DriftMonitor(
            detector=self,
            reference_data=reference_data,
            check_interval=check_interval,
        )


class DriftMonitor:
    """
    Continuous drift monitoring.

    Usage:
        monitor = drift_detector.create_drift_monitor(reference_data)
        monitor.add_data(new_data)

        if monitor.should_alert():
            print("Drift detected!")
    """

    def __init__(
        self,
        detector: DriftDetector,
        reference_data: pd.DataFrame,
        check_interval: int = 3600,
        window_size: int = 1000,
    ) -> None:
        self._detector = detector
        self._reference_data = reference_data
        self._check_interval = check_interval
        self._window_size = window_size

        self._current_window: list[pd.Series] = []
        self._last_check = datetime.now()
        self._drift_history: list[DataDriftReport] = []

    def add_data(self, data: pd.DataFrame | pd.Series) -> DataDriftReport | None:
        """Add new data point(s) to monitor."""
        if isinstance(data, pd.DataFrame):
            for _, row in data.iterrows():
                self._current_window.append(row)
        else:
            self._current_window.append(data)

        # Keep window size
        if len(self._current_window) > self._window_size:
            self._current_window = self._current_window[-self._window_size :]

        # Check if should run detection
        if len(self._current_window) >= self._window_size:
            current_df = pd.DataFrame(self._current_window)
            report = self._detector.detect_data_drift(
                self._reference_data,
                current_df,
            )
            self._drift_history.append(report)
            return report

        return None

    def should_alert(self) -> bool:
        """Check if alert should be raised."""
        if not self._drift_history:
            return False

        latest = self._drift_history[-1]
        return latest.severity in [DriftSeverity.HIGH, DriftSeverity.CRITICAL]

    @property
    def latest_report(self) -> DataDriftReport | None:
        """Get latest drift report."""
        return self._drift_history[-1] if self._drift_history else None

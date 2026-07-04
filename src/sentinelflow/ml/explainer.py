# =============================================================================
# SentinelFlow - SHAP-Based Explainability
# =============================================================================
"""
Explainable AI (XAI) module for fraud detection explanations.

Uses SHAP (SHapley Additive exPlanations) to explain why a transaction
was flagged as fraudulent, providing human-readable reasons.

Usage:
    explainer = FraudExplainer(feature_names=FEATURE_NAMES)
    explanation = explainer.explain(features, model)
    # explanation = {"top_reasons": [...], "shap_values": {...}}
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
from loguru import logger

try:
    import shap

    HAS_SHAP = True
except ImportError:
    HAS_SHAP = False
    logger.warning("shap not available, explainability will use fallback method")

# Feature açıklamaları feature_engine.py'de tek kaynak (single source of truth)
# olarak tanımlıdır; burada yeniden tanımlamak yerine import ediyoruz.
from sentinelflow.ml.feature_engine import FEATURE_DESCRIPTIONS


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FraudExplanation:
    """Human-readable explanation of why a transaction was flagged."""

    top_reasons: list[str] = field(default_factory=list)
    feature_contributions: dict[str, float] = field(default_factory=dict)
    risk_factors: list[dict[str, Any]] = field(default_factory=list)
    explanation_method: str = "shap"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for evidence/serialization."""
        return {
            "top_reasons": self.top_reasons,
            "feature_contributions": {
                k: round(v, 4) for k, v in self.feature_contributions.items()
            },
            "risk_factors": self.risk_factors,
            "method": self.explanation_method,
        }

    def summary(self) -> str:
        """Return a concise one-line summary of the explanation."""
        if self.top_reasons:
            return " | ".join(self.top_reasons[:3])
        return "Bilinmeyen risk faktörleri"


# =============================================================================
# Fraud Explainer
# =============================================================================


class FraudExplainer:
    """
    SHAP-based explainability for fraud predictions.

    Generates human-readable explanations for why a transaction was
    flagged as fraudulent, showing the top contributing features.
    """

    def __init__(
        self,
        feature_names: list[str] | None = None,
        top_n: int = 5,
        enable_shap: bool = True,
    ) -> None:
        """
        Initialize the explainer.

        Args:
            feature_names: Ordered list of feature names
            top_n: Number of top reasons to include
            enable_shap: Whether to use SHAP (falls back to feature-based if False)
        """
        self._feature_names = feature_names or []
        self._top_n = top_n
        self._use_shap = enable_shap and HAS_SHAP
        self._shap_explainer: Any = None
        self._background_data: np.ndarray | None = None

        logger.info(
            f"FraudExplainer initialized "
            f"(shap={'enabled' if self._use_shap else 'disabled'}, top_n={top_n})"
        )

    def set_background_data(self, X: np.ndarray) -> None:
        """
        Set background data for SHAP explainer.

        Args:
            X: Sample of normal transactions for SHAP baseline
        """
        if self._use_shap:
            # Use a subsample for efficiency
            if len(X) > 100:
                indices = np.random.choice(len(X), 100, replace=False)
                self._background_data = X[indices]
            else:
                self._background_data = X.copy()
            logger.info(f"SHAP background data set: {len(self._background_data)} samples")

    def explain(
        self,
        features: np.ndarray,
        feature_values: dict[str, float] | None = None,
        model: Any = None,
    ) -> FraudExplanation:
        """
        Generate explanation for a fraud prediction.

        Args:
            features: Feature vector used for prediction
            feature_values: Named feature values dict (for human-readable output)
            model: The model to explain (optional, for SHAP)

        Returns:
            FraudExplanation with top reasons and contributions
        """
        explanation = FraudExplanation()

        if self._use_shap and model is not None and self._background_data is not None:
            explanation = self._explain_with_shap(features, model)
        else:
            explanation = self._explain_with_features(features, feature_values)

        return explanation

    def _explain_with_shap(self, features: np.ndarray, model: Any) -> FraudExplanation:
        """Generate SHAP-based explanation."""
        explanation = FraudExplanation(explanation_method="shap")

        try:
            # Create SHAP explainer
            if hasattr(model, "predict_proba"):
                predict_fn = lambda x: (
                    model.predict_proba(x)
                    if len(x.shape) == 2
                    else model.predict_proba(x.reshape(1, -1))
                )
            elif hasattr(model, "predict_single"):
                predict_fn = lambda x: np.array([model.predict_single(row) for row in x])
            else:
                return self._explain_with_features(features, None)

            explainer = shap.KernelExplainer(predict_fn, self._background_data)
            shap_values = explainer.shap_values(features.reshape(1, -1))[0]

            # Build contributions dict
            for i, name in enumerate(self._feature_names):
                if i < len(shap_values):
                    explanation.feature_contributions[name] = float(shap_values[i])

            # Sort by absolute contribution
            sorted_features = sorted(
                explanation.feature_contributions.items(),
                key=lambda x: abs(x[1]),
                reverse=True,
            )

            # Build top reasons
            for name, value in sorted_features[: self._top_n]:
                desc = FEATURE_DESCRIPTIONS.get(name, name)
                direction = "↑ artırıyor" if value > 0 else "↓ azaltıyor"
                feature_val = (
                    features[self._feature_names.index(name)] if name in self._feature_names else 0
                )
                explanation.top_reasons.append(f"{desc}: {feature_val:.2f} (risk {direction})")
                explanation.risk_factors.append(
                    {
                        "feature": name,
                        "description": desc,
                        "value": float(feature_val),
                        "shap_contribution": float(value),
                        "direction": "increase" if value > 0 else "decrease",
                    }
                )

        except Exception as e:
            logger.error(f"SHAP explanation failed: {e}")
            return self._explain_with_features(features, None)

        return explanation

    def _explain_with_features(
        self,
        features: np.ndarray,
        feature_values: dict[str, float] | None = None,
    ) -> FraudExplanation:
        """
        Fallback: Generate explanation based on feature anomaly scores.

        Uses z-score-like analysis to identify which features deviate most
        from expected values.
        """
        explanation = FraudExplanation(explanation_method="feature_analysis")

        # If we have named feature values, use those
        if feature_values:
            features_dict = feature_values
        elif self._feature_names:
            features_dict = {
                name: float(features[i])
                for i, name in enumerate(self._feature_names)
                if i < len(features)
            }
        else:
            return explanation

        # Heuristic risk scoring per feature
        risk_scores: list[tuple[str, float, str]] = []

        # Amount anomalies
        amount_zscore = features_dict.get("amount_zscore", 0.0)
        if abs(amount_zscore) > 2.0:
            risk_scores.append(
                (
                    "amount_zscore",
                    abs(amount_zscore),
                    f"İşlem tutarı normalden {abs(amount_zscore):.1f}σ sapıyor",
                )
            )

        # High velocity
        tx_count_1h = features_dict.get("sender_tx_count_1h", 0)
        if tx_count_1h > 5:
            risk_scores.append(
                (
                    "sender_tx_count_1h",
                    tx_count_1h,
                    f"Son 1 saatte {int(tx_count_1h)} işlem (yüksek frekans)",
                )
            )

        # Night transaction
        if features_dict.get("is_night", 0) > 0:
            risk_scores.append(("is_night", 2.0, "Gece saatlerinde yapılan işlem"))

        # Keywords
        keyword_score = features_dict.get("keyword_score", 0)
        if keyword_score > 0:
            risk_scores.append(
                (
                    "keyword_score",
                    keyword_score * 3,
                    f"Açıklamada {int(keyword_score)} şüpheli anahtar kelime",
                )
            )

        # International
        if features_dict.get("is_international", 0) > 0:
            risk_scores.append(("is_international", 1.5, "Uluslararası transfer tespit edildi"))

        # Large distance
        distance = features_dict.get("city_distance_km", 0)
        if distance > 500:
            risk_scores.append(
                ("city_distance_km", distance / 200, f"Şehirler arası mesafe: {distance:.0f} km")
            )

        # High amount percentile
        percentile = features_dict.get("amount_percentile", 0.5)
        if percentile > 0.95:
            risk_scores.append(
                (
                    "amount_percentile",
                    percentile * 5,
                    f"Tutar üst %{(1-percentile)*100:.1f} diliminde",
                )
            )

        # Weekend
        if features_dict.get("is_weekend", 0) > 0:
            risk_scores.append(("is_weekend", 0.5, "Hafta sonu işlemi"))

        # Sort by risk score
        risk_scores.sort(key=lambda x: x[1], reverse=True)

        for name, score, reason in risk_scores[: self._top_n]:
            explanation.top_reasons.append(reason)
            explanation.feature_contributions[name] = score
            explanation.risk_factors.append(
                {
                    "feature": name,
                    "description": FEATURE_DESCRIPTIONS.get(name, name),
                    "value": features_dict.get(name, 0),
                    "risk_score": score,
                }
            )

        if not explanation.top_reasons:
            explanation.top_reasons.append("Genel anomaly skoru eşik değerini aştı")

        return explanation

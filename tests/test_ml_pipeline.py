# =============================================================================
# SentinelFlow - ML Pipeline Unit Tests
# =============================================================================
"""
Tests for the ML feature engineering, models, and ensemble components.
"""

from datetime import datetime

import numpy as np
import pytest

from sentinelflow.ml.ensemble import EnsemblePrediction, EnsembleVoter
from sentinelflow.ml.explainer import FraudExplainer, FraudExplanation
from sentinelflow.ml.feature_engine import (
    FEATURE_NAMES,
    NUM_FEATURES,
    TransactionFeatureEngine,
)
from sentinelflow.ml.models import (
    AutoEncoderModel,
    IsolationForestModel,
    XGBoostFraudModel,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_transaction() -> dict:
    """Create a sample normal transaction."""
    return {
        "transaction_id": "TX-001",
        "sender_iban": "TR1234567890123456789012",
        "sender_name": "Ahmet Yılmaz",
        "sender_city": "İstanbul",
        "receiver_iban": "TR9876543210987654321098",
        "receiver_name": "Fatma Demir",
        "receiver_city": "Ankara",
        "amount": 1500.0,
        "currency": "TRY",
        "description": "Kira ödemesi",
        "timestamp": datetime.utcnow().isoformat(),
    }


@pytest.fixture
def suspicious_transaction() -> dict:
    """Create a suspicious transaction with anomalous features."""
    return {
        "transaction_id": "TX-002",
        "sender_iban": "TR1111111111111111111111",
        "sender_name": "Test User",
        "sender_city": "İstanbul",
        "receiver_iban": "TR2222222222222222222222",
        "receiver_name": "Anonymous Receiver",
        "receiver_city": "Dubai",
        "amount": 250000.0,
        "currency": "TRY",
        "description": "bitcoin kripto acil transfer offshore",
        "timestamp": "2026-01-15T03:30:00",
    }


@pytest.fixture
def feature_engine() -> TransactionFeatureEngine:
    """Create a feature engine instance."""
    return TransactionFeatureEngine(history_window_size=100)


@pytest.fixture
def trained_isolation_forest() -> IsolationForestModel:
    """Create and train an IsolationForest model."""
    model = IsolationForestModel(
        contamination=0.1,
        n_estimators=50,
        min_samples_to_train=20,
    )
    # Generate normal training data
    rng = np.random.default_rng(42)
    X_train = rng.normal(loc=0, scale=1, size=(100, NUM_FEATURES))
    model.fit(X_train)
    return model


# =============================================================================
# Feature Engine Tests
# =============================================================================


class TestTransactionFeatureEngine:
    """Tests for TransactionFeatureEngine."""

    def test_extract_returns_correct_feature_count(
        self, feature_engine: TransactionFeatureEngine, sample_transaction: dict
    ):
        """Feature extraction should return all expected features."""
        features = feature_engine.extract(sample_transaction)
        assert len(features) == NUM_FEATURES
        for name in FEATURE_NAMES:
            assert name in features, f"Missing feature: {name}"

    def test_extract_vector_shape(
        self, feature_engine: TransactionFeatureEngine, sample_transaction: dict
    ):
        """Feature vector should have correct shape."""
        vector = feature_engine.extract_vector(sample_transaction)
        assert vector.shape == (NUM_FEATURES,)
        assert vector.dtype == np.float64

    def test_amount_features(
        self, feature_engine: TransactionFeatureEngine, sample_transaction: dict
    ):
        """Amount features should be correctly calculated."""
        features = feature_engine.extract(sample_transaction)

        assert features["amount_raw"] == 1500.0
        assert features["amount_log"] == pytest.approx(np.log1p(1500.0), rel=1e-5)

    def test_temporal_features(self, feature_engine: TransactionFeatureEngine):
        """Temporal features should correctly extract hour and day."""
        tx = {
            "amount": 1000.0,
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "",
            "sender_city": "",
            "receiver_city": "",
            "timestamp": "2026-01-15T14:30:00",
        }
        features = feature_engine.extract(tx)

        assert features["hour_of_day"] == 14.0
        assert features["day_of_week"] == 3.0  # Thursday
        assert features["is_weekend"] == 0.0
        assert features["is_night"] == 0.0

    def test_night_detection(self, feature_engine: TransactionFeatureEngine):
        """Night transactions should be detected."""
        tx = {
            "amount": 1000.0,
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "",
            "sender_city": "",
            "receiver_city": "",
            "timestamp": "2026-01-15T03:00:00",
        }
        features = feature_engine.extract(tx)
        assert features["is_night"] == 1.0

    def test_keyword_score(self, feature_engine: TransactionFeatureEngine):
        """Suspicious keywords should be counted."""
        tx = {
            "amount": 1000.0,
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "bitcoin kripto acil transfer",
            "sender_city": "",
            "receiver_city": "",
            "timestamp": "",
        }
        features = feature_engine.extract(tx)
        assert features["keyword_score"] >= 3.0  # bitcoin, kripto, acil

    def test_geographic_features(self, feature_engine: TransactionFeatureEngine):
        """Geographic distance should be calculated between known cities."""
        tx = {
            "amount": 1000.0,
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "",
            "sender_city": "İstanbul",
            "receiver_city": "Ankara",
            "timestamp": "",
        }
        features = feature_engine.extract(tx)

        # İstanbul-Ankara distance should be ~350 km
        assert 300 < features["city_distance_km"] < 500
        assert features["sender_receiver_same_city"] == 0.0
        assert features["is_international"] == 0.0

    def test_international_detection(self, feature_engine: TransactionFeatureEngine):
        """International transfers should be detected."""
        tx = {
            "amount": 1000.0,
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "",
            "sender_city": "İstanbul",
            "receiver_city": "Dubai",
            "timestamp": "",
        }
        features = feature_engine.extract(tx)
        assert features["is_international"] == 1.0

    def test_velocity_tracking(self, feature_engine: TransactionFeatureEngine):
        """Velocity features should track sender transaction frequency."""
        base_tx = {
            "sender_iban": "TR0000000000000000000001",
            "receiver_iban": "TR0000000000000000000002",
            "description": "",
            "sender_city": "",
            "receiver_city": "",
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Process multiple transactions from same sender
        for i in range(5):
            tx = {**base_tx, "amount": 1000.0 + i * 100}
            features = feature_engine.extract(tx)

        # After 5 transactions, count should be tracked
        assert features["sender_tx_count_1h"] >= 4  # At least previous ones

    def test_missing_fields_no_crash(self, feature_engine: TransactionFeatureEngine):
        """Feature extraction should handle missing fields gracefully."""
        tx = {"amount": 500.0}
        features = feature_engine.extract(tx)
        assert len(features) == NUM_FEATURES

    def test_get_feature_names_returns_list(self):
        """Feature names should be returned as a list."""
        names = TransactionFeatureEngine.get_feature_names()
        assert isinstance(names, list)
        assert len(names) == NUM_FEATURES


# =============================================================================
# Model Tests
# =============================================================================


class TestIsolationForestModel:
    """Tests for IsolationForestModel."""

    def test_not_ready_before_training(self):
        """Model should not be ready before training."""
        model = IsolationForestModel(min_samples_to_train=10)
        assert not model.is_ready
        assert model.name == "IsolationForest"

    def test_predict_returns_zero_when_not_trained(self):
        """Prediction should return 0.0 when model is not trained."""
        model = IsolationForestModel()
        features = np.random.randn(NUM_FEATURES)
        assert model.predict_single(features) == 0.0

    def test_train_and_predict(self, trained_isolation_forest: IsolationForestModel):
        """Model should be able to train and predict."""
        assert trained_isolation_forest.is_ready

        # Normal point
        normal = np.zeros(NUM_FEATURES)
        score_normal = trained_isolation_forest.predict_single(normal)
        assert 0.0 <= score_normal <= 1.0

        # Extreme outlier
        outlier = np.full(NUM_FEATURES, 100.0)
        score_outlier = trained_isolation_forest.predict_single(outlier)
        assert 0.0 <= score_outlier <= 1.0

        # Outlier should have higher score than normal
        assert score_outlier > score_normal

    def test_online_learning(self):
        """IsolationForest should auto-retrain at intervals."""
        model = IsolationForestModel(
            min_samples_to_train=10,
            retrain_interval=15,
        )

        rng = np.random.default_rng(42)
        for _ in range(20):
            features = rng.normal(size=NUM_FEATURES)
            model.add_sample_and_maybe_retrain(features)

        assert model.is_ready


class TestXGBoostModel:
    """Tests for XGBoostFraudModel."""

    def test_not_ready_without_training(self):
        """XGBoost should not be ready without training data."""
        model = XGBoostFraudModel()
        assert not model.is_ready
        assert model.name == "XGBoost"

    def test_train_with_labels(self):
        """XGBoost should train with labeled data."""
        model = XGBoostFraudModel(n_estimators=10, max_depth=3)

        rng = np.random.default_rng(42)
        X = rng.normal(size=(100, NUM_FEATURES))
        y = np.zeros(100)
        y[:10] = 1.0  # 10% fraud

        model.fit(X, y)
        assert model.is_ready

        score = model.predict_single(X[0])
        assert 0.0 <= score <= 1.0


class TestAutoEncoderModel:
    """Tests for AutoEncoderModel."""

    def test_not_ready_without_training(self):
        """AutoEncoder should not be ready without training."""
        model = AutoEncoderModel(input_dim=NUM_FEATURES)
        assert not model.is_ready
        assert model.name == "AutoEncoder"

    def test_train_and_predict(self):
        """AutoEncoder should train and predict anomaly scores."""
        model = AutoEncoderModel(input_dim=NUM_FEATURES, encoding_dim=4)

        rng = np.random.default_rng(42)
        X = rng.normal(size=(200, NUM_FEATURES))

        model.fit(X, epochs=10)
        assert model.is_ready

        score = model.predict_single(X[0])
        assert 0.0 <= score <= 1.0


# =============================================================================
# Ensemble Tests
# =============================================================================


class TestEnsembleVoter:
    """Tests for EnsembleVoter."""

    def test_empty_ensemble(self):
        """Empty ensemble should return zero score."""
        voter = EnsembleVoter(threshold=0.5)
        features = np.random.randn(NUM_FEATURES)
        result = voter.predict(features)

        assert result.is_fraud is False
        assert result.final_score == 0.0
        assert result.active_models == 0

    def test_weighted_voting(self, trained_isolation_forest: IsolationForestModel):
        """Ensemble should combine model scores with weights."""
        voter = EnsembleVoter(threshold=0.5)
        voter.add_model(trained_isolation_forest, weight=1.0)

        features = np.random.randn(NUM_FEATURES)
        result = voter.predict(features)

        assert result.active_models == 1
        assert "IsolationForest" in result.model_scores
        assert 0.0 <= result.final_score <= 1.0

    def test_prediction_to_dict(self):
        """EnsemblePrediction should serialize correctly."""
        pred = EnsemblePrediction(
            is_fraud=True,
            final_score=0.85,
            model_scores={"IsolationForest": 0.9, "XGBoost": 0.8},
            model_weights={"IsolationForest": 0.5, "XGBoost": 0.5},
            active_models=2,
        )
        d = pred.to_dict()

        assert d["is_fraud"] is True
        assert d["ensemble_score"] == 0.85
        assert "IsolationForest" in d["model_scores"]

    def test_threshold_property(self):
        """Threshold should be settable."""
        voter = EnsembleVoter(threshold=0.5)
        assert voter.threshold == 0.5

        voter.threshold = 0.8
        assert voter.threshold == 0.8


# =============================================================================
# Explainer Tests
# =============================================================================


class TestFraudExplainer:
    """Tests for FraudExplainer."""

    def test_fallback_explanation(self):
        """Explainer should work without SHAP using feature analysis."""
        explainer = FraudExplainer(
            feature_names=FEATURE_NAMES,
            top_n=3,
            enable_shap=False,
        )

        # Create feature values with suspicious patterns
        features = np.zeros(NUM_FEATURES)
        feature_values = dict.fromkeys(FEATURE_NAMES, 0.0)
        feature_values["amount_zscore"] = 5.0  # High z-score
        feature_values["is_night"] = 1.0  # Night transaction
        feature_values["keyword_score"] = 3.0  # Suspicious keywords

        explanation = explainer.explain(features, feature_values=feature_values)

        assert isinstance(explanation, FraudExplanation)
        assert len(explanation.top_reasons) > 0
        assert explanation.explanation_method == "feature_analysis"

    def test_explanation_to_dict(self):
        """Explanation should serialize correctly."""
        explanation = FraudExplanation(
            top_reasons=["Test reason"],
            feature_contributions={"amount_zscore": 3.5},
            explanation_method="test",
        )
        d = explanation.to_dict()

        assert "top_reasons" in d
        assert "feature_contributions" in d
        assert d["method"] == "test"

    def test_summary(self):
        """Summary should return top reasons as string."""
        explanation = FraudExplanation(
            top_reasons=["Reason 1", "Reason 2", "Reason 3"],
        )
        summary = explanation.summary()
        assert "Reason 1" in summary
        assert "Reason 2" in summary

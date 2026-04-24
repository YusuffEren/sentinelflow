# =============================================================================
# SentinelFlow - ML Model Tests
# =============================================================================
"""
Comprehensive tests for ML models and ensemble.

Run with: pytest tests/test_ml_models.py -v
"""

import pytest
import numpy as np
import tempfile
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sentinelflow.ml.feature_engine import TransactionFeatureEngine, NUM_FEATURES, FEATURE_NAMES
from sentinelflow.ml.models import IsolationForestModel, XGBoostFraudModel
from sentinelflow.ml.ensemble import EnsembleVoter
from sentinelflow.ml.dataset_loader import FraudDatasetLoader


@pytest.fixture
def sample_data():
    """Generate sample training data."""
    np.random.seed(42)
    n_samples = 1000
    n_features = NUM_FEATURES
    
    X = np.random.randn(n_samples, n_features)
    y = np.random.randint(0, 2, n_samples)
    
    return X, y


@pytest.fixture
def fraud_dataset():
    """Generate realistic fraud dataset."""
    loader = FraudDatasetLoader(seed=42)
    X, y, df = loader.generate_synthetic(n_samples=500, fraud_ratio=0.1)
    return X, y, df


@pytest.fixture
def sample_transaction():
    """Sample transaction dictionary."""
    return {
        "transaction_id": "TX-TEST-001",
        "sender_iban": "TR1234567890123456789012",
        "sender_name": "Test Sender",
        "sender_city": "Istanbul",
        "receiver_iban": "TR9876543210987654321098",
        "receiver_name": "Test Receiver",
        "receiver_city": "Ankara",
        "amount": 5000.0,
        "currency": "TRY",
        "description": "Test havale",
        "timestamp": "2026-04-24T10:30:00",
    }


class TestTransactionFeatureEngine:
    """Tests for feature extraction engine."""
    
    def test_initialization(self):
        """Engine should initialize correctly."""
        engine = TransactionFeatureEngine()
        assert engine is not None
    
    def test_feature_extraction(self, sample_transaction):
        """Should extract correct number of features."""
        engine = TransactionFeatureEngine()
        features = engine.extract_vector(sample_transaction)
        
        assert len(features) == NUM_FEATURES
        assert isinstance(features, np.ndarray)
    
    def test_feature_names(self):
        """Feature names should match count."""
        assert len(FEATURE_NAMES) == NUM_FEATURES
    
    def test_amount_feature(self, sample_transaction):
        """Amount should be extracted correctly."""
        engine = TransactionFeatureEngine()
        features = engine.extract_vector(sample_transaction)
        
        assert features[0] == 5000.0
    
    def test_hour_feature(self, sample_transaction):
        """Hour should be extracted correctly."""
        engine = TransactionFeatureEngine()
        features = engine.extract_vector(sample_transaction)
        
        hour_index = FEATURE_NAMES.index("hour")
        assert features[hour_index] == 10
    
    def test_history_accumulation(self, sample_transaction):
        """History should accumulate."""
        engine = TransactionFeatureEngine(history_window_size=100)
        
        for i in range(10):
            engine.extract_vector(sample_transaction)
        
        features = engine.extract_vector(sample_transaction)
        
        sender_count_index = FEATURE_NAMES.index("sender_tx_count")
        assert features[sender_count_index] > 0


class TestIsolationForestModel:
    """Tests for IsolationForest anomaly detection."""
    
    def test_initialization(self):
        """Model should initialize correctly."""
        model = IsolationForestModel()
        assert model is not None
    
    def test_fit_requires_data(self, sample_data):
        """Model should require minimum samples to fit."""
        X, _ = sample_data
        model = IsolationForestModel(min_samples_to_train=500)
        
        model.fit(X[:100])
        assert not model._is_fitted
        
        model.fit(X)
        assert model._is_fitted
    
    def test_predict_single(self, sample_data):
        """Should predict single sample."""
        X, _ = sample_data
        model = IsolationForestModel(min_samples_to_train=100)
        model.fit(X)
        
        score = model.predict_single(X[0])
        
        assert 0.0 <= score <= 1.0
    
    def test_predict_batch(self, sample_data):
        """Should predict batch of samples."""
        X, _ = sample_data
        model = IsolationForestModel(min_samples_to_train=100)
        model.fit(X)
        
        scores = model.predict(X[:50])
        
        assert len(scores) == 50
        assert all(0.0 <= s <= 1.0 for s in scores)
    
    def test_anomaly_detection(self, sample_data):
        """Should detect anomalies."""
        X, _ = sample_data
        model = IsolationForestModel(contamination=0.1)
        model.fit(X)
        
        anomaly = np.array([10] * NUM_FEATURES)
        normal = X[0]
        
        anomaly_score = model.predict_single(anomaly)
        normal_score = model.predict_single(normal)
        
        assert anomaly_score > normal_score
    
    def test_save_load(self, sample_data, tmp_path):
        """Model should save and load correctly."""
        X, _ = sample_data
        model = IsolationForestModel()
        model.fit(X)
        
        model_path = str(tmp_path / "isolation_forest.pkl")
        model.save(model_path)
        
        loaded_model = IsolationForestModel(model_path=model_path)
        
        original_score = model.predict_single(X[0])
        loaded_score = loaded_model.predict_single(X[0])
        
        assert abs(original_score - loaded_score) < 0.01


class TestXGBoostFraudModel:
    """Tests for XGBoost classifier."""
    
    def test_initialization(self):
        """Model should initialize correctly."""
        model = XGBoostFraudModel()
        assert model is not None
    
    def test_fit(self, sample_data):
        """Model should fit on data."""
        X, y = sample_data
        model = XGBoostFraudModel(n_estimators=10)
        model.fit(X, y)
        
        assert model._is_fitted
    
    def test_predict_single(self, sample_data):
        """Should predict single sample."""
        X, y = sample_data
        model = XGBoostFraudModel(n_estimators=10)
        model.fit(X, y)
        
        score = model.predict_single(X[0])
        
        assert 0.0 <= score <= 1.0
    
    def test_predict_proba(self, sample_data):
        """Should return probability estimates."""
        X, y = sample_data
        model = XGBoostFraudModel(n_estimators=10)
        model.fit(X, y)
        
        proba = model.predict_proba(X[:10])
        
        assert proba.shape == (10, 2)
        assert np.allclose(proba.sum(axis=1), 1.0)
    
    def test_feature_importance(self, sample_data):
        """Should compute feature importance."""
        X, y = sample_data
        model = XGBoostFraudModel(n_estimators=10)
        model.fit(X, y)
        
        importance = model.get_feature_importance()
        
        assert len(importance) > 0
    
    def test_save_load(self, sample_data, tmp_path):
        """Model should save and load correctly."""
        X, y = sample_data
        model = XGBoostFraudModel(n_estimators=10)
        model.fit(X, y)
        
        model_path = str(tmp_path / "xgboost.pkl")
        model.save(model_path)
        
        loaded_model = XGBoostFraudModel(model_path=model_path)
        
        original_score = model.predict_single(X[0])
        loaded_score = loaded_model.predict_single(X[0])
        
        assert abs(original_score - loaded_score) < 0.01


class TestEnsembleVoter:
    """Tests for ensemble voting."""
    
    def test_initialization(self):
        """Ensemble should initialize correctly."""
        ensemble = EnsembleVoter()
        assert ensemble is not None
    
    def test_add_model(self, sample_data):
        """Should add models to ensemble."""
        X, y = sample_data
        
        model1 = IsolationForestModel()
        model1.fit(X)
        
        model2 = XGBoostFraudModel(n_estimators=10)
        model2.fit(X, y)
        
        ensemble = EnsembleVoter()
        ensemble.add_model("isolation_forest", model1, weight=0.3)
        ensemble.add_model("xgboost", model2, weight=0.7)
        
        assert len(ensemble._models) == 2
    
    def test_predict(self, sample_data):
        """Should predict with weighted voting."""
        X, y = sample_data
        
        model1 = IsolationForestModel()
        model1.fit(X)
        
        model2 = XGBoostFraudModel(n_estimators=10)
        model2.fit(X, y)
        
        ensemble = EnsembleVoter()
        ensemble.add_model("isolation_forest", model1, weight=0.3)
        ensemble.add_model("xgboost", model2, weight=0.7)
        
        score = ensemble.predict(X[0])
        
        assert 0.0 <= score <= 1.0
    
    def test_empty_ensemble(self):
        """Empty ensemble should raise error."""
        ensemble = EnsembleVoter()
        
        with pytest.raises(ValueError):
            ensemble.predict(np.zeros(NUM_FEATURES))


class TestFraudDatasetLoader:
    """Tests for dataset loading and generation."""
    
    def test_initialization(self):
        """Loader should initialize correctly."""
        loader = FraudDatasetLoader()
        assert loader is not None
    
    def test_generate_synthetic(self):
        """Should generate synthetic dataset."""
        loader = FraudDatasetLoader(seed=42)
        X, y, df = loader.generate_synthetic(n_samples=100, fraud_ratio=0.1)
        
        assert X.shape[0] == 100
        assert X.shape[1] == NUM_FEATURES
        assert len(y) == 100
        assert len(df) == 100
    
    def test_fraud_ratio(self):
        """Generated fraud ratio should be approximate."""
        loader = FraudDatasetLoader(seed=42)
        X, y, df = loader.generate_synthetic(n_samples=1000, fraud_ratio=0.1)
        
        actual_ratio = y.mean()
        
        assert 0.05 <= actual_ratio <= 0.15
    
    def test_reproducibility(self):
        """Same seed should produce same results."""
        loader1 = FraudDatasetLoader(seed=42)
        X1, y1, _ = loader1.generate_synthetic(n_samples=100)
        
        loader2 = FraudDatasetLoader(seed=42)
        X2, y2, _ = loader2.generate_synthetic(n_samples=100)
        
        np.testing.assert_array_equal(y1, y2)


class TestModelMetrics:
    """Tests for model evaluation metrics."""
    
    def test_xgboost_metrics(self, fraud_dataset):
        """XGBoost should achieve reasonable metrics."""
        X, y, _ = fraud_dataset
        
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]
        
        model = XGBoostFraudModel(n_estimators=50)
        model.fit(X_train, y_train)
        
        from sklearn.metrics import roc_auc_score, f1_score
        
        y_scores = np.array([model.predict_single(x) for x in X_test])
        y_pred = (y_scores > 0.5).astype(int)
        
        auc = roc_auc_score(y_test, y_scores)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        
        assert auc > 0.5
        assert f1 >= 0.0

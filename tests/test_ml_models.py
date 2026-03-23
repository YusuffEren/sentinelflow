# SentinelFlow - ML Models Tests

import pytest
import numpy as np


class TestIsolationForestModel:
    """Tests for IsolationForestModel."""

    def test_model_initialization(self):
        """Test model initializes correctly."""
        from sentinelflow.ml import IsolationForestModel

        model = IsolationForestModel()
        assert model is not None
        assert model.name == "IsolationForest"

    def test_model_fit(self, ml_feature_batch):
        """Test model training."""
        from sentinelflow.ml import IsolationForestModel

        model = IsolationForestModel()

        model.fit(ml_feature_batch)

        assert model.is_ready

    def test_model_predict(self, ml_feature_batch, ml_feature_vector):
        """Test model prediction."""
        from sentinelflow.ml import IsolationForestModel

        model = IsolationForestModel()
        model.fit(ml_feature_batch)

        score = model.predict(ml_feature_vector)

        assert 0.0 <= score <= 1.0

    def test_model_save_load(self, ml_feature_batch, temp_model_dir):
        """Test model save and load."""
        from sentinelflow.ml import IsolationForestModel
        import os

        model = IsolationForestModel()
        model.fit(ml_feature_batch)

        model_path = os.path.join(temp_model_dir, "isolation_forest.pkl")
        model.save(model_path)

        loaded_model = IsolationForestModel()
        loaded_model.load(model_path)

        assert loaded_model.is_ready


class TestXGBoostModel:
    """Tests for XGBoostFraudModel."""

    def test_model_initialization(self):
        """Test model initializes correctly."""
        from sentinelflow.ml import XGBoostFraudModel

        model = XGBoostFraudModel()
        assert model is not None
        assert model.name == "XGBoost"

    def test_model_fit_with_labels(self, ml_feature_batch, ml_labels):
        """Test model training with labels."""
        from sentinelflow.ml import XGBoostFraudModel

        model = XGBoostFraudModel()

        model.fit(ml_feature_batch, ml_labels)

        assert model.is_ready

    def test_model_predict(self, ml_feature_batch, ml_labels, ml_feature_vector):
        """Test model prediction."""
        from sentinelflow.ml import XGBoostFraudModel

        model = XGBoostFraudModel()
        model.fit(ml_feature_batch, ml_labels)

        score = model.predict(ml_feature_vector)

        assert 0.0 <= score <= 1.0


class TestAutoEncoderModel:
    """Tests for AutoEncoderModel."""

    def test_model_initialization(self):
        """Test model initializes correctly."""
        from sentinelflow.ml import AutoEncoderModel

        model = AutoEncoderModel(input_dim=21)
        assert model is not None
        assert model.name == "AutoEncoder"

    def test_model_fit(self, ml_feature_batch):
        """Test model training."""
        from sentinelflow.ml import AutoEncoderModel

        model = AutoEncoderModel(input_dim=21)

        # Short training for test
        model.fit(ml_feature_batch, epochs=5)

        assert model.is_ready

    def test_model_predict(self, ml_feature_batch, ml_feature_vector):
        """Test model prediction."""
        from sentinelflow.ml import AutoEncoderModel

        model = AutoEncoderModel(input_dim=21)
        model.fit(ml_feature_batch, epochs=5)

        score = model.predict(ml_feature_vector)

        assert 0.0 <= score <= 1.0


class TestEnsembleVoter:
    """Tests for EnsembleVoter."""

    def test_ensemble_initialization(self):
        """Test ensemble initializes correctly."""
        from sentinelflow.ml import EnsembleVoter, IsolationForestModel

        models = [IsolationForestModel()]
        ensemble = EnsembleVoter(models)

        assert ensemble is not None

    def test_ensemble_predict(self, ml_feature_batch, ml_labels, ml_feature_vector):
        """Test ensemble prediction."""
        from sentinelflow.ml import (
            EnsembleVoter,
            IsolationForestModel,
            XGBoostFraudModel,
        )

        if_model = IsolationForestModel()
        if_model.fit(ml_feature_batch)

        xgb_model = XGBoostFraudModel()
        xgb_model.fit(ml_feature_batch, ml_labels)

        ensemble = EnsembleVoter([if_model, xgb_model])

        prediction = ensemble.predict(ml_feature_vector)

        assert prediction is not None
        assert 0.0 <= prediction.ensemble_score <= 1.0


class TestFeatureEngine:
    """Tests for TransactionFeatureEngine."""

    def test_engine_initialization(self):
        """Test engine initializes correctly."""
        from sentinelflow.ml import TransactionFeatureEngine

        engine = TransactionFeatureEngine()
        assert engine is not None

    def test_extract_features(self, sample_transaction):
        """Test feature extraction."""
        from sentinelflow.ml import TransactionFeatureEngine

        engine = TransactionFeatureEngine()

        features = engine.extract(sample_transaction)

        assert features is not None
        assert len(features) == 21  # Expected feature count


class TestFraudExplainer:
    """Tests for FraudExplainer."""

    def test_explainer_initialization(self):
        """Test explainer initializes correctly."""
        from sentinelflow.ml import FraudExplainer

        explainer = FraudExplainer()
        assert explainer is not None

    def test_explain_prediction(self, ml_feature_batch, ml_labels, ml_feature_vector):
        """Test explanation generation."""
        from sentinelflow.ml import FraudExplainer, XGBoostFraudModel

        model = XGBoostFraudModel()
        model.fit(ml_feature_batch, ml_labels)

        explainer = FraudExplainer(model)

        # Get prediction score first
        score = model.predict(ml_feature_vector)

        # Generate explanation
        explanation = explainer.explain(ml_feature_vector, score)

        assert explanation is not None

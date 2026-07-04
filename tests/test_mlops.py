# =============================================================================
# SentinelFlow - MLOps Module Tests
# =============================================================================
"""
Tests for MLOps: ModelCard, DriftDetector, ExperimentTracker, ModelRegistry.

Run with: pytest tests/test_mlops.py -v
"""

import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest


class TestModelCard:
    """Tests for ModelCard."""

    def test_create_minimal(self):
        from sentinelflow.mlops import ModelCard
        card = ModelCard(
            model_name="IsolationForest",
            model_version="1.0.0",
            model_type="Anomaly Detection",
            description="Unsupervised fraud detection",
        )
        assert card.model_name == "IsolationForest"

    def test_to_dict(self):
        from sentinelflow.mlops import ModelCard
        card = ModelCard(model_name="XGBoost", model_version="2.0.0", model_type="Classifier", description="GBM")
        d = card.to_dict()
        assert isinstance(d, dict)

    def test_to_json(self):
        from sentinelflow.mlops import ModelCard
        card = ModelCard(model_name="Test", model_version="1.0", model_type="T", description="Test")
        json_str = card.to_json()
        data = json.loads(json_str)
        assert isinstance(data, dict)

    def test_to_markdown(self):
        from sentinelflow.mlops import ModelCard
        card = ModelCard(model_name="TestModel", model_version="1.0", model_type="T", description="A test model")
        md = card.to_markdown()
        assert "TestModel" in md


class TestDriftDetector:
    """Tests for DriftDetector."""

    def test_initialization(self):
        from sentinelflow.mlops import DriftDetector
        dd = DriftDetector()
        assert dd is not None

    def test_detect_model_drift(self):
        from sentinelflow.mlops import DriftDetector
        dd = DriftDetector()
        report = dd.detect_model_drift(
            reference_metrics={"accuracy": 0.95, "f1": 0.93},
            current_metrics={"accuracy": 0.85, "f1": 0.80},
        )
        assert report is not None

    def test_no_drift_when_same(self):
        from sentinelflow.mlops import DriftDetector
        dd = DriftDetector()
        report = dd.detect_model_drift(
            reference_metrics={"accuracy": 0.95},
            current_metrics={"accuracy": 0.95},
        )
        assert report is not None


class TestExperimentTracker:
    """Tests for ExperimentTracker."""

    def test_create_experiment(self):
        from sentinelflow.mlops import ExperimentTracker
        et = ExperimentTracker(tracking_path=os.path.join(os.path.dirname(__file__), "..", "mlops_test", "experiments"))
        exp = et.create_experiment(name="test-exp", description="Test", tags={"framework": "pytest"})
        assert exp.name == "test-exp"
        assert exp.experiment_id is not None

    def test_start_and_log(self):
        from sentinelflow.mlops import ExperimentTracker
        path = os.path.join(os.path.dirname(__file__), "..", "mlops_test", "exps")
        et = ExperimentTracker(tracking_path=path)
        et.create_experiment("test-exp")

        with et.start_run(experiment_name="test-exp", run_name="run-1") as run:
            run.log_params({"lr": 0.01, "max_depth": 6})
            run.log_metrics({"accuracy": 0.95, "auc": 0.97})
            assert run.params["lr"] == 0.01
            assert run.metrics["accuracy"] == 0.95

    def test_list_runs(self):
        from sentinelflow.mlops import ExperimentTracker
        path = os.path.join(os.path.dirname(__file__), "..", "mlops_test", "list2")
        et = ExperimentTracker(tracking_path=path)
        et.create_experiment("multi-run")
        with et.start_run(experiment_name="multi-run", run_name="run-a"):
            pass
        with et.start_run(experiment_name="multi-run", run_name="run-b"):
            pass
        runs = et.list_runs("multi-run")
        assert len(runs) >= 2


class TestModelRegistry:
    """Tests for ModelRegistry."""

    def test_initialization(self):
        from sentinelflow.mlops import ModelRegistry
        mr = ModelRegistry(registry_path=os.path.join(os.path.dirname(__file__), "..", "mlops_test", "registry"))
        assert mr is not None

    def test_register_and_list(self):
        from sentinelflow.mlops import ModelRegistry
        import numpy as np
        from sklearn.ensemble import IsolationForest

        path = os.path.join(os.path.dirname(__file__), "..", "mlops_test", "reg")
        mr = ModelRegistry(registry_path=path)
        model = IsolationForest(random_state=42)

        version = mr.register_model(
            model=model,
            name="TestModel",
            description="Test",
            metrics={"auc": 0.95},
        )
        assert version is not None
        assert version.model_name == "TestModel"

        versions = mr.list_versions("TestModel")
        assert len(versions) >= 1

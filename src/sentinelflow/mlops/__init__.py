# =============================================================================
# SentinelFlow - MLOps Module
# =============================================================================
"""
Enterprise-grade MLOps for fraud detection models.

Components:
- Model Registry: Version control and model lifecycle management
- Experiment Tracking: MLflow-compatible experiment logging
- Drift Detection: Data and model drift monitoring
- Feature Store: Centralized feature management
- A/B Testing: Safe model deployment and comparison
- Model Cards: Documentation and transparency

TEKNOFEST 2026 - Production-ready ML infrastructure
"""

from sentinelflow.mlops.ab_testing import (
    ABTest,
    ABTestManager,
    ABTestResult,
)
from sentinelflow.mlops.drift_detector import (
    DataDriftReport,
    DriftDetector,
    ModelDriftReport,
)
from sentinelflow.mlops.experiment_tracker import (
    Experiment,
    ExperimentTracker,
    Run,
)
from sentinelflow.mlops.feature_store import (
    Feature,
    FeatureGroup,
    FeatureStore,
)
from sentinelflow.mlops.model_card import (
    ModelCard,
    generate_model_card,
)
from sentinelflow.mlops.registry import (
    ModelMetadata,
    ModelRegistry,
    ModelVersion,
)

__all__ = [
    # Registry
    "ModelRegistry",
    "ModelVersion",
    "ModelMetadata",
    # Experiments
    "ExperimentTracker",
    "Experiment",
    "Run",
    # Drift
    "DriftDetector",
    "DataDriftReport",
    "ModelDriftReport",
    # Feature Store
    "FeatureStore",
    "FeatureGroup",
    "Feature",
    # A/B Testing
    "ABTestManager",
    "ABTest",
    "ABTestResult",
    # Model Cards
    "ModelCard",
    "generate_model_card",
]

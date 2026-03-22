# =============================================================================
# SentinelFlow - Machine Learning Pipeline (TEKNOFEST Edition)
# =============================================================================
"""
Advanced ML-based fraud detection pipeline - Optimized for TEKNOFEST competition.

Hedef: %99.5+ doğruluk oranı (Geçen yıl 1.: %99.2)

Core Components:
- TransactionFeatureEngine: Extracts 21 base features from transactions
- AdvancedFeatureEngine: 32 additional behavioral/statistical features
- CombinedFeatureEngine: Full 53-feature extraction

Base Models:
- IsolationForestModel: Unsupervised anomaly detection
- XGBoostFraudModel: Supervised gradient boosting
- AutoEncoderModel: Deep learning reconstruction-based
- LightGBMFraudModel: Fast gradient boosting with DART
- CatBoostFraudModel: Categorical feature optimized

Advanced Models:
- GNNFraudModel: Graph Neural Network (PyTorch Geometric)
- TemporalFraudModel: LSTM/Transformer time-series
- StackingEnsemble: Meta-learner based model combination

Graph Features:
- GraphFeatureEngine: Neo4j-based graph analytics
- InMemoryGraphFeatureEngine: NetworkX-based (for testing)

Utilities:
- EnsembleVoter: Weighted multi-model voting
- FraudExplainer: SHAP-based explainability
- DataBalancer: SMOTE/ADASYN for imbalanced data
- FraudDatasetLoader: Dataset generation and loading
- TrainPipeline: End-to-end model training
"""

from loguru import logger

from sentinelflow.ml.dataset_loader import FraudDatasetLoader
from sentinelflow.ml.ensemble import EnsembleVoter

# Base components - always available
from sentinelflow.ml.feature_engine import FEATURE_NAMES, NUM_FEATURES, TransactionFeatureEngine

# Models that don't require torch
from sentinelflow.ml.models import IsolationForestModel, XGBoostFraudModel

# Optional torch-dependent components
try:
    from sentinelflow.ml.models import AutoEncoderModel

    HAS_AUTOENCODER = True
except (ImportError, OSError) as e:
    logger.warning(f"AutoEncoder not available (torch issue): {e}")
    HAS_AUTOENCODER = False
    AutoEncoderModel = None

try:
    from sentinelflow.ml.explainer import FraudExplainer
except ImportError:
    FraudExplainer = None

try:
    from sentinelflow.ml.replay_producer import DatasetReplayProducer
except ImportError:
    DatasetReplayProducer = None

try:
    from sentinelflow.ml.train_pipeline import TrainPipeline
except ImportError:
    TrainPipeline = None

try:
    from sentinelflow.ml.gnn_model import GNNFraudModel
except ImportError:
    GNNFraudModel = None

try:
    from sentinelflow.ml.temporal_model import TemporalFraudModel
except ImportError:
    TemporalFraudModel = None

# Advanced components for TEKNOFEST
try:
    from sentinelflow.ml.advanced_models import (
        CatBoostFraudModel,
        DataBalancer,
        FocalLoss,
        LightGBMFraudModel,
        StackingEnsemble,
        create_competition_ensemble,
    )
except ImportError:
    LightGBMFraudModel = None
    CatBoostFraudModel = None
    StackingEnsemble = None
    DataBalancer = None
    FocalLoss = None
    create_competition_ensemble = None

try:
    from sentinelflow.ml.advanced_features import (
        ADVANCED_FEATURE_NAMES,
        AdvancedFeatureEngine,
        CombinedFeatureEngine,
    )
except ImportError:
    AdvancedFeatureEngine = None
    CombinedFeatureEngine = None
    ADVANCED_FEATURE_NAMES = []

try:
    from sentinelflow.ml.graph_features import (
        GRAPH_FEATURE_NAMES,
        GraphFeatureEngine,
        InMemoryGraphFeatureEngine,
    )
except ImportError:
    GraphFeatureEngine = None
    InMemoryGraphFeatureEngine = None
    GRAPH_FEATURE_NAMES = []

__all__ = [
    # Base feature extraction
    "TransactionFeatureEngine",
    "FEATURE_NAMES",
    "NUM_FEATURES",
    "AdvancedFeatureEngine",
    "CombinedFeatureEngine",
    "ADVANCED_FEATURE_NAMES",
    # Graph features
    "GraphFeatureEngine",
    "InMemoryGraphFeatureEngine",
    "GRAPH_FEATURE_NAMES",
    # Base models
    "IsolationForestModel",
    "XGBoostFraudModel",
    "AutoEncoderModel",
    "HAS_AUTOENCODER",
    # Advanced models (TEKNOFEST)
    "LightGBMFraudModel",
    "CatBoostFraudModel",
    "GNNFraudModel",
    "TemporalFraudModel",
    # Ensemble methods
    "EnsembleVoter",
    "StackingEnsemble",
    "create_competition_ensemble",
    # Utilities
    "FraudExplainer",
    "DataBalancer",
    "FocalLoss",
    "FraudDatasetLoader",
    "DatasetReplayProducer",
    "TrainPipeline",
]

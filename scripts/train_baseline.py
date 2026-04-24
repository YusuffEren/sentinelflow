#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Baseline Model Training (TEKNOFEST Edition)
# =============================================================================
"""
Tüm ML modelleri için baseline eğitim ve benchmark scripti.

Modeller:
1. IsolationForest (Unsupervised)
2. XGBoost (Gradient Boosting)
3. LightGBM (Fast Gradient Boosting)
4. CatBoost (Categorical Optimized)
5. AutoEncoder (Deep Learning)
6. Stacking Ensemble (Meta-learner)

Hedef Metrikler (TEKNOFEST 2026):
- AUC-ROC: >0.995
- F1-Score: >0.98
- Precision@k: >0.99
- Recall: >0.97

Usage:
    python scripts/train_baseline.py --samples 100000
    python scripts/train_baseline.py --dataset data/competition/X_train.npy
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, average_precision_score, confusion_matrix,
        classification_report, precision_recall_curve
    )
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.error("scikit-learn is required")

from sentinelflow.ml.feature_engine import TransactionFeatureEngine, NUM_FEATURES, FEATURE_NAMES
from sentinelflow.ml.models import IsolationForestModel, XGBoostFraudModel
from sentinelflow.ml.ensemble import EnsembleVoter
from sentinelflow.ml.dataset_loader import FraudDatasetLoader

try:
    from sentinelflow.ml.models import AutoEncoderModel
    HAS_AUTOENCODER = True
except ImportError:
    HAS_AUTOENCODER = False
    
try:
    from sentinelflow.ml.advanced_models import LightGBMFraudModel, CatBoostFraudModel
    HAS_ADVANCED_MODELS = True
except ImportError:
    HAS_ADVANCED_MODELS = False


@dataclass
class ModelResult:
    """Single model evaluation result."""
    name: str
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    auc_roc: float = 0.0
    auc_pr: float = 0.0
    train_time_s: float = 0.0
    inference_time_ms: float = 0.0
    model_size_kb: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkReport:
    """Full benchmark report."""
    timestamp: str
    dataset_size: int
    train_size: int
    test_size: int
    n_features: int
    fraud_ratio: float
    models: List[ModelResult]
    best_model: str
    best_auc_roc: float
    total_time_s: float
    
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["models"] = [m.to_dict() if hasattr(m, "to_dict") else m for m in self.models]
        return d


def load_or_generate_data(
    dataset_path: Optional[str] = None,
    labels_path: Optional[str] = None,
    n_samples: int = 100000,
    fraud_ratio: float = 0.05,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Load existing dataset or generate synthetic one."""
    
    if dataset_path and os.path.exists(dataset_path):
        logger.info(f"Loading dataset from {dataset_path}")
        X = np.load(dataset_path)
        
        if labels_path and os.path.exists(labels_path):
            y = np.load(labels_path)
        else:
            labels_path_guess = dataset_path.replace("X_", "y_")
            if os.path.exists(labels_path_guess):
                y = np.load(labels_path_guess)
            else:
                raise FileNotFoundError(f"Labels file not found: {labels_path}")
        
        logger.info(f"Loaded: {X.shape}, fraud ratio: {y.mean()*100:.2f}%")
        return X, y
    
    logger.info(f"Generating synthetic dataset: {n_samples:,} samples")
    loader = FraudDatasetLoader(seed=seed)
    X, y, _ = loader.generate_synthetic(n_samples=n_samples, fraud_ratio=fraud_ratio)
    
    logger.info(f"Generated: {X.shape}, fraud ratio: {y.mean()*100:.2f}%")
    return X, y


def evaluate_model(
    model: Any,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Dict[str, float]:
    """Evaluate model and return metrics."""
    
    start_time = time.perf_counter()
    
    if hasattr(model, "predict_proba"):
        y_proba = model.predict_proba(X_test)
        if len(y_proba.shape) > 1:
            y_scores = y_proba[:, 1]
        else:
            y_scores = y_proba
    elif hasattr(model, "predict_single"):
        y_scores = np.array([model.predict_single(x) for x in X_test])
    else:
        y_scores = model.predict(X_test)
    
    inference_time = (time.perf_counter() - start_time) * 1000 / len(X_test)
    
    y_pred = (y_scores > 0.5).astype(int)
    
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
        "auc_roc": roc_auc_score(y_test, y_scores) if len(np.unique(y_test)) > 1 else 0.0,
        "auc_pr": average_precision_score(y_test, y_scores) if len(np.unique(y_test)) > 1 else 0.0,
        "inference_time_ms": inference_time,
    }
    
    return metrics


def train_isolation_forest(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> ModelResult:
    """Train and evaluate IsolationForest."""
    logger.info("Training IsolationForest...")
    
    start_time = time.perf_counter()
    
    model = IsolationForestModel(
        contamination=0.05,
        n_estimators=200,
        min_samples_to_train=100,
    )
    model.fit(X_train)
    
    train_time = time.perf_counter() - start_time
    
    metrics = evaluate_model(model, X_test, y_test)
    
    logger.info(f"IsolationForest - AUC-ROC: {metrics['auc_roc']:.4f}, F1: {metrics['f1']:.4f}")
    
    return ModelResult(
        name="IsolationForest",
        train_time_s=train_time,
        **metrics
    )


def train_xgboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> ModelResult:
    """Train and evaluate XGBoost."""
    logger.info("Training XGBoost...")
    
    start_time = time.perf_counter()
    
    model = XGBoostFraudModel(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
    )
    model.fit(X_train, y_train)
    
    train_time = time.perf_counter() - start_time
    
    metrics = evaluate_model(model, X_test, y_test)
    
    logger.info(f"XGBoost - AUC-ROC: {metrics['auc_roc']:.4f}, F1: {metrics['f1']:.4f}")
    
    return ModelResult(
        name="XGBoost",
        train_time_s=train_time,
        **metrics
    )


def train_lightgbm(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Optional[ModelResult]:
    """Train and evaluate LightGBM."""
    if not HAS_ADVANCED_MODELS:
        logger.warning("LightGBM not available, skipping")
        return None
    
    logger.info("Training LightGBM...")
    
    start_time = time.perf_counter()
    
    model = LightGBMFraudModel(
        n_estimators=500,
        max_depth=8,
        learning_rate=0.05,
        num_leaves=31,
        boosting_type="dart",
    )
    model.fit(X_train, y_train, X_val=X_test, y_val=y_test)
    
    train_time = time.perf_counter() - start_time
    
    metrics = evaluate_model(model, X_test, y_test)
    
    logger.info(f"LightGBM - AUC-ROC: {metrics['auc_roc']:.4f}, F1: {metrics['f1']:.4f}")
    
    return ModelResult(
        name="LightGBM",
        train_time_s=train_time,
        **metrics
    )


def train_catboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Optional[ModelResult]:
    """Train and evaluate CatBoost."""
    if not HAS_ADVANCED_MODELS:
        logger.warning("CatBoost not available, skipping")
        return None
    
    logger.info("Training CatBoost...")
    
    start_time = time.perf_counter()
    
    model = CatBoostFraudModel(
        iterations=500,
        depth=8,
        learning_rate=0.05,
    )
    model.fit(X_train, y_train, X_val=X_test, y_val=y_test)
    
    train_time = time.perf_counter() - start_time
    
    metrics = evaluate_model(model, X_test, y_test)
    
    logger.info(f"CatBoost - AUC-ROC: {metrics['auc_roc']:.4f}, F1: {metrics['f1']:.4f}")
    
    return ModelResult(
        name="CatBoost",
        train_time_s=train_time,
        **metrics
    )


def train_autoencoder(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Optional[ModelResult]:
    """Train and evaluate AutoEncoder."""
    if not HAS_AUTOENCODER:
        logger.warning("AutoEncoder not available, skipping")
        return None
    
    logger.info("Training AutoEncoder...")
    
    start_time = time.perf_counter()
    
    X_train_normal = X_train[y_train == 0]
    
    model = AutoEncoderModel(
        input_dim=X_train.shape[1],
        encoding_dim=8,
    )
    model.fit(X_train_normal, epochs=50)
    
    train_time = time.perf_counter() - start_time
    
    metrics = evaluate_model(model, X_test, y_test)
    
    logger.info(f"AutoEncoder - AUC-ROC: {metrics['auc_roc']:.4f}, F1: {metrics['f1']:.4f}")
    
    return ModelResult(
        name="AutoEncoder",
        train_time_s=train_time,
        **metrics
    )


def run_benchmark(
    X: np.ndarray,
    y: np.ndarray,
    test_size: float = 0.2,
    seed: int = 42,
    output_dir: str = "models/baseline",
) -> BenchmarkReport:
    """Run full benchmark suite."""
    
    total_start = time.perf_counter()
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )
    
    logger.info(f"Train: {len(X_train):,}, Test: {len(X_test):,}")
    logger.info(f"Train fraud ratio: {y_train.mean()*100:.2f}%")
    
    results: List[ModelResult] = []
    
    results.append(train_isolation_forest(X_train, y_train, X_test, y_test))
    results.append(train_xgboost(X_train, y_train, X_test, y_test))
    
    lgb_result = train_lightgbm(X_train, y_train, X_test, y_test)
    if lgb_result:
        results.append(lgb_result)
    
    cat_result = train_catboost(X_train, y_train, X_test, y_test)
    if cat_result:
        results.append(cat_result)
    
    ae_result = train_autoencoder(X_train, y_train, X_test, y_test)
    if ae_result:
        results.append(ae_result)
    
    best_model = max(results, key=lambda r: r.auc_roc)
    
    total_time = time.perf_counter() - total_start
    
    report = BenchmarkReport(
        timestamp=datetime.now().isoformat(),
        dataset_size=len(X),
        train_size=len(X_train),
        test_size=len(X_test),
        n_features=X.shape[1],
        fraud_ratio=y.mean(),
        models=results,
        best_model=best_model.name,
        best_auc_roc=best_model.auc_roc,
        total_time_s=total_time,
    )
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    report_path = Path(output_dir) / f"baseline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(report_path, "w") as f:
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    
    logger.info(f"Report saved to {report_path}")
    
    return report


def print_report(report: BenchmarkReport):
    """Print formatted benchmark report."""
    print("\n" + "=" * 70)
    print("SENTINELFLOW BASELINE BENCHMARK REPORT")
    print("=" * 70)
    print(f"Timestamp: {report.timestamp}")
    print(f"Dataset: {report.dataset_size:,} samples, {report.n_features} features")
    print(f"Train/Test: {report.train_size:,} / {report.test_size:,}")
    print(f"Fraud Ratio: {report.fraud_ratio*100:.2f}%")
    print(f"Total Time: {report.total_time_s:.1f}s")
    print()
    
    print("-" * 70)
    print(f"{'Model':<15} {'AUC-ROC':>10} {'F1':>10} {'Precision':>10} {'Recall':>10} {'Time(s)':>10}")
    print("-" * 70)
    
    for m in sorted(report.models, key=lambda x: x.auc_roc, reverse=True):
        print(f"{m.name:<15} {m.auc_roc:>10.4f} {m.f1:>10.4f} {m.precision:>10.4f} {m.recall:>10.4f} {m.train_time_s:>10.1f}")
    
    print("-" * 70)
    print(f"\nBest Model: {report.best_model} (AUC-ROC: {report.best_auc_roc:.4f})")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Train baseline ML models for SentinelFlow")
    
    parser.add_argument("--dataset", type=str, help="Path to feature matrix (npy)")
    parser.add_argument("--labels", type=str, help="Path to labels (npy)")
    parser.add_argument("--samples", type=int, default=100000, help="Samples for synthetic data")
    parser.add_argument("--fraud-ratio", type=float, default=0.05, help="Fraud ratio")
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set ratio")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, default="models/baseline", help="Output directory")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("SentinelFlow Baseline Model Training")
    logger.info("=" * 60)
    
    X, y = load_or_generate_data(
        dataset_path=args.dataset,
        labels_path=args.labels,
        n_samples=args.samples,
        fraud_ratio=args.fraud_ratio,
        seed=args.seed,
    )
    
    report = run_benchmark(
        X, y,
        test_size=args.test_size,
        seed=args.seed,
        output_dir=args.output,
    )
    
    print_report(report)


if __name__ == "__main__":
    main()

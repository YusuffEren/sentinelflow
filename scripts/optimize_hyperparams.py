#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Optuna Hyperparameter Optimization (TEKNOFEST Edition)
# =============================================================================
"""
Optuna ile ML model hyperparameter optimization.

Hedef: %99.5+ AUC-ROC elde etmek için optimal parametreleri bul.

Optimize edilen modeller:
1. XGBoost
2. LightGBM
3. CatBoost
4. Ensemble weights

Usage:
    python scripts/optimize_hyperparams.py --model xgboost --trials 100
    python scripts/optimize_hyperparams.py --model all --trials 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import MedianPruner
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False
    logger.error("Optuna is required: pip install optuna")

try:
    from sklearn.model_selection import cross_val_score, StratifiedKFold
    from sklearn.metrics import roc_auc_score, f1_score
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

from sentinelflow.ml.dataset_loader import FraudDatasetLoader
from sentinelflow.ml.models import XGBoostFraudModel

try:
    from sentinelflow.ml.advanced_models import LightGBMFraudModel, CatBoostFraudModel
    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


def load_data(n_samples: int = 50000, fraud_ratio: float = 0.05, seed: int = 42):
    """Load or generate dataset for optimization."""
    loader = FraudDatasetLoader(seed=seed)
    X, y, _ = loader.generate_synthetic(n_samples=n_samples, fraud_ratio=fraud_ratio)
    return X, y


def objective_xgboost(trial: optuna.Trial, X: np.ndarray, y: np.ndarray) -> float:
    """Optuna objective for XGBoost."""
    
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
        "max_depth": trial.suggest_int("max_depth", 3, 12),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        "gamma": trial.suggest_float("gamma", 0, 5),
    }
    
    model = XGBoostFraudModel(**params)
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    
    for train_idx, val_idx in skf.split(X, y):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]
        
        model.fit(X_train, y_train)
        
        y_scores = np.array([model.predict_single(x) for x in X_val])
        auc = roc_auc_score(y_val, y_scores)
        scores.append(auc)
    
    return np.mean(scores)


def objective_lightgbm(trial: optuna.Trial, X: np.ndarray, y: np.ndarray) -> float:
    """Optuna objective for LightGBM."""
    if not HAS_ADVANCED:
        raise ImportError("LightGBM not available")
    
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
        "max_depth": trial.suggest_int("max_depth", 3, 15),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "boosting_type": trial.suggest_categorical("boosting_type", ["gbdt", "dart", "goss"]),
    }
    
    model = LightGBMFraudModel(**params)
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    
    for train_idx, val_idx in skf.split(X, y):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]
        
        model.fit(X_train, y_train)
        
        y_scores = np.array([model.predict_single(x) for x in X_val])
        auc = roc_auc_score(y_val, y_scores)
        scores.append(auc)
    
    return np.mean(scores)


def objective_catboost(trial: optuna.Trial, X: np.ndarray, y: np.ndarray) -> float:
    """Optuna objective for CatBoost."""
    if not HAS_ADVANCED:
        raise ImportError("CatBoost not available")
    
    params = {
        "iterations": trial.suggest_int("iterations", 100, 1000),
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-8, 10.0, log=True),
        "border_count": trial.suggest_int("border_count", 32, 255),
        "bagging_temperature": trial.suggest_float("bagging_temperature", 0, 10),
        "random_strength": trial.suggest_float("random_strength", 0, 10),
    }
    
    model = CatBoostFraudModel(**params)
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    
    for train_idx, val_idx in skf.split(X, y):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]
        
        model.fit(X_train, y_train, X_val=X_val, y_val=y_val)
        
        y_scores = np.array([model.predict_single(x) for x in X_val])
        auc = roc_auc_score(y_val, y_scores)
        scores.append(auc)
    
    return np.mean(scores)


def run_optimization(
    model_name: str,
    X: np.ndarray,
    y: np.ndarray,
    n_trials: int = 100,
    timeout: Optional[int] = None,
    output_dir: str = "models/optuna",
) -> Dict[str, Any]:
    """Run Optuna optimization for a specific model."""
    
    if not HAS_OPTUNA:
        raise ImportError("Optuna not installed")
    
    logger.info(f"Starting optimization for {model_name} ({n_trials} trials)")
    
    objectives = {
        "xgboost": objective_xgboost,
        "lightgbm": objective_lightgbm,
        "catboost": objective_catboost,
    }
    
    if model_name not in objectives:
        raise ValueError(f"Unknown model: {model_name}")
    
    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=42),
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=5),
        study_name=f"sentinelflow_{model_name}",
    )
    
    objective_fn = lambda trial: objectives[model_name](trial, X, y)
    
    study.optimize(
        objective_fn,
        n_trials=n_trials,
        timeout=timeout,
        show_progress_bar=True,
    )
    
    best_params = study.best_params
    best_value = study.best_value
    
    logger.info(f"Best {model_name} AUC-ROC: {best_value:.4f}")
    logger.info(f"Best params: {best_params}")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    result = {
        "model": model_name,
        "best_auc_roc": best_value,
        "best_params": best_params,
        "n_trials": n_trials,
        "timestamp": datetime.now().isoformat(),
    }
    
    result_path = Path(output_dir) / f"{model_name}_best_params.json"
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)
    
    study_path = Path(output_dir) / f"{model_name}_study.pkl"
    with open(study_path, "wb") as f:
        import pickle
        pickle.dump(study, f)
    
    logger.info(f"Results saved to {output_dir}")
    
    return result


def main():
    if not HAS_OPTUNA:
        logger.error("Optuna is required: pip install optuna")
        sys.exit(1)
    
    parser = argparse.ArgumentParser(description="Hyperparameter optimization with Optuna")
    
    parser.add_argument("--model", type=str, default="xgboost",
                       choices=["xgboost", "lightgbm", "catboost", "all"],
                       help="Model to optimize")
    parser.add_argument("--trials", type=int, default=100, help="Number of trials")
    parser.add_argument("--timeout", type=int, default=None, help="Timeout in seconds")
    parser.add_argument("--samples", type=int, default=50000, help="Dataset size")
    parser.add_argument("--fraud-ratio", type=float, default=0.05, help="Fraud ratio")
    parser.add_argument("--output", type=str, default="models/optuna", help="Output directory")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("SentinelFlow Hyperparameter Optimization")
    logger.info("=" * 60)
    
    X, y = load_data(n_samples=args.samples, fraud_ratio=args.fraud_ratio)
    logger.info(f"Dataset: {X.shape}, fraud ratio: {y.mean()*100:.2f}%")
    
    models_to_optimize = (
        ["xgboost", "lightgbm", "catboost"] if args.model == "all" 
        else [args.model]
    )
    
    results = []
    
    for model_name in models_to_optimize:
        try:
            result = run_optimization(
                model_name=model_name,
                X=X,
                y=y,
                n_trials=args.trials,
                timeout=args.timeout,
                output_dir=args.output,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to optimize {model_name}: {e}")
    
    print("\n" + "=" * 60)
    print("OPTIMIZATION SUMMARY")
    print("=" * 60)
    
    for r in results:
        print(f"\n{r['model'].upper()}:")
        print(f"  Best AUC-ROC: {r['best_auc_roc']:.4f}")
        print(f"  Best params: {json.dumps(r['best_params'], indent=4)}")
    
    print("=" * 60)


if __name__ == "__main__":
    main()

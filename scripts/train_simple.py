#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Simple Training Script (No Torch)
# =============================================================================
"""
TEKNOFEST yarışması için model eğitim scripti.
200K+ veri seti ile IsolationForest ve XGBoost eğitir.
Torch/AutoEncoder kullanmaz.

Hedef: %99+ accuracy
"""

import argparse
import json
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
)
from sklearn.preprocessing import StandardScaler

try:
    import xgboost as xgb

    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    print("WARNING: xgboost not available")

try:
    import lightgbm as lgb

    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False
    print("WARNING: lightgbm not available")

try:
    import catboost as cb

    HAS_CATBOOST = True
except ImportError:
    HAS_CATBOOST = False
    print("WARNING: catboost not available")


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def extract_features(df: pd.DataFrame) -> np.ndarray:
    """Extract features from transaction DataFrame."""
    log("Extracting features...")

    # Numeric features
    features = []

    # Amount features
    features.append(df["amount"].values)
    features.append(np.log1p(df["amount"].values))  # log amount

    # Time features from timestamp
    if "timestamp" in df.columns:
        timestamps = pd.to_datetime(df["timestamp"])
        features.append(timestamps.dt.hour.values)
        features.append(timestamps.dt.dayofweek.values)
        features.append(
            (timestamps.dt.hour < 6).astype(int).values
            | (timestamps.dt.hour > 22).astype(int).values
        )  # is_night
    else:
        features.append(np.zeros(len(df)))
        features.append(np.zeros(len(df)))
        features.append(np.zeros(len(df)))

    # Channel encoding
    channel_map = {"mobile": 0, "web": 1, "atm": 2, "branch": 3, "eft": 4, "pos": 5}
    if "channel" in df.columns:
        features.append(df["channel"].map(channel_map).fillna(0).values)
    else:
        features.append(np.zeros(len(df)))

    # City features (is_international)
    turkish_cities = {
        "İstanbul",
        "Istanbul",
        "Ankara",
        "İzmir",
        "Izmir",
        "Bursa",
        "Antalya",
        "Adana",
        "Konya",
        "Gaziantep",
        "Mersin",
        "Diyarbakır",
        "Kayseri",
        "Eskişehir",
        "Trabzon",
        "Samsun",
        "Denizli",
    }
    if "receiver_city" in df.columns:
        is_intl = df["receiver_city"].apply(lambda x: 0 if x in turkish_cities else 1).values
        features.append(is_intl)
    else:
        features.append(np.zeros(len(df)))

    # Description-based features
    suspicious_keywords = [
        "acil",
        "bitcoin",
        "kripto",
        "casino",
        "bahis",
        "offshore",
        "anonim",
        "kumar",
        "usdt",
        "gizli",
    ]
    if "description" in df.columns:
        desc_lower = df["description"].str.lower().fillna("")
        has_suspicious = desc_lower.apply(
            lambda x: 1 if any(kw in str(x) for kw in suspicious_keywords) else 0
        ).values
        features.append(has_suspicious)
    else:
        features.append(np.zeros(len(df)))

    # MASAK threshold features
    MASAK_THRESHOLD = 75000
    features.append((df["amount"] > MASAK_THRESHOLD).astype(int).values)
    features.append(
        (df["amount"] > MASAK_THRESHOLD * 0.9).astype(int).values
        & (df["amount"] < MASAK_THRESHOLD).astype(int).values
    )  # structuring

    # Amount statistics per sender (aggregated features)
    sender_stats = df.groupby("sender_iban")["amount"].agg(["mean", "std", "count"]).fillna(0)
    sender_mean = df["sender_iban"].map(sender_stats["mean"]).fillna(0).values
    sender_std = df["sender_iban"].map(sender_stats["std"]).fillna(0).values
    sender_count = df["sender_iban"].map(sender_stats["count"]).fillna(0).values

    features.append(sender_mean)
    features.append(sender_std)
    features.append(sender_count)

    # Z-score of amount compared to sender's mean
    with np.errstate(divide="ignore", invalid="ignore"):
        z_score = np.where(sender_std > 0, (df["amount"].values - sender_mean) / sender_std, 0)
        z_score = np.nan_to_num(z_score, nan=0.0, posinf=10.0, neginf=-10.0)
    features.append(z_score)

    # Stack features
    X = np.column_stack(features)

    # Handle NaN/Inf
    X = np.nan_to_num(X, nan=0.0, posinf=1e6, neginf=-1e6)

    log(f"Extracted {X.shape[1]} features for {X.shape[0]} samples")
    return X


def train_isolation_forest(X_train: np.ndarray, contamination: float = 0.03) -> IsolationForest:
    """Train Isolation Forest model."""
    log("Training IsolationForest...")

    model = IsolationForest(
        n_estimators=300,
        contamination=contamination,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
        verbose=0,
    )
    model.fit(X_train)

    log("IsolationForest trained")
    return model


def train_xgboost(X_train: np.ndarray, y_train: np.ndarray) -> xgb.XGBClassifier:
    """Train XGBoost model."""
    log("Training XGBoost...")

    # Calculate scale_pos_weight for imbalanced data
    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0

    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=8,
        learning_rate=0.03,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        use_label_encoder=False,
        eval_metric="logloss",
    )
    model.fit(X_train, y_train)

    log("XGBoost trained")
    return model


def train_lightgbm(X_train: np.ndarray, y_train: np.ndarray) -> lgb.LGBMClassifier:
    """Train LightGBM model."""
    log("Training LightGBM...")

    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0

    model = lgb.LGBMClassifier(
        n_estimators=500,
        max_depth=8,
        learning_rate=0.03,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train)

    log("LightGBM trained")
    return model


def train_catboost(X_train: np.ndarray, y_train: np.ndarray) -> cb.CatBoostClassifier:
    """Train CatBoost model."""
    log("Training CatBoost...")

    model = cb.CatBoostClassifier(
        iterations=500,
        depth=8,
        learning_rate=0.03,
        random_state=42,
        verbose=False,
        thread_count=-1,
        auto_class_weights="Balanced",
    )
    model.fit(X_train, y_train)

    log("CatBoost trained")
    return model


def evaluate_model(name: str, y_true: np.ndarray, y_scores: np.ndarray) -> dict:
    """Evaluate model performance."""

    # Find optimal threshold
    best_f1 = 0
    best_threshold = 0.5

    for threshold in np.arange(0.1, 0.9, 0.05):
        preds = (y_scores >= threshold).astype(int)
        f1 = f1_score(y_true, preds, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold

    y_pred = (y_scores >= best_threshold).astype(int)

    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "auc_roc": float(roc_auc_score(y_true, y_scores)),
        "auc_pr": float(average_precision_score(y_true, y_scores)),
        "optimal_threshold": float(best_threshold),
    }

    cm = confusion_matrix(y_true, y_pred)
    if cm.shape == (2, 2):
        metrics["tn"] = int(cm[0, 0])
        metrics["fp"] = int(cm[0, 1])
        metrics["fn"] = int(cm[1, 0])
        metrics["tp"] = int(cm[1, 1])

    log(
        f"{name}: Acc={metrics['accuracy']:.4f} P={metrics['precision']:.4f} "
        f"R={metrics['recall']:.4f} F1={metrics['f1']:.4f} AUC={metrics['auc_roc']:.4f}"
    )

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Train fraud detection models")
    parser.add_argument("--csv", type=str, required=True, help="Path to CSV dataset")
    parser.add_argument("--output", type=str, default="models", help="Output directory")
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set fraction")

    args = parser.parse_args()

    start_time = time.time()
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load data
    log(f"Loading data from {args.csv}")
    df = pd.read_csv(args.csv)
    log(f"Loaded {len(df)} transactions")

    # Extract features
    X = extract_features(df)
    y = df["is_fraud"].astype(int).values

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=y
    )
    log(f"Train: {len(y_train)} ({y_train.mean()*100:.2f}% fraud)")
    log(f"Test: {len(y_test)} ({y_test.mean()*100:.2f}% fraud)")

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Save scaler
    with open(output_path / "scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)

    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "train_size": len(y_train),
        "test_size": len(y_test),
        "fraud_ratio": float(y.mean()),
        "models": {},
    }

    # ==========================================================================
    # Train models
    # ==========================================================================

    # 1. Isolation Forest
    log("=" * 60)
    if_model = train_isolation_forest(X_train_scaled, contamination=y.mean())

    # IF scoring (decision_function: more negative = more anomalous)
    if_scores = -if_model.decision_function(X_test_scaled)
    if_scores = (if_scores - if_scores.min()) / (if_scores.max() - if_scores.min() + 1e-10)

    results["models"]["IsolationForest"] = evaluate_model("IsolationForest", y_test, if_scores)

    with open(output_path / "isolation_forest.pkl", "wb") as f:
        pickle.dump(if_model, f)

    # 2. XGBoost
    log("=" * 60)
    if HAS_XGBOOST:
        xgb_model = train_xgboost(X_train_scaled, y_train)
        xgb_scores = xgb_model.predict_proba(X_test_scaled)[:, 1]
        results["models"]["XGBoost"] = evaluate_model("XGBoost", y_test, xgb_scores)
        xgb_model.save_model(str(output_path / "xgboost.json"))

    # 3. LightGBM
    log("=" * 60)
    if HAS_LIGHTGBM:
        lgb_model = train_lightgbm(X_train_scaled, y_train)
        lgb_scores = lgb_model.predict_proba(X_test_scaled)[:, 1]
        results["models"]["LightGBM"] = evaluate_model("LightGBM", y_test, lgb_scores)
        lgb_model.booster_.save_model(str(output_path / "lightgbm.txt"))

    # 4. CatBoost
    log("=" * 60)
    if HAS_CATBOOST:
        cb_model = train_catboost(X_train_scaled, y_train)
        cb_scores = cb_model.predict_proba(X_test_scaled)[:, 1]
        results["models"]["CatBoost"] = evaluate_model("CatBoost", y_test, cb_scores)
        cb_model.save_model(str(output_path / "catboost.cbm"))

    # ==========================================================================
    # Ensemble (weighted average)
    # ==========================================================================
    log("=" * 60)
    log("Creating Ensemble...")

    ensemble_scores = np.zeros(len(y_test))
    weights = {}

    if "IsolationForest" in results["models"]:
        weights["if"] = 0.15
        ensemble_scores += weights["if"] * if_scores

    if HAS_XGBOOST and "XGBoost" in results["models"]:
        weights["xgb"] = 0.30
        ensemble_scores += weights["xgb"] * xgb_scores

    if HAS_LIGHTGBM and "LightGBM" in results["models"]:
        weights["lgb"] = 0.30
        ensemble_scores += weights["lgb"] * lgb_scores

    if HAS_CATBOOST and "CatBoost" in results["models"]:
        weights["cb"] = 0.25
        ensemble_scores += weights["cb"] * cb_scores

    # Normalize
    total_weight = sum(weights.values())
    ensemble_scores = ensemble_scores / total_weight

    results["models"]["Ensemble"] = evaluate_model("Ensemble", y_test, ensemble_scores)

    # ==========================================================================
    # Find best model
    # ==========================================================================
    best_model = max(results["models"].items(), key=lambda x: x[1]["f1"])
    results["best_model"] = best_model[0]
    results["best_f1"] = best_model[1]["f1"]
    results["best_accuracy"] = best_model[1]["accuracy"]
    results["training_time_seconds"] = round(time.time() - start_time, 2)

    # Save report
    with open(output_path / "training_report.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Print summary
    print("\n" + "=" * 70)
    print("  SentinelFlow Training Report")
    print("=" * 70)
    print(f"  Dataset Size   : {len(df):,}")
    print(f"  Train / Test   : {len(y_train):,} / {len(y_test):,}")
    print(f"  Fraud Ratio    : {y.mean()*100:.2f}%")
    print(f"  Training Time  : {results['training_time_seconds']:.1f}s")
    print()
    print(f"  {'Model':<20} {'Accuracy':>10} {'Precision':>10} {'Recall':>8} {'F1':>8} {'AUC':>8}")
    print("-" * 70)

    for name, m in results["models"].items():
        print(
            f"  {name:<20} {m['accuracy']:>10.4f} {m['precision']:>10.4f} "
            f"{m['recall']:>8.4f} {m['f1']:>8.4f} {m['auc_roc']:>8.4f}"
        )

    print("-" * 70)
    print(f"\n  BEST MODEL: {results['best_model']}")
    print(f"  Accuracy: {results['best_accuracy']*100:.2f}%")
    print(f"  F1 Score: {results['best_f1']:.4f}")
    print("=" * 70)

    # Confusion matrix for best
    best = results["models"][results["best_model"]]
    if "tp" in best:
        print(f"\n  Confusion Matrix ({results['best_model']}):")
        print(f"  {'':15} Predicted Normal  Predicted Fraud")
        print(f"  {'Actual Normal':15} {best['tn']:>16,}  {best['fp']:>15,}")
        print(f"  {'Actual Fraud':15} {best['fn']:>16,}  {best['tp']:>15,}")
    print()


if __name__ == "__main__":
    main()

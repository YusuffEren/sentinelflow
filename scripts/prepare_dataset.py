#!/usr/bin/env python
# =============================================================================
# SentinelFlow - Dataset Preparation Script (TEKNOFEST Edition)
# =============================================================================
"""
Competition dataset hazırlama scripti.

Özellikler:
1. Synthetic dataset generation (500K+ transaction)
2. Data augmentation (SMOTE, ADASYN, SMOTETomek)
3. Feature engineering (53 features)
4. Train/val/test split
5. Class balancing

Usage:
    python scripts/prepare_dataset.py --size 500000 --output data/competition
    python scripts/prepare_dataset.py --augment smote --fraud-ratio 0.03
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sentinelflow.ml.competition_dataset import CompetitionDatasetGenerator
from sentinelflow.ml.dataset_loader import FraudDatasetLoader
from sentinelflow.ml.feature_engine import TransactionFeatureEngine, NUM_FEATURES, FEATURE_NAMES

try:
    from sentinelflow.ml.advanced_features import CombinedFeatureEngine
    HAS_ADVANCED_FEATURES = True
except ImportError:
    HAS_ADVANCED_FEATURES = False

try:
    from imblearn.over_sampling import SMOTE, ADASYN
    from imblearn.combine import SMOTETomek
    HAS_IMBLEARN = True
except ImportError:
    HAS_IMBLEARN = False
    logger.warning("imbalanced-learn not installed, data augmentation disabled")

try:
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


def generate_dataset(
    n_transactions: int = 500000,
    n_users: int = 50000,
    fraud_ratio: float = 0.03,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate competition dataset."""
    logger.info(f"Generating {n_transactions:,} transactions with {fraud_ratio*100:.1f}% fraud")
    
    generator = CompetitionDatasetGenerator(seed=seed, n_users=n_users)
    df = generator.generate(
        n_transactions=n_transactions,
        fraud_ratio=fraud_ratio,
    )
    
    logger.info(f"Generated dataset: {len(df):,} transactions")
    return df


def extract_features(
    df: pd.DataFrame,
    use_advanced: bool = True,
) -> tuple[np.ndarray, np.ndarray]:
    """Extract features from transaction DataFrame."""
    logger.info("Extracting features...")
    
    if use_advanced and HAS_ADVANCED_FEATURES:
        from sentinelflow.ml.advanced_features import CombinedFeatureEngine
        engine = CombinedFeatureEngine()
        feature_names = engine.get_feature_names()
    else:
        engine = TransactionFeatureEngine(history_window_size=1000)
        feature_names = FEATURE_NAMES
    
    n_features = len(feature_names)
    X = np.zeros((len(df), n_features))
    
    for i, row in df.iterrows():
        tx = row.to_dict()
        if use_advanced and HAS_ADVANCED_FEATURES:
            X[i] = engine.extract_vector(tx)
        else:
            X[i] = engine.extract_vector(tx)
        
        if (i + 1) % 50000 == 0:
            logger.info(f"Processed {i+1:,}/{len(df):,} transactions")
    
    y = df["is_fraud"].values.astype(int)
    
    logger.info(f"Feature extraction complete: {X.shape}")
    return X, y


def augment_data(
    X: np.ndarray,
    y: np.ndarray,
    method: str = "smote",
    target_ratio: float = 0.2,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Apply data augmentation for class balancing."""
    if not HAS_IMBLEARN:
        logger.warning("imbalanced-learn not available, skipping augmentation")
        return X, y
    
    fraud_count = y.sum()
    normal_count = len(y) - fraud_count
    current_ratio = fraud_count / len(y)
    
    logger.info(f"Before augmentation: {len(y):,} samples, {fraud_count:,} fraud ({current_ratio*100:.2f}%)")
    
    if method == "smote":
        sampler = SMOTE(
            sampling_strategy=target_ratio,
            random_state=seed,
            k_neighbors=5,
            n_jobs=-1,
        )
    elif method == "adasyn":
        sampler = ADASYN(
            sampling_strategy=target_ratio,
            random_state=seed,
            n_neighbors=5,
            n_jobs=-1,
        )
    elif method == "smote_tomek":
        sampler = SMOTETomek(
            sampling_strategy=target_ratio,
            random_state=seed,
            n_jobs=-1,
        )
    else:
        logger.warning(f"Unknown augmentation method: {method}")
        return X, y
    
    logger.info(f"Applying {method.upper()} augmentation (target ratio: {target_ratio*100:.1f}%)")
    
    X_resampled, y_resampled = sampler.fit_resample(X, y)
    
    new_fraud_count = y_resampled.sum()
    new_ratio = new_fraud_count / len(y_resampled)
    
    logger.info(
        f"After augmentation: {len(y_resampled):,} samples, "
        f"{new_fraud_count:,} fraud ({new_ratio*100:.2f}%)"
    )
    
    return X_resampled, y_resampled


def split_dataset(
    X: np.ndarray,
    y: np.ndarray,
    test_size: float = 0.2,
    val_size: float = 0.1,
    seed: int = 42,
) -> dict:
    """Split dataset into train/val/test sets with stratification."""
    if not HAS_SKLEARN:
        logger.error("scikit-learn required for splitting")
        return {}
    
    logger.info(f"Splitting dataset (test={test_size}, val={val_size})")
    
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )
    
    val_ratio = val_size / (1 - test_size)
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_ratio, random_state=seed, stratify=y_temp
    )
    
    logger.info(f"Train: {len(X_train):,}, Val: {len(X_val):,}, Test: {len(X_test):,}")
    
    return {
        "X_train": X_train,
        "y_train": y_train,
        "X_val": X_val,
        "y_val": y_val,
        "X_test": X_test,
        "y_test": y_test,
    }


def normalize_features(
    splits: dict,
) -> tuple[dict, StandardScaler]:
    """Normalize features using StandardScaler."""
    if not HAS_SKLEARN:
        return splits, None
    
    logger.info("Normalizing features...")
    
    scaler = StandardScaler()
    splits["X_train"] = scaler.fit_transform(splits["X_train"])
    splits["X_val"] = scaler.transform(splits["X_val"])
    splits["X_test"] = scaler.transform(splits["X_test"])
    
    return splits, scaler


def save_dataset(
    splits: dict,
    output_dir: str,
    df: pd.DataFrame = None,
    scaler: StandardScaler = None,
) -> dict:
    """Save prepared dataset to disk."""
    import pickle
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    np.save(output_path / f"X_train_{timestamp}.npy", splits["X_train"])
    np.save(output_path / f"y_train_{timestamp}.npy", splits["y_train"])
    np.save(output_path / f"X_val_{timestamp}.npy", splits["X_val"])
    np.save(output_path / f"y_val_{timestamp}.npy", splits["y_val"])
    np.save(output_path / f"X_test_{timestamp}.npy", splits["X_test"])
    np.save(output_path / f"y_test_{timestamp}.npy", splits["y_test"])
    
    if df is not None:
        df.to_parquet(output_path / f"raw_dataset_{timestamp}.parquet", index=False)
    
    if scaler is not None:
        with open(output_path / f"scaler_{timestamp}.pkl", "wb") as f:
            pickle.dump(scaler, f)
    
    metadata = {
        "timestamp": timestamp,
        "train_size": len(splits["X_train"]),
        "val_size": len(splits["X_val"]),
        "test_size": len(splits["X_test"]),
        "n_features": splits["X_train"].shape[1],
        "fraud_ratio_train": float(splits["y_train"].mean()),
        "fraud_ratio_val": float(splits["y_val"].mean()),
        "fraud_ratio_test": float(splits["y_test"].mean()),
    }
    
    with open(output_path / f"metadata_{timestamp}.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Dataset saved to {output_path}")
    
    return metadata


def main():
    parser = argparse.ArgumentParser(description="Prepare TEKNOFEST competition dataset")
    
    parser.add_argument("--size", type=int, default=500000, help="Number of transactions")
    parser.add_argument("--users", type=int, default=50000, help="Number of synthetic users")
    parser.add_argument("--fraud-ratio", type=float, default=0.03, help="Initial fraud ratio")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, default="data/competition", help="Output directory")
    
    parser.add_argument("--augment", type=str, choices=["none", "smote", "adasyn", "smote_tomek"],
                       default="smote", help="Data augmentation method")
    parser.add_argument("--target-ratio", type=float, default=0.15, 
                       help="Target fraud ratio after augmentation")
    
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set ratio")
    parser.add_argument("--val-size", type=float, default=0.1, help="Validation set ratio")
    
    parser.add_argument("--advanced-features", action="store_true", 
                       help="Use advanced 53-feature extraction")
    parser.add_argument("--no-normalize", action="store_true", help="Skip feature normalization")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("SentinelFlow Dataset Preparation")
    logger.info("=" * 60)
    
    df = generate_dataset(
        n_transactions=args.size,
        n_users=args.users,
        fraud_ratio=args.fraud_ratio,
        seed=args.seed,
    )
    
    X, y = extract_features(df, use_advanced=args.advanced_features)
    
    if args.augment != "none":
        X, y = augment_data(
            X, y,
            method=args.augment,
            target_ratio=args.target_ratio,
            seed=args.seed,
        )
    
    splits = split_dataset(
        X, y,
        test_size=args.test_size,
        val_size=args.val_size,
        seed=args.seed,
    )
    
    scaler = None
    if not args.no_normalize:
        splits, scaler = normalize_features(splits)
    
    metadata = save_dataset(splits, args.output, df, scaler)
    
    logger.info("=" * 60)
    logger.info("Dataset Preparation Complete!")
    logger.info("=" * 60)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()

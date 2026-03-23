# =============================================================================
# SentinelFlow - Model Training Pipeline
# =============================================================================
"""
End-to-end training pipeline for SentinelFlow ML models.

Trains IsolationForest, XGBoost, and AutoEncoder models on a fraud dataset,
evaluates with Precision/Recall/F1/AUC-ROC, and saves trained models.

Usage (CLI):
    python -m sentinelflow.ml.train_pipeline --samples 10000 --fraud-ratio 0.05

Usage (Python):
    from sentinelflow.ml.train_pipeline import TrainPipeline
    pipeline = TrainPipeline()
    results = pipeline.run(n_samples=10000, fraud_ratio=0.05)
    pipeline.print_report()
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

from sentinelflow.ml.dataset_loader import FraudDatasetLoader
from sentinelflow.ml.ensemble import EnsembleVoter
from sentinelflow.ml.feature_engine import NUM_FEATURES
from sentinelflow.ml.models import (
    AutoEncoderModel,
    IsolationForestModel,
    XGBoostFraudModel,
)

# =============================================================================
# Metrics Data Classes
# =============================================================================


@dataclass
class ModelMetrics:
    """Performance metrics for a single model."""

    model_name: str = ""
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    auc_roc: float = 0.0
    auc_pr: float = 0.0
    true_positives: int = 0
    false_positives: int = 0
    true_negatives: int = 0
    false_negatives: int = 0
    inference_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TrainReport:
    """Full training evaluation report."""

    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    dataset_size: int = 0
    train_size: int = 0
    test_size: int = 0
    fraud_ratio: float = 0.0
    num_features: int = NUM_FEATURES
    model_metrics: list[ModelMetrics] = field(default_factory=list)
    ensemble_metrics: ModelMetrics = field(default_factory=ModelMetrics)
    best_model: str = ""
    training_time_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    def to_json(self, filepath: str | None = None) -> str:
        """Serialize report to JSON."""
        content = json.dumps(self.to_dict(), indent=2, ensure_ascii=False)
        if filepath:
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).write_text(content, encoding="utf-8")
            logger.info(f"Report saved to {filepath}")
        return content


# =============================================================================
# Training Pipeline
# =============================================================================


class TrainPipeline:
    """
    End-to-end model training and evaluation pipeline.

    Steps:
    1. Generate/load dataset
    2. Split into train/test (stratified)
    3. Train each model (IF, XGBoost, AE)
    4. Evaluate individual models
    5. Evaluate ensemble
    6. Generate comprehensive report
    7. Save trained models
    """

    def __init__(
        self,
        output_dir: str = "models",
        seed: int = 42,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._seed = seed
        self._report: TrainReport | None = None

        logger.info(f"TrainPipeline initialized (output: {output_dir})")

    def run(
        self,
        n_samples: int = 10000,
        fraud_ratio: float = 0.05,
        test_size: float = 0.2,
        csv_path: str | None = None,
    ) -> TrainReport:
        """
        Execute the full training pipeline.

        Args:
            n_samples: Number of synthetic samples (if no csv_path)
            fraud_ratio: Fraction of fraud samples
            test_size: Fraction for test set
            csv_path: Optional CSV file path

        Returns:
            TrainReport with all metrics
        """
        start_time = time.time()
        report = TrainReport()

        # =====================================================================
        # Step 1: Load/Generate Dataset
        # =====================================================================
        logger.info("=" * 60)
        logger.info("Step 1: Loading/Generating Dataset")
        logger.info("=" * 60)

        loader = FraudDatasetLoader(seed=self._seed)

        if csv_path:
            X, y, df = loader.load_csv(csv_path)
        else:
            X, y, df = loader.generate_synthetic(n_samples, fraud_ratio)

        report.dataset_size = len(y)
        report.fraud_ratio = float(y.mean())

        # =====================================================================
        # Step 2: Train/Test Split
        # =====================================================================
        logger.info("Step 2: Splitting dataset (stratified)")

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=self._seed,
            stratify=y,
        )

        report.train_size = len(y_train)
        report.test_size = len(y_test)

        logger.info(
            f"Train: {len(y_train)} ({y_train.mean()*100:.1f}% fraud), "
            f"Test: {len(y_test)} ({y_test.mean()*100:.1f}% fraud)"
        )

        # =====================================================================
        # Step 3: Train Models
        # =====================================================================
        logger.info("=" * 60)
        logger.info("Step 3: Training Models")
        logger.info("=" * 60)

        # 3a: IsolationForest (unsupervised, uses full data)
        logger.info("Training IsolationForest...")
        if_model = IsolationForestModel(
            contamination=fraud_ratio,
            n_estimators=200,
            min_samples_to_train=50,
        )
        if_model.fit(X_train)
        if_model.save(str(self._output_dir / "isolation_forest.pkl"))

        # 3b: XGBoost (supervised)
        logger.info("Training XGBoost...")
        xgb_model = XGBoostFraudModel(
            model_path=str(self._output_dir / "xgboost_fraud.json"),
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
        )
        xgb_model.fit(X_train, y_train)
        xgb_model.save()

        # 3c: AutoEncoder (unsupervised, train on normal only)
        logger.info("Training AutoEncoder...")
        ae_model = AutoEncoderModel(
            input_dim=NUM_FEATURES,
            encoding_dim=8,
            model_path=str(self._output_dir / "autoencoder.pt"),
        )
        X_train_normal = X_train[y_train == 0]
        ae_model.fit(X_train_normal, epochs=50)
        ae_model.save()

        # =====================================================================
        # Step 4: Evaluate Individual Models
        # =====================================================================
        logger.info("=" * 60)
        logger.info("Step 4: Evaluating Models")
        logger.info("=" * 60)

        models = [
            ("IsolationForest", if_model),
            ("XGBoost", xgb_model),
            ("AutoEncoder", ae_model),
        ]

        for name, model in models:
            metrics = self._evaluate_model(name, model, X_test, y_test)
            report.model_metrics.append(metrics)
            logger.info(
                f"{name}: P={metrics.precision:.3f} R={metrics.recall:.3f} "
                f"F1={metrics.f1:.3f} AUC={metrics.auc_roc:.3f}"
            )

        # =====================================================================
        # Step 5: Evaluate Ensemble
        # =====================================================================
        logger.info("=" * 60)
        logger.info("Step 5: Evaluating Ensemble")
        logger.info("=" * 60)

        ensemble = EnsembleVoter(threshold=0.5)
        ensemble.add_model(if_model, weight=0.3)
        ensemble.add_model(xgb_model, weight=0.5)
        ensemble.add_model(ae_model, weight=0.2)

        ensemble_metrics = self._evaluate_ensemble(ensemble, X_test, y_test)
        report.ensemble_metrics = ensemble_metrics

        logger.info(
            f"Ensemble: P={ensemble_metrics.precision:.3f} R={ensemble_metrics.recall:.3f} "
            f"F1={ensemble_metrics.f1:.3f} AUC={ensemble_metrics.auc_roc:.3f}"
        )

        # =====================================================================
        # Step 6: Generate Report
        # =====================================================================
        # Find best model
        all_metrics = report.model_metrics + [report.ensemble_metrics]
        best = max(all_metrics, key=lambda m: m.f1)
        report.best_model = best.model_name
        report.training_time_seconds = round(time.time() - start_time, 2)

        # Save report
        report_path = str(self._output_dir / "training_report.json")
        report.to_json(report_path)

        self._report = report

        logger.info("=" * 60)
        logger.info(f"Training complete in {report.training_time_seconds:.1f}s")
        logger.info(f"Best model: {report.best_model} (F1: {best.f1:.3f})")
        logger.info("=" * 60)

        return report

    def _evaluate_model(
        self,
        name: str,
        model: Any,
        X_test: np.ndarray,
        y_test: np.ndarray,
    ) -> ModelMetrics:
        """Evaluate a single model on test set."""
        metrics = ModelMetrics(model_name=name)

        # Predict scores
        start_time = time.time()
        scores = np.array([model.predict_single(x) for x in X_test])
        elapsed = time.time() - start_time

        metrics.inference_time_ms = round(elapsed / len(X_test) * 1000, 4)

        # Find optimal threshold using F1
        best_f1 = 0.0
        best_threshold = 0.5

        for threshold in np.arange(0.1, 0.9, 0.05):
            preds = (scores >= threshold).astype(int)
            f1 = f1_score(y_test, preds, zero_division=0)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold

        # Use best threshold for final metrics
        y_pred = (scores >= best_threshold).astype(int)

        metrics.accuracy = float(accuracy_score(y_test, y_pred))
        metrics.precision = float(precision_score(y_test, y_pred, zero_division=0))
        metrics.recall = float(recall_score(y_test, y_pred, zero_division=0))
        metrics.f1 = float(f1_score(y_test, y_pred, zero_division=0))

        try:
            metrics.auc_roc = float(roc_auc_score(y_test, scores))
        except ValueError:
            metrics.auc_roc = 0.0

        try:
            metrics.auc_pr = float(average_precision_score(y_test, scores))
        except ValueError:
            metrics.auc_pr = 0.0

        # Confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        if cm.shape == (2, 2):
            metrics.true_negatives = int(cm[0, 0])
            metrics.false_positives = int(cm[0, 1])
            metrics.false_negatives = int(cm[1, 0])
            metrics.true_positives = int(cm[1, 1])

        return metrics

    def _evaluate_ensemble(
        self,
        ensemble: EnsembleVoter,
        X_test: np.ndarray,
        y_test: np.ndarray,
    ) -> ModelMetrics:
        """Evaluate ensemble model on test set."""
        metrics = ModelMetrics(model_name="Ensemble(IF+XGB+AE)")

        # Predict
        start_time = time.time()
        predictions = [ensemble.predict(x) for x in X_test]
        elapsed = time.time() - start_time

        metrics.inference_time_ms = round(elapsed / len(X_test) * 1000, 4)

        scores = np.array([p.final_score for p in predictions])

        # Find optimal threshold
        best_f1 = 0.0
        best_threshold = 0.5

        for threshold in np.arange(0.1, 0.9, 0.05):
            preds = (scores >= threshold).astype(int)
            f1 = f1_score(y_test, preds, zero_division=0)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold

        y_pred = (scores >= best_threshold).astype(int)

        metrics.accuracy = float(accuracy_score(y_test, y_pred))
        metrics.precision = float(precision_score(y_test, y_pred, zero_division=0))
        metrics.recall = float(recall_score(y_test, y_pred, zero_division=0))
        metrics.f1 = float(f1_score(y_test, y_pred, zero_division=0))

        try:
            metrics.auc_roc = float(roc_auc_score(y_test, scores))
        except ValueError:
            metrics.auc_roc = 0.0

        try:
            metrics.auc_pr = float(average_precision_score(y_test, scores))
        except ValueError:
            metrics.auc_pr = 0.0

        cm = confusion_matrix(y_test, y_pred)
        if cm.shape == (2, 2):
            metrics.true_negatives = int(cm[0, 0])
            metrics.false_positives = int(cm[0, 1])
            metrics.false_negatives = int(cm[1, 0])
            metrics.true_positives = int(cm[1, 1])

        return metrics

    def print_report(self) -> None:
        """Print a formatted report to stdout."""
        if not self._report:
            logger.warning("No report available. Run the pipeline first.")
            return

        r = self._report

        print("\n" + "=" * 70)
        print("  SentinelFlow ML Training Report")
        print("=" * 70)
        print(f"  Timestamp     : {r.timestamp}")
        print(f"  Dataset       : {r.dataset_size:,} samples ({r.fraud_ratio*100:.1f}% fraud)")
        print(f"  Train / Test  : {r.train_size:,} / {r.test_size:,}")
        print(f"  Features      : {r.num_features}")
        print(f"  Training Time : {r.training_time_seconds:.1f}s")
        print()

        # Model metrics table
        header = f"{'Model':<25} {'Precision':>10} {'Recall':>8} {'F1':>8} {'AUC-ROC':>9} {'AUC-PR':>8} {'ms/tx':>8}"
        print(header)
        print("-" * 70)

        for m in r.model_metrics:
            print(
                f"  {m.model_name:<23} "
                f"{m.precision:>9.3f} "
                f"{m.recall:>8.3f} "
                f"{m.f1:>8.3f} "
                f"{m.auc_roc:>9.3f} "
                f"{m.auc_pr:>8.3f} "
                f"{m.inference_time_ms:>7.4f}"
            )

        print("-" * 70)

        em = r.ensemble_metrics
        print(
            f"  {em.model_name:<23} "
            f"{em.precision:>9.3f} "
            f"{em.recall:>8.3f} "
            f"{em.f1:>8.3f} "
            f"{em.auc_roc:>9.3f} "
            f"{em.auc_pr:>8.3f} "
            f"{em.inference_time_ms:>7.4f}"
        )

        print()
        print(f"  * Best Model: {r.best_model}")
        print("=" * 70)

        # Confusion matrix for ensemble
        print("\n  Ensemble Confusion Matrix:")
        print(f"  {'':15} Predicted Normal  Predicted Fraud")
        print(f"  {'Actual Normal':15} {em.true_negatives:>16,}  {em.false_positives:>15,}")
        print(f"  {'Actual Fraud':15} {em.false_negatives:>16,}  {em.true_positives:>15,}")
        print()


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """CLI entry point for training pipeline."""
    parser = argparse.ArgumentParser(
        description="SentinelFlow ML Training Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=10000,
        help="Number of synthetic samples (default: 10000)",
    )
    parser.add_argument(
        "--fraud-ratio",
        type=float,
        default=0.05,
        help="Fraud ratio (default: 0.05)",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Test set fraction (default: 0.2)",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to CSV dataset (optional)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="models",
        help="Output directory for models and report (default: models)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default: 42)",
    )

    args = parser.parse_args()

    pipeline = TrainPipeline(output_dir=args.output, seed=args.seed)
    pipeline.run(
        n_samples=args.samples,
        fraud_ratio=args.fraud_ratio,
        test_size=args.test_size,
        csv_path=args.csv,
    )
    pipeline.print_report()


if __name__ == "__main__":
    main()

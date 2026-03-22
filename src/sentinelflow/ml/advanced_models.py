# =============================================================================
# SentinelFlow - Advanced ML Models for TEKNOFEST Competition
# =============================================================================
"""
Gelişmiş ML modelleri - TEKNOFEST birinciliği için optimize edilmiş.

Yeni Modeller:
1. LightGBMFraudModel   - Hızlı gradient boosting, kategorik değişken desteği
2. CatBoostFraudModel   - Kategorik veriler için optimize, Türkiye verisi uyumlu
3. TabNetFraudModel     - Derin öğrenme + attention mekanizması
4. StackingEnsemble     - Meta-learner ile model birleştirme

Hedef: %99.5+ doğruluk oranı (geçen yıl 1.: %99.2)
"""

from __future__ import annotations

import os
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger

from sentinelflow.ml.models import BaseFraudModel

# =============================================================================
# LightGBM
# =============================================================================

try:
    import lightgbm as lgb

    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False
    logger.warning("LightGBM not available, LightGBMFraudModel disabled")

# =============================================================================
# CatBoost
# =============================================================================

try:
    from catboost import CatBoostClassifier, Pool

    HAS_CATBOOST = True
except ImportError:
    HAS_CATBOOST = False
    logger.warning("CatBoost not available, CatBoostFraudModel disabled")

# =============================================================================
# Sklearn
# =============================================================================

try:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
    from sklearn.model_selection import StratifiedKFold
    from sklearn.preprocessing import StandardScaler

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.warning("scikit-learn not available")

# =============================================================================
# Imbalanced Learn
# =============================================================================

try:
    from imblearn.combine import SMOTETomek
    from imblearn.over_sampling import ADASYN, SMOTE
    from imblearn.under_sampling import TomekLinks

    HAS_IMBLEARN = True
except ImportError:
    HAS_IMBLEARN = False
    logger.warning("imbalanced-learn not available, SMOTE disabled")

# =============================================================================
# PyTorch (for Focal Loss and advanced training)
# =============================================================================

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ModelMetrics:
    """Model performans metrikleri."""

    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    auc_roc: float = 0.0
    auc_pr: float = 0.0
    inference_time_ms: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "accuracy": round(self.accuracy, 4),
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "auc_roc": round(self.auc_roc, 4),
            "auc_pr": round(self.auc_pr, 4),
            "inference_time_ms": round(self.inference_time_ms, 4),
        }


# =============================================================================
# LightGBM Model
# =============================================================================


class LightGBMFraudModel(BaseFraudModel):
    """
    LightGBM gradient boosting classifier.

    Avantajlar:
    - XGBoost'tan 10x daha hızlı eğitim
    - Kategorik değişkenleri native destekler
    - Düşük bellek kullanımı
    - DART boosting ile overfitting önleme

    TEKNOFEST için optimize edilmiş hiperparametreler.
    """

    def __init__(
        self,
        model_path: str | None = None,
        n_estimators: int = 500,
        max_depth: int = 8,
        learning_rate: float = 0.05,
        num_leaves: int = 31,
        min_child_samples: int = 20,
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        reg_alpha: float = 0.1,
        reg_lambda: float = 0.1,
        boosting_type: str = "dart",  # gbdt, dart, goss
        class_weight: str = "balanced",
        categorical_features: list[int] | None = None,
    ) -> None:
        self._n_estimators = n_estimators
        self._max_depth = max_depth
        self._learning_rate = learning_rate
        self._num_leaves = num_leaves
        self._min_child_samples = min_child_samples
        self._subsample = subsample
        self._colsample_bytree = colsample_bytree
        self._reg_alpha = reg_alpha
        self._reg_lambda = reg_lambda
        self._boosting_type = boosting_type
        self._class_weight = class_weight
        self._categorical_features = categorical_features or []

        self._model: Any = None
        self._scaler = StandardScaler() if HAS_SKLEARN else None
        self._is_fitted = False
        self._feature_importance: dict[str, float] = {}

        if model_path and os.path.exists(model_path):
            self.load(model_path)

        logger.info(
            f"LightGBMFraudModel initialized "
            f"(n_estimators={n_estimators}, boosting={boosting_type})"
        )

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray | None = None,
        X_val: np.ndarray | None = None,
        y_val: np.ndarray | None = None,
        feature_names: list[str] | None = None,
    ) -> None:
        """
        LightGBM modelini eğit.

        Args:
            X: Özellik matrisi
            y: Etiketler
            X_val: Doğrulama verisi (early stopping için)
            y_val: Doğrulama etiketleri
            feature_names: Özellik isimleri
        """
        if not HAS_LIGHTGBM or y is None:
            logger.warning("LightGBM requires labels for training")
            return

        if len(X) < 100:
            logger.debug(f"LightGBM: Need at least 100 samples, have {len(X)}")
            return

        # Scale non-categorical features
        X_scaled = self._scaler.fit_transform(X) if self._scaler else X

        # Callbacks
        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=100),
        ]

        self._model = lgb.LGBMClassifier(
            n_estimators=self._n_estimators,
            max_depth=self._max_depth,
            learning_rate=self._learning_rate,
            num_leaves=self._num_leaves,
            min_child_samples=self._min_child_samples,
            subsample=self._subsample,
            colsample_bytree=self._colsample_bytree,
            reg_alpha=self._reg_alpha,
            reg_lambda=self._reg_lambda,
            boosting_type=self._boosting_type,
            class_weight=self._class_weight,
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )

        # Validation set for early stopping
        eval_set = None
        if X_val is not None and y_val is not None:
            X_val_scaled = self._scaler.transform(X_val) if self._scaler else X_val
            eval_set = [(X_val_scaled, y_val)]

        self._model.fit(
            X_scaled,
            y,
            eval_set=eval_set,
            callbacks=callbacks if eval_set else None,
        )

        self._is_fitted = True

        # Feature importance
        if feature_names:
            importances = self._model.feature_importances_
            self._feature_importance = dict(zip(feature_names, importances))

        fraud_count = int(y.sum())
        logger.info(
            f"LightGBM trained on {len(X)} samples "
            f"({fraud_count} fraud, {len(X) - fraud_count} normal)"
        )

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Fraud olasılığını döndür."""
        if not self._is_fitted or self._model is None:
            return np.full(len(X), 0.0)

        X_scaled = self._scaler.transform(X) if self._scaler else X
        probs = self._model.predict_proba(X_scaled)[:, 1]
        return probs

    def predict_single(self, features: np.ndarray) -> float:
        """Tek bir işlem için fraud olasılığı."""
        if not self._is_fitted:
            return 0.0
        X = features.reshape(1, -1)
        return float(self.predict_proba(X)[0])

    def save(self, path: str | None = None) -> None:
        """Modeli kaydet."""
        if not self._is_fitted or self._model is None:
            return

        path = path or "models/lightgbm_fraud.txt"
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # LightGBM native format
        self._model.booster_.save_model(path)

        # Scaler'ı ayrı kaydet
        scaler_path = path.replace(".txt", "_scaler.pkl")
        with open(scaler_path, "wb") as f:
            pickle.dump(self._scaler, f)

        logger.info(f"LightGBM model saved to {path}")

    def load(self, path: str) -> None:
        """Modeli yükle."""
        if not HAS_LIGHTGBM:
            return

        try:
            self._model = lgb.Booster(model_file=path)

            # Scaler'ı yükle
            scaler_path = path.replace(".txt", "_scaler.pkl")
            if os.path.exists(scaler_path):
                with open(scaler_path, "rb") as f:
                    self._scaler = pickle.load(f)

            self._is_fitted = True
            logger.info(f"LightGBM model loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load LightGBM model: {e}")

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return "LightGBM"

    @property
    def feature_importance(self) -> dict[str, float]:
        """Özellik önem sıralaması."""
        return self._feature_importance


# =============================================================================
# CatBoost Model
# =============================================================================


class CatBoostFraudModel(BaseFraudModel):
    """
    CatBoost gradient boosting classifier.

    Avantajlar:
    - Kategorik değişkenler için en iyi performans
    - Türkiye bankacılık verileri için ideal (banka, şehir, sektör)
    - Otomatik sınıf dengeleme
    - GPU desteği

    Özellikle Türkiye'ye özel veriler için optimize edilmiş.
    """

    def __init__(
        self,
        model_path: str | None = None,
        iterations: int = 500,
        depth: int = 8,
        learning_rate: float = 0.05,
        l2_leaf_reg: float = 3.0,
        border_count: int = 254,
        auto_class_weights: str = "Balanced",
        cat_features: list[int | str] | None = None,
        use_gpu: bool = False,
    ) -> None:
        self._iterations = iterations
        self._depth = depth
        self._learning_rate = learning_rate
        self._l2_leaf_reg = l2_leaf_reg
        self._border_count = border_count
        self._auto_class_weights = auto_class_weights
        self._cat_features = cat_features or []
        self._use_gpu = use_gpu

        self._model: CatBoostClassifier | None = None
        self._is_fitted = False
        self._feature_importance: dict[str, float] = {}

        if model_path and os.path.exists(model_path):
            self.load(model_path)

        logger.info(
            f"CatBoostFraudModel initialized "
            f"(iterations={iterations}, depth={depth}, gpu={use_gpu})"
        )

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray | None = None,
        X_val: np.ndarray | None = None,
        y_val: np.ndarray | None = None,
        feature_names: list[str] | None = None,
        cat_features: list[int] | None = None,
    ) -> None:
        """
        CatBoost modelini eğit.

        Args:
            X: Özellik matrisi
            y: Etiketler
            X_val: Doğrulama verisi
            y_val: Doğrulama etiketleri
            feature_names: Özellik isimleri
            cat_features: Kategorik özellik indeksleri
        """
        if not HAS_CATBOOST or y is None:
            logger.warning("CatBoost requires labels for training")
            return

        if len(X) < 100:
            logger.debug(f"CatBoost: Need at least 100 samples, have {len(X)}")
            return

        cat_feats = cat_features or self._cat_features

        self._model = CatBoostClassifier(
            iterations=self._iterations,
            depth=self._depth,
            learning_rate=self._learning_rate,
            l2_leaf_reg=self._l2_leaf_reg,
            border_count=self._border_count,
            auto_class_weights=self._auto_class_weights,
            random_state=42,
            verbose=100,
            task_type="GPU" if self._use_gpu else "CPU",
            early_stopping_rounds=50,
        )

        # Training pool
        train_pool = Pool(
            data=X,
            label=y,
            cat_features=cat_feats if cat_feats else None,
            feature_names=feature_names,
        )

        # Validation pool
        eval_pool = None
        if X_val is not None and y_val is not None:
            eval_pool = Pool(
                data=X_val,
                label=y_val,
                cat_features=cat_feats if cat_feats else None,
            )

        self._model.fit(train_pool, eval_set=eval_pool)
        self._is_fitted = True

        # Feature importance
        if feature_names:
            importances = self._model.get_feature_importance()
            self._feature_importance = dict(zip(feature_names, importances))

        fraud_count = int(y.sum())
        logger.info(
            f"CatBoost trained on {len(X)} samples "
            f"({fraud_count} fraud, {len(X) - fraud_count} normal)"
        )

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Fraud olasılığını döndür."""
        if not self._is_fitted or self._model is None:
            return np.full(len(X), 0.0)

        probs = self._model.predict_proba(X)[:, 1]
        return probs

    def predict_single(self, features: np.ndarray) -> float:
        """Tek bir işlem için fraud olasılığı."""
        if not self._is_fitted:
            return 0.0
        X = features.reshape(1, -1)
        return float(self.predict_proba(X)[0])

    def save(self, path: str | None = None) -> None:
        """Modeli kaydet."""
        if not self._is_fitted or self._model is None:
            return

        path = path or "models/catboost_fraud.cbm"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._model.save_model(path)
        logger.info(f"CatBoost model saved to {path}")

    def load(self, path: str) -> None:
        """Modeli yükle."""
        if not HAS_CATBOOST:
            return

        try:
            self._model = CatBoostClassifier()
            self._model.load_model(path)
            self._is_fitted = True
            logger.info(f"CatBoost model loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load CatBoost model: {e}")

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def name(self) -> str:
        return "CatBoost"

    @property
    def feature_importance(self) -> dict[str, float]:
        return self._feature_importance


# =============================================================================
# Focal Loss (Imbalanced Data için)
# =============================================================================

if HAS_TORCH:

    class FocalLoss(nn.Module):
        """
        Focal Loss - imbalanced fraud detection için.

        Kolay sınıflandırılan örnekleri down-weight eder,
        zor fraud vakalarına odaklanır.

        Paper: "Focal Loss for Dense Object Detection" (Lin et al., 2017)

        FL(p_t) = -alpha_t * (1 - p_t)^gamma * log(p_t)

        Args:
            alpha: Class weight (default: 0.25 for fraud)
            gamma: Focusing parameter (default: 2.0)
        """

        def __init__(self, alpha: float = 0.25, gamma: float = 2.0):
            super().__init__()
            self.alpha = alpha
            self.gamma = gamma

        def forward(
            self,
            inputs: torch.Tensor,
            targets: torch.Tensor,
        ) -> torch.Tensor:
            """
            Forward pass.

            Args:
                inputs: Model predictions (logits)
                targets: Ground truth labels (0 or 1)

            Returns:
                Focal loss value
            """
            BCE_loss = F.binary_cross_entropy_with_logits(inputs, targets, reduction="none")
            pt = torch.exp(-BCE_loss)

            # Alpha weighting
            alpha_t = self.alpha * targets + (1 - self.alpha) * (1 - targets)

            # Focal term
            focal_weight = alpha_t * (1 - pt) ** self.gamma

            focal_loss = focal_weight * BCE_loss
            return focal_loss.mean()


# =============================================================================
# Stacking Ensemble (Meta-Learner)
# =============================================================================


@dataclass
class StackingPrediction:
    """Stacking ensemble tahmin sonucu."""

    is_fraud: bool = False
    final_score: float = 0.0
    meta_score: float = 0.0
    base_scores: dict[str, float] = field(default_factory=dict)
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_fraud": self.is_fraud,
            "final_score": round(self.final_score, 4),
            "meta_score": round(self.meta_score, 4),
            "base_scores": {k: round(v, 4) for k, v in self.base_scores.items()},
            "confidence": round(self.confidence, 4),
        }


class StackingEnsemble:
    """
    Stacking Ensemble - İki seviyeli model birleştirme.

    Level 1: Base models (IF, XGBoost, LightGBM, CatBoost, AutoEncoder, GNN)
    Level 2: Meta-learner (XGBoost veya LightGBM)

    Geçen yılın birincisini geçmek için tasarlandı.
    Cross-validation ile base model tahminleri üretilir,
    meta-learner bu tahminleri birleştirir.

    Hedef: %99.5+ doğruluk
    """

    def __init__(
        self,
        meta_learner: str = "lightgbm",  # lightgbm, xgboost, logistic
        threshold: float = 0.5,
        cv_folds: int = 5,
        calibrate: bool = True,
    ) -> None:
        self._meta_learner_type = meta_learner
        self._threshold = threshold
        self._cv_folds = cv_folds
        self._calibrate = calibrate

        self._base_models: list[tuple[str, BaseFraudModel, float]] = []
        self._meta_learner: Any = None
        self._meta_scaler = StandardScaler() if HAS_SKLEARN else None
        self._is_fitted = False
        self._metrics: ModelMetrics = ModelMetrics()

        logger.info(f"StackingEnsemble initialized " f"(meta={meta_learner}, cv_folds={cv_folds})")

    def add_base_model(
        self,
        model: BaseFraudModel,
        weight: float = 1.0,
        name: str | None = None,
    ) -> None:
        """
        Base model ekle.

        Args:
            model: BaseFraudModel instance
            weight: Model ağırlığı (meta-features için)
            name: Model ismi (opsiyonel)
        """
        model_name = name or model.name
        self._base_models.append((model_name, model, weight))
        logger.info(f"Added base model '{model_name}' with weight {weight}")

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Stacking ensemble'ı eğit.

        1. Her base model için K-fold CV tahminleri üret
        2. CV tahminlerini stack et (meta-features)
        3. Meta-learner'ı meta-features üzerinde eğit

        Args:
            X: Özellik matrisi
            y: Etiketler
            feature_names: Özellik isimleri

        Returns:
            Training metrics
        """
        if not HAS_SKLEARN:
            logger.error("sklearn required for StackingEnsemble")
            return {}

        if len(self._base_models) == 0:
            logger.error("No base models added!")
            return {}

        logger.info(f"Training StackingEnsemble with {len(self._base_models)} base models...")

        n_samples = len(X)
        n_models = len(self._base_models)

        # Meta-features: Her model için CV tahminleri
        meta_features = np.zeros((n_samples, n_models))

        skf = StratifiedKFold(n_splits=self._cv_folds, shuffle=True, random_state=42)

        # Her base model için CV tahminleri üret
        for i, (name, model, weight) in enumerate(self._base_models):
            logger.info(f"  Training base model {i+1}/{n_models}: {name}")

            cv_predictions = np.zeros(n_samples)

            for _fold, (train_idx, val_idx) in enumerate(skf.split(X, y)):
                X_train, X_val = X[train_idx], X[val_idx]
                y_train, _y_val = y[train_idx], y[val_idx]

                # Model'i eğit
                model.fit(X_train, y_train)

                # Validation tahminleri
                if model.is_ready:
                    preds = model.predict_proba(X_val)
                    cv_predictions[val_idx] = preds

            meta_features[:, i] = cv_predictions * weight

            # Tüm veri üzerinde final eğitim
            model.fit(X, y)

        # Meta-learner eğitimi
        logger.info("Training meta-learner...")

        meta_features_scaled = self._meta_scaler.fit_transform(meta_features)

        if self._meta_learner_type == "lightgbm" and HAS_LIGHTGBM:
            self._meta_learner = lgb.LGBMClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                random_state=42,
                verbose=-1,
            )
        elif self._meta_learner_type == "xgboost":
            import xgboost as xgb

            self._meta_learner = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                random_state=42,
                eval_metric="logloss",
            )
        else:
            from sklearn.linear_model import LogisticRegression

            self._meta_learner = LogisticRegression(
                C=1.0,
                random_state=42,
                max_iter=1000,
            )

        self._meta_learner.fit(meta_features_scaled, y)

        # Calibration (opsiyonel)
        if self._calibrate:
            self._meta_learner = CalibratedClassifierCV(
                self._meta_learner,
                method="isotonic",
                cv=3,
            )
            self._meta_learner.fit(meta_features_scaled, y)

        self._is_fitted = True

        # Eğitim metrikleri
        final_preds = self._meta_learner.predict_proba(meta_features_scaled)[:, 1]
        pred_labels = (final_preds >= self._threshold).astype(int)

        self._metrics = ModelMetrics(
            accuracy=float((pred_labels == y).mean()),
            precision=float(precision_score(y, pred_labels, zero_division=0)),
            recall=float(recall_score(y, pred_labels, zero_division=0)),
            f1=float(f1_score(y, pred_labels, zero_division=0)),
            auc_roc=float(roc_auc_score(y, final_preds)),
        )

        logger.info(f"StackingEnsemble trained - AUC: {self._metrics.auc_roc:.4f}")

        return self._metrics.to_dict()

    def predict(self, features: np.ndarray) -> StackingPrediction:
        """
        Tek bir örnek için tahmin yap.

        Args:
            features: 1D feature vector

        Returns:
            StackingPrediction
        """
        result = StackingPrediction()

        if not self._is_fitted:
            return result

        # Base model tahminleri
        meta_features = []
        for name, model, weight in self._base_models:
            if model.is_ready:
                score = model.predict_single(features)
                result.base_scores[name] = score
                meta_features.append(score * weight)
            else:
                meta_features.append(0.0)

        # Meta-learner tahmini
        meta_array = np.array(meta_features).reshape(1, -1)
        meta_scaled = self._meta_scaler.transform(meta_array)

        meta_proba = self._meta_learner.predict_proba(meta_scaled)[0, 1]
        result.meta_score = float(meta_proba)
        result.final_score = float(meta_proba)
        result.is_fraud = meta_proba >= self._threshold
        result.confidence = abs(meta_proba - 0.5) * 2

        return result

    def predict_batch(self, X: np.ndarray) -> list[StackingPrediction]:
        """Batch tahmin."""
        return [self.predict(X[i]) for i in range(len(X))]

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Fraud olasılıkları."""
        if not self._is_fitted:
            return np.zeros(len(X))

        # Base model tahminleri
        n_samples = len(X)
        n_models = len(self._base_models)
        meta_features = np.zeros((n_samples, n_models))

        for i, (_name, model, weight) in enumerate(self._base_models):
            if model.is_ready:
                preds = model.predict_proba(X)
                meta_features[:, i] = preds * weight

        # Meta-learner tahmini
        meta_scaled = self._meta_scaler.transform(meta_features)
        return self._meta_learner.predict_proba(meta_scaled)[:, 1]

    def save(self, path: str | None = None) -> None:
        """Ensemble'ı kaydet."""
        if not self._is_fitted:
            return

        path = path or "models/stacking_ensemble.pkl"
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        checkpoint = {
            "meta_learner": self._meta_learner,
            "meta_scaler": self._meta_scaler,
            "threshold": self._threshold,
            "metrics": self._metrics.to_dict(),
            "base_model_names": [name for name, _, _ in self._base_models],
        }

        with open(path, "wb") as f:
            pickle.dump(checkpoint, f)

        # Base modelleri ayrı kaydet
        for name, model, _ in self._base_models:
            model_path = str(Path(path).parent / f"{name.lower()}_base.pkl")
            model.save(model_path)

        logger.info(f"StackingEnsemble saved to {path}")

    def load(self, path: str) -> None:
        """Ensemble'ı yükle."""
        try:
            with open(path, "rb") as f:
                checkpoint = pickle.load(f)

            self._meta_learner = checkpoint["meta_learner"]
            self._meta_scaler = checkpoint["meta_scaler"]
            self._threshold = checkpoint["threshold"]
            self._is_fitted = True

            logger.info(f"StackingEnsemble loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load StackingEnsemble: {e}")

    @property
    def is_ready(self) -> bool:
        return self._is_fitted

    @property
    def metrics(self) -> ModelMetrics:
        return self._metrics

    @property
    def threshold(self) -> float:
        return self._threshold

    @threshold.setter
    def threshold(self, value: float) -> None:
        self._threshold = max(0.0, min(1.0, value))


# =============================================================================
# Data Balancing Utilities
# =============================================================================


class DataBalancer:
    """
    Imbalanced fraud data için veri dengeleme.

    Fraud detection'da genelde %1-5 fraud oranı vardır.
    Bu sınıf oversampling ve undersampling teknikleri uygular.
    """

    def __init__(
        self,
        strategy: str = "smote_tomek",  # smote, adasyn, smote_tomek, tomek
        sampling_ratio: float = 0.3,  # Hedef fraud oranı
        random_state: int = 42,
    ) -> None:
        self._strategy = strategy
        self._sampling_ratio = sampling_ratio
        self._random_state = random_state

        logger.info(f"DataBalancer initialized (strategy={strategy})")

    def balance(
        self,
        X: np.ndarray,
        y: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Veri setini dengele.

        Args:
            X: Özellik matrisi
            y: Etiketler

        Returns:
            Dengelenmiş X, y
        """
        if not HAS_IMBLEARN:
            logger.warning("imbalanced-learn not available, returning original data")
            return X, y

        original_ratio = y.mean()
        logger.info(f"Original fraud ratio: {original_ratio:.4f}")

        if self._strategy == "smote":
            sampler = SMOTE(
                sampling_strategy=self._sampling_ratio,
                random_state=self._random_state,
                k_neighbors=5,
            )
        elif self._strategy == "adasyn":
            sampler = ADASYN(
                sampling_strategy=self._sampling_ratio,
                random_state=self._random_state,
            )
        elif self._strategy == "smote_tomek":
            sampler = SMOTETomek(
                random_state=self._random_state,
            )
        elif self._strategy == "tomek":
            sampler = TomekLinks()
        else:
            logger.warning(f"Unknown strategy: {self._strategy}")
            return X, y

        X_balanced, y_balanced = sampler.fit_resample(X, y)

        new_ratio = y_balanced.mean()
        logger.info(
            f"Balanced: {len(X)} -> {len(X_balanced)} samples, "
            f"fraud ratio: {original_ratio:.4f} -> {new_ratio:.4f}"
        )

        return X_balanced, y_balanced


# =============================================================================
# Model Factory
# =============================================================================


def create_competition_ensemble(
    use_lightgbm: bool = True,
    use_catboost: bool = True,
    use_xgboost: bool = True,
    use_isolation_forest: bool = True,
    use_autoencoder: bool = True,
) -> StackingEnsemble:
    """
    TEKNOFEST yarışması için optimize edilmiş ensemble oluştur.

    Returns:
        Yapılandırılmış StackingEnsemble
    """
    from sentinelflow.ml.models import (
        AutoEncoderModel,
        IsolationForestModel,
        XGBoostFraudModel,
    )

    ensemble = StackingEnsemble(
        meta_learner="lightgbm",
        threshold=0.5,
        cv_folds=5,
        calibrate=True,
    )

    if use_isolation_forest:
        ensemble.add_base_model(
            IsolationForestModel(contamination=0.05, n_estimators=200),
            weight=0.15,
            name="IsolationForest",
        )

    if use_xgboost:
        ensemble.add_base_model(
            XGBoostFraudModel(n_estimators=300, max_depth=6),
            weight=0.25,
            name="XGBoost",
        )

    if use_lightgbm and HAS_LIGHTGBM:
        ensemble.add_base_model(
            LightGBMFraudModel(n_estimators=500, boosting_type="dart"),
            weight=0.25,
            name="LightGBM",
        )

    if use_catboost and HAS_CATBOOST:
        ensemble.add_base_model(
            CatBoostFraudModel(iterations=500, depth=8),
            weight=0.20,
            name="CatBoost",
        )

    if use_autoencoder:
        ensemble.add_base_model(
            AutoEncoderModel(input_dim=21, encoding_dim=8),
            weight=0.15,
            name="AutoEncoder",
        )

    logger.info(f"Competition ensemble created with {len(ensemble._base_models)} models")

    return ensemble

# =============================================================================
# SentinelFlow MLOps - Model Registry
# =============================================================================
"""
Model Registry for version control and lifecycle management.

Features:
- Semantic versioning (major.minor.patch)
- Model staging (development, staging, production)
- Model metadata and lineage tracking
- Automatic model comparison
- Rollback support

Compatible with:
- MLflow Model Registry
- DVC (Data Version Control)
- Custom local storage

Usage:
    registry = ModelRegistry()

    # Register a new model
    version = registry.register_model(
        model=trained_model,
        name="fraud_detector",
        metrics={"f1": 0.9952, "auc": 0.9978},
    )

    # Promote to production
    registry.transition_stage(version.version_id, "production")

    # Load production model
    model = registry.load_model("fraud_detector", stage="production")
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import threading

import numpy as np
from loguru import logger


# =============================================================================
# Enums
# =============================================================================


class ModelStage(str, Enum):
    """Model lifecycle stages."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class ModelStatus(str, Enum):
    """Model status."""

    ACTIVE = "active"
    DEPRECATED = "deprecated"
    FAILED = "failed"


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class ModelMetadata:
    """Model metadata for tracking and documentation."""

    # Basic info
    name: str = ""
    version: str = "1.0.0"
    description: str = ""
    author: str = "SentinelFlow Team"

    # Training info
    training_date: str = ""
    training_duration_seconds: float = 0.0
    training_dataset: str = ""
    training_dataset_size: int = 0

    # Model info
    model_type: str = ""
    framework: str = "scikit-learn"
    input_features: List[str] = field(default_factory=list)
    output_type: str = "binary_classification"

    # Performance metrics
    metrics: Dict[str, float] = field(default_factory=dict)

    # Environment
    python_version: str = ""
    dependencies: Dict[str, str] = field(default_factory=dict)

    # Tags
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelMetadata":
        return cls(**data)


@dataclass
class ModelVersion:
    """Represents a specific version of a model."""

    version_id: str = ""
    model_name: str = ""
    version: str = "1.0.0"
    stage: ModelStage = ModelStage.DEVELOPMENT
    status: ModelStatus = ModelStatus.ACTIVE

    # Paths
    model_path: str = ""
    metadata_path: str = ""

    # Timestamps
    created_at: str = ""
    updated_at: str = ""

    # Metadata
    metadata: Optional[ModelMetadata] = None

    # Lineage
    parent_version: Optional[str] = None

    # Hash for integrity
    model_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "version_id": self.version_id,
            "model_name": self.model_name,
            "version": self.version,
            "stage": self.stage.value,
            "status": self.status.value,
            "model_path": self.model_path,
            "metadata_path": self.metadata_path,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "parent_version": self.parent_version,
            "model_hash": self.model_hash,
        }
        if self.metadata:
            data["metadata"] = self.metadata.to_dict()
        return data


# =============================================================================
# Model Registry
# =============================================================================


class ModelRegistry:
    """
    Central model registry for version control and lifecycle management.

    Provides:
    - Model versioning with semantic versioning
    - Stage transitions (dev → staging → production)
    - Model comparison and rollback
    - Metadata and lineage tracking

    Storage Structure:
        registry_path/
        ├── models/
        │   ├── fraud_detector/
        │   │   ├── 1.0.0/
        │   │   │   ├── model.pkl
        │   │   │   └── metadata.json
        │   │   ├── 1.0.1/
        │   │   └── ...
        │   └── ...
        └── registry.json
    """

    def __init__(
        self,
        registry_path: str = "mlops/registry",
        auto_save: bool = True,
    ) -> None:
        """
        Initialize model registry.

        Args:
            registry_path: Path to registry storage
            auto_save: Automatically save registry state
        """
        self._registry_path = Path(registry_path)
        self._models_path = self._registry_path / "models"
        self._registry_file = self._registry_path / "registry.json"
        self._auto_save = auto_save

        # In-memory registry
        self._versions: Dict[str, ModelVersion] = {}
        self._model_names: Dict[str, List[str]] = {}  # model_name -> [version_ids]

        # Thread safety
        self._lock = threading.RLock()

        # Initialize storage
        self._initialize_storage()
        self._load_registry()

        logger.info(f"ModelRegistry initialized at {registry_path}")

    def _initialize_storage(self) -> None:
        """Create storage directories."""
        self._registry_path.mkdir(parents=True, exist_ok=True)
        self._models_path.mkdir(parents=True, exist_ok=True)

    def _load_registry(self) -> None:
        """Load registry state from disk."""
        if self._registry_file.exists():
            try:
                with open(self._registry_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                for version_data in data.get("versions", []):
                    metadata = None
                    if "metadata" in version_data:
                        metadata = ModelMetadata.from_dict(version_data.pop("metadata"))

                    version_data["stage"] = ModelStage(version_data["stage"])
                    version_data["status"] = ModelStatus(version_data["status"])
                    version_data["metadata"] = metadata

                    version = ModelVersion(**version_data)
                    self._versions[version.version_id] = version

                    if version.model_name not in self._model_names:
                        self._model_names[version.model_name] = []
                    self._model_names[version.model_name].append(version.version_id)

                logger.info(f"Loaded {len(self._versions)} model versions from registry")
            except Exception as e:
                logger.error(f"Failed to load registry: {e}")

    def _save_registry(self) -> None:
        """Save registry state to disk."""
        if not self._auto_save:
            return

        data = {
            "versions": [v.to_dict() for v in self._versions.values()],
            "updated_at": datetime.now().isoformat(),
        }

        with open(self._registry_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _generate_version_id(self, model_name: str, version: str) -> str:
        """Generate unique version ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{model_name}_{version}_{timestamp}"

    def _compute_model_hash(self, model: Any) -> str:
        """Compute hash of model for integrity checking."""
        model_bytes = pickle.dumps(model)
        return hashlib.sha256(model_bytes).hexdigest()[:16]

    def _get_next_version(self, model_name: str, bump: str = "patch") -> str:
        """Get next semantic version for a model."""
        versions = self.list_versions(model_name)

        if not versions:
            return "1.0.0"

        # Get latest version
        latest = max(versions, key=lambda v: [int(x) for x in v.version.split(".")])
        major, minor, patch = map(int, latest.version.split("."))

        if bump == "major":
            return f"{major + 1}.0.0"
        elif bump == "minor":
            return f"{major}.{minor + 1}.0"
        else:
            return f"{major}.{minor}.{patch + 1}"

    def register_model(
        self,
        model: Any,
        name: str,
        version: Optional[str] = None,
        metrics: Optional[Dict[str, float]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        input_features: Optional[List[str]] = None,
        training_info: Optional[Dict[str, Any]] = None,
        auto_bump: str = "patch",
    ) -> ModelVersion:
        """
        Register a new model version.

        Args:
            model: Trained model object
            name: Model name
            version: Semantic version (auto-generated if None)
            metrics: Performance metrics
            description: Model description
            tags: Custom tags
            input_features: List of input feature names
            training_info: Training metadata
            auto_bump: Version bump type (major, minor, patch)

        Returns:
            ModelVersion object
        """
        with self._lock:
            # Generate version
            if version is None:
                version = self._get_next_version(name, auto_bump)

            version_id = self._generate_version_id(name, version)

            # Create paths
            model_dir = self._models_path / name / version
            model_dir.mkdir(parents=True, exist_ok=True)

            model_path = model_dir / "model.pkl"
            metadata_path = model_dir / "metadata.json"

            # Save model
            with open(model_path, "wb") as f:
                pickle.dump(model, f)

            # Create metadata
            metadata = ModelMetadata(
                name=name,
                version=version,
                description=description,
                training_date=datetime.now().isoformat(),
                metrics=metrics or {},
                input_features=input_features or [],
                tags=tags or {},
            )

            if training_info:
                metadata.training_duration_seconds = training_info.get("duration", 0)
                metadata.training_dataset = training_info.get("dataset", "")
                metadata.training_dataset_size = training_info.get("dataset_size", 0)

            # Get model type
            metadata.model_type = type(model).__name__

            # Save metadata
            with open(metadata_path, "w", encoding="utf-8") as f:
                f.write(metadata.to_json())

            # Create version object
            model_version = ModelVersion(
                version_id=version_id,
                model_name=name,
                version=version,
                stage=ModelStage.DEVELOPMENT,
                status=ModelStatus.ACTIVE,
                model_path=str(model_path),
                metadata_path=str(metadata_path),
                created_at=datetime.now().isoformat(),
                updated_at=datetime.now().isoformat(),
                metadata=metadata,
                model_hash=self._compute_model_hash(model),
            )

            # Register
            self._versions[version_id] = model_version

            if name not in self._model_names:
                self._model_names[name] = []
            self._model_names[name].append(version_id)

            self._save_registry()

            logger.info(f"Registered model: {name} v{version} (id={version_id})")

            return model_version

    def load_model(
        self,
        name: str,
        version: Optional[str] = None,
        stage: Optional[ModelStage] = None,
    ) -> Any:
        """
        Load a model from registry.

        Args:
            name: Model name
            version: Specific version (latest if None)
            stage: Load from specific stage (e.g., production)

        Returns:
            Loaded model object
        """
        with self._lock:
            versions = self.list_versions(name)

            if not versions:
                raise ValueError(f"No versions found for model: {name}")

            # Filter by stage
            if stage:
                versions = [v for v in versions if v.stage == stage]
                if not versions:
                    raise ValueError(f"No {stage.value} version for model: {name}")

            # Filter by version
            if version:
                versions = [v for v in versions if v.version == version]
                if not versions:
                    raise ValueError(f"Version {version} not found for model: {name}")

            # Get latest
            target = max(versions, key=lambda v: v.created_at)

            # Load model
            with open(target.model_path, "rb") as f:
                model = pickle.load(f)

            logger.info(f"Loaded model: {name} v{target.version} ({target.stage.value})")

            return model

    def transition_stage(
        self,
        version_id: str,
        target_stage: Union[str, ModelStage],
    ) -> ModelVersion:
        """
        Transition a model version to a new stage.

        Args:
            version_id: Version ID
            target_stage: Target stage

        Returns:
            Updated ModelVersion
        """
        with self._lock:
            if version_id not in self._versions:
                raise ValueError(f"Version not found: {version_id}")

            version = self._versions[version_id]

            if isinstance(target_stage, str):
                target_stage = ModelStage(target_stage)

            old_stage = version.stage
            version.stage = target_stage
            version.updated_at = datetime.now().isoformat()

            # If promoting to production, archive old production versions
            if target_stage == ModelStage.PRODUCTION:
                for vid in self._model_names.get(version.model_name, []):
                    v = self._versions[vid]
                    if v.stage == ModelStage.PRODUCTION and vid != version_id:
                        v.stage = ModelStage.ARCHIVED
                        v.updated_at = datetime.now().isoformat()

            self._save_registry()

            logger.info(
                f"Transitioned {version.model_name} v{version.version}: "
                f"{old_stage.value} → {target_stage.value}"
            )

            return version

    def list_versions(
        self,
        model_name: str,
        stage: Optional[ModelStage] = None,
    ) -> List[ModelVersion]:
        """List all versions of a model."""
        with self._lock:
            version_ids = self._model_names.get(model_name, [])
            versions = [self._versions[vid] for vid in version_ids]

            if stage:
                versions = [v for v in versions if v.stage == stage]

            return sorted(versions, key=lambda v: v.created_at, reverse=True)

    def list_models(self) -> List[str]:
        """List all registered model names."""
        return list(self._model_names.keys())

    def get_production_model(self, name: str) -> Optional[ModelVersion]:
        """Get the current production version of a model."""
        versions = self.list_versions(name, stage=ModelStage.PRODUCTION)
        return versions[0] if versions else None

    def compare_versions(
        self,
        version_id_1: str,
        version_id_2: str,
    ) -> Dict[str, Any]:
        """Compare two model versions."""
        v1 = self._versions.get(version_id_1)
        v2 = self._versions.get(version_id_2)

        if not v1 or not v2:
            raise ValueError("Version not found")

        comparison = {
            "version_1": v1.version,
            "version_2": v2.version,
            "metrics_comparison": {},
        }

        if v1.metadata and v2.metadata:
            for metric in set(v1.metadata.metrics.keys()) | set(v2.metadata.metrics.keys()):
                val1 = v1.metadata.metrics.get(metric, 0)
                val2 = v2.metadata.metrics.get(metric, 0)
                comparison["metrics_comparison"][metric] = {
                    "v1": val1,
                    "v2": val2,
                    "diff": val2 - val1,
                    "improvement": val2 > val1,
                }

        return comparison

    def rollback(self, model_name: str) -> Optional[ModelVersion]:
        """
        Rollback to previous production version.

        Returns:
            New production version or None
        """
        with self._lock:
            archived = self.list_versions(model_name, stage=ModelStage.ARCHIVED)

            if not archived:
                logger.warning(f"No archived versions to rollback for: {model_name}")
                return None

            # Get most recent archived
            prev_version = max(archived, key=lambda v: v.updated_at)

            # Demote current production
            current_prod = self.get_production_model(model_name)
            if current_prod:
                self.transition_stage(current_prod.version_id, ModelStage.ARCHIVED)

            # Promote archived to production
            return self.transition_stage(prev_version.version_id, ModelStage.PRODUCTION)

    def delete_version(self, version_id: str, force: bool = False) -> bool:
        """Delete a model version."""
        with self._lock:
            if version_id not in self._versions:
                return False

            version = self._versions[version_id]

            # Don't delete production unless forced
            if version.stage == ModelStage.PRODUCTION and not force:
                raise ValueError("Cannot delete production model without force=True")

            # Remove files
            model_dir = Path(version.model_path).parent
            if model_dir.exists():
                shutil.rmtree(model_dir)

            # Remove from registry
            del self._versions[version_id]
            self._model_names[version.model_name].remove(version_id)

            self._save_registry()

            logger.info(f"Deleted model version: {version_id}")

            return True

    def get_version(self, version_id: str) -> Optional[ModelVersion]:
        """Get a specific version."""
        return self._versions.get(version_id)

    def search(
        self,
        model_name: Optional[str] = None,
        stage: Optional[ModelStage] = None,
        tags: Optional[Dict[str, str]] = None,
        min_metric: Optional[Dict[str, float]] = None,
    ) -> List[ModelVersion]:
        """Search for model versions matching criteria."""
        results = list(self._versions.values())

        if model_name:
            results = [v for v in results if v.model_name == model_name]

        if stage:
            results = [v for v in results if v.stage == stage]

        if tags and results:
            results = [
                v
                for v in results
                if v.metadata and all(v.metadata.tags.get(k) == val for k, val in tags.items())
            ]

        if min_metric and results:
            results = [
                v
                for v in results
                if v.metadata
                and all(v.metadata.metrics.get(k, 0) >= val for k, val in min_metric.items())
            ]

        return results

    @property
    def stats(self) -> Dict[str, Any]:
        """Registry statistics."""
        return {
            "total_models": len(self._model_names),
            "total_versions": len(self._versions),
            "by_stage": {
                stage.value: len([v for v in self._versions.values() if v.stage == stage])
                for stage in ModelStage
            },
            "models": list(self._model_names.keys()),
        }

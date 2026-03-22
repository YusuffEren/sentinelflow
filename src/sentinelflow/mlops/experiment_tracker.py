# =============================================================================
# SentinelFlow MLOps - Experiment Tracking
# =============================================================================
"""
Experiment tracking for ML experiments and hyperparameter tuning.

Features:
- MLflow-compatible API
- Experiment and run management
- Metric and parameter logging
- Artifact storage
- Hyperparameter tuning with Optuna

Usage:
    tracker = ExperimentTracker()

    with tracker.start_run(experiment_name="fraud_detection") as run:
        run.log_params({"n_estimators": 100, "max_depth": 10})

        model.fit(X_train, y_train)

        run.log_metrics({"f1": 0.95, "auc": 0.98})
        run.log_artifact(model, "model.pkl")

    # Hyperparameter tuning
    best_params = tracker.tune_hyperparameters(
        model_class=XGBClassifier,
        param_space={...},
        X_train=X_train,
        y_train=y_train,
        n_trials=50,
    )
"""

from __future__ import annotations

import json
import pickle
import threading
import time
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger
from sklearn.model_selection import cross_val_score

try:
    import optuna

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False


# =============================================================================
# Enums
# =============================================================================


class RunStatus(str, Enum):
    """Run status."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class Run:
    """Represents a single experiment run."""

    run_id: str = ""
    experiment_name: str = ""
    status: RunStatus = RunStatus.RUNNING

    # Timing
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0.0

    # Logged data
    params: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    metric_history: dict[str, list[tuple]] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    artifacts: dict[str, str] = field(default_factory=dict)

    # Metadata
    notes: str = ""

    # Internal
    _tracker: ExperimentTracker | None = field(default=None, repr=False)
    _run_dir: Path | None = field(default=None, repr=False)

    def log_param(self, key: str, value: Any) -> None:
        """Log a single parameter."""
        self.params[key] = value
        logger.debug(f"Logged param: {key}={value}")

    def log_params(self, params: dict[str, Any]) -> None:
        """Log multiple parameters."""
        self.params.update(params)
        logger.debug(f"Logged {len(params)} params")

    def log_metric(
        self,
        key: str,
        value: float,
        step: int | None = None,
    ) -> None:
        """Log a single metric."""
        self.metrics[key] = value

        # Track history
        if key not in self.metric_history:
            self.metric_history[key] = []

        timestamp = time.time()
        self.metric_history[key].append((step or len(self.metric_history[key]), value, timestamp))

        logger.debug(f"Logged metric: {key}={value}")

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
    ) -> None:
        """Log multiple metrics."""
        for key, value in metrics.items():
            self.log_metric(key, value, step)

    def log_artifact(
        self,
        artifact: Any,
        filename: str,
        artifact_type: str = "pickle",
    ) -> str:
        """
        Log an artifact (model, data, etc.).

        Returns:
            Path to saved artifact
        """
        if not self._run_dir:
            raise RuntimeError("Run not properly initialized")

        artifacts_dir = self._run_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        artifact_path = artifacts_dir / filename

        if artifact_type == "pickle":
            with open(artifact_path, "wb") as f:
                pickle.dump(artifact, f)
        elif artifact_type == "json":
            with open(artifact_path, "w", encoding="utf-8") as f:
                json.dump(artifact, f, indent=2, ensure_ascii=False)
        elif artifact_type == "text":
            with open(artifact_path, "w", encoding="utf-8") as f:
                f.write(str(artifact))
        else:
            with open(artifact_path, "wb") as f:
                f.write(artifact)

        self.artifacts[filename] = str(artifact_path)
        logger.debug(f"Logged artifact: {filename}")

        return str(artifact_path)

    def set_tag(self, key: str, value: str) -> None:
        """Set a tag."""
        self.tags[key] = value

    def set_tags(self, tags: dict[str, str]) -> None:
        """Set multiple tags."""
        self.tags.update(tags)

    def add_note(self, note: str) -> None:
        """Add a note to the run."""
        if self.notes:
            self.notes += "\n" + note
        else:
            self.notes = note

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": self.run_id,
            "experiment_name": self.experiment_name,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "params": self.params,
            "metrics": self.metrics,
            "tags": self.tags,
            "artifacts": self.artifacts,
            "notes": self.notes,
        }


@dataclass
class Experiment:
    """Represents an experiment (collection of runs)."""

    experiment_id: str = ""
    name: str = ""
    description: str = ""
    created_at: str = ""

    # Runs
    run_ids: list[str] = field(default_factory=list)

    # Tags
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# =============================================================================
# Experiment Tracker
# =============================================================================


class ExperimentTracker:
    """
    Experiment tracking system for ML experiments.

    Provides:
    - Experiment and run management
    - Parameter and metric logging
    - Artifact storage
    - Hyperparameter tuning integration

    Storage Structure:
        tracking_path/
        ├── experiments/
        │   ├── fraud_detection/
        │   │   ├── experiment.json
        │   │   └── runs/
        │   │       ├── run_001/
        │   │       │   ├── run.json
        │   │       │   └── artifacts/
        │   │       └── ...
        │   └── ...
        └── tracker.json
    """

    def __init__(
        self,
        tracking_path: str = "mlops/experiments",
    ) -> None:
        """
        Initialize experiment tracker.

        Args:
            tracking_path: Path to tracking storage
        """
        self._tracking_path = Path(tracking_path)
        self._experiments_path = self._tracking_path / "experiments"
        self._tracker_file = self._tracking_path / "tracker.json"

        # In-memory state
        self._experiments: dict[str, Experiment] = {}
        self._runs: dict[str, Run] = {}
        self._active_run: Run | None = None

        # Thread safety
        self._lock = threading.RLock()

        # Initialize
        self._initialize_storage()
        self._load_tracker()

        logger.info(f"ExperimentTracker initialized at {tracking_path}")

    def _initialize_storage(self) -> None:
        """Create storage directories."""
        self._tracking_path.mkdir(parents=True, exist_ok=True)
        self._experiments_path.mkdir(parents=True, exist_ok=True)

    def _load_tracker(self) -> None:
        """Load tracker state from disk."""
        if self._tracker_file.exists():
            try:
                with open(self._tracker_file, encoding="utf-8") as f:
                    data = json.load(f)

                for exp_data in data.get("experiments", []):
                    exp = Experiment(**exp_data)
                    self._experiments[exp.experiment_id] = exp

                logger.info(f"Loaded {len(self._experiments)} experiments")
            except Exception as e:
                logger.error(f"Failed to load tracker: {e}")

    def _save_tracker(self) -> None:
        """Save tracker state to disk."""
        data = {
            "experiments": [e.to_dict() for e in self._experiments.values()],
            "updated_at": datetime.now().isoformat(),
        }

        with open(self._tracker_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _save_run(self, run: Run) -> None:
        """Save run to disk."""
        if run._run_dir:
            run_file = run._run_dir / "run.json"
            with open(run_file, "w", encoding="utf-8") as f:
                json.dump(run.to_dict(), f, indent=2, ensure_ascii=False)

    def create_experiment(
        self,
        name: str,
        description: str = "",
        tags: dict[str, str] | None = None,
    ) -> Experiment:
        """Create a new experiment."""
        with self._lock:
            # Check if exists
            existing = self.get_experiment(name)
            if existing:
                return existing

            experiment_id = f"exp_{name}_{datetime.now().strftime('%Y%m%d')}"

            experiment = Experiment(
                experiment_id=experiment_id,
                name=name,
                description=description,
                created_at=datetime.now().isoformat(),
                tags=tags or {},
            )

            # Create directory
            exp_dir = self._experiments_path / name
            exp_dir.mkdir(parents=True, exist_ok=True)
            (exp_dir / "runs").mkdir(exist_ok=True)

            # Save experiment metadata
            with open(exp_dir / "experiment.json", "w", encoding="utf-8") as f:
                json.dump(experiment.to_dict(), f, indent=2, ensure_ascii=False)

            self._experiments[experiment_id] = experiment
            self._save_tracker()

            logger.info(f"Created experiment: {name}")

            return experiment

    def get_experiment(self, name: str) -> Experiment | None:
        """Get experiment by name."""
        for exp in self._experiments.values():
            if exp.name == name:
                return exp
        return None

    @contextmanager
    def start_run(
        self,
        experiment_name: str = "default",
        run_name: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> Generator[Run, None, None]:
        """
        Start a new experiment run.

        Usage:
            with tracker.start_run("fraud_detection") as run:
                run.log_params({"n_estimators": 100})
                run.log_metrics({"f1": 0.95})
        """
        with self._lock:
            # Create experiment if needed
            experiment = self.get_experiment(experiment_name)
            if not experiment:
                experiment = self.create_experiment(experiment_name)

            # Generate run ID
            run_id = (
                run_name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
            )

            # Create run directory
            run_dir = self._experiments_path / experiment_name / "runs" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)

            # Create run
            run = Run(
                run_id=run_id,
                experiment_name=experiment_name,
                status=RunStatus.RUNNING,
                start_time=datetime.now().isoformat(),
                tags=tags or {},
                _tracker=self,
                _run_dir=run_dir,
            )

            self._runs[run_id] = run
            self._active_run = run
            experiment.run_ids.append(run_id)

            logger.info(f"Started run: {run_id}")

        start_time = time.time()

        try:
            yield run
            run.status = RunStatus.COMPLETED
        except Exception as e:
            run.status = RunStatus.FAILED
            run.add_note(f"Error: {str(e)}")
            raise
        finally:
            run.end_time = datetime.now().isoformat()
            run.duration_seconds = time.time() - start_time

            self._save_run(run)
            self._save_tracker()
            self._active_run = None

            logger.info(
                f"Run {run_id} {run.status.value} " f"(duration: {run.duration_seconds:.2f}s)"
            )

    def get_run(self, run_id: str) -> Run | None:
        """Get a run by ID."""
        return self._runs.get(run_id)

    def list_runs(
        self,
        experiment_name: str,
        status: RunStatus | None = None,
    ) -> list[Run]:
        """List runs for an experiment."""
        experiment = self.get_experiment(experiment_name)
        if not experiment:
            return []

        runs = [self._runs[rid] for rid in experiment.run_ids if rid in self._runs]

        if status:
            runs = [r for r in runs if r.status == status]

        return sorted(runs, key=lambda r: r.start_time, reverse=True)

    def get_best_run(
        self,
        experiment_name: str,
        metric: str,
        mode: str = "max",
    ) -> Run | None:
        """
        Get the best run for an experiment based on a metric.

        Args:
            experiment_name: Experiment name
            metric: Metric to compare
            mode: "max" or "min"
        """
        runs = self.list_runs(experiment_name, status=RunStatus.COMPLETED)

        if not runs:
            return None

        # Filter runs with the metric
        runs = [r for r in runs if metric in r.metrics]

        if not runs:
            return None

        if mode == "max":
            return max(runs, key=lambda r: r.metrics[metric])
        else:
            return min(runs, key=lambda r: r.metrics[metric])

    def compare_runs(
        self,
        run_ids: list[str],
        metrics: list[str] | None = None,
    ) -> dict[str, Any]:
        """Compare multiple runs."""
        comparison = {
            "runs": [],
            "params": {},
            "metrics": {},
        }

        all_params = set()
        all_metrics = set()

        for run_id in run_ids:
            run = self._runs.get(run_id)
            if run:
                comparison["runs"].append(run.to_dict())
                all_params.update(run.params.keys())
                all_metrics.update(run.metrics.keys())

        # Compare params
        for param in all_params:
            values = [self._runs[rid].params.get(param) for rid in run_ids if rid in self._runs]
            comparison["params"][param] = values

        # Compare metrics
        target_metrics = metrics or list(all_metrics)
        for metric in target_metrics:
            values = [self._runs[rid].metrics.get(metric) for rid in run_ids if rid in self._runs]
            comparison["metrics"][metric] = {
                "values": values,
                "best": (
                    max(v for v in values if v is not None)
                    if any(v is not None for v in values)
                    else None
                ),
                "mean": (
                    np.mean([v for v in values if v is not None])
                    if any(v is not None for v in values)
                    else None
                ),
            }

        return comparison

    def tune_hyperparameters(
        self,
        model_class: type,
        param_space: dict[str, Any],
        X_train: np.ndarray,
        y_train: np.ndarray,
        scoring: str = "f1",
        cv: int = 5,
        n_trials: int = 50,
        experiment_name: str = "hyperparameter_tuning",
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Hyperparameter tuning with Optuna.

        Args:
            model_class: Model class to tune
            param_space: Dictionary defining search space
            X_train: Training features
            y_train: Training labels
            scoring: Scoring metric
            cv: Cross-validation folds
            n_trials: Number of trials
            experiment_name: Experiment name for tracking
            timeout: Timeout in seconds

        Returns:
            Best parameters found

        Example param_space:
            {
                "n_estimators": ("int", 50, 500),
                "max_depth": ("int", 3, 15),
                "learning_rate": ("float_log", 0.001, 0.3),
                "subsample": ("float", 0.5, 1.0),
            }
        """
        if not OPTUNA_AVAILABLE:
            raise ImportError("Optuna required for hyperparameter tuning")

        def objective(trial):
            params = {}

            for name, spec in param_space.items():
                if spec[0] == "int":
                    params[name] = trial.suggest_int(name, spec[1], spec[2])
                elif spec[0] == "float":
                    params[name] = trial.suggest_float(name, spec[1], spec[2])
                elif spec[0] == "float_log":
                    params[name] = trial.suggest_float(name, spec[1], spec[2], log=True)
                elif spec[0] == "categorical":
                    params[name] = trial.suggest_categorical(name, spec[1])

            model = model_class(**params)
            scores = cross_val_score(model, X_train, y_train, cv=cv, scoring=scoring)

            return scores.mean()

        # Create study
        study = optuna.create_study(direction="maximize")

        logger.info(f"Starting hyperparameter tuning with {n_trials} trials")

        # Optimize
        study.optimize(objective, n_trials=n_trials, timeout=timeout)

        # Log best results
        with self.start_run(experiment_name=experiment_name, tags={"type": "tuning"}) as run:
            run.log_params(study.best_params)
            run.log_metric("best_score", study.best_value)
            run.log_param("n_trials", n_trials)
            run.log_artifact(study, "optuna_study.pkl")

        logger.info(f"Best score: {study.best_value:.4f}")
        logger.info(f"Best params: {study.best_params}")

        return {
            "best_params": study.best_params,
            "best_score": study.best_value,
            "study": study,
        }

    @property
    def active_run(self) -> Run | None:
        """Get the currently active run."""
        return self._active_run

    @property
    def stats(self) -> dict[str, Any]:
        """Tracker statistics."""
        return {
            "total_experiments": len(self._experiments),
            "total_runs": len(self._runs),
            "by_status": {
                status.value: len([r for r in self._runs.values() if r.status == status])
                for status in RunStatus
            },
            "experiments": [
                {
                    "name": e.name,
                    "run_count": len(e.run_ids),
                }
                for e in self._experiments.values()
            ],
        }

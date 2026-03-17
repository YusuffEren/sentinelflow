# =============================================================================
# SentinelFlow MLOps - A/B Testing Framework
# =============================================================================
"""
A/B Testing framework for safe model deployment.

Features:
- Traffic splitting (percentage-based, user-based)
- Statistical significance testing
- Multi-armed bandit support
- Automatic winner detection
- Shadow mode deployment

Usage:
    ab_manager = ABTestManager()
    
    # Create test
    test = ab_manager.create_test(
        name="fraud_model_v2",
        variants=[
            {"name": "control", "model": model_v1, "traffic": 0.5},
            {"name": "treatment", "model": model_v2, "traffic": 0.5},
        ],
    )
    
    # Route traffic
    variant = ab_manager.get_variant(test.test_id, user_id="user_123")
    prediction = variant.model.predict(features)
    
    # Log outcome
    ab_manager.log_outcome(test.test_id, variant.name, success=True)
    
    # Get results
    results = ab_manager.get_results(test.test_id)
"""

from __future__ import annotations

import hashlib
import json
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
from scipy import stats
from loguru import logger


# =============================================================================
# Enums
# =============================================================================

class TestStatus(str, Enum):
    """Test status."""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    TERMINATED = "terminated"


class SplitStrategy(str, Enum):
    """Traffic splitting strategy."""
    PERCENTAGE = "percentage"
    USER_HASH = "user_hash"
    RANDOM = "random"
    MULTI_ARMED_BANDIT = "multi_armed_bandit"


class TestResult(str, Enum):
    """Test result."""
    INCONCLUSIVE = "inconclusive"
    CONTROL_WINS = "control_wins"
    TREATMENT_WINS = "treatment_wins"
    NO_DIFFERENCE = "no_difference"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class Variant:
    """Test variant (model version)."""
    
    name: str
    model: Any = None
    traffic_percentage: float = 0.5
    
    # Metrics
    total_samples: int = 0
    successes: int = 0
    failures: int = 0
    
    # Performance metrics
    metrics: Dict[str, List[float]] = field(default_factory=dict)
    
    # Metadata
    model_version: str = ""
    description: str = ""
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_samples == 0:
            return 0.0
        return self.successes / self.total_samples
    
    @property
    def conversion_rate(self) -> float:
        """Alias for success rate."""
        return self.success_rate
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "traffic_percentage": self.traffic_percentage,
            "total_samples": self.total_samples,
            "successes": self.successes,
            "failures": self.failures,
            "success_rate": self.success_rate,
            "model_version": self.model_version,
            "description": self.description,
            "metrics": {k: len(v) for k, v in self.metrics.items()},
        }


@dataclass
class ABTest:
    """A/B test configuration."""
    
    test_id: str = ""
    name: str = ""
    description: str = ""
    
    # Variants
    variants: List[Variant] = field(default_factory=list)
    control_variant: str = ""  # Name of control variant
    
    # Configuration
    split_strategy: SplitStrategy = SplitStrategy.USER_HASH
    status: TestStatus = TestStatus.DRAFT
    
    # Statistical settings
    confidence_level: float = 0.95
    min_sample_size: int = 1000
    min_effect_size: float = 0.02  # 2% minimum detectable effect
    
    # Timing
    created_at: str = ""
    started_at: str = ""
    ended_at: str = ""
    
    # Auto-termination
    auto_terminate: bool = True
    max_duration_hours: int = 168  # 7 days
    
    # Tags
    tags: Dict[str, str] = field(default_factory=dict)
    
    def get_variant(self, name: str) -> Optional[Variant]:
        """Get variant by name."""
        for v in self.variants:
            if v.name == name:
                return v
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_id": self.test_id,
            "name": self.name,
            "description": self.description,
            "variants": [v.to_dict() for v in self.variants],
            "control_variant": self.control_variant,
            "split_strategy": self.split_strategy.value,
            "status": self.status.value,
            "confidence_level": self.confidence_level,
            "min_sample_size": self.min_sample_size,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "tags": self.tags,
        }


@dataclass
class ABTestResult:
    """A/B test results with statistical analysis."""
    
    test_id: str = ""
    timestamp: str = ""
    
    # Status
    result: TestResult = TestResult.INCONCLUSIVE
    is_significant: bool = False
    
    # Statistics
    p_value: float = 1.0
    confidence_interval: Tuple[float, float] = (0.0, 0.0)
    effect_size: float = 0.0
    
    # Variant metrics
    control_rate: float = 0.0
    treatment_rate: float = 0.0
    relative_improvement: float = 0.0
    
    # Sample sizes
    control_samples: int = 0
    treatment_samples: int = 0
    
    # Power analysis
    current_power: float = 0.0
    samples_needed: int = 0
    
    # Recommendations
    recommendation: str = ""
    can_conclude: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_id": self.test_id,
            "timestamp": self.timestamp,
            "result": self.result.value,
            "is_significant": self.is_significant,
            "p_value": self.p_value,
            "confidence_interval": list(self.confidence_interval),
            "effect_size": self.effect_size,
            "control_rate": self.control_rate,
            "treatment_rate": self.treatment_rate,
            "relative_improvement": self.relative_improvement,
            "control_samples": self.control_samples,
            "treatment_samples": self.treatment_samples,
            "current_power": self.current_power,
            "samples_needed": self.samples_needed,
            "recommendation": self.recommendation,
            "can_conclude": self.can_conclude,
        }


# =============================================================================
# A/B Test Manager
# =============================================================================

class ABTestManager:
    """
    A/B Testing manager for safe model deployment.
    
    Provides:
    - Test creation and management
    - Traffic routing
    - Statistical analysis
    - Automatic winner detection
    
    Storage Structure:
        ab_testing_path/
        ├── tests/
        │   ├── test_001/
        │   │   ├── config.json
        │   │   └── outcomes.json
        │   └── ...
        └── manager.json
    """
    
    def __init__(
        self,
        storage_path: str = "mlops/ab_testing",
    ) -> None:
        """
        Initialize A/B test manager.
        
        Args:
            storage_path: Path to test storage
        """
        self._storage_path = Path(storage_path)
        self._tests_path = self._storage_path / "tests"
        self._manager_file = self._storage_path / "manager.json"
        
        # In-memory state
        self._tests: Dict[str, ABTest] = {}
        
        # Multi-armed bandit state
        self._bandit_state: Dict[str, Dict[str, Tuple[int, int]]] = {}  # test_id -> variant -> (successes, trials)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Initialize
        self._initialize_storage()
        self._load_manager()
        
        logger.info(f"ABTestManager initialized at {storage_path}")
    
    def _initialize_storage(self) -> None:
        """Create storage directories."""
        self._storage_path.mkdir(parents=True, exist_ok=True)
        self._tests_path.mkdir(parents=True, exist_ok=True)
    
    def _load_manager(self) -> None:
        """Load manager state from disk."""
        if self._manager_file.exists():
            try:
                with open(self._manager_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                for test_data in data.get("tests", []):
                    variants = [
                        Variant(**v) for v in test_data.pop("variants", [])
                    ]
                    test_data["split_strategy"] = SplitStrategy(test_data["split_strategy"])
                    test_data["status"] = TestStatus(test_data["status"])
                    test_data["variants"] = variants
                    
                    test = ABTest(**test_data)
                    self._tests[test.test_id] = test
                
                logger.info(f"Loaded {len(self._tests)} A/B tests")
            except Exception as e:
                logger.error(f"Failed to load manager: {e}")
    
    def _save_manager(self) -> None:
        """Save manager state to disk."""
        data = {
            "tests": [t.to_dict() for t in self._tests.values()],
            "updated_at": datetime.now().isoformat(),
        }
        
        with open(self._manager_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _generate_test_id(self, name: str) -> str:
        """Generate unique test ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"test_{name}_{timestamp}"
    
    def create_test(
        self,
        name: str,
        variants: List[Dict[str, Any]],
        description: str = "",
        split_strategy: SplitStrategy = SplitStrategy.USER_HASH,
        confidence_level: float = 0.95,
        min_sample_size: int = 1000,
        auto_start: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> ABTest:
        """
        Create a new A/B test.
        
        Args:
            name: Test name
            variants: List of variant configs
            description: Test description
            split_strategy: Traffic splitting strategy
            confidence_level: Statistical confidence level
            min_sample_size: Minimum samples before analysis
            auto_start: Start test immediately
            tags: Custom tags
        
        Returns:
            Created ABTest
        
        Example:
            test = manager.create_test(
                name="fraud_model_v2",
                variants=[
                    {"name": "control", "model": model_v1, "traffic": 0.5},
                    {"name": "treatment", "model": model_v2, "traffic": 0.5},
                ],
            )
        """
        with self._lock:
            test_id = self._generate_test_id(name)
            
            # Create variants
            variant_objects = []
            control_name = None
            
            for i, v in enumerate(variants):
                variant = Variant(
                    name=v.get("name", f"variant_{i}"),
                    model=v.get("model"),
                    traffic_percentage=v.get("traffic", 1.0 / len(variants)),
                    model_version=v.get("version", ""),
                    description=v.get("description", ""),
                )
                variant_objects.append(variant)
                
                if i == 0:
                    control_name = variant.name
            
            test = ABTest(
                test_id=test_id,
                name=name,
                description=description,
                variants=variant_objects,
                control_variant=control_name or "",
                split_strategy=split_strategy,
                confidence_level=confidence_level,
                min_sample_size=min_sample_size,
                created_at=datetime.now().isoformat(),
                tags=tags or {},
            )
            
            if auto_start:
                test.status = TestStatus.RUNNING
                test.started_at = datetime.now().isoformat()
            
            # Create test directory
            test_path = self._tests_path / test_id
            test_path.mkdir(parents=True, exist_ok=True)
            
            self._tests[test_id] = test
            self._save_manager()
            
            logger.info(f"Created A/B test: {name} (id={test_id})")
            
            return test
    
    def start_test(self, test_id: str) -> ABTest:
        """Start an A/B test."""
        with self._lock:
            test = self._tests.get(test_id)
            if not test:
                raise ValueError(f"Test not found: {test_id}")
            
            test.status = TestStatus.RUNNING
            test.started_at = datetime.now().isoformat()
            
            self._save_manager()
            
            logger.info(f"Started A/B test: {test.name}")
            
            return test
    
    def stop_test(self, test_id: str, status: TestStatus = TestStatus.COMPLETED) -> ABTest:
        """Stop an A/B test."""
        with self._lock:
            test = self._tests.get(test_id)
            if not test:
                raise ValueError(f"Test not found: {test_id}")
            
            test.status = status
            test.ended_at = datetime.now().isoformat()
            
            self._save_manager()
            
            logger.info(f"Stopped A/B test: {test.name} ({status.value})")
            
            return test
    
    def get_variant(
        self,
        test_id: str,
        user_id: Optional[str] = None,
    ) -> Variant:
        """
        Get variant for a user.
        
        Args:
            test_id: Test ID
            user_id: User identifier for consistent bucketing
        
        Returns:
            Selected Variant
        """
        with self._lock:
            test = self._tests.get(test_id)
            if not test:
                raise ValueError(f"Test not found: {test_id}")
            
            if test.status != TestStatus.RUNNING:
                # Return control if not running
                return test.get_variant(test.control_variant) or test.variants[0]
            
            # Select variant based on strategy
            if test.split_strategy == SplitStrategy.USER_HASH and user_id:
                return self._hash_based_selection(test, user_id)
            elif test.split_strategy == SplitStrategy.MULTI_ARMED_BANDIT:
                return self._bandit_selection(test)
            else:
                return self._random_selection(test)
    
    def _hash_based_selection(self, test: ABTest, user_id: str) -> Variant:
        """Select variant based on user ID hash."""
        hash_input = f"{test.test_id}_{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        bucket = (hash_value % 100) / 100.0
        
        cumulative = 0.0
        for variant in test.variants:
            cumulative += variant.traffic_percentage
            if bucket < cumulative:
                return variant
        
        return test.variants[-1]
    
    def _random_selection(self, test: ABTest) -> Variant:
        """Random variant selection."""
        rand = random.random()
        cumulative = 0.0
        
        for variant in test.variants:
            cumulative += variant.traffic_percentage
            if rand < cumulative:
                return variant
        
        return test.variants[-1]
    
    def _bandit_selection(self, test: ABTest) -> Variant:
        """Thompson Sampling for multi-armed bandit."""
        if test.test_id not in self._bandit_state:
            self._bandit_state[test.test_id] = {
                v.name: (1, 1) for v in test.variants  # (alpha, beta) priors
            }
        
        samples = {}
        for variant in test.variants:
            alpha, beta = self._bandit_state[test.test_id][variant.name]
            samples[variant.name] = np.random.beta(alpha, beta)
        
        best_variant = max(samples, key=samples.get)
        return test.get_variant(best_variant) or test.variants[0]
    
    def log_outcome(
        self,
        test_id: str,
        variant_name: str,
        success: bool,
        metric_name: str = "conversion",
        metric_value: Optional[float] = None,
    ) -> None:
        """
        Log outcome for a variant.
        
        Args:
            test_id: Test ID
            variant_name: Variant name
            success: Whether outcome was successful
            metric_name: Metric name for custom metrics
            metric_value: Metric value
        """
        with self._lock:
            test = self._tests.get(test_id)
            if not test:
                return
            
            variant = test.get_variant(variant_name)
            if not variant:
                return
            
            # Update counts
            variant.total_samples += 1
            if success:
                variant.successes += 1
            else:
                variant.failures += 1
            
            # Log metric
            if metric_value is not None:
                if metric_name not in variant.metrics:
                    variant.metrics[metric_name] = []
                variant.metrics[metric_name].append(metric_value)
            
            # Update bandit state
            if test.split_strategy == SplitStrategy.MULTI_ARMED_BANDIT:
                if test.test_id in self._bandit_state:
                    alpha, beta = self._bandit_state[test.test_id][variant_name]
                    if success:
                        alpha += 1
                    else:
                        beta += 1
                    self._bandit_state[test.test_id][variant_name] = (alpha, beta)
            
            self._save_manager()
    
    def get_results(self, test_id: str) -> ABTestResult:
        """
        Get statistical analysis of test results.
        
        Args:
            test_id: Test ID
        
        Returns:
            ABTestResult with statistical analysis
        """
        test = self._tests.get(test_id)
        if not test:
            raise ValueError(f"Test not found: {test_id}")
        
        result = ABTestResult(
            test_id=test_id,
            timestamp=datetime.now().isoformat(),
        )
        
        # Get control and treatment
        control = test.get_variant(test.control_variant)
        treatment = None
        
        for v in test.variants:
            if v.name != test.control_variant:
                treatment = v
                break
        
        if not control or not treatment:
            return result
        
        result.control_samples = control.total_samples
        result.treatment_samples = treatment.total_samples
        result.control_rate = control.success_rate
        result.treatment_rate = treatment.success_rate
        
        # Calculate relative improvement
        if control.success_rate > 0:
            result.relative_improvement = (
                (treatment.success_rate - control.success_rate) / control.success_rate
            )
        
        result.effect_size = treatment.success_rate - control.success_rate
        
        # Check minimum sample size
        if control.total_samples < test.min_sample_size or treatment.total_samples < test.min_sample_size:
            result.result = TestResult.INCONCLUSIVE
            result.recommendation = (
                f"Need more samples. Control: {control.total_samples}/{test.min_sample_size}, "
                f"Treatment: {treatment.total_samples}/{test.min_sample_size}"
            )
            result.samples_needed = test.min_sample_size - min(control.total_samples, treatment.total_samples)
            return result
        
        # Statistical test (two-proportion z-test)
        n1, p1 = control.total_samples, control.success_rate
        n2, p2 = treatment.total_samples, treatment.success_rate
        
        # Pooled proportion
        p_pool = (control.successes + treatment.successes) / (n1 + n2)
        
        # Standard error
        se = np.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        
        if se > 0:
            z_stat = (p2 - p1) / se
            p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))
            
            result.p_value = float(p_value)
            
            # Confidence interval
            z_critical = stats.norm.ppf(1 - (1 - test.confidence_level) / 2)
            ci_margin = z_critical * se
            result.confidence_interval = (
                float(result.effect_size - ci_margin),
                float(result.effect_size + ci_margin),
            )
            
            # Determine significance
            result.is_significant = p_value < (1 - test.confidence_level)
            
            if result.is_significant:
                if result.effect_size > 0:
                    result.result = TestResult.TREATMENT_WINS
                    result.recommendation = (
                        f"Treatment variant is significantly better. "
                        f"Improvement: {result.relative_improvement*100:.2f}%. "
                        f"Consider deploying treatment."
                    )
                else:
                    result.result = TestResult.CONTROL_WINS
                    result.recommendation = (
                        f"Control variant is significantly better. "
                        f"Keep current model."
                    )
                result.can_conclude = True
            else:
                if abs(result.effect_size) < test.min_effect_size:
                    result.result = TestResult.NO_DIFFERENCE
                    result.recommendation = (
                        "No practical difference between variants. "
                        "Consider other factors for decision."
                    )
                    result.can_conclude = True
                else:
                    result.result = TestResult.INCONCLUSIVE
                    result.recommendation = "Continue test for more data."
        
        return result
    
    def get_test(self, test_id: str) -> Optional[ABTest]:
        """Get test by ID."""
        return self._tests.get(test_id)
    
    def list_tests(
        self,
        status: Optional[TestStatus] = None,
    ) -> List[ABTest]:
        """List all tests."""
        tests = list(self._tests.values())
        
        if status:
            tests = [t for t in tests if t.status == status]
        
        return sorted(tests, key=lambda t: t.created_at, reverse=True)
    
    def delete_test(self, test_id: str) -> bool:
        """Delete a test."""
        with self._lock:
            if test_id not in self._tests:
                return False
            
            import shutil
            
            test_path = self._tests_path / test_id
            if test_path.exists():
                shutil.rmtree(test_path)
            
            del self._tests[test_id]
            self._save_manager()
            
            logger.info(f"Deleted A/B test: {test_id}")
            
            return True
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Manager statistics."""
        return {
            "total_tests": len(self._tests),
            "by_status": {
                status.value: len([t for t in self._tests.values() if t.status == status])
                for status in TestStatus
            },
            "active_tests": [
                {
                    "test_id": t.test_id,
                    "name": t.name,
                    "total_samples": sum(v.total_samples for v in t.variants),
                }
                for t in self._tests.values()
                if t.status == TestStatus.RUNNING
            ],
        }

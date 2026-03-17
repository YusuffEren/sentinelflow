# =============================================================================
# SentinelFlow MLOps - Feature Store
# =============================================================================
"""
Centralized feature store for fraud detection.

Features:
- Feature registration and versioning
- Offline (batch) and online (real-time) serving
- Feature groups and lineage
- Point-in-time correct feature retrieval
- Feature statistics and monitoring

Usage:
    store = FeatureStore()
    
    # Register feature group
    fg = store.create_feature_group(
        name="transaction_features",
        features=[
            Feature("amount_zscore", "float", "Z-score of transaction amount"),
            Feature("tx_velocity_1h", "float", "Transaction count in last hour"),
        ],
    )
    
    # Store features
    store.ingest(fg, features_df)
    
    # Online serving
    features = store.get_online_features(
        feature_group="transaction_features",
        entity_id="user_123",
    )
"""

from __future__ import annotations

import hashlib
import json
import pickle
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from loguru import logger


# =============================================================================
# Enums
# =============================================================================

class FeatureType(str, Enum):
    """Feature data types."""
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    STRING = "string"
    ARRAY = "array"
    EMBEDDING = "embedding"


class FeatureStatus(str, Enum):
    """Feature status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    EXPERIMENTAL = "experimental"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class Feature:
    """Represents a single feature."""
    
    name: str
    dtype: FeatureType = FeatureType.FLOAT
    description: str = ""
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    status: FeatureStatus = FeatureStatus.ACTIVE
    
    # Transformation
    transformation: Optional[str] = None
    
    # Statistics
    mean: Optional[float] = None
    std: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    null_percentage: Optional[float] = None
    
    # Versioning
    version: int = 1
    created_at: str = ""
    updated_at: str = ""
    
    def __post_init__(self):
        if isinstance(self.dtype, str):
            self.dtype = FeatureType(self.dtype)
        if isinstance(self.status, str):
            self.status = FeatureStatus(self.status)
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype.value,
            "description": self.description,
            "tags": self.tags,
            "status": self.status.value,
            "transformation": self.transformation,
            "mean": self.mean,
            "std": self.std,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "null_percentage": self.null_percentage,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class FeatureGroup:
    """A group of related features."""
    
    name: str
    description: str = ""
    
    # Features
    features: List[Feature] = field(default_factory=list)
    feature_names: List[str] = field(default_factory=list)
    
    # Entity (key column)
    entity_column: str = "entity_id"
    timestamp_column: str = "event_timestamp"
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    
    # Versioning
    version: int = 1
    created_at: str = ""
    updated_at: str = ""
    
    # Storage
    online_enabled: bool = True
    offline_enabled: bool = True
    ttl_seconds: int = 86400 * 30  # 30 days default
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at
        if not self.feature_names and self.features:
            self.feature_names = [f.name for f in self.features]
    
    def add_feature(self, feature: Feature) -> None:
        """Add a feature to the group."""
        self.features.append(feature)
        self.feature_names.append(feature.name)
        self.updated_at = datetime.now().isoformat()
    
    def get_feature(self, name: str) -> Optional[Feature]:
        """Get a feature by name."""
        for f in self.features:
            if f.name == name:
                return f
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "features": [f.to_dict() for f in self.features],
            "feature_names": self.feature_names,
            "entity_column": self.entity_column,
            "timestamp_column": self.timestamp_column,
            "tags": self.tags,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "online_enabled": self.online_enabled,
            "offline_enabled": self.offline_enabled,
            "ttl_seconds": self.ttl_seconds,
        }


# =============================================================================
# Feature Store
# =============================================================================

class FeatureStore:
    """
    Centralized feature store for fraud detection.
    
    Provides:
    - Feature group management
    - Online (real-time) feature serving
    - Offline (batch) feature serving
    - Point-in-time correct feature retrieval
    - Feature statistics and monitoring
    
    Storage Structure:
        feature_store_path/
        ├── feature_groups/
        │   ├── transaction_features/
        │   │   ├── metadata.json
        │   │   ├── offline/
        │   │   │   └── data.parquet
        │   │   └── online/
        │   │       └── cache.pkl
        │   └── ...
        └── store.json
    """
    
    def __init__(
        self,
        store_path: str = "mlops/feature_store",
        online_cache_size: int = 100000,
    ) -> None:
        """
        Initialize feature store.
        
        Args:
            store_path: Path to feature store storage
            online_cache_size: Maximum entities in online cache
        """
        self._store_path = Path(store_path)
        self._feature_groups_path = self._store_path / "feature_groups"
        self._store_file = self._store_path / "store.json"
        self._online_cache_size = online_cache_size
        
        # In-memory state
        self._feature_groups: Dict[str, FeatureGroup] = {}
        self._online_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}  # fg_name -> entity_id -> features
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Initialize
        self._initialize_storage()
        self._load_store()
        
        logger.info(f"FeatureStore initialized at {store_path}")
    
    def _initialize_storage(self) -> None:
        """Create storage directories."""
        self._store_path.mkdir(parents=True, exist_ok=True)
        self._feature_groups_path.mkdir(parents=True, exist_ok=True)
    
    def _load_store(self) -> None:
        """Load store state from disk."""
        if self._store_file.exists():
            try:
                with open(self._store_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                for fg_data in data.get("feature_groups", []):
                    features = [Feature(**f) for f in fg_data.pop("features", [])]
                    fg = FeatureGroup(**fg_data, features=features)
                    self._feature_groups[fg.name] = fg
                
                logger.info(f"Loaded {len(self._feature_groups)} feature groups")
            except Exception as e:
                logger.error(f"Failed to load store: {e}")
    
    def _save_store(self) -> None:
        """Save store state to disk."""
        data = {
            "feature_groups": [fg.to_dict() for fg in self._feature_groups.values()],
            "updated_at": datetime.now().isoformat(),
        }
        
        with open(self._store_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def create_feature_group(
        self,
        name: str,
        features: List[Feature],
        description: str = "",
        entity_column: str = "entity_id",
        timestamp_column: str = "event_timestamp",
        tags: Optional[Dict[str, str]] = None,
        online_enabled: bool = True,
        offline_enabled: bool = True,
        ttl_seconds: int = 86400 * 30,
    ) -> FeatureGroup:
        """
        Create a new feature group.
        
        Args:
            name: Feature group name
            features: List of Feature objects
            description: Description
            entity_column: Primary key column name
            timestamp_column: Timestamp column name
            tags: Custom tags
            online_enabled: Enable online serving
            offline_enabled: Enable offline serving
            ttl_seconds: Time-to-live for cached features
        
        Returns:
            Created FeatureGroup
        """
        with self._lock:
            if name in self._feature_groups:
                logger.warning(f"Feature group '{name}' already exists")
                return self._feature_groups[name]
            
            fg = FeatureGroup(
                name=name,
                description=description,
                features=features,
                entity_column=entity_column,
                timestamp_column=timestamp_column,
                tags=tags or {},
                online_enabled=online_enabled,
                offline_enabled=offline_enabled,
                ttl_seconds=ttl_seconds,
            )
            
            # Create directories
            fg_path = self._feature_groups_path / name
            fg_path.mkdir(parents=True, exist_ok=True)
            (fg_path / "offline").mkdir(exist_ok=True)
            (fg_path / "online").mkdir(exist_ok=True)
            
            # Save metadata
            with open(fg_path / "metadata.json", "w", encoding="utf-8") as f:
                json.dump(fg.to_dict(), f, indent=2, ensure_ascii=False)
            
            self._feature_groups[name] = fg
            self._online_cache[name] = {}
            self._save_store()
            
            logger.info(f"Created feature group: {name} with {len(features)} features")
            
            return fg
    
    def get_feature_group(self, name: str) -> Optional[FeatureGroup]:
        """Get a feature group by name."""
        return self._feature_groups.get(name)
    
    def list_feature_groups(self) -> List[str]:
        """List all feature group names."""
        return list(self._feature_groups.keys())
    
    def ingest(
        self,
        feature_group: Union[str, FeatureGroup],
        data: pd.DataFrame,
        update_statistics: bool = True,
    ) -> int:
        """
        Ingest features into the store.
        
        Args:
            feature_group: Feature group name or object
            data: DataFrame with features
            update_statistics: Update feature statistics
        
        Returns:
            Number of records ingested
        """
        with self._lock:
            if isinstance(feature_group, str):
                fg = self._feature_groups.get(feature_group)
                if not fg:
                    raise ValueError(f"Feature group not found: {feature_group}")
            else:
                fg = feature_group
            
            fg_path = self._feature_groups_path / fg.name
            
            # Validate columns
            required_cols = [fg.entity_column, fg.timestamp_column] + fg.feature_names
            missing = set(required_cols) - set(data.columns)
            if missing:
                raise ValueError(f"Missing columns: {missing}")
            
            # Update statistics
            if update_statistics:
                self._update_feature_statistics(fg, data)
            
            # Offline storage (append or create)
            if fg.offline_enabled:
                offline_path = fg_path / "offline" / "data.parquet"
                
                if offline_path.exists():
                    existing = pd.read_parquet(offline_path)
                    data = pd.concat([existing, data], ignore_index=True)
                
                data.to_parquet(offline_path, index=False)
            
            # Online cache
            if fg.online_enabled:
                for _, row in data.iterrows():
                    entity_id = str(row[fg.entity_column])
                    features = {
                        col: row[col] for col in fg.feature_names
                        if col in row
                    }
                    features["_timestamp"] = row[fg.timestamp_column]
                    
                    self._online_cache[fg.name][entity_id] = features
                
                # Limit cache size
                if len(self._online_cache[fg.name]) > self._online_cache_size:
                    # Remove oldest entries
                    sorted_entries = sorted(
                        self._online_cache[fg.name].items(),
                        key=lambda x: x[1].get("_timestamp", ""),
                    )
                    to_remove = len(sorted_entries) - self._online_cache_size
                    for entity_id, _ in sorted_entries[:to_remove]:
                        del self._online_cache[fg.name][entity_id]
            
            logger.debug(f"Ingested {len(data)} records into {fg.name}")
            
            return len(data)
    
    def _update_feature_statistics(self, fg: FeatureGroup, data: pd.DataFrame) -> None:
        """Update feature statistics."""
        for feature in fg.features:
            if feature.name not in data.columns:
                continue
            
            col = data[feature.name]
            
            if feature.dtype in [FeatureType.INT, FeatureType.FLOAT]:
                feature.mean = float(col.mean()) if not col.isna().all() else None
                feature.std = float(col.std()) if not col.isna().all() else None
                feature.min_value = float(col.min()) if not col.isna().all() else None
                feature.max_value = float(col.max()) if not col.isna().all() else None
            
            feature.null_percentage = float(col.isna().sum() / len(col) * 100)
            feature.updated_at = datetime.now().isoformat()
    
    def get_online_features(
        self,
        feature_group: str,
        entity_id: str,
        feature_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get features for online serving (real-time).
        
        Args:
            feature_group: Feature group name
            entity_id: Entity identifier
            feature_names: Specific features to retrieve (all if None)
        
        Returns:
            Dictionary of feature values
        """
        with self._lock:
            if feature_group not in self._online_cache:
                return {}
            
            entity_features = self._online_cache[feature_group].get(str(entity_id), {})
            
            if feature_names:
                return {k: v for k, v in entity_features.items() if k in feature_names}
            
            return {k: v for k, v in entity_features.items() if not k.startswith("_")}
    
    def get_online_features_batch(
        self,
        feature_group: str,
        entity_ids: List[str],
        feature_names: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get features for multiple entities (batch online serving).
        
        Args:
            feature_group: Feature group name
            entity_ids: List of entity identifiers
            feature_names: Specific features to retrieve
        
        Returns:
            Dictionary mapping entity_id to features
        """
        return {
            entity_id: self.get_online_features(feature_group, entity_id, feature_names)
            for entity_id in entity_ids
        }
    
    def get_offline_features(
        self,
        feature_group: str,
        entity_ids: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Get features for offline serving (batch).
        
        Args:
            feature_group: Feature group name
            entity_ids: Filter by entity IDs
            start_time: Filter by start time
            end_time: Filter by end time
            feature_names: Specific features to retrieve
        
        Returns:
            DataFrame with features
        """
        fg = self._feature_groups.get(feature_group)
        if not fg:
            raise ValueError(f"Feature group not found: {feature_group}")
        
        offline_path = self._feature_groups_path / fg.name / "offline" / "data.parquet"
        
        if not offline_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(offline_path)
        
        # Filter by entity IDs
        if entity_ids:
            df = df[df[fg.entity_column].isin(entity_ids)]
        
        # Filter by time
        if start_time:
            df = df[pd.to_datetime(df[fg.timestamp_column]) >= start_time]
        if end_time:
            df = df[pd.to_datetime(df[fg.timestamp_column]) <= end_time]
        
        # Select columns
        if feature_names:
            cols = [fg.entity_column, fg.timestamp_column] + [
                f for f in feature_names if f in df.columns
            ]
            df = df[cols]
        
        return df
    
    def get_historical_features(
        self,
        feature_group: str,
        entity_df: pd.DataFrame,
        feature_names: Optional[List[str]] = None,
        ttl: Optional[timedelta] = None,
    ) -> pd.DataFrame:
        """
        Point-in-time correct feature retrieval.
        
        Args:
            feature_group: Feature group name
            entity_df: DataFrame with entity_id and event_timestamp columns
            feature_names: Specific features to retrieve
            ttl: Time-to-live for feature validity
        
        Returns:
            DataFrame with point-in-time correct features
        """
        fg = self._feature_groups.get(feature_group)
        if not fg:
            raise ValueError(f"Feature group not found: {feature_group}")
        
        # Get all offline features
        features_df = self.get_offline_features(feature_group, feature_names=feature_names)
        
        if features_df.empty:
            return entity_df.copy()
        
        # Ensure datetime types
        entity_df = entity_df.copy()
        entity_df[fg.timestamp_column] = pd.to_datetime(entity_df[fg.timestamp_column])
        features_df[fg.timestamp_column] = pd.to_datetime(features_df[fg.timestamp_column])
        
        # Sort by timestamp
        features_df = features_df.sort_values(fg.timestamp_column)
        
        # Point-in-time join
        result = pd.merge_asof(
            entity_df.sort_values(fg.timestamp_column),
            features_df,
            on=fg.timestamp_column,
            by=fg.entity_column,
            direction="backward",
            tolerance=ttl if ttl else None,
        )
        
        return result
    
    def delete_feature_group(self, name: str) -> bool:
        """Delete a feature group."""
        with self._lock:
            if name not in self._feature_groups:
                return False
            
            import shutil
            
            fg_path = self._feature_groups_path / name
            if fg_path.exists():
                shutil.rmtree(fg_path)
            
            del self._feature_groups[name]
            if name in self._online_cache:
                del self._online_cache[name]
            
            self._save_store()
            
            logger.info(f"Deleted feature group: {name}")
            
            return True
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Feature store statistics."""
        return {
            "total_feature_groups": len(self._feature_groups),
            "total_features": sum(
                len(fg.features) for fg in self._feature_groups.values()
            ),
            "online_cache_size": sum(
                len(cache) for cache in self._online_cache.values()
            ),
            "feature_groups": [
                {
                    "name": fg.name,
                    "feature_count": len(fg.features),
                    "online_entities": len(self._online_cache.get(fg.name, {})),
                }
                for fg in self._feature_groups.values()
            ],
        }


# =============================================================================
# Pre-built Feature Groups for Fraud Detection
# =============================================================================

def create_fraud_detection_feature_groups(store: FeatureStore) -> None:
    """Create pre-defined feature groups for fraud detection."""
    
    # Transaction features
    store.create_feature_group(
        name="transaction_features",
        description="Real-time transaction features",
        features=[
            Feature("amount_zscore", FeatureType.FLOAT, "Z-score of transaction amount"),
            Feature("tx_velocity_1h", FeatureType.FLOAT, "Transactions in last hour"),
            Feature("tx_velocity_24h", FeatureType.FLOAT, "Transactions in last 24 hours"),
            Feature("unique_receivers_1d", FeatureType.INT, "Unique receivers in last day"),
            Feature("is_new_receiver", FeatureType.BOOL, "First time sending to receiver"),
            Feature("time_since_last_tx", FeatureType.FLOAT, "Seconds since last transaction"),
            Feature("amount_to_avg_ratio", FeatureType.FLOAT, "Amount / average amount"),
        ],
        entity_column="user_id",
        timestamp_column="event_timestamp",
        tags={"domain": "fraud", "type": "realtime"},
    )
    
    # User behavior features
    store.create_feature_group(
        name="user_behavior_features",
        description="User behavioral patterns",
        features=[
            Feature("login_hour_consistency", FeatureType.FLOAT, "Hour consistency score"),
            Feature("device_change_frequency", FeatureType.FLOAT, "Device changes per week"),
            Feature("ip_entropy", FeatureType.FLOAT, "IP address entropy"),
            Feature("channel_distribution", FeatureType.ARRAY, "Channel usage distribution"),
            Feature("typical_tx_hour", FeatureType.FLOAT, "Most common transaction hour"),
            Feature("weekend_tx_ratio", FeatureType.FLOAT, "Weekend transaction ratio"),
        ],
        entity_column="user_id",
        timestamp_column="event_timestamp",
        tags={"domain": "fraud", "type": "behavioral"},
    )
    
    # Network features
    store.create_feature_group(
        name="network_features",
        description="Graph-based network features",
        features=[
            Feature("pagerank", FeatureType.FLOAT, "PageRank centrality"),
            Feature("out_degree", FeatureType.INT, "Number of outgoing connections"),
            Feature("in_degree", FeatureType.INT, "Number of incoming connections"),
            Feature("clustering_coefficient", FeatureType.FLOAT, "Local clustering"),
            Feature("hub_score", FeatureType.FLOAT, "Hub detection score"),
            Feature("community_id", FeatureType.INT, "Community membership"),
        ],
        entity_column="user_id",
        timestamp_column="event_timestamp",
        tags={"domain": "fraud", "type": "graph"},
    )
    
    logger.info("Created fraud detection feature groups")

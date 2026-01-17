# =============================================================================
# SentinelFlow Settings Configuration
# =============================================================================
"""
Centralized configuration management using Pydantic Settings.

This module provides type-safe configuration loaded from environment variables
and .env files, following the 12-factor app methodology.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka connection and topic configuration."""
    
    model_config = SettingsConfigDict(env_prefix="KAFKA_")
    
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker addresses"
    )
    topic_transactions: str = Field(
        default="transactions",
        description="Topic for incoming transactions"
    )
    topic_alerts: str = Field(
        default="alerts",
        description="Topic for fraud alerts"
    )
    consumer_group: str = Field(
        default="sentinelflow-consumers",
        description="Consumer group ID"
    )


class Neo4jSettings(BaseSettings):
    """Neo4j graph database configuration."""
    
    model_config = SettingsConfigDict(env_prefix="NEO4J_")
    
    uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j Bolt protocol URI"
    )
    user: str = Field(
        default="neo4j",
        description="Neo4j username"
    )
    password: str = Field(
        default="sentinelflow_secret_2024",
        description="Neo4j password"
    )
    database: str = Field(
        default="neo4j",
        description="Neo4j database name"
    )


class RedisSettings(BaseSettings):
    """Redis cache and geo configuration."""
    
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    
    @property
    def url(self) -> str:
        """Construct Redis URL."""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class FraudDetectionSettings(BaseSettings):
    """Fraud detection thresholds and parameters."""
    
    # Circular transaction detection
    circular_min_depth: int = Field(
        default=3,
        ge=2,
        description="Minimum cycle length to detect"
    )
    circular_max_depth: int = Field(
        default=6,
        le=10,
        description="Maximum cycle length to search"
    )
    
    # Impossible travel detection
    max_travel_speed_kmh: float = Field(
        default=900.0,
        description="Maximum plausible travel speed (commercial flight ~900 km/h)"
    )
    time_window_minutes: int = Field(
        default=30,
        description="Time window for travel analysis"
    )
    
    # NLP blacklist
    blacklist_keywords: list[str] = Field(
        default_factory=lambda: [
            "betting", "casino", "gambling", "poker",
            "crypto", "bitcoin", "exchange",
            "offshore", "anonymous"
        ],
        description="Keywords to flag in transaction descriptions"
    )
    
    @field_validator("circular_max_depth")
    @classmethod
    def validate_max_depth(cls, v: int, info) -> int:
        """Ensure max_depth >= min_depth."""
        min_depth = info.data.get("circular_min_depth", 3)
        if v < min_depth:
            raise ValueError(f"max_depth ({v}) must be >= min_depth ({min_depth})")
        return v


class GeneratorSettings(BaseSettings):
    """Synthetic data generator configuration."""
    
    model_config = SettingsConfigDict(env_prefix="GENERATOR_")
    
    batch_size: int = Field(default=100, ge=1, description="Transactions per batch")
    fraud_ratio: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Ratio of fraudulent transactions"
    )
    seed: Optional[int] = Field(default=None, description="Random seed for reproducibility")


class Settings(BaseSettings):
    """
    Master settings aggregating all configuration sections.
    
    Usage:
        settings = get_settings()
        print(settings.kafka.bootstrap_servers)
        print(settings.neo4j.uri)
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    # Application metadata
    app_name: str = Field(default="SentinelFlow")
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    
    # Nested settings
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    fraud: FraudDetectionSettings = Field(default_factory=FraudDetectionSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Returns:
        Settings: Application settings loaded from environment.
        
    Note:
        Settings are cached for performance. Call `get_settings.cache_clear()`
        to reload settings if environment changes.
    """
    return Settings()

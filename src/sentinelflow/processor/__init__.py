"""
Processing module for fraud detection engines.

This module contains:
- GraphEngine: Neo4j integration for circular ring detection
- RedisGeoClient: Redis integration for impossible travel detection
- FraudDetectorService: Main Kafka consumer orchestrating all engines
"""

from sentinelflow.processor.graph_engine import GraphEngine, FraudRing, TransactionData
from sentinelflow.processor.redis_geo import RedisGeoClient, UserLocation, haversine_distance
from sentinelflow.processor.detector import FraudDetectorService, FraudAlert, DetectorStats

__all__ = [
    # Graph Engine
    "GraphEngine",
    "FraudRing", 
    "TransactionData",
    # Redis Geo
    "RedisGeoClient",
    "UserLocation",
    "haversine_distance",
    # Detector Service
    "FraudDetectorService",
    "FraudAlert",
    "DetectorStats",
]

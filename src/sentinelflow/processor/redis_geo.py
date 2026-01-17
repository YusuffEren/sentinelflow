# =============================================================================
# SentinelFlow - Redis Geo Client
# =============================================================================
"""
Redis client for geo-spatial operations and user location tracking.

This module provides location tracking for the "Impossible Travel" fraud
detection: storing each user's last known location and timestamp to detect
unrealistic travel speeds.

Example:
    client = RedisGeoClient()
    
    # Store a transaction location
    client.update_user_location("TR123...", "İstanbul", 41.0082, 28.9784)
    
    # Check if travel is possible
    last_loc = client.get_last_location("TR123...")
    if last_loc:
        km_distance = haversine(last_loc.lat, last_loc.lon, new_lat, new_lon)
        hours_elapsed = (now - last_loc.timestamp).total_seconds() / 3600
        speed_kmh = km_distance / hours_elapsed
        if speed_kmh > 900:
            # IMPOSSIBLE TRAVEL DETECTED!
            pass
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import redis
from loguru import logger

from sentinelflow.config import get_settings


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class UserLocation:
    """Represents a user's last known location."""
    
    iban: str
    city: str
    latitude: float
    longitude: float
    timestamp: datetime
    transaction_id: str | None = None
    
    def to_dict(self) -> dict:
        """Serialize to dictionary for Redis storage."""
        return {
            "iban": self.iban,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": self.timestamp.isoformat(),
            "transaction_id": self.transaction_id,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "UserLocation":
        """Deserialize from dictionary."""
        return cls(
            iban=data["iban"],
            city=data["city"],
            latitude=float(data["latitude"]),
            longitude=float(data["longitude"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            transaction_id=data.get("transaction_id"),
        )


# =============================================================================
# Geo Utilities
# =============================================================================

def haversine_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Calculate the great-circle distance between two points on Earth.
    
    Uses the Haversine formula to compute distance in kilometers.
    
    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)
    
    Returns:
        Distance in kilometers
    """
    from math import radians, sin, cos, sqrt, atan2
    
    # Earth's radius in kilometers
    R = 6371.0
    
    # Convert to radians
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    
    # Haversine formula
    a = sin(delta_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c


# City coordinates lookup (for cities we know about)
CITY_COORDINATES: dict[str, tuple[float, float]] = {
    # Turkish cities
    "İstanbul": (41.0082, 28.9784),
    "Istanbul": (41.0082, 28.9784),
    "Ankara": (39.9334, 32.8597),
    "İzmir": (38.4192, 27.1287),
    "Izmir": (38.4192, 27.1287),
    "Bursa": (40.1885, 29.0610),
    "Antalya": (36.8969, 30.7133),
    "Adana": (37.0000, 35.3213),
    "Konya": (37.8746, 32.4932),
    "Gaziantep": (37.0662, 37.3833),
    "Mersin": (36.8121, 34.6415),
    "Diyarbakır": (37.9144, 40.2306),
    "Diyarbakir": (37.9144, 40.2306),
    "Kayseri": (38.7312, 35.4787),
    "Eskişehir": (39.7767, 30.5206),
    "Eskisehir": (39.7767, 30.5206),
    "Trabzon": (41.0027, 39.7168),
    "Samsun": (41.2867, 36.3300),
    "Denizli": (37.7765, 29.0864),
    # Foreign cities (for impossible travel)
    "Berlin": (52.5200, 13.4050),
    "London": (51.5074, -0.1278),
    "Paris": (48.8566, 2.3522),
    "Dubai": (25.2048, 55.2708),
    "Moscow": (55.7558, 37.6173),
    "New York": (40.7128, -74.0060),
    "Tokyo": (35.6762, 139.6503),
}


def get_city_coordinates(city_name: str) -> tuple[float, float] | None:
    """
    Get coordinates for a city name.
    
    Args:
        city_name: Name of the city
    
    Returns:
        (latitude, longitude) tuple or None if unknown
    """
    return CITY_COORDINATES.get(city_name)


# =============================================================================
# Redis Geo Client
# =============================================================================

class RedisGeoClient:
    """
    Redis client for tracking user locations.
    
    Stores the last known location of each user (by IBAN) for
    impossible travel detection.
    
    Data is stored with a TTL so old entries automatically expire.
    """
    
    # Redis key prefixes
    LOCATION_PREFIX = "sentinelflow:location:"
    GEO_KEY = "sentinelflow:geo:users"
    
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
        password: str | None = None,
        ttl_hours: int = 24,
    ) -> None:
        """
        Initialize Redis connection.
        
        Args:
            host: Redis host (default: from config)
            port: Redis port (default: from config)
            db: Redis database number
            password: Redis password (optional)
            ttl_hours: TTL for location entries in hours
        """
        settings = get_settings()
        
        self._host = host or os.getenv("REDIS_HOST") or settings.redis.host
        self._port = port or int(os.getenv("REDIS_PORT", "0")) or settings.redis.port
        self._db = db
        self._password = password or os.getenv("REDIS_PASSWORD") or settings.redis.password
        self._ttl_seconds = ttl_hours * 3600
        
        self._client: redis.Redis | None = None
        self._connect()
        
        logger.info(f"RedisGeoClient initialized: {self._host}:{self._port}")
    
    def _connect(self) -> None:
        """Establish Redis connection."""
        try:
            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            # Verify connection
            self._client.ping()
            logger.debug("Redis connection verified")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._client = None
            raise
    
    @property
    def is_connected(self) -> bool:
        """Check if Redis is available."""
        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except redis.ConnectionError:
            return False
    
    def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.debug("Redis connection closed")
    
    def update_user_location(
        self,
        iban: str,
        city: str,
        latitude: float,
        longitude: float,
        timestamp: datetime | None = None,
        transaction_id: str | None = None,
    ) -> None:
        """
        Update a user's last known location.
        
        Args:
            iban: User's IBAN (unique identifier)
            city: City name
            latitude: Latitude in degrees
            longitude: Longitude in degrees
            timestamp: Transaction timestamp (default: now)
            transaction_id: Associated transaction ID
        """
        if self._client is None:
            logger.warning("Redis not connected, skipping location update")
            return
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        location = UserLocation(
            iban=iban,
            city=city,
            latitude=latitude,
            longitude=longitude,
            timestamp=timestamp,
            transaction_id=transaction_id,
        )
        
        key = f"{self.LOCATION_PREFIX}{iban}"
        
        try:
            # Store as JSON with TTL
            self._client.setex(
                key,
                self._ttl_seconds,
                json.dumps(location.to_dict()),
            )
            
            # Also store in Redis GEO set for proximity queries
            self._client.geoadd(
                self.GEO_KEY,
                (longitude, latitude, iban),
            )
            
            logger.debug(f"Location updated: {iban[:12]}... -> {city}")
            
        except redis.RedisError as e:
            logger.error(f"Failed to update location: {e}")
    
    def get_last_location(self, iban: str) -> UserLocation | None:
        """
        Get a user's last known location.
        
        Args:
            iban: User's IBAN
        
        Returns:
            UserLocation or None if not found
        """
        if self._client is None:
            logger.warning("Redis not connected")
            return None
        
        key = f"{self.LOCATION_PREFIX}{iban}"
        
        try:
            data = self._client.get(key)
            if data:
                return UserLocation.from_dict(json.loads(data))
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get location: {e}")
            return None
    
    def check_impossible_travel(
        self,
        iban: str,
        new_city: str,
        new_latitude: float,
        new_longitude: float,
        new_timestamp: datetime,
        max_speed_kmh: float = 900.0,
    ) -> tuple[bool, dict | None]:
        """
        Check if travel from last location is physically possible.
        
        Args:
            iban: User's IBAN
            new_city: New transaction city
            new_latitude: New latitude
            new_longitude: New longitude
            new_timestamp: New transaction timestamp
            max_speed_kmh: Maximum realistic speed (default: 900 km/h = fast jet)
        
        Returns:
            (is_impossible, details_dict)
            - is_impossible: True if travel is impossible
            - details: Dict with distance, time, speed info (or None)
        """
        last_location = self.get_last_location(iban)
        
        if last_location is None:
            # First transaction for this user, no prior location
            return False, None
        
        # Calculate distance
        distance_km = haversine_distance(
            last_location.latitude,
            last_location.longitude,
            new_latitude,
            new_longitude,
        )
        
        # Calculate time difference
        time_diff = new_timestamp - last_location.timestamp
        hours_elapsed = time_diff.total_seconds() / 3600
        
        # Avoid division by zero
        if hours_elapsed <= 0:
            hours_elapsed = 0.001  # Assume ~3.6 seconds minimum
        
        # Calculate required speed
        required_speed_kmh = distance_km / hours_elapsed
        
        details = {
            "from_city": last_location.city,
            "to_city": new_city,
            "from_coords": (last_location.latitude, last_location.longitude),
            "to_coords": (new_latitude, new_longitude),
            "distance_km": round(distance_km, 2),
            "time_elapsed_hours": round(hours_elapsed, 4),
            "time_elapsed_minutes": round(hours_elapsed * 60, 2),
            "required_speed_kmh": round(required_speed_kmh, 2),
            "max_allowed_speed_kmh": max_speed_kmh,
            "last_transaction_id": last_location.transaction_id,
        }
        
        # Check if speed exceeds maximum
        is_impossible = required_speed_kmh > max_speed_kmh
        
        if is_impossible:
            logger.warning(
                f"🚨 IMPOSSIBLE TRAVEL: {last_location.city} → {new_city} "
                f"({distance_km:.0f} km in {hours_elapsed * 60:.1f} min = "
                f"{required_speed_kmh:.0f} km/h)"
            )
        
        return is_impossible, details
    
    def clear_all(self) -> None:
        """Clear all location data. USE WITH CAUTION!"""
        if self._client:
            # Delete all location keys
            cursor = 0
            while True:
                cursor, keys = self._client.scan(
                    cursor,
                    match=f"{self.LOCATION_PREFIX}*",
                    count=100,
                )
                if keys:
                    self._client.delete(*keys)
                if cursor == 0:
                    break
            
            # Delete geo set
            self._client.delete(self.GEO_KEY)
            
            logger.warning("All location data cleared!")

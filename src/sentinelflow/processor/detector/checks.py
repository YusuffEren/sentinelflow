# =============================================================================
# SentinelFlow - Fraud Detection Engines
# =============================================================================
"""
Detection stage mixins for FraudDetectorService.

Each _check_* method inspects one transaction with a single engine and returns
a FraudAlert (or None). Mixed into the service class so the methods keep
access to the shared engines/stats via `self`.
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
from loguru import logger

from sentinelflow.ml.feature_engine import TransactionFeatureEngine
from sentinelflow.processor.redis_geo import get_city_coordinates

from .types import BLACKLIST_KEYWORDS, FraudAlert, FraudType


class DetectionChecksMixin:
    """The four fraud detection engines (graph, geo, blacklist, ML)."""

    def _check_circular_ring(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 1: Check for circular transaction rings using Neo4j.

        This detects patterns like: A → B → C → A
        Where money flows in a circle, potentially indicating money laundering.

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if ring detected, None otherwise
        """
        if self._graph_engine is None:
            return None

        try:
            # Step 1: Add transaction to graph
            self._graph_engine.add_transaction(tx_data)

            # Step 2: Check for rings starting from this sender
            rings = self._graph_engine.detect_fraud_rings(
                sender_iban=tx_data.get("sender_iban"),
                min_hops=3,
                max_hops=5,
            )

            if rings:
                ring = rings[0]  # Take the first detected ring

                self.stats.circular_rings += 1

                return FraudAlert(
                    fraud_type=FraudType.CIRCULAR_RING,
                    severity="critical",
                    confidence=0.95,
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=tx_data.get("sender_iban", ""),
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=tx_data.get("amount", 0),
                    description=(
                        f"Circular transaction ring detected: {' → '.join(ring['path'][:4])}..."
                    ),
                    evidence={
                        "ring_id": ring["ring_id"],
                        "ring_path": ring["path"],
                        "total_amount": ring["total_amount"],
                        "transaction_count": ring["transaction_count"],
                    },
                )

        except Exception as e:
            logger.error(f"Graph analysis error: {e}")
            self.stats.errors += 1

        return None

    def _check_impossible_travel(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 2: Check for impossible travel using Redis.

        Detects when a user makes transactions from locations that are
        physically impossible to travel between in the given time.

        Example: İstanbul at 12:00, Berlin at 12:10 (1,500 km in 10 min = 9,000 km/h!)

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if impossible travel detected, None otherwise
        """
        if self._redis_client is None:
            return None

        try:
            sender_iban = tx_data.get("sender_iban", "")
            sender_city = tx_data.get("sender_city", "")

            # Get coordinates for the city
            coords = get_city_coordinates(sender_city)
            if coords is None:
                # Unknown city, cannot check travel
                logger.debug(f"Unknown city: {sender_city}")
                return None

            latitude, longitude = coords

            # Parse timestamp
            timestamp_str = tx_data.get("timestamp", "")
            try:
                if "T" in timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                else:
                    timestamp = datetime.now(timezone.utc)
            except ValueError:
                timestamp = datetime.now(timezone.utc)

            # Check for impossible travel
            is_impossible, details = self._redis_client.check_impossible_travel(
                iban=sender_iban,
                new_city=sender_city,
                new_latitude=latitude,
                new_longitude=longitude,
                new_timestamp=timestamp,
                max_speed_kmh=self.settings.fraud.max_travel_speed_kmh,
            )

            # Update location for future checks
            self._redis_client.update_user_location(
                iban=sender_iban,
                city=sender_city,
                latitude=latitude,
                longitude=longitude,
                timestamp=timestamp,
                transaction_id=tx_data.get("transaction_id"),
            )

            if is_impossible and details:
                self.stats.impossible_travel += 1

                return FraudAlert(
                    fraud_type=FraudType.IMPOSSIBLE_TRAVEL,
                    severity="high",
                    confidence=0.90,
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=sender_iban,
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=tx_data.get("amount", 0),
                    description=(
                        f"Impossible travel detected: {details['from_city']} → {details['to_city']} "
                        f"({details['distance_km']} km in {details['time_elapsed_minutes']} min = "
                        f"{details['required_speed_kmh']} km/h)"
                    ),
                    evidence=details,
                )

        except Exception as e:
            logger.error(f"Geo analysis error: {e}")
            self.stats.errors += 1

        return None

    def _check_blacklist_keywords(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 3: Check for blacklisted keywords in transaction descriptions.

        Flags transactions with suspicious terms like:
        - Gambling: "bahis", "kumar", "casino"
        - Crypto: "bitcoin", "kripto", "usdt"
        - Anonymity: "offshore", "anonim", "gizli"

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if blacklist hit detected, None otherwise
        """
        description = tx_data.get("description", "").lower()

        if not description:
            return None

        # Check for any blacklisted keyword
        found_keywords = [kw for kw in BLACKLIST_KEYWORDS if kw in description]

        if found_keywords:
            self.stats.blacklist_hits += 1

            # Determine severity based on keyword type
            critical_keywords = ["casino", "kumar", "offshore", "anonymous"]
            is_critical = any(kw in critical_keywords for kw in found_keywords)

            return FraudAlert(
                fraud_type=FraudType.BLACKLIST_KEYWORD,
                severity="critical" if is_critical else "medium",
                confidence=0.85,
                transaction_id=tx_data.get("transaction_id", ""),
                sender_iban=tx_data.get("sender_iban", ""),
                sender_name=tx_data.get("sender_name", ""),
                receiver_iban=tx_data.get("receiver_iban", ""),
                receiver_name=tx_data.get("receiver_name", ""),
                amount=tx_data.get("amount", 0),
                description=(
                    f"Suspicious keywords detected in description: {', '.join(found_keywords)}"
                ),
                evidence={
                    "keywords_found": found_keywords,
                    "original_description": tx_data.get("description", ""),
                },
            )

        return None

    def _check_ml_ensemble(self, tx_data: dict) -> FraudAlert | None:
        """
        ENGINE 4: ML Ensemble Fraud Detection.

        Uses multiple ML models (IsolationForest, XGBoost, AutoEncoder) with
        weighted voting to detect anomalous transactions. Provides SHAP-based
        explanations for flagged transactions.

        Pipeline:
        1. Feature Engineering → 21 features from raw transaction
        2. Multi-Model Prediction → Ensemble weighted vote
        3. Explainability → SHAP/heuristic top reasons

        Args:
            tx_data: Transaction data dictionary

        Returns:
            FraudAlert if ensemble flags fraud, None otherwise
        """
        try:
            # Step 1: Extract features
            features_dict = self._feature_engine.extract(tx_data)
            features_vector = np.array(
                [
                    features_dict.get(name, 0.0)
                    for name in TransactionFeatureEngine.get_feature_names()
                ],
                dtype=np.float64,
            )

            # Step 2: Feed to IsolationForest for online learning
            self._isolation_forest_model.add_sample_and_maybe_retrain(features_vector)

            # Step 3: Ensemble prediction
            prediction = self._ensemble.predict(features_vector)

            # Also keep legacy amount buffer for backward compat
            amount = float(tx_data.get("amount", 0.0))
            self._amount_buffer.append(amount)

            if prediction.is_fraud:
                self.stats.ml_ensemble_hits += 1
                self.stats.ai_anomalies += 1

                # Step 4: Generate explanation
                explanation = self._explainer.explain(
                    features=features_vector,
                    feature_values=features_dict,
                )

                # Determine severity based on score
                if prediction.final_score >= 0.85:
                    severity = "critical"
                elif prediction.final_score >= 0.75:
                    severity = "high"
                else:
                    severity = "medium"

                return FraudAlert(
                    fraud_type=FraudType.ML_ENSEMBLE,
                    severity=severity,
                    confidence=min(0.99, prediction.final_score),
                    transaction_id=tx_data.get("transaction_id", ""),
                    sender_iban=tx_data.get("sender_iban", ""),
                    sender_name=tx_data.get("sender_name", ""),
                    receiver_iban=tx_data.get("receiver_iban", ""),
                    receiver_name=tx_data.get("receiver_name", ""),
                    amount=amount,
                    description=(
                        f"ML Ensemble detected fraud (score: {prediction.final_score:.2f}): "
                        f"{explanation.summary()}"
                    ),
                    evidence={
                        **prediction.to_dict(),
                        "xai_explanation": explanation.to_dict(),
                        "features": {k: round(v, 4) for k, v in features_dict.items()},
                    },
                )

        except Exception as e:
            logger.error(f"ML ensemble error: {e}")
            self.stats.errors += 1

        return None

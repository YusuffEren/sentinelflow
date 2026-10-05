# =============================================================================
# SentinelFlow - Integration Tests (require live Redis / Neo4j)
# =============================================================================
"""
Smoke tests against real backing services.

Run with the services up (``docker-compose up -d redis neo4j``):

    pytest tests/ -m integration

Tests skip automatically when the target service is not reachable, so the
plain unit-test run stays green without infrastructure. The CI "Integration
Tests" job provisions Redis + Neo4j and exercises these for real.
"""

import os
import socket
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2.0):
            return True
    except OSError:
        return False


class TestRedisGeo:
    """Impossible-travel detection against a real Redis."""

    @pytest.fixture()
    def geo(self):
        from sentinelflow.processor.redis_geo import RedisGeoClient

        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        if not _reachable(host, port):
            pytest.skip(f"Redis not reachable at {host}:{port}")

        client = RedisGeoClient(host=host, port=port)
        yield client
        client.close()

    def test_impossible_travel_flagged(self, geo):
        # Unique IBAN per run; entries expire via TTL so no cleanup is needed.
        iban = f"TR-ITEST-{uuid4().hex[:12]}"
        t0 = datetime.now(timezone.utc) - timedelta(minutes=10)

        geo.update_user_location(iban, "Istanbul", 41.0082, 28.9784, timestamp=t0)

        impossible, details = geo.check_impossible_travel(
            iban,
            "Tokyo",
            35.6762,
            139.6503,
            new_timestamp=t0 + timedelta(minutes=1),
        )

        assert impossible is True
        assert details is not None
        assert details["required_speed_kmh"] > details["max_allowed_speed_kmh"]

    def test_realistic_travel_not_flagged(self, geo):
        iban = f"TR-ITEST-{uuid4().hex[:12]}"
        t0 = datetime.now(timezone.utc) - timedelta(minutes=10)

        geo.update_user_location(iban, "Ankara", 39.9334, 32.8597, timestamp=t0)

        impossible, details = geo.check_impossible_travel(
            iban,
            "Ankara",
            39.9400,
            32.8600,
            new_timestamp=t0 + timedelta(minutes=30),
        )

        assert impossible is False
        assert details is not None
        assert details["required_speed_kmh"] <= details["max_allowed_speed_kmh"]


class TestGraphEngine:
    """Circular money-ring detection against a real Neo4j."""

    @pytest.fixture()
    def engine(self):
        from sentinelflow.processor.graph_engine import GraphEngine

        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        hostport = uri.split("//", 1)[-1]
        host, _, port = hostport.partition(":")
        if not _reachable(host, int(port or "7687")):
            pytest.skip(f"Neo4j not reachable at {uri}")

        engine = GraphEngine()
        engine.setup_constraints()
        yield engine
        engine.close()

    @staticmethod
    def _txn(sender: str, receiver: str, ts: datetime) -> dict:
        return {
            "transaction_id": str(uuid4()),
            "sender_iban": sender,
            "sender_name": "ITest Sender",
            "sender_city": "Istanbul",
            "receiver_iban": receiver,
            "receiver_name": "ITest Receiver",
            "receiver_city": "Ankara",
            "amount": 10000.0,
            "timestamp": ts.isoformat(),
            "description": "itest flow",
        }

    def test_circular_ring_detected(self, engine):
        ibans = [f"TR{uuid4().hex[:24].upper()}" for _ in range(3)]
        base_time = datetime.now(timezone.utc)

        legs = [(ibans[0], ibans[1]), (ibans[1], ibans[2]), (ibans[2], ibans[0])]
        for hour, (sender, receiver) in enumerate(legs):
            engine.add_transaction(self._txn(sender, receiver, base_time + timedelta(hours=hour)))

        try:
            rings = engine.detect_fraud_rings(sender_iban=ibans[0], min_hops=3, max_hops=3)

            assert rings, "3-hop circular ring A -> B -> C -> A should be detected"
            paths = [set(ring["path"]) for ring in rings]
            assert any(set(ibans) <= path for path in paths)
        finally:
            # Remove only this test's nodes; unique IBANs keep it isolated.
            engine.query(
                "MATCH (u:User) WHERE u.iban IN $ibans DETACH DELETE u",
                {"ibans": ibans},
            )

    def test_linear_flow_has_no_ring(self, engine):
        ibans = [f"TR{uuid4().hex[:24].upper()}" for _ in range(3)]
        base_time = datetime.now(timezone.utc)

        for hour, (sender, receiver) in enumerate(zip(ibans, ibans[1:])):
            engine.add_transaction(self._txn(sender, receiver, base_time + timedelta(hours=hour)))

        try:
            rings = engine.detect_fraud_rings(sender_iban=ibans[0])
            paths = [set(ring["path"]) for ring in rings]
            assert not any(set(ibans) <= path for path in paths)
        finally:
            engine.query(
                "MATCH (u:User) WHERE u.iban IN $ibans DETACH DELETE u",
                {"ibans": ibans},
            )

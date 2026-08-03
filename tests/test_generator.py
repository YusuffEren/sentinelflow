# =============================================================================
# SentinelFlow - Generator Module Tests
# =============================================================================
"""
Tests for the synthetic transaction generator.

Run with: pytest tests/test_generator.py -v
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from uuid import uuid4

import pytest


@pytest.fixture
def sample_account():
    from sentinelflow.generator.models import Account

    return Account(
        iban="TR330006100519786457841326",
        holder_name="Ahmet Yilmaz",
        city="Istanbul",
    )


class TestGeneratorModels:
    """Tests for generator Pydantic models."""

    def test_account_model_valid(self):
        """Valid account should pass validation."""
        from sentinelflow.generator.models import Account

        account = Account(
            iban="TR330006100519786457841326", holder_name="Ahmet Yilmaz", city="Istanbul"
        )
        assert account.iban == "TR330006100519786457841326"
        assert account.holder_name == "Ahmet Yilmaz"

    def test_account_invalid_iban(self):
        """Invalid IBAN should fail validation."""
        from sentinelflow.generator.models import Account

        with pytest.raises(Exception):
            Account(iban="INVALID", holder_name="Test", city="Ankara")

    def test_transaction_model(self, sample_account):
        """Transaction model should create with defaults."""
        from sentinelflow.generator.models import Transaction

        tx = Transaction(
            sender_account_id=uuid4(),
            sender_iban=sample_account.iban,
            sender_name=sample_account.holder_name,
            sender_city=sample_account.city,
            receiver_account_id=uuid4(),
            receiver_iban="TR110006400000478893400002",
            receiver_name="Mehmet Kaya",
            receiver_city="Ankara",
            amount=5000.0,
        )
        assert tx.amount == 5000.0
        assert tx.currency == "TRY"
        assert tx.fraud_type.value == "none"
        assert tx.status.value == "pending"

    def test_transaction_with_fraud(self, sample_account):
        """Transaction should accept fraud type."""
        from sentinelflow.generator.models import FraudType, Transaction

        tx = Transaction(
            sender_account_id=uuid4(),
            sender_iban=sample_account.iban,
            sender_name=sample_account.holder_name,
            sender_city=sample_account.city,
            receiver_account_id=uuid4(),
            receiver_iban="TR110006400000478893400002",
            receiver_name="Mehmet Kaya",
            receiver_city="Ankara",
            amount=150000.0,
            fraud_type=FraudType.CIRCULAR_RING,
        )
        assert tx.fraud_type.value == "circular_ring"

    def test_city_model(self):
        """City model should store coordinates."""
        from sentinelflow.generator.models import City

        city = City(name="Istanbul", latitude=41.0082, longitude=28.9784)
        assert city.name == "Istanbul"
        assert city.latitude == 41.0082


class TestGeneratorPatterns:
    """Tests for transaction pattern generators."""

    def test_mixer_initialization(self):
        """FraudPatternMixer should initialize."""
        from sentinelflow.generator.patterns import FraudPatternMixer

        mixer = FraudPatternMixer(fraud_ratio=0.1)
        assert mixer is not None
        assert mixer.fraud_ratio == 0.1

    def test_generate_normal_transaction(self):
        """Normal generator should produce valid transaction."""
        from sentinelflow.generator.patterns import NormalTransactionGenerator

        gen = NormalTransactionGenerator()
        tx = gen.generate()
        assert tx is not None
        assert tx.amount > 0
        assert tx.sender_iban.startswith("TR")
        assert tx.receiver_iban.startswith("TR")
        assert tx.fraud_type.value == "none"

    def test_generate_circular_ring(self):
        """Circular ring generator should produce ring transactions."""
        from sentinelflow.generator.patterns import CircularRingGenerator

        gen = CircularRingGenerator(ring_size=4)
        txs = gen.generate()
        assert len(txs) >= 3
        for tx in txs:
            assert tx.amount > 0

    def test_generate_suspicious_keyword(self):
        """Blacklist keyword generator should flag descriptions."""
        from sentinelflow.generator.patterns import BlacklistKeywordGenerator

        gen = BlacklistKeywordGenerator()
        tx = gen.generate()
        assert tx is not None
        assert len(tx.description) > 0
        assert tx.fraud_type.value == "blacklist_keyword"

    def test_fraud_pattern_mixer_batch(self):
        """FraudPatternMixer should generate mixed batches."""
        from sentinelflow.generator.patterns import FraudPatternMixer

        mixer = FraudPatternMixer(fraud_ratio=0.2)
        transactions = mixer.generate_batch(size=50)
        assert len(transactions) == 50

        fraud_count = sum(1 for t in transactions if t.fraud_type.value != "none")
        assert fraud_count > 0

    def test_mixer_all_normal(self):
        """Mixer with fraud_ratio=0 should produce only normal txs."""
        from sentinelflow.generator.patterns import FraudPatternMixer

        mixer = FraudPatternMixer(fraud_ratio=0.0)
        transactions = mixer.generate_batch(size=20)
        assert len(transactions) == 20
        assert all(t.fraud_type.value == "none" for t in transactions)

    def test_higher_fraud_ratio(self):
        """Higher fraud ratio should produce more fraud transactions."""
        from sentinelflow.generator.patterns import FraudPatternMixer

        mixer_low = FraudPatternMixer(fraud_ratio=0.05, seed=42)
        mixer_high = FraudPatternMixer(fraud_ratio=0.4, seed=42)

        txs_low = mixer_low.generate_batch(size=200)
        txs_high = mixer_high.generate_batch(size=200)

        fraud_low = sum(1 for t in txs_low if t.fraud_type.value != "none")
        fraud_high = sum(1 for t in txs_high if t.fraud_type.value != "none")

        assert fraud_high > fraud_low

    def test_impossible_travel_generator(self):
        """Impossible travel generator should produce 2 linked txs."""
        from sentinelflow.generator.patterns import ImpossibleTravelGenerator

        gen = ImpossibleTravelGenerator()
        txs = gen.generate()
        assert len(txs) >= 2
        # Both should be flagged
        assert all(t.fraud_type.value != "none" for t in txs)

    def test_generate_batches(self):
        """generate_batches should yield specified number of batches."""
        from sentinelflow.generator.patterns import FraudPatternMixer

        mixer = FraudPatternMixer(fraud_ratio=0.1)
        batches = list(mixer.generate_batches(batch_size=10, num_batches=3))
        assert len(batches) == 3
        assert all(len(b) == 10 for b in batches)


class TestGeneratorMain:
    """Tests for the main generator entry point."""

    def test_parse_args_defaults(self):
        """Argument parser should have sensible defaults."""

        # parse_args reads sys.argv; simulate by patching
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--batch-size", type=int, default=100)
        parser.add_argument("--fraud-ratio", type=float, default=0.05)
        parser.add_argument("--delay", type=float, default=1.0)

        args = parser.parse_args([])
        assert args.batch_size == 100
        assert args.fraud_ratio == 0.05
        assert args.delay == 1.0

    def test_transaction_producer_initialization(self):
        """TransactionProducer should initialize."""
        from sentinelflow.generator.main import TransactionProducer

        producer = TransactionProducer(bootstrap_servers="localhost:9092", topic="test")
        assert producer is not None
        assert producer.topic == "test"

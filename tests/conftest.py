# SentinelFlow - Test Configuration and Fixtures

import os
import sys
from datetime import datetime

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def sample_transaction():
    return {
        "transaction_id": "TX-TEST-001",
        "sender_iban": "TR000000000000000000000001",
        "sender_name": "Test Gonderici",
        "sender_city": "Istanbul",
        "receiver_iban": "TR000000000000000000000002",
        "receiver_name": "Test Alici",
        "receiver_city": "Ankara",
        "amount": 5000.0,
        "description": "Test islemi",
        "timestamp": datetime.now().isoformat(),
    }


@pytest.fixture
def fraud_transaction():
    return {
        "transaction_id": "TX-FRAUD-001",
        "sender_iban": "TR000000000000000000000003",
        "sender_name": "Supheli Kisi",
        "receiver_iban": "TR000000000000000000000004",
        "receiver_name": "Kara Para",
        "amount": 150000.0,
        "description": "bahis komisyon",
        "timestamp": datetime.now().isoformat(),
    }


@pytest.fixture
def ml_feature_vector():
    return np.random.randn(21).astype(np.float32)


@pytest.fixture
def ml_feature_batch():
    np.random.seed(42)
    return np.random.randn(100, 21).astype(np.float32)


@pytest.fixture
def ml_labels():
    np.random.seed(42)
    labels = np.zeros(100, dtype=np.int64)
    fraud_indices = np.random.choice(100, 10, replace=False)
    labels[fraud_indices] = 1
    return labels


@pytest.fixture
def temp_model_dir(tmp_path):
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    return str(model_dir)

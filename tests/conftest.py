# SentinelFlow - Test Configuration and Fixtures

import pytest
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Generator
import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _is_db_available():
    """Check if database dependencies are available."""
    try:
        import sqlalchemy
        return True
    except ImportError:
        return False


@pytest.fixture
def mock_db_session():
    """
    Override the database dependency with a mock session.
    This allows API tests to run without a real PostgreSQL database.
    """
    from sentinelflow.api import deps
    from sentinelflow.api.app import app

    # Create a mock session
    mock_session = MagicMock()

    # Mock execute to return empty results by default
    mock_result = MagicMock()
    mock_result.scalar.return_value = 0
    mock_result.scalars.return_value.all.return_value = []
    mock_result.all.return_value = []
    mock_session.execute.return_value = mock_result

    # Mock flush, close, commit, rollback as no-ops
    mock_session.flush.return_value = None
    mock_session.close.return_value = None
    mock_session.commit.return_value = None
    mock_session.rollback.return_value = None

    # Mock add as no-op
    mock_session.add.return_value = None

    # Override the dependency
    app.dependency_overrides[deps.get_db_session] = lambda: mock_session

    yield mock_session

    # Clean up overrides after test
    app.dependency_overrides.clear()


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

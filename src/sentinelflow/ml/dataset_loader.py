# =============================================================================
# SentinelFlow - Fraud Dataset Loader & Generator
# =============================================================================
"""
Dataset utilities for loading, generating, and preparing fraud detection data.

Supports:
1. Built-in synthetic dataset generation (PaySim-inspired)
2. CSV file loading (Kaggle IEEE-CIS, PaySim format)
3. SentinelFlow transaction format conversion
4. Train/test splitting with stratification

Usage:
    loader = FraudDatasetLoader()

    # Generate synthetic labeled data
    X, y, df = loader.generate_synthetic(n_samples=10000, fraud_ratio=0.05)

    # Load from CSV
    X, y, df = loader.load_csv("data/fraud_dataset.csv")
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from sentinelflow.ml.feature_engine import (
    TransactionFeatureEngine,
    FEATURE_NAMES,
    NUM_FEATURES,
    CITY_COORDS,
    SUSPICIOUS_KEYWORDS,
)


# =============================================================================
# Constants
# =============================================================================

TURKISH_CITIES = [
    "İstanbul",
    "Ankara",
    "İzmir",
    "Bursa",
    "Antalya",
    "Adana",
    "Konya",
    "Gaziantep",
    "Mersin",
    "Diyarbakır",
    "Kayseri",
    "Eskişehir",
    "Trabzon",
    "Samsun",
    "Denizli",
]

FOREIGN_CITIES = ["Berlin", "London", "Paris", "Dubai", "Moscow", "New York", "Tokyo"]

NORMAL_DESCRIPTIONS = [
    "Kira ödemesi",
    "Market alışverişi",
    "Fatura ödemesi",
    "Maaş transferi",
    "Hediye",
    "Borç ödeme",
    "Yemek ücreti",
    "Elektrik faturası",
    "Su faturası",
    "Doğalgaz",
    "İnternet ödemesi",
    "Telefon faturası",
    "Okul taksiti",
    "Araç taksiti",
    "Sigorta ödemesi",
    "Sağlık harcaması",
]

SUSPICIOUS_DESCRIPTIONS = [
    "bitcoin satış acil",
    "kripto transfer hemen",
    "casino ödemesi",
    "bahis kazancı transfer",
    "offshore hesap aktarım",
    "anonim transfer",
    "kumar borcu ödeme",
    "usdt binance çekim",
    "acil nakit gönder",
    "gizli transfer yapılacak",
    "poker masası ödemesi",
    "slot makinesi",
]


def _generate_iban() -> str:
    """Generate a fake Turkish IBAN."""
    return f"TR{random.randint(10, 99)}{''.join([str(random.randint(0, 9)) for _ in range(22)])}"


def _generate_name() -> str:
    """Generate a random Turkish name."""
    first_names = [
        "Ahmet",
        "Mehmet",
        "Ali",
        "Fatma",
        "Ayşe",
        "Zeynep",
        "Emre",
        "Burak",
        "Selin",
        "Merve",
    ]
    last_names = ["Yılmaz", "Kaya", "Demir", "Çelik", "Şahin", "Öztürk", "Aydın", "Arslan"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"


# =============================================================================
# Fraud Dataset Loader
# =============================================================================


class FraudDatasetLoader:
    """
    Loads and prepares fraud detection datasets.

    Generates synthetic data with realistic fraud patterns or loads
    from external CSV datasets (PaySim, IEEE-CIS format).
    """

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)
        self._feature_engine = TransactionFeatureEngine(history_window_size=500)

        logger.info("FraudDatasetLoader initialized")

    def generate_synthetic(
        self,
        n_samples: int = 10000,
        fraud_ratio: float = 0.05,
    ) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
        """
        Generate a synthetic fraud detection dataset.

        Creates realistic transactions with the following fraud patterns:
        - High-value anomalies (z-score > 3)
        - Night-time suspicious transfers
        - High-velocity senders (burst transactions)
        - International transfers with suspicious keywords
        - Structuring (just-under-threshold amounts)

        Args:
            n_samples: Total number of transactions
            fraud_ratio: Fraction of fraudulent transactions

        Returns:
            Tuple of (feature_matrix, labels, dataframe)
        """
        n_fraud = int(n_samples * fraud_ratio)
        n_normal = n_samples - n_fraud

        logger.info(f"Generating {n_samples} transactions ({n_fraud} fraud, {n_normal} normal)")

        transactions: list[dict[str, Any]] = []
        labels: list[int] = []

        # Reset feature engine for clean state
        self._feature_engine = TransactionFeatureEngine(history_window_size=500)

        # Generate normal transactions
        for i in range(n_normal):
            tx = self._generate_normal_transaction(i)
            transactions.append(tx)
            labels.append(0)

        # Generate fraud transactions with diverse patterns
        fraud_patterns = [
            self._generate_high_value_fraud,
            self._generate_night_fraud,
            self._generate_velocity_fraud,
            self._generate_international_fraud,
            self._generate_keyword_fraud,
            self._generate_structuring_fraud,
        ]

        for i in range(n_fraud):
            pattern_fn = self._rng.choice(fraud_patterns)
            tx = pattern_fn(n_normal + i)
            transactions.append(tx)
            labels.append(1)

        # Shuffle
        combined = list(zip(transactions, labels))
        self._rng.shuffle(combined)
        transactions, labels = zip(*combined)
        transactions = list(transactions)
        labels = list(labels)

        # Extract features using feature engine
        feature_matrix = np.zeros((len(transactions), NUM_FEATURES))
        for i, tx in enumerate(transactions):
            feature_matrix[i] = self._feature_engine.extract_vector(tx)

        # Create DataFrame
        df = pd.DataFrame(transactions)
        df["is_fraud"] = labels

        logger.info(
            f"Dataset generated: {feature_matrix.shape}, "
            f"fraud rate: {sum(labels)/len(labels)*100:.1f}%"
        )

        return feature_matrix, np.array(labels), df

    def load_csv(
        self,
        filepath: str,
        amount_col: str = "amount",
        label_col: str = "isFraud",
        timestamp_col: str | None = None,
    ) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
        """
        Load a fraud detection dataset from CSV.

        Supports PaySim and IEEE-CIS dataset formats.

        Args:
            filepath: Path to CSV file
            amount_col: Column name for transaction amount
            label_col: Column name for fraud label
            timestamp_col: Column name for timestamp (optional)

        Returns:
            Tuple of (feature_matrix, labels, dataframe)
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {filepath}")

        logger.info(f"Loading dataset from {filepath}")
        df = pd.read_csv(filepath)

        logger.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

        # Convert to SentinelFlow transaction format
        transactions = self._convert_csv_to_transactions(df, amount_col, label_col, timestamp_col)

        # Extract features
        self._feature_engine = TransactionFeatureEngine(history_window_size=500)
        feature_matrix = np.zeros((len(transactions), NUM_FEATURES))

        for i, tx in enumerate(transactions):
            feature_matrix[i] = self._feature_engine.extract_vector(tx)

        labels = df[label_col].values.astype(int)

        logger.info(
            f"Features extracted: {feature_matrix.shape}, " f"fraud rate: {labels.mean()*100:.1f}%"
        )

        return feature_matrix, labels, df

    # =========================================================================
    # Normal Transaction Generators
    # =========================================================================

    def _generate_normal_transaction(self, idx: int) -> dict[str, Any]:
        """Generate a normal (legitimate) transaction."""
        sender_city = self._rng.choice(TURKISH_CITIES)
        receiver_city = self._rng.choice(TURKISH_CITIES)

        # Normal hours (7-22), weekday-heavy
        hour = self._rng.choices(
            range(24),
            weights=[
                1,
                1,
                1,
                1,
                1,
                1,
                2,  # 0-6
                5,
                8,
                10,
                10,
                10,  # 7-11
                10,
                10,
                10,
                8,
                8,  # 12-16
                6,
                5,
                4,
                3,
                2,
                1,
                1,  # 17-23
            ],
        )[0]

        day_offset = self._rng.randint(0, 30)
        base_time = datetime(2026, 1, 1) + timedelta(
            days=day_offset, hours=hour, minutes=self._rng.randint(0, 59)
        )

        # Normal amount distribution (lognormal, mean ~2000 TL)
        amount = float(self._np_rng.lognormal(mean=7.0, sigma=1.2))
        amount = min(amount, 100000)  # Cap at 100K
        amount = round(amount, 2)

        return {
            "transaction_id": f"TX-N-{idx:06d}",
            "sender_iban": _generate_iban(),
            "sender_name": _generate_name(),
            "sender_city": sender_city,
            "receiver_iban": _generate_iban(),
            "receiver_name": _generate_name(),
            "receiver_city": receiver_city,
            "amount": amount,
            "currency": "TRY",
            "description": self._rng.choice(NORMAL_DESCRIPTIONS),
            "timestamp": base_time.isoformat(),
        }

    # =========================================================================
    # Fraud Pattern Generators
    # =========================================================================

    def _generate_high_value_fraud(self, idx: int) -> dict[str, Any]:
        """Generate a high-value anomalous transaction."""
        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-HV-{idx:06d}"
        tx["amount"] = round(float(self._np_rng.uniform(150000, 500000)), 2)
        return tx

    def _generate_night_fraud(self, idx: int) -> dict[str, Any]:
        """Generate a suspicious night-time transaction."""
        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-NT-{idx:06d}"
        hour = self._rng.choice([0, 1, 2, 3, 4, 5, 23])
        day_offset = self._rng.randint(0, 30)
        tx["timestamp"] = (
            datetime(2026, 1, 1)
            + timedelta(days=day_offset, hours=hour, minutes=self._rng.randint(0, 59))
        ).isoformat()
        tx["amount"] = round(float(self._np_rng.uniform(50000, 200000)), 2)
        return tx

    def _generate_velocity_fraud(self, idx: int) -> dict[str, Any]:
        """Generate a burst of transactions from same sender (velocity abuse)."""
        sender_iban = _generate_iban()
        sender_name = _generate_name()

        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-VL-{idx:06d}"
        tx["sender_iban"] = sender_iban
        tx["sender_name"] = sender_name
        tx["amount"] = round(float(self._np_rng.uniform(5000, 50000)), 2)
        return tx

    def _generate_international_fraud(self, idx: int) -> dict[str, Any]:
        """Generate a suspicious international transfer."""
        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-INT-{idx:06d}"
        tx["receiver_city"] = self._rng.choice(FOREIGN_CITIES)
        tx["amount"] = round(float(self._np_rng.uniform(20000, 300000)), 2)
        tx["description"] = self._rng.choice(SUSPICIOUS_DESCRIPTIONS[:3])
        return tx

    def _generate_keyword_fraud(self, idx: int) -> dict[str, Any]:
        """Generate a transaction with suspicious keywords."""
        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-KW-{idx:06d}"
        tx["description"] = self._rng.choice(SUSPICIOUS_DESCRIPTIONS)
        tx["amount"] = round(float(self._np_rng.uniform(1000, 100000)), 2)
        return tx

    def _generate_structuring_fraud(self, idx: int) -> dict[str, Any]:
        """Generate structuring (smurfing) transactions just under reporting threshold."""
        tx = self._generate_normal_transaction(idx)
        tx["transaction_id"] = f"TX-F-ST-{idx:06d}"
        # Turkish reporting threshold is ~75,000 TL, fraudsters stay just under
        tx["amount"] = round(float(self._np_rng.uniform(70000, 74999)), 2)
        return tx

    def _convert_csv_to_transactions(
        self,
        df: pd.DataFrame,
        amount_col: str,
        label_col: str,
        timestamp_col: str | None,
    ) -> list[dict[str, Any]]:
        """Convert external CSV to SentinelFlow transaction format."""
        transactions = []

        for i, row in df.iterrows():
            tx: dict[str, Any] = {
                "transaction_id": f"CSV-{i:06d}",
                "sender_iban": _generate_iban(),
                "sender_name": _generate_name(),
                "sender_city": self._rng.choice(TURKISH_CITIES),
                "receiver_iban": _generate_iban(),
                "receiver_name": _generate_name(),
                "receiver_city": self._rng.choice(TURKISH_CITIES),
                "amount": float(row.get(amount_col, 0)),
                "currency": "TRY",
                "description": "",
                "timestamp": "",
            }

            if timestamp_col and timestamp_col in df.columns:
                tx["timestamp"] = str(row[timestamp_col])
            else:
                tx["timestamp"] = (datetime(2026, 1, 1) + timedelta(hours=i)).isoformat()

            transactions.append(tx)

        return transactions

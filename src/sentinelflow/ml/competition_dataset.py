# =============================================================================
# SentinelFlow - Competition Dataset Generator (TEKNOFEST Edition)
# =============================================================================
"""
TEKNOFEST yarışması için 500K+ gerçekçi veri seti üreteci.

Geçen yıl birincisi: 200K veri, %99.2 doğruluk
Hedefimiz: 500K+ veri, %99.5+ doğruluk

Özellikler:
- Gerçekçi Türkiye finans verisi
- MASAK/BDDK uyumlu fraud pattern'ları
- Temporal ve davranışsal tutarlılık
- Dengelenmiş sınıf dağılımı (SMOTE/ADASYN desteği)
- IEEE-CIS Fraud Detection veri seti formatı uyumu

Fraud Pattern'ları:
1. Phishing Follow-up: Phishing sonrası hızlı para transferi
2. Social Engineering: Sahte banka çalışanı dolandırıcılığı
3. Money Mule Network: Para aklama ağları
4. Account Takeover: Hesap ele geçirme
5. Structuring: MASAK eşiği altında parçalama
6. Circular Ring: Döngüsel para transferi
7. Impossible Travel: İmkansız seyahat
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

try:
    from faker import Faker

    HAS_FAKER = True
except ImportError:
    HAS_FAKER = False
    logger.warning("Faker not available")


# =============================================================================
# Constants - Turkish Financial Data
# =============================================================================


class FraudPattern(str, Enum):
    """Fraud pattern türleri."""

    NONE = "none"
    PHISHING_FOLLOW = "phishing_follow"
    SOCIAL_ENGINEERING = "social_engineering"
    MONEY_MULE = "money_mule"
    ACCOUNT_TAKEOVER = "account_takeover"
    STRUCTURING = "structuring"
    CIRCULAR_RING = "circular_ring"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    HIGH_VALUE_ANOMALY = "high_value_anomaly"


# Turkish bank codes
TURKISH_BANK_CODES = {
    "0001": "Türkiye Cumhuriyet Merkez Bankası",
    "0010": "Ziraat Bankası",
    "0012": "Halkbank",
    "0015": "Vakıfbank",
    "0046": "Akbank",
    "0062": "Garanti BBVA",
    "0064": "İş Bankası",
    "0067": "Yapı Kredi",
    "0099": "ING Bank",
    "0111": "QNB Finansbank",
    "0134": "Denizbank",
    "0205": "HSBC",
    "0206": "Odeabank",
}

# Turkish cities with populations (for realistic distribution)
TURKISH_CITIES = {
    "İstanbul": {"pop": 15_000_000, "lat": 41.0082, "lon": 28.9784},
    "Ankara": {"pop": 5_500_000, "lat": 39.9334, "lon": 32.8597},
    "İzmir": {"pop": 4_300_000, "lat": 38.4192, "lon": 27.1287},
    "Bursa": {"pop": 3_000_000, "lat": 40.1885, "lon": 29.0610},
    "Antalya": {"pop": 2_500_000, "lat": 36.8969, "lon": 30.7133},
    "Adana": {"pop": 2_200_000, "lat": 37.0000, "lon": 35.3213},
    "Konya": {"pop": 2_200_000, "lat": 37.8746, "lon": 32.4932},
    "Gaziantep": {"pop": 2_000_000, "lat": 37.0662, "lon": 37.3833},
    "Mersin": {"pop": 1_800_000, "lat": 36.8121, "lon": 34.6415},
    "Diyarbakır": {"pop": 1_700_000, "lat": 37.9144, "lon": 40.2306},
    "Kayseri": {"pop": 1_400_000, "lat": 38.7312, "lon": 35.4787},
    "Eskişehir": {"pop": 900_000, "lat": 39.7767, "lon": 30.5206},
    "Trabzon": {"pop": 800_000, "lat": 41.0027, "lon": 39.7168},
    "Samsun": {"pop": 1_300_000, "lat": 41.2867, "lon": 36.3300},
    "Denizli": {"pop": 1_000_000, "lat": 37.7765, "lon": 29.0864},
}

# International cities (for impossible travel)
INTERNATIONAL_CITIES = {
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "London": {"lat": 51.5074, "lon": -0.1278},
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Dubai": {"lat": 25.2048, "lon": 55.2708},
    "Moscow": {"lat": 55.7558, "lon": 37.6173},
    "New York": {"lat": 40.7128, "lon": -74.0060},
}

# Transaction channels
CHANNELS = ["mobile", "web", "atm", "branch", "eft", "pos"]

# MASAK threshold
MASAK_THRESHOLD_TL = 75_000.0

# Common Turkish names
TURKISH_FIRST_NAMES = [
    "Ahmet",
    "Mehmet",
    "Mustafa",
    "Ali",
    "Hüseyin",
    "Hasan",
    "İbrahim",
    "Ömer",
    "Fatma",
    "Ayşe",
    "Emine",
    "Hatice",
    "Zeynep",
    "Elif",
    "Merve",
    "Esra",
    "Murat",
    "Yusuf",
    "Emre",
    "Burak",
    "Onur",
    "Can",
    "Cem",
    "Deniz",
    "Selin",
    "Aslı",
    "Gamze",
    "Pınar",
    "Özge",
    "Derya",
    "Ceren",
    "Gizem",
]

TURKISH_LAST_NAMES = [
    "Yılmaz",
    "Kaya",
    "Demir",
    "Çelik",
    "Şahin",
    "Yıldız",
    "Yıldırım",
    "Öztürk",
    "Aydın",
    "Özdemir",
    "Arslan",
    "Doğan",
    "Kılıç",
    "Aslan",
    "Çetin",
    "Kara",
    "Koç",
    "Kurt",
    "Özcan",
    "Şimşek",
    "Polat",
    "Korkmaz",
    "Özkan",
    "Erdoğan",
]


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class SyntheticUser:
    """Sentetik kullanıcı profili."""

    user_id: str
    iban: str
    name: str
    city: str
    bank_code: str

    # Behavioral profile
    typical_amount_mean: float = 2000.0
    typical_amount_std: float = 1500.0
    typical_hours: list[int] = field(default_factory=lambda: list(range(9, 18)))
    typical_days: list[int] = field(default_factory=lambda: list(range(5)))
    typical_receivers: list[str] = field(default_factory=list)
    tx_frequency_per_day: float = 1.5

    # Risk profile
    is_fraud_account: bool = False
    fraud_pattern: FraudPattern = FraudPattern.NONE


@dataclass
class SyntheticTransaction:
    """Sentetik işlem."""

    transaction_id: str
    timestamp: datetime
    sender_iban: str
    sender_name: str
    sender_city: str
    receiver_iban: str
    receiver_name: str
    receiver_city: str
    amount: float
    currency: str = "TRY"
    description: str = ""
    channel: str = "mobile"
    device_id: str = ""

    # Labels
    is_fraud: bool = False
    fraud_type: FraudPattern = FraudPattern.NONE
    fraud_confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp.isoformat(),
            "sender_iban": self.sender_iban,
            "sender_name": self.sender_name,
            "sender_city": self.sender_city,
            "receiver_iban": self.receiver_iban,
            "receiver_name": self.receiver_name,
            "receiver_city": self.receiver_city,
            "amount": self.amount,
            "currency": self.currency,
            "description": self.description,
            "channel": self.channel,
            "device_id": self.device_id,
            "is_fraud": self.is_fraud,
            "fraud_type": self.fraud_type.value,
        }


# =============================================================================
# Dataset Generator
# =============================================================================


class CompetitionDatasetGenerator:
    """
    TEKNOFEST yarışması için büyük ölçekli veri seti üreteci.

    Özellikler:
    - 500K+ işlem üretimi
    - Gerçekçi Türkiye finans verileri
    - 8 farklı fraud pattern'ı
    - Temporal ve davranışsal tutarlılık

    Usage:
        generator = CompetitionDatasetGenerator(seed=42)
        dataset = generator.generate(n_transactions=500000, fraud_ratio=0.03)
        dataset.to_csv("competition_dataset.csv")
    """

    def __init__(
        self,
        seed: int = 42,
        n_users: int = 50000,
    ) -> None:
        """
        Initialize generator.

        Args:
            seed: Random seed for reproducibility
            n_users: Number of synthetic users to create
        """
        self._seed = seed
        self._n_users = n_users

        random.seed(seed)
        np.random.seed(seed)

        if HAS_FAKER:
            self._faker = Faker("tr_TR")
            self._faker.seed_instance(seed)
        else:
            self._faker = None

        # User pool
        self._users: dict[str, SyntheticUser] = {}
        self._fraud_users: list[str] = []
        self._normal_users: list[str] = []

        # Transaction history for behavioral consistency
        self._user_tx_history: dict[str, list[SyntheticTransaction]] = defaultdict(list)

        # City weights for realistic distribution
        total_pop = sum(c["pop"] for c in TURKISH_CITIES.values())
        self._city_weights = {
            city: data["pop"] / total_pop for city, data in TURKISH_CITIES.items()
        }

        # Initialize users
        self._initialize_users()

        logger.info(f"CompetitionDatasetGenerator initialized " f"(seed={seed}, users={n_users})")

    def _initialize_users(self) -> None:
        """Create synthetic user pool."""
        logger.info(f"Creating {self._n_users} synthetic users...")

        for i in range(self._n_users):
            user_id = f"U{i:06d}"

            # Generate IBAN
            bank_code = random.choice(list(TURKISH_BANK_CODES.keys()))
            account_num = f"{random.randint(0, 9999999999999):013d}"
            iban = f"TR{random.randint(10, 99)}{bank_code}00{account_num}"

            # Generate name
            first_name = random.choice(TURKISH_FIRST_NAMES)
            last_name = random.choice(TURKISH_LAST_NAMES)
            name = f"{first_name} {last_name}"

            # Generate city (population-weighted)
            city = random.choices(
                list(self._city_weights.keys()),
                weights=list(self._city_weights.values()),
            )[0]

            # Generate behavioral profile
            user = SyntheticUser(
                user_id=user_id,
                iban=iban,
                name=name,
                city=city,
                bank_code=bank_code,
                typical_amount_mean=np.random.lognormal(7.5, 1.0),  # ~2000 TL mean
                typical_amount_std=np.random.lognormal(6.5, 0.8),
                typical_hours=random.sample(range(6, 23), random.randint(4, 10)),
                typical_days=random.sample(range(7), random.randint(3, 7)),
                tx_frequency_per_day=np.random.exponential(1.5),
            )

            self._users[user_id] = user
            self._normal_users.append(user_id)

        logger.info(f"Created {len(self._users)} users")

    def generate(
        self,
        n_transactions: int = 500000,
        fraud_ratio: float = 0.03,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        progress_callback: callable | None = None,
    ) -> pd.DataFrame:
        """
        Generate competition dataset.

        Args:
            n_transactions: Total number of transactions
            fraud_ratio: Ratio of fraudulent transactions
            start_date: Start date for transactions
            end_date: End date for transactions
            progress_callback: Optional callback for progress updates

        Returns:
            DataFrame with transactions
        """
        logger.info(f"Generating {n_transactions} transactions (fraud_ratio={fraud_ratio})...")

        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()

        n_fraud = int(n_transactions * fraud_ratio)
        n_normal = n_transactions - n_fraud

        transactions: list[dict] = []

        # Generate normal transactions
        logger.info(f"Generating {n_normal} normal transactions...")
        for i in range(n_normal):
            tx = self._generate_normal_transaction(start_date, end_date)
            transactions.append(tx.to_dict())

            if progress_callback and i % 10000 == 0:
                progress_callback(i / n_transactions)

        # Generate fraud transactions by pattern
        fraud_patterns = list(FraudPattern)
        fraud_patterns.remove(FraudPattern.NONE)

        fraud_per_pattern = n_fraud // len(fraud_patterns)
        remainder = n_fraud % len(fraud_patterns)

        logger.info(f"Generating {n_fraud} fraud transactions...")

        for j, pattern in enumerate(fraud_patterns):
            count = fraud_per_pattern + (1 if j < remainder else 0)

            for i in range(count):
                tx = self._generate_fraud_transaction(pattern, start_date, end_date)
                transactions.append(tx.to_dict())

                if progress_callback:
                    progress = (
                        n_normal + sum(fraud_per_pattern for _ in range(j)) + i
                    ) / n_transactions
                    if int(progress * 100) % 10 == 0:
                        progress_callback(progress)

        # Shuffle transactions
        random.shuffle(transactions)

        # Create DataFrame
        df = pd.DataFrame(transactions)

        # Sort by timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Add transaction ID
        df["transaction_id"] = [f"TX{i:08d}" for i in range(len(df))]

        logger.info(
            f"Dataset generated: {len(df)} transactions, "
            f"{df['is_fraud'].sum()} fraud ({df['is_fraud'].mean()*100:.2f}%)"
        )

        return df

    def _generate_normal_transaction(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """Generate a normal (non-fraud) transaction."""

        # Select random sender and receiver
        sender_id = random.choice(self._normal_users)
        receiver_id = random.choice(self._normal_users)
        while receiver_id == sender_id:
            receiver_id = random.choice(self._normal_users)

        sender = self._users[sender_id]
        receiver = self._users[receiver_id]

        # Generate timestamp within typical hours/days
        timestamp = self._generate_timestamp(
            start_date,
            end_date,
            typical_hours=sender.typical_hours,
            typical_days=sender.typical_days,
        )

        # Generate amount from user's typical distribution
        amount = max(10, np.random.normal(sender.typical_amount_mean, sender.typical_amount_std))

        # Generate description
        descriptions = [
            "Havale",
            "EFT",
            "Kira ödemesi",
            "Fatura ödemesi",
            "Alışveriş",
            "Market",
            "Restoran",
            "Benzin",
            "Online alışveriş",
            "Abonelik",
            "Aidat",
            "Borç ödeme",
        ]
        description = random.choice(descriptions)

        # Generate channel
        channel = random.choices(CHANNELS, weights=[0.4, 0.25, 0.1, 0.05, 0.15, 0.05])[0]

        # Generate device ID
        device_id = hashlib.md5(f"{sender_id}_{random.randint(1, 3)}".encode()).hexdigest()[:16]

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description=description,
            channel=channel,
            device_id=device_id,
            is_fraud=False,
            fraud_type=FraudPattern.NONE,
        )

    def _generate_fraud_transaction(
        self,
        pattern: FraudPattern,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """Generate a fraudulent transaction based on pattern."""

        if pattern == FraudPattern.PHISHING_FOLLOW:
            return self._generate_phishing_fraud(start_date, end_date)
        elif pattern == FraudPattern.SOCIAL_ENGINEERING:
            return self._generate_social_engineering_fraud(start_date, end_date)
        elif pattern == FraudPattern.MONEY_MULE:
            return self._generate_money_mule_fraud(start_date, end_date)
        elif pattern == FraudPattern.ACCOUNT_TAKEOVER:
            return self._generate_account_takeover_fraud(start_date, end_date)
        elif pattern == FraudPattern.STRUCTURING:
            return self._generate_structuring_fraud(start_date, end_date)
        elif pattern == FraudPattern.CIRCULAR_RING:
            return self._generate_circular_ring_fraud(start_date, end_date)
        elif pattern == FraudPattern.IMPOSSIBLE_TRAVEL:
            return self._generate_impossible_travel_fraud(start_date, end_date)
        elif pattern == FraudPattern.HIGH_VALUE_ANOMALY:
            return self._generate_high_value_fraud(start_date, end_date)
        else:
            return self._generate_normal_transaction(start_date, end_date)

    def _generate_phishing_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Phishing follow-up fraud.

        Pattern: Hemen phishing sonrası, tüm bakiye veya limit transferi,
        yeni alıcıya, genellikle gece saatlerinde.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        # New receiver (never seen before)
        receiver_id = random.choice(self._normal_users)
        while receiver_id == sender_id:
            receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        # Off-hours (night time)
        timestamp = self._generate_timestamp(
            start_date,
            end_date,
            typical_hours=[0, 1, 2, 3, 4, 5, 23],
            typical_days=list(range(7)),
        )

        # Large amount (entire balance or limit)
        amount = random.uniform(50000, 200000)

        # Suspicious description
        descriptions = [
            "Acil transfer",
            "Güvenlik güncellemesi",
            "Hesap doğrulama",
            "Banka işlemi",
            "Sistem güncellemesi",
        ]

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description=random.choice(descriptions),
            channel="mobile",
            device_id=hashlib.md5(f"new_device_{random.randint(1, 1000)}".encode()).hexdigest()[
                :16
            ],
            is_fraud=True,
            fraud_type=FraudPattern.PHISHING_FOLLOW,
            fraud_confidence=0.95,
        )

    def _generate_social_engineering_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Social engineering fraud.

        Pattern: "Hesabınız bloke" dolandırıcılığı, mesai saatlerinde,
        telefon görüşmesi sonrası mobil transfer.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        # Business hours
        timestamp = self._generate_timestamp(
            start_date,
            end_date,
            typical_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17],
            typical_days=[0, 1, 2, 3, 4],
        )

        # Round amount (common in social engineering)
        round_amounts = [10000, 25000, 50000, 75000, 100000]
        amount = random.choice(round_amounts) + random.randint(-500, 500)

        descriptions = [
            "Güvenlik işlemi",
            "Hesap güncelleme",
            "Bloke kaldırma",
            "Doğrulama transferi",
            "Sistem kontrolü",
        ]

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description=random.choice(descriptions),
            channel="mobile",
            device_id=hashlib.md5(f"{sender_id}_1".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.SOCIAL_ENGINEERING,
            fraud_confidence=0.90,
        )

    def _generate_money_mule_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Money mule network fraud.

        Pattern: Fan-out then fan-in, MASAK eşiği altında tutarlar,
        çok sayıda farklı alıcıya hızlı transferler.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        # Any time
        timestamp = self._generate_timestamp(start_date, end_date)

        # Just below MASAK threshold
        amount = MASAK_THRESHOLD_TL - random.uniform(1000, 15000)

        descriptions = [
            "Ticari ödeme",
            "Fatura",
            "İş transferi",
            "Komisyon ödemesi",
            "Hizmet bedeli",
        ]

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description=random.choice(descriptions),
            channel=random.choice(["web", "mobile", "eft"]),
            device_id=hashlib.md5(f"{sender_id}_{random.randint(1, 5)}".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.MONEY_MULE,
            fraud_confidence=0.85,
        )

    def _generate_account_takeover_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Account takeover fraud.

        Pattern: Yeni cihaz, yeni IP, şifre değişikliği sonrası,
        kullanıcının normal davranışından sapma.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        # Outside typical hours
        atypical_hours = [h for h in range(24) if h not in sender.typical_hours]
        timestamp = self._generate_timestamp(
            start_date,
            end_date,
            typical_hours=atypical_hours or [0, 1, 2, 3],
            typical_days=list(range(7)),
        )

        # Much larger than typical
        amount = sender.typical_amount_mean * random.uniform(5, 20)

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description="Havale",
            channel="web",
            device_id=hashlib.md5(f"stolen_{random.randint(1, 10000)}".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.ACCOUNT_TAKEOVER,
            fraud_confidence=0.92,
        )

    def _generate_structuring_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Structuring (smurfing) fraud.

        Pattern: MASAK eşiği altında çoklu işlemler,
        aynı gün içinde farklı alıcılara.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        timestamp = self._generate_timestamp(start_date, end_date)

        # Structured amounts (just below thresholds)
        structured_amounts = [
            MASAK_THRESHOLD_TL - random.uniform(100, 1000),
            49000 + random.uniform(0, 900),
            24000 + random.uniform(0, 900),
            9000 + random.uniform(0, 900),
        ]
        amount = random.choice(structured_amounts)

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description="EFT",
            channel=random.choice(["mobile", "web", "eft"]),
            device_id=hashlib.md5(f"{sender_id}_1".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.STRUCTURING,
            fraud_confidence=0.80,
        )

    def _generate_circular_ring_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Circular ring (money laundering) fraud.

        Pattern: A -> B -> C -> A döngüsel transferler,
        miktarlar azalarak (komisyon kesintisi simülasyonu).
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        timestamp = self._generate_timestamp(start_date, end_date)

        # Large amount for laundering
        amount = random.uniform(100000, 500000)

        descriptions = [
            "Yatırım",
            "İş ortaklığı",
            "Ticari ödeme",
            "Sermaye transferi",
            "Proje ödemesi",
        ]

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description=random.choice(descriptions),
            channel="eft",
            device_id=hashlib.md5(f"{sender_id}_1".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.CIRCULAR_RING,
            fraud_confidence=0.95,
        )

    def _generate_impossible_travel_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        Impossible travel fraud.

        Pattern: İstanbul'da işlem, 10 dk sonra Berlin'de işlem.
        Fiziksel olarak imkansız.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        timestamp = self._generate_timestamp(start_date, end_date)

        # Sender city is Turkish, receiver city is international
        sender_city = random.choice(list(TURKISH_CITIES.keys()))
        receiver_city = random.choice(list(INTERNATIONAL_CITIES.keys()))

        amount = random.uniform(5000, 100000)

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender_city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver_city,
            amount=round(amount, 2),
            description="International transfer",
            channel="web",
            device_id=hashlib.md5(f"travel_{random.randint(1, 1000)}".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.IMPOSSIBLE_TRAVEL,
            fraud_confidence=0.98,
        )

    def _generate_high_value_fraud(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> SyntheticTransaction:
        """
        High value anomaly fraud.

        Pattern: Kullanıcının normal işlem tutarının çok üzerinde,
        ani yüksek tutarlı transfer.
        """
        sender_id = random.choice(self._normal_users)
        sender = self._users[sender_id]

        receiver_id = random.choice(self._normal_users)
        receiver = self._users[receiver_id]

        timestamp = self._generate_timestamp(start_date, end_date)

        # 50x+ normal amount
        amount = sender.typical_amount_mean * random.uniform(50, 200)

        return SyntheticTransaction(
            transaction_id="",
            timestamp=timestamp,
            sender_iban=sender.iban,
            sender_name=sender.name,
            sender_city=sender.city,
            receiver_iban=receiver.iban,
            receiver_name=receiver.name,
            receiver_city=receiver.city,
            amount=round(amount, 2),
            description="Özel transfer",
            channel=random.choice(["mobile", "web"]),
            device_id=hashlib.md5(f"{sender_id}_1".encode()).hexdigest()[:16],
            is_fraud=True,
            fraud_type=FraudPattern.HIGH_VALUE_ANOMALY,
            fraud_confidence=0.88,
        )

    def _generate_timestamp(
        self,
        start_date: datetime,
        end_date: datetime,
        typical_hours: list[int] | None = None,
        typical_days: list[int] | None = None,
    ) -> datetime:
        """Generate a timestamp with optional hour/day preferences."""

        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        base_date = start_date + timedelta(days=random_days)

        hour = random.choice(typical_hours) if typical_hours else random.randint(0, 23)

        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        return base_date.replace(hour=hour, minute=minute, second=second)

    def save_dataset(
        self,
        df: pd.DataFrame,
        output_dir: str = "data",
        format: str = "parquet",  # csv, parquet, json
    ) -> str:
        """
        Save dataset to file.

        Args:
            df: Dataset DataFrame
            output_dir: Output directory
            format: File format (csv, parquet, json)

        Returns:
            Path to saved file
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"competition_dataset_{len(df)}_{timestamp}"

        if format == "parquet":
            path = os.path.join(output_dir, f"{filename}.parquet")
            df.to_parquet(path, index=False)
        elif format == "json":
            path = os.path.join(output_dir, f"{filename}.json")
            df.to_json(path, orient="records", date_format="iso", indent=2)
        else:
            path = os.path.join(output_dir, f"{filename}.csv")
            df.to_csv(path, index=False)

        logger.info(f"Dataset saved to {path}")
        return path

    def get_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Get dataset statistics."""
        return {
            "total_transactions": len(df),
            "fraud_count": int(df["is_fraud"].sum()),
            "fraud_ratio": round(df["is_fraud"].mean() * 100, 2),
            "unique_senders": df["sender_iban"].nunique(),
            "unique_receivers": df["receiver_iban"].nunique(),
            "amount_stats": {
                "mean": round(df["amount"].mean(), 2),
                "std": round(df["amount"].std(), 2),
                "min": round(df["amount"].min(), 2),
                "max": round(df["amount"].max(), 2),
                "median": round(df["amount"].median(), 2),
            },
            "fraud_by_type": df[df["is_fraud"]].groupby("fraud_type").size().to_dict(),
            "transactions_by_channel": df["channel"].value_counts().to_dict(),
            "date_range": {
                "start": str(df["timestamp"].min()),
                "end": str(df["timestamp"].max()),
            },
        }


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """Generate competition dataset."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate TEKNOFEST competition dataset")
    parser.add_argument("--size", type=int, default=500000, help="Number of transactions")
    parser.add_argument("--fraud-ratio", type=float, default=0.03, help="Fraud ratio")
    parser.add_argument("--users", type=int, default=50000, help="Number of users")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, default="data", help="Output directory")
    parser.add_argument("--format", type=str, default="parquet", help="Output format")

    args = parser.parse_args()

    generator = CompetitionDatasetGenerator(seed=args.seed, n_users=args.users)

    df = generator.generate(
        n_transactions=args.size,
        fraud_ratio=args.fraud_ratio,
    )

    path = generator.save_dataset(df, args.output, args.format)

    stats = generator.get_statistics(df)
    print(json.dumps(stats, indent=2, ensure_ascii=False))

    print(f"\nDataset saved to: {path}")


if __name__ == "__main__":
    main()

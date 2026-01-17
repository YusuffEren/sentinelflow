# =============================================================================
# SentinelFlow - Fraud Pattern Generators
# =============================================================================
"""
Fraud pattern injection module for synthetic data generation.

This module creates realistic fraud scenarios including:
- Circular transaction rings (money laundering)
- Rapid geographic movements (impossible travel)
- Suspicious transaction descriptions
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Generator
from uuid import uuid4

from sentinelflow.generator.models import (
    Account,
    City,
    FraudType,
    Transaction,
)


# =============================================================================
# Turkish Geographic Data
# =============================================================================

TURKISH_CITIES: list[City] = [
    City(name="İstanbul", latitude=41.0082, longitude=28.9784),
    City(name="Ankara", latitude=39.9334, longitude=32.8597),
    City(name="İzmir", latitude=38.4192, longitude=27.1287),
    City(name="Bursa", latitude=40.1885, longitude=29.0610),
    City(name="Antalya", latitude=36.8969, longitude=30.7133),
    City(name="Adana", latitude=37.0000, longitude=35.3213),
    City(name="Konya", latitude=37.8746, longitude=32.4932),
    City(name="Gaziantep", latitude=37.0662, longitude=37.3833),
    City(name="Mersin", latitude=36.8121, longitude=34.6415),
    City(name="Diyarbakır", latitude=37.9144, longitude=40.2306),
    City(name="Kayseri", latitude=38.7312, longitude=35.4787),
    City(name="Eskişehir", latitude=39.7767, longitude=30.5206),
    City(name="Trabzon", latitude=41.0027, longitude=39.7168),
    City(name="Samsun", latitude=41.2867, longitude=36.3300),
    City(name="Denizli", latitude=37.7765, longitude=29.0864),
]

# Foreign cities for impossible travel scenarios
FOREIGN_CITIES: list[City] = [
    City(name="Berlin", latitude=52.5200, longitude=13.4050),
    City(name="London", latitude=51.5074, longitude=-0.1278),
    City(name="Paris", latitude=48.8566, longitude=2.3522),
    City(name="Dubai", latitude=25.2048, longitude=55.2708),
    City(name="Moscow", latitude=55.7558, longitude=37.6173),
]


# =============================================================================
# Turkish Name Data
# =============================================================================

TURKISH_FIRST_NAMES_MALE: list[str] = [
    "Ahmet", "Mehmet", "Mustafa", "Ali", "Hüseyin", "Hasan", "İbrahim",
    "Osman", "Yusuf", "Ömer", "Murat", "Fatih", "Emre", "Burak", "Serkan",
    "Kemal", "Cem", "Onur", "Barış", "Tolga", "Erkan", "Volkan", "Sinan",
    "Caner", "Uğur", "Deniz", "Kaan", "Efe", "Berk", "Arda",
]

TURKISH_FIRST_NAMES_FEMALE: list[str] = [
    "Ayşe", "Fatma", "Emine", "Hatice", "Zeynep", "Elif", "Merve", "Esra",
    "Zehra", "Sultan", "Hanife", "Hacer", "Gül", "Sevgi", "Derya", "Selin",
    "İpek", "Büşra", "Gamze", "Tuğba", "Cansu", "Ece", "Seda", "Özge",
    "Aslı", "Ebru", "Pınar", "Sibel", "Gizem", "Ceren",
]

TURKISH_LAST_NAMES: list[str] = [
    "Yılmaz", "Kaya", "Demir", "Çelik", "Şahin", "Yıldız", "Yıldırım",
    "Öztürk", "Aydın", "Özdemir", "Arslan", "Doğan", "Kılıç", "Aslan",
    "Çetin", "Kara", "Koç", "Kurt", "Özkan", "Şimşek", "Polat", "Korkmaz",
    "Erdoğan", "Güneş", "Çakır", "Akın", "Ünal", "Bulut", "Aktaş", "Taş",
]

TURKISH_BANK_NAMES: list[str] = [
    "Ziraat Bankası", "İş Bankası", "Garanti BBVA", "Yapı Kredi",
    "Akbank", "Halkbank", "VakıfBank", "QNB Finansbank", "Denizbank",
    "TEB", "ING Bank", "HSBC", "Şekerbank", "Odeabank",
]


# =============================================================================
# Suspicious Description Keywords
# =============================================================================

SUSPICIOUS_KEYWORDS: list[str] = [
    "bahis", "casino", "kumar", "poker", "bet365",
    "kripto", "bitcoin", "usdt", "binance",
    "offshore", "anonim", "gizli", "acil nakit",
]

NORMAL_DESCRIPTIONS: list[str] = [
    "Kira ödemesi", "Market alışverişi", "Fatura ödemesi", "Maaş transferi",
    "Hediye", "Borç ödeme", "Yemek ücreti", "Elektrik faturası",
    "Su faturası", "Doğalgaz", "İnternet ödemesi", "Telefon faturası",
    "Okul taksiti", "Araç taksiti", "Sigorta ödemesi", "Sağlık harcaması",
    "Tatil transferi", "Düğün hediyesi", "Doğum günü hediyesi",
    "Ev eşyası", "Giyim alışverişi", "Spor salonu üyeliği",
]


# =============================================================================
# Helper Functions
# =============================================================================

def generate_turkish_iban() -> str:
    """Generate a valid-format Turkish IBAN."""
    # TR + 2 check digits + 5 bank code + 1 reserve + 16 account number
    bank_code = str(random.randint(10, 99)).zfill(5)
    account_number = "".join([str(random.randint(0, 9)) for _ in range(17)])
    return f"TR{random.randint(10, 99)}{bank_code}{account_number}"


def generate_turkish_name() -> str:
    """Generate a random Turkish full name."""
    if random.random() < 0.5:
        first_name = random.choice(TURKISH_FIRST_NAMES_MALE)
    else:
        first_name = random.choice(TURKISH_FIRST_NAMES_FEMALE)
    last_name = random.choice(TURKISH_LAST_NAMES)
    return f"{first_name} {last_name}"


def generate_account(city: City | None = None) -> Account:
    """Generate a random Turkish bank account."""
    if city is None:
        city = random.choice(TURKISH_CITIES)
    
    return Account(
        iban=generate_turkish_iban(),
        holder_name=generate_turkish_name(),
        bank_name=random.choice(TURKISH_BANK_NAMES),
        city=city.name,
    )


# =============================================================================
# Fraud Pattern Generators
# =============================================================================

class CircularRingGenerator:
    """
    Generate circular transaction patterns (A → B → C → A).
    
    These patterns simulate money laundering rings where funds
    flow in a circle to obscure their origin.
    """
    
    def __init__(
        self,
        ring_size: int = 3,
        amount_range: tuple[float, float] = (5000.0, 50000.0),
        time_spread_minutes: int = 120,
    ):
        self.ring_size = ring_size
        self.amount_range = amount_range
        self.time_spread_minutes = time_spread_minutes
    
    def generate(self) -> list[Transaction]:
        """Generate a complete circular transaction ring."""
        ring_id = f"RING-{uuid4().hex[:8].upper()}"
        base_time = datetime.utcnow() - timedelta(minutes=random.randint(0, 60))
        
        # Create accounts in the ring
        accounts = [generate_account() for _ in range(self.ring_size)]
        
        # Base amount with small variations to avoid detection
        base_amount = Decimal(str(random.uniform(*self.amount_range)))
        
        transactions: list[Transaction] = []
        
        for i in range(self.ring_size):
            sender = accounts[i]
            receiver = accounts[(i + 1) % self.ring_size]  # Circular
            
            # Vary amount slightly
            variation = Decimal(str(random.uniform(-0.05, 0.05)))
            amount = base_amount * (1 + variation)
            
            # Spread transactions over time
            tx_time = base_time + timedelta(
                minutes=i * (self.time_spread_minutes // self.ring_size)
            )
            
            tx = Transaction(
                sender_account_id=sender.account_id,
                sender_iban=sender.iban,
                sender_name=sender.holder_name,
                sender_city=sender.city,
                receiver_account_id=receiver.account_id,
                receiver_iban=receiver.iban,
                receiver_name=receiver.holder_name,
                receiver_city=receiver.city,
                amount=round(amount, 2),
                description=random.choice(NORMAL_DESCRIPTIONS),
                timestamp=tx_time,
                ring_id=ring_id,
                pattern_label=f"circular_ring_{self.ring_size}",
                fraud_type=FraudType.CIRCULAR_RING,
            )
            transactions.append(tx)
        
        return transactions


class ImpossibleTravelGenerator:
    """
    Generate impossible travel patterns.
    
    Creates transactions from geographically distant locations
    within impossibly short time windows.
    """
    
    def __init__(
        self,
        time_gap_minutes: int = 10,
        amount_range: tuple[float, float] = (1000.0, 20000.0),
    ):
        self.time_gap_minutes = time_gap_minutes
        self.amount_range = amount_range
    
    def generate(self) -> list[Transaction]:
        """Generate an impossible travel pair."""
        # Same sender, different cities
        sender_name = generate_turkish_name()
        sender_iban = generate_turkish_iban()
        sender_account_id = uuid4()
        
        # First transaction in Turkey
        city1 = random.choice(TURKISH_CITIES)
        receiver1 = generate_account(city1)
        
        # Second transaction far away (foreign city or distant Turkish city)
        if random.random() < 0.7:
            city2 = random.choice(FOREIGN_CITIES)
        else:
            city2 = random.choice([c for c in TURKISH_CITIES if c.name != city1.name])
        receiver2 = generate_account(city2)
        
        base_time = datetime.utcnow() - timedelta(minutes=random.randint(0, 30))
        
        tx1 = Transaction(
            sender_account_id=sender_account_id,
            sender_iban=sender_iban,
            sender_name=sender_name,
            sender_city=city1.name,
            receiver_account_id=receiver1.account_id,
            receiver_iban=receiver1.iban,
            receiver_name=receiver1.holder_name,
            receiver_city=receiver1.city,
            amount=Decimal(str(round(random.uniform(*self.amount_range), 2))),
            description=random.choice(NORMAL_DESCRIPTIONS),
            timestamp=base_time,
            pattern_label="impossible_travel",
            fraud_type=FraudType.IMPOSSIBLE_TRAVEL,
        )
        
        tx2 = Transaction(
            sender_account_id=sender_account_id,
            sender_iban=sender_iban,
            sender_name=sender_name,
            sender_city=city2.name,
            receiver_account_id=receiver2.account_id,
            receiver_iban=receiver2.iban,
            receiver_name=receiver2.holder_name,
            receiver_city=receiver2.city,
            amount=Decimal(str(round(random.uniform(*self.amount_range), 2))),
            description=random.choice(NORMAL_DESCRIPTIONS),
            timestamp=base_time + timedelta(minutes=self.time_gap_minutes),
            pattern_label="impossible_travel",
            fraud_type=FraudType.IMPOSSIBLE_TRAVEL,
        )
        
        return [tx1, tx2]


class BlacklistKeywordGenerator:
    """Generate transactions with suspicious keywords in descriptions."""
    
    def __init__(self, amount_range: tuple[float, float] = (500.0, 15000.0)):
        self.amount_range = amount_range
    
    def generate(self) -> Transaction:
        """Generate a transaction with blacklisted keyword."""
        sender = generate_account()
        receiver = generate_account()
        
        # Suspicious description
        keyword = random.choice(SUSPICIOUS_KEYWORDS)
        description = f"{keyword} - transfer"
        
        return Transaction(
            sender_account_id=sender.account_id,
            sender_iban=sender.iban,
            sender_name=sender.holder_name,
            sender_city=sender.city,
            receiver_account_id=receiver.account_id,
            receiver_iban=receiver.iban,
            receiver_name=receiver.holder_name,
            receiver_city=receiver.city,
            amount=Decimal(str(round(random.uniform(*self.amount_range), 2))),
            description=description,
            timestamp=datetime.utcnow() - timedelta(minutes=random.randint(0, 60)),
            pattern_label="blacklist_keyword",
            fraud_type=FraudType.BLACKLIST_KEYWORD,
        )


class NormalTransactionGenerator:
    """Generate legitimate, non-fraudulent transactions."""
    
    def __init__(self, amount_range: tuple[float, float] = (50.0, 25000.0)):
        self.amount_range = amount_range
    
    def generate(self) -> Transaction:
        """Generate a normal transaction."""
        sender = generate_account()
        receiver = generate_account()
        
        return Transaction(
            sender_account_id=sender.account_id,
            sender_iban=sender.iban,
            sender_name=sender.holder_name,
            sender_city=sender.city,
            receiver_account_id=receiver.account_id,
            receiver_iban=receiver.iban,
            receiver_name=receiver.holder_name,
            receiver_city=receiver.city,
            amount=Decimal(str(round(random.uniform(*self.amount_range), 2))),
            description=random.choice(NORMAL_DESCRIPTIONS),
            timestamp=datetime.utcnow() - timedelta(
                minutes=random.randint(0, 120),
                seconds=random.randint(0, 59)
            ),
            fraud_type=FraudType.NONE,
        )


# =============================================================================
# Master Generator
# =============================================================================

class FraudPatternMixer:
    """
    Orchestrates generation of mixed legitimate and fraudulent transactions.
    
    Usage:
        mixer = FraudPatternMixer(fraud_ratio=0.1)
        for batch in mixer.generate_batches(batch_size=100, num_batches=10):
            for transaction in batch:
                producer.send(transaction)
    """
    
    def __init__(
        self,
        fraud_ratio: float = 0.05,
        circular_ring_sizes: list[int] | None = None,
        seed: int | None = None,
    ):
        if seed is not None:
            random.seed(seed)
        
        self.fraud_ratio = fraud_ratio
        self.circular_ring_sizes = circular_ring_sizes or [3, 4, 5]
        
        # Pattern generators
        self.normal_gen = NormalTransactionGenerator()
        self.blacklist_gen = BlacklistKeywordGenerator()
        self.travel_gen = ImpossibleTravelGenerator()
    
    def generate_batch(self, size: int) -> list[Transaction]:
        """Generate a batch of transactions with mixed patterns."""
        transactions: list[Transaction] = []
        fraud_count = int(size * self.fraud_ratio)
        normal_count = size - fraud_count
        
        # Generate normal transactions
        for _ in range(normal_count):
            transactions.append(self.normal_gen.generate())
        
        # Generate fraud patterns
        remaining_fraud = fraud_count
        while remaining_fraud > 0:
            pattern_type = random.choice(["circular", "travel", "blacklist"])
            
            if pattern_type == "circular" and remaining_fraud >= 3:
                ring_size = random.choice(self.circular_ring_sizes)
                ring_gen = CircularRingGenerator(ring_size=min(ring_size, remaining_fraud))
                ring_txs = ring_gen.generate()
                transactions.extend(ring_txs)
                remaining_fraud -= len(ring_txs)
            
            elif pattern_type == "travel" and remaining_fraud >= 2:
                travel_txs = self.travel_gen.generate()
                transactions.extend(travel_txs)
                remaining_fraud -= 2
            
            else:
                transactions.append(self.blacklist_gen.generate())
                remaining_fraud -= 1
        
        # Shuffle to mix patterns
        random.shuffle(transactions)
        return transactions
    
    def generate_batches(
        self,
        batch_size: int = 100,
        num_batches: int | None = None,
    ) -> Generator[list[Transaction], None, None]:
        """Generate continuous batches of transactions."""
        batch_count = 0
        while num_batches is None or batch_count < num_batches:
            yield self.generate_batch(batch_size)
            batch_count += 1

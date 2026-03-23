# =============================================================================
# SentinelFlow - Compliance Engine
# =============================================================================
"""
Regulatory compliance engine for transaction screening.

This module implements real-time compliance checks against Turkish
financial regulations including:
- MASAK thresholds (5549 Sayılı Kanun)
- BDDK requirements
- International sanctions screening
- PEP (Politically Exposed Persons) checks

The engine runs before ML-based fraud detection to ensure
regulatory compliance is always checked first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from loguru import logger


# =============================================================================
# Constants
# =============================================================================


class RiskLevel(str, Enum):
    """Transaction and customer risk levels."""

    LOW = "DUSUK"
    MEDIUM = "ORTA"
    HIGH = "YUKSEK"
    CRITICAL = "KRITIK"


class ComplianceRule(str, Enum):
    """Compliance rule identifiers."""

    # MASAK Rules
    MASAK_CASH_SINGLE = "MASAK_NAKIT_TEK"
    MASAK_CASH_MONTHLY = "MASAK_NAKIT_AYLIK"
    MASAK_WIRE_HIGH = "MASAK_HAVALE_YUKSEK"
    MASAK_STRUCTURING = "MASAK_PARCALAMA"

    # Sanctions
    SANCTIONS_OFAC = "YAPTIRIM_OFAC"
    SANCTIONS_EU = "YAPTIRIM_AB"
    SANCTIONS_UN = "YAPTIRIM_BM"

    # PEP
    PEP_CHECK = "PEP_KONTROLU"

    # Velocity
    VELOCITY_1H = "HIZ_1SAAT"
    VELOCITY_24H = "HIZ_24SAAT"

    # Geographic
    HIGH_RISK_COUNTRY = "YUKSEK_RISKLI_ULKE"

    # Pattern
    UNUSUAL_PATTERN = "OLAGAN_DISI_KALIP"


# Regulatory thresholds (TRY)
THRESHOLDS = {
    "cash_single_report": 85_000.0,  # MASAK nakit bildirim
    "cash_monthly_report": 340_000.0,  # MASAK aylık nakit
    "wire_high_risk": 250_000.0,  # Yüksek riskli havale
    "wire_report": 50_000.0,  # Şüpheli havale eşik
    "structuring_threshold": 80_000.0,  # Parçalama şüphesi
    "pep_threshold": 25_000.0,  # PEP düşük eşik
    "velocity_1h_count": 5,  # 1 saatte max işlem
    "velocity_24h_count": 20,  # 24 saatte max işlem
    "velocity_1h_amount": 100_000.0,  # 1 saatte max tutar
}

# High-risk countries (FATF grey/black list)
HIGH_RISK_COUNTRIES = [
    "North Korea",
    "Iran",
    "Myanmar",
    "Syria",
    "Yemen",
    "Afghanistan",
    "South Sudan",
    # Add more as per current FATF list
]

# Demo PEP list (in production, use external service)
DEMO_PEP_LIST = [
    "Ali Veli",  # Demo PEP
    "Test PEP",
]

# Demo sanctions list (in production, use external service)
DEMO_SANCTIONS_LIST = [
    "Yasaklı Kişi",  # Demo sanctioned person
    "Test Sanctions",
]


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class ComplianceViolation:
    """A compliance rule violation."""

    rule: ComplianceRule
    severity: RiskLevel
    message: str
    threshold: float | None = None
    actual_value: float | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule.value,
            "severity": self.severity.value,
            "message": self.message,
            "threshold": self.threshold,
            "actual_value": self.actual_value,
            "details": self.details,
        }


@dataclass
class ComplianceResult:
    """Result of compliance check on a transaction."""

    transaction_id: str
    is_compliant: bool
    risk_level: RiskLevel
    violations: list[ComplianceViolation] = field(default_factory=list)
    requires_str: bool = False  # Requires Suspicious Transaction Report
    requires_ctr: bool = False  # Requires Cash Transaction Report
    checked_rules: list[str] = field(default_factory=list)
    check_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "is_compliant": self.is_compliant,
            "risk_level": self.risk_level.value,
            "violations": [v.to_dict() for v in self.violations],
            "requires_str": self.requires_str,
            "requires_ctr": self.requires_ctr,
            "checked_rules": self.checked_rules,
            "check_timestamp": self.check_timestamp,
        }

    def summary(self) -> str:
        """Generate a summary string."""
        status = "✅ UYUMLU" if self.is_compliant else "❌ UYUMSUZ"
        violations_str = ", ".join(v.rule.value for v in self.violations)
        return (
            f"[{status}] Transaction: {self.transaction_id[:12]}... | "
            f"Risk: {self.risk_level.value} | "
            f"Violations: {violations_str or 'Yok'}"
        )


# =============================================================================
# Account History Tracker
# =============================================================================


class AccountHistoryTracker:
    """Tracks account transaction history for velocity checks."""

    def __init__(self, window_hours: int = 24):
        self._history: dict[str, list[dict]] = {}
        self._window_hours = window_hours

    def add_transaction(
        self,
        iban: str,
        amount: float,
        timestamp: datetime,
        transaction_id: str,
    ) -> None:
        """Record a transaction for an account."""
        if iban not in self._history:
            self._history[iban] = []

        self._history[iban].append(
            {
                "amount": amount,
                "timestamp": timestamp,
                "transaction_id": transaction_id,
            }
        )

        # Cleanup old entries
        self._cleanup(iban)

    def _cleanup(self, iban: str) -> None:
        """Remove transactions older than window."""
        if iban not in self._history:
            return

        cutoff = datetime.now(timezone.utc).replace(tzinfo=None)
        from datetime import timedelta

        cutoff = cutoff - timedelta(hours=self._window_hours)

        self._history[iban] = [tx for tx in self._history[iban] if tx["timestamp"] > cutoff]

    def get_velocity(
        self,
        iban: str,
        hours: int = 1,
    ) -> tuple[int, float]:
        """
        Get transaction velocity for an account.

        Returns:
            (count, total_amount) in the specified time window
        """
        if iban not in self._history:
            return 0, 0.0

        cutoff = datetime.now(timezone.utc).replace(tzinfo=None)
        from datetime import timedelta

        cutoff = cutoff - timedelta(hours=hours)

        recent = [tx for tx in self._history[iban] if tx["timestamp"] > cutoff]

        count = len(recent)
        total = sum(tx["amount"] for tx in recent)

        return count, total


# =============================================================================
# Compliance Engine
# =============================================================================


class ComplianceEngine:
    """
    Real-time regulatory compliance checking engine.

    Performs comprehensive compliance checks on transactions before
    they are processed by the fraud detection system.

    Example:
        >>> engine = ComplianceEngine()
        >>> result = engine.check_transaction(tx_data)
        >>> if not result.is_compliant:
        ...     print(result.summary())
    """

    def __init__(
        self,
        thresholds: dict[str, float] | None = None,
        enable_pep_check: bool = True,
        enable_sanctions_check: bool = True,
        enable_velocity_check: bool = True,
    ):
        """
        Initialize compliance engine.

        Args:
            thresholds: Custom threshold values
            enable_pep_check: Enable PEP screening
            enable_sanctions_check: Enable sanctions screening
            enable_velocity_check: Enable velocity checks
        """
        self._thresholds = {**THRESHOLDS, **(thresholds or {})}
        self._enable_pep = enable_pep_check
        self._enable_sanctions = enable_sanctions_check
        self._enable_velocity = enable_velocity_check

        self._history_tracker = AccountHistoryTracker(window_hours=24)

        logger.info("ComplianceEngine initialized")

    def check_transaction(
        self,
        tx_data: dict[str, Any],
        customer_info: dict[str, Any] | None = None,
    ) -> ComplianceResult:
        """
        Perform comprehensive compliance check on a transaction.

        Args:
            tx_data: Transaction data dictionary
            customer_info: Additional customer information

        Returns:
            ComplianceResult with all violations
        """
        transaction_id = tx_data.get("transaction_id", "unknown")
        violations: list[ComplianceViolation] = []
        checked_rules: list[str] = []
        requires_str = False
        requires_ctr = False

        # Extract transaction details
        amount = float(tx_data.get("amount", 0))
        sender_iban = tx_data.get("sender_iban", "")
        sender_name = tx_data.get("sender_name", "")
        receiver_name = tx_data.get("receiver_name", "")
        description = tx_data.get("description", "")
        sender_city = tx_data.get("sender_city", "")
        receiver_city = tx_data.get("receiver_city", "")

        # Parse timestamp
        ts_str = tx_data.get("timestamp", "")
        try:
            if "T" in ts_str:
                timestamp = datetime.fromisoformat(ts_str.replace("Z", ""))
            else:
                timestamp = datetime.now()
        except:
            timestamp = datetime.now()

        # =====================================================================
        # Check 1: MASAK Cash Thresholds
        # =====================================================================
        checked_rules.append(ComplianceRule.MASAK_CASH_SINGLE.value)

        if amount >= self._thresholds["cash_single_report"]:
            violations.append(
                ComplianceViolation(
                    rule=ComplianceRule.MASAK_CASH_SINGLE,
                    severity=RiskLevel.HIGH,
                    message=f"İşlem tutarı MASAK nakit bildirim eşiğini aşıyor",
                    threshold=self._thresholds["cash_single_report"],
                    actual_value=amount,
                )
            )
            requires_ctr = True

        # =====================================================================
        # Check 2: High-Risk Wire Transfer
        # =====================================================================
        checked_rules.append(ComplianceRule.MASAK_WIRE_HIGH.value)

        if amount >= self._thresholds["wire_high_risk"]:
            violations.append(
                ComplianceViolation(
                    rule=ComplianceRule.MASAK_WIRE_HIGH,
                    severity=RiskLevel.HIGH,
                    message=f"Yüksek tutarlı havale/EFT işlemi",
                    threshold=self._thresholds["wire_high_risk"],
                    actual_value=amount,
                )
            )
            requires_str = True

        # =====================================================================
        # Check 3: Velocity Checks
        # =====================================================================
        if self._enable_velocity:
            checked_rules.append(ComplianceRule.VELOCITY_1H.value)
            checked_rules.append(ComplianceRule.VELOCITY_24H.value)

            count_1h, amount_1h = self._history_tracker.get_velocity(sender_iban, hours=1)
            count_24h, amount_24h = self._history_tracker.get_velocity(sender_iban, hours=24)

            # 1-hour velocity
            if count_1h >= self._thresholds["velocity_1h_count"]:
                violations.append(
                    ComplianceViolation(
                        rule=ComplianceRule.VELOCITY_1H,
                        severity=RiskLevel.MEDIUM,
                        message=f"Son 1 saatte çok sayıda işlem: {count_1h} adet",
                        threshold=self._thresholds["velocity_1h_count"],
                        actual_value=count_1h,
                    )
                )

            if amount_1h >= self._thresholds["velocity_1h_amount"]:
                violations.append(
                    ComplianceViolation(
                        rule=ComplianceRule.VELOCITY_1H,
                        severity=RiskLevel.HIGH,
                        message=f"Son 1 saatte yüksek işlem hacmi: {amount_1h:,.2f} TL",
                        threshold=self._thresholds["velocity_1h_amount"],
                        actual_value=amount_1h,
                    )
                )

            # 24-hour velocity
            if count_24h >= self._thresholds["velocity_24h_count"]:
                violations.append(
                    ComplianceViolation(
                        rule=ComplianceRule.VELOCITY_24H,
                        severity=RiskLevel.MEDIUM,
                        message=f"Son 24 saatte çok sayıda işlem: {count_24h} adet",
                        threshold=self._thresholds["velocity_24h_count"],
                        actual_value=count_24h,
                    )
                )

            # Record this transaction
            self._history_tracker.add_transaction(sender_iban, amount, timestamp, transaction_id)

        # =====================================================================
        # Check 4: Structuring Detection (Smurfing)
        # =====================================================================
        checked_rules.append(ComplianceRule.MASAK_STRUCTURING.value)

        # Check if amount is just below reporting threshold (suspicious)
        structuring_threshold = self._thresholds["structuring_threshold"]
        if structuring_threshold * 0.9 <= amount < structuring_threshold:
            violations.append(
                ComplianceViolation(
                    rule=ComplianceRule.MASAK_STRUCTURING,
                    severity=RiskLevel.MEDIUM,
                    message=f"Parçalama şüphesi: Tutar bildirim eşiğinin hemen altında",
                    threshold=structuring_threshold,
                    actual_value=amount,
                )
            )

        # =====================================================================
        # Check 5: PEP Screening
        # =====================================================================
        if self._enable_pep:
            checked_rules.append(ComplianceRule.PEP_CHECK.value)

            pep_threshold = self._thresholds["pep_threshold"]

            if self._is_pep(sender_name) or self._is_pep(receiver_name):
                if amount >= pep_threshold:
                    violations.append(
                        ComplianceViolation(
                            rule=ComplianceRule.PEP_CHECK,
                            severity=RiskLevel.HIGH,
                            message="Siyasi açıdan maruz kişi (PEP) ile işlem",
                            threshold=pep_threshold,
                            actual_value=amount,
                            details={
                                "pep_sender": self._is_pep(sender_name),
                                "pep_receiver": self._is_pep(receiver_name),
                            },
                        )
                    )
                    requires_str = True

        # =====================================================================
        # Check 6: Sanctions Screening
        # =====================================================================
        if self._enable_sanctions:
            checked_rules.append(ComplianceRule.SANCTIONS_OFAC.value)

            if self._is_sanctioned(sender_name) or self._is_sanctioned(receiver_name):
                violations.append(
                    ComplianceViolation(
                        rule=ComplianceRule.SANCTIONS_OFAC,
                        severity=RiskLevel.CRITICAL,
                        message="Yaptırım listesindeki kişi ile işlem!",
                        details={
                            "sanctioned_sender": self._is_sanctioned(sender_name),
                            "sanctioned_receiver": self._is_sanctioned(receiver_name),
                        },
                    )
                )
                requires_str = True

        # =====================================================================
        # Check 7: High-Risk Country
        # =====================================================================
        checked_rules.append(ComplianceRule.HIGH_RISK_COUNTRY.value)

        if self._is_high_risk_country(sender_city) or self._is_high_risk_country(receiver_city):
            violations.append(
                ComplianceViolation(
                    rule=ComplianceRule.HIGH_RISK_COUNTRY,
                    severity=RiskLevel.HIGH,
                    message="Yüksek riskli ülke ile işlem",
                    details={
                        "sender_city": sender_city,
                        "receiver_city": receiver_city,
                    },
                )
            )
            requires_str = True

        # =====================================================================
        # Determine Overall Risk Level
        # =====================================================================
        risk_level = self._calculate_risk_level(violations)
        is_compliant = len(violations) == 0

        # Auto-require STR for critical violations
        if risk_level == RiskLevel.CRITICAL:
            requires_str = True

        result = ComplianceResult(
            transaction_id=transaction_id,
            is_compliant=is_compliant,
            risk_level=risk_level,
            violations=violations,
            requires_str=requires_str,
            requires_ctr=requires_ctr,
            checked_rules=checked_rules,
        )

        if violations:
            logger.warning(f"Compliance violations: {result.summary()}")

        return result

    def _is_pep(self, name: str) -> bool:
        """Check if name is in PEP list."""
        if not name:
            return False
        name_lower = name.lower()
        return any(pep.lower() in name_lower for pep in DEMO_PEP_LIST)

    def _is_sanctioned(self, name: str) -> bool:
        """Check if name is in sanctions list."""
        if not name:
            return False
        name_lower = name.lower()
        return any(s.lower() in name_lower for s in DEMO_SANCTIONS_LIST)

    def _is_high_risk_country(self, location: str) -> bool:
        """Check if location is in high-risk country."""
        if not location:
            return False
        location_lower = location.lower()
        return any(c.lower() in location_lower for c in HIGH_RISK_COUNTRIES)

    def _calculate_risk_level(self, violations: list[ComplianceViolation]) -> RiskLevel:
        """Calculate overall risk level from violations."""
        if not violations:
            return RiskLevel.LOW

        # Get highest severity
        severities = [v.severity for v in violations]

        if RiskLevel.CRITICAL in severities:
            return RiskLevel.CRITICAL
        elif RiskLevel.HIGH in severities:
            return RiskLevel.HIGH
        elif RiskLevel.MEDIUM in severities:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW

    def get_threshold(self, name: str) -> float:
        """Get a threshold value."""
        return self._thresholds.get(name, 0.0)

    def set_threshold(self, name: str, value: float) -> None:
        """Set a threshold value."""
        self._thresholds[name] = value
        logger.info(f"Threshold updated: {name} = {value}")

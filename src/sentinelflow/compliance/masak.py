# =============================================================================
# SentinelFlow - MASAK Compliance Module
# =============================================================================
"""
MASAK (Mali Suçları Araştırma Kurulu) compliance and reporting.

This module implements:
- Suspicious Transaction Report (STR/ŞİB) generation
- MASAK reporting format compliance
- Automatic threshold-based reporting triggers
- Report archiving and tracking

MASAK Requirements (5549 Sayılı Kanun):
- Financial institutions must report suspicious transactions
- Reports must include specific information about parties and transactions
- Reports must be filed within 10 business days
- All reports must be archived for at least 8 years

Reference:
- MASAK Website: https://www.masak.gov.tr
- 5549 Sayılı Suç Gelirlerinin Aklanmasının Önlenmesi Hakkında Kanun
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from loguru import logger


# =============================================================================
# Constants and Enums
# =============================================================================


class ReportType(str, Enum):
    """MASAK report types."""

    STR = "SIB"  # Şüpheli İşlem Bildirimi (Suspicious Transaction Report)
    CTR = "NIB"  # Nakit İşlem Bildirimi (Cash Transaction Report)
    ESTR = "ESIB"  # Elektronik Şüpheli İşlem Bildirimi


class SuspicionCategory(str, Enum):
    """Şüphe kategorileri - Categories of suspicion."""

    MONEY_LAUNDERING = "AKLAMA"  # Para Aklama
    TERRORIST_FINANCING = "TEROR_FINANSMANI"  # Terör Finansmanı
    FRAUD = "DOLANDIRICILIK"  # Dolandırıcılık
    TAX_EVASION = "VERGI_KACAKCILIK"  # Vergi Kaçakçılığı
    EMBEZZLEMENT = "ZIMMET"  # Zimmet
    OTHER = "DIGER"  # Diğer


class ReportStatus(str, Enum):
    """Report submission status."""

    DRAFT = "TASLAK"
    PENDING = "BEKLEMEDE"
    SUBMITTED = "GONDERILDI"
    ACKNOWLEDGED = "ALINDI"
    UNDER_REVIEW = "INCELEMEDE"
    CLOSED = "KAPANDI"


# MASAK thresholds (as of 2024)
MASAK_THRESHOLDS = {
    "cash_single": 85_000.0,  # Tek seferde nakit işlem bildirimi
    "cash_monthly": 340_000.0,  # Aylık toplam nakit işlem
    "wire_single": 50_000.0,  # Tek seferde havale/EFT şüpheli eşik
    "high_risk": 250_000.0,  # Yüksek riskli işlem eşiği
    "pep_threshold": 25_000.0,  # PEP için düşük eşik
}


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class PartyInfo:
    """
    Taraf bilgileri - Information about a party in a transaction.

    MASAK requires detailed information about both sender and receiver.
    """

    # Kimlik Bilgileri
    full_name: str = ""
    tc_kimlik_no: str = ""  # T.C. Kimlik Numarası
    passport_no: str = ""
    nationality: str = "TC"
    birth_date: str = ""
    birth_place: str = ""

    # İletişim Bilgileri
    address: str = ""
    city: str = ""
    postal_code: str = ""
    phone: str = ""
    email: str = ""

    # Hesap Bilgileri
    iban: str = ""
    account_type: str = ""
    bank_name: str = ""
    branch_code: str = ""

    # Meslek ve Gelir
    occupation: str = ""
    employer: str = ""
    monthly_income: float = 0.0

    # Risk Bilgileri
    is_pep: bool = False  # Politically Exposed Person
    risk_level: str = "DUSUK"  # DUSUK, ORTA, YUKSEK
    customer_since: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kimlik": {
                "ad_soyad": self.full_name,
                "tc_kimlik": self.tc_kimlik_no,
                "pasaport": self.passport_no,
                "uyruk": self.nationality,
                "dogum_tarihi": self.birth_date,
                "dogum_yeri": self.birth_place,
            },
            "iletisim": {
                "adres": self.address,
                "sehir": self.city,
                "posta_kodu": self.postal_code,
                "telefon": self.phone,
                "email": self.email,
            },
            "hesap": {
                "iban": self.iban,
                "hesap_turu": self.account_type,
                "banka": self.bank_name,
                "sube_kodu": self.branch_code,
            },
            "meslek_gelir": {
                "meslek": self.occupation,
                "isveren": self.employer,
                "aylik_gelir": self.monthly_income,
            },
            "risk": {
                "pep": self.is_pep,
                "risk_seviyesi": self.risk_level,
                "musteri_baslangic": self.customer_since,
            },
        }


@dataclass
class TransactionDetails:
    """İşlem detayları - Transaction details for MASAK report."""

    transaction_id: str = ""
    transaction_type: str = ""  # HAVALE, EFT, NAKIT, KRIPTO
    amount: float = 0.0
    currency: str = "TRY"
    description: str = ""
    timestamp: str = ""
    channel: str = ""  # INTERNET, MOBIL, SUBE, ATM

    # Related transactions
    related_transaction_ids: list[str] = field(default_factory=list)
    total_related_amount: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "islem_id": self.transaction_id,
            "islem_turu": self.transaction_type,
            "tutar": self.amount,
            "para_birimi": self.currency,
            "aciklama": self.description,
            "tarih_saat": self.timestamp,
            "kanal": self.channel,
            "iliskili_islemler": {
                "islem_idleri": self.related_transaction_ids,
                "toplam_tutar": self.total_related_amount,
            },
        }


@dataclass
class STRReport:
    """
    Şüpheli İşlem Bildirimi (STR) - Suspicious Transaction Report.

    Complete MASAK-compliant suspicious transaction report format.
    """

    # Rapor Kimlik Bilgileri
    report_id: str = field(default_factory=lambda: f"STR-{uuid.uuid4().hex[:12].upper()}")
    report_type: ReportType = ReportType.STR
    status: ReportStatus = ReportStatus.DRAFT
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    submitted_at: str | None = None

    # Bildiren Kuruluş
    reporting_institution: str = "SentinelFlow Demo Bankası"
    reporting_branch: str = "Merkez Şube"
    reporting_officer: str = ""
    reporting_officer_title: str = "Uyum Görevlisi"

    # Taraflar
    sender: PartyInfo = field(default_factory=PartyInfo)
    receiver: PartyInfo = field(default_factory=PartyInfo)

    # İşlem Bilgileri
    transaction: TransactionDetails = field(default_factory=TransactionDetails)

    # Şüphe Bilgileri
    suspicion_category: SuspicionCategory = SuspicionCategory.MONEY_LAUNDERING
    suspicion_indicators: list[str] = field(default_factory=list)
    suspicion_description: str = ""
    risk_score: float = 0.0

    # İlgili Tespitler
    fraud_type: str = ""
    detection_method: str = ""
    ml_confidence: float = 0.0
    evidence: dict[str, Any] = field(default_factory=dict)

    # Ek Bilgiler
    additional_notes: str = ""
    attachments: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to MASAK-compatible dictionary format."""
        return {
            "bildirim_bilgileri": {
                "bildirim_no": self.report_id,
                "bildirim_turu": self.report_type.value,
                "durum": self.status.value,
                "olusturma_tarihi": self.created_at,
                "gonderim_tarihi": self.submitted_at,
            },
            "bildiren_kurulus": {
                "kurum_adi": self.reporting_institution,
                "sube": self.reporting_branch,
                "yetkili": self.reporting_officer,
                "unvan": self.reporting_officer_title,
            },
            "gonderen": self.sender.to_dict(),
            "alici": self.receiver.to_dict(),
            "islem": self.transaction.to_dict(),
            "suphe_bilgileri": {
                "kategori": self.suspicion_category.value,
                "gostergeler": self.suspicion_indicators,
                "aciklama": self.suspicion_description,
                "risk_skoru": self.risk_score,
            },
            "tespit_bilgileri": {
                "dolandiricilik_turu": self.fraud_type,
                "tespit_yontemi": self.detection_method,
                "ml_guven": self.ml_confidence,
                "kanitlar": self.evidence,
            },
            "ek_bilgiler": {
                "notlar": self.additional_notes,
                "ekler": self.attachments,
            },
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    def to_xml(self) -> str:
        """Convert to MASAK XML format."""
        # Simplified XML representation
        data = self.to_dict()

        def dict_to_xml(d: dict, root_name: str = "root") -> str:
            xml_parts = [f"<{root_name}>"]
            for key, value in d.items():
                if isinstance(value, dict):
                    xml_parts.append(dict_to_xml(value, key))
                elif isinstance(value, list):
                    xml_parts.append(f"<{key}>")
                    for item in value:
                        if isinstance(item, dict):
                            xml_parts.append(dict_to_xml(item, "item"))
                        else:
                            xml_parts.append(f"<item>{item}</item>")
                    xml_parts.append(f"</{key}>")
                else:
                    xml_parts.append(f"<{key}>{value}</{key}>")
            xml_parts.append(f"</{root_name}>")
            return "\n".join(xml_parts)

        xml_header = '<?xml version="1.0" encoding="UTF-8"?>\n'
        return xml_header + dict_to_xml(data, "MASAK_STR")


# =============================================================================
# MASAK Reporter
# =============================================================================


class MASAKReporter:
    """
    MASAK Şüpheli İşlem Bildirim Sistemi.

    Generates, validates, and manages MASAK-compliant suspicious
    transaction reports based on detected fraud alerts.

    Example:
        >>> reporter = MASAKReporter()
        >>> str_report = reporter.create_str_from_alert(fraud_alert)
        >>> reporter.submit_report(str_report)
    """

    def __init__(
        self,
        institution_name: str = "SentinelFlow Demo Bankası",
        output_dir: str = "reports/masak",
        auto_archive: bool = True,
    ):
        """
        Initialize MASAK reporter.

        Args:
            institution_name: Reporting institution name
            output_dir: Directory for report storage
            auto_archive: Automatically archive generated reports
        """
        self.institution_name = institution_name
        self.output_dir = Path(output_dir)
        self.auto_archive = auto_archive

        self._reports: dict[str, STRReport] = {}
        self._report_counter = 0

        if auto_archive:
            self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"MASAKReporter initialized for {institution_name}")

    def create_str_from_alert(
        self,
        alert_data: dict[str, Any],
        additional_info: dict[str, Any] | None = None,
    ) -> STRReport:
        """
        Create an STR report from a fraud alert.

        Args:
            alert_data: Fraud alert dictionary from detector
            additional_info: Additional customer/transaction info

        Returns:
            STRReport ready for review/submission
        """
        self._report_counter += 1

        # Map fraud type to suspicion category
        fraud_type = alert_data.get("fraud_type", "")
        suspicion_category = self._map_fraud_to_suspicion(fraud_type)

        # Generate suspicion indicators
        indicators = self._generate_indicators(alert_data)

        # Build report
        report = STRReport(
            reporting_institution=self.institution_name,
            reporting_officer="Otomatik Sistem",
            reporting_officer_title="AI Fraud Detection System",
            # Sender info
            sender=PartyInfo(
                full_name=alert_data.get("sender_name", "Bilinmiyor"),
                iban=alert_data.get("sender_iban", ""),
            ),
            # Receiver info
            receiver=PartyInfo(
                full_name=alert_data.get("receiver_name", "Bilinmiyor"),
                iban=alert_data.get("receiver_iban", ""),
            ),
            # Transaction info
            transaction=TransactionDetails(
                transaction_id=alert_data.get("transaction_id", ""),
                amount=float(alert_data.get("amount", 0)),
                description=alert_data.get("description", ""),
                timestamp=alert_data.get("detected_at", datetime.now(timezone.utc).isoformat()),
                related_transaction_ids=alert_data.get("related_transactions", []),
            ),
            # Suspicion info
            suspicion_category=suspicion_category,
            suspicion_indicators=indicators,
            suspicion_description=self._generate_description(alert_data),
            risk_score=float(alert_data.get("confidence", 0)) * 100,
            # Detection info
            fraud_type=fraud_type,
            detection_method="SentinelFlow ML Ensemble",
            ml_confidence=float(alert_data.get("confidence", 0)),
            evidence=alert_data.get("evidence", {}),
        )

        # Store report
        self._reports[report.report_id] = report

        # Archive if enabled
        if self.auto_archive:
            self._archive_report(report)

        logger.info(f"STR created: {report.report_id}")

        return report

    def _map_fraud_to_suspicion(self, fraud_type: str) -> SuspicionCategory:
        """Map fraud type to MASAK suspicion category."""
        mapping = {
            "circular_ring": SuspicionCategory.MONEY_LAUNDERING,
            "impossible_travel": SuspicionCategory.FRAUD,
            "blacklist_keyword": SuspicionCategory.MONEY_LAUNDERING,
            "ai_detected_anomaly": SuspicionCategory.FRAUD,
            "ml_ensemble": SuspicionCategory.FRAUD,
            "mule_account": SuspicionCategory.MONEY_LAUNDERING,
        }
        return mapping.get(fraud_type, SuspicionCategory.OTHER)

    def _generate_indicators(self, alert_data: dict[str, Any]) -> list[str]:
        """Generate list of suspicion indicators."""
        indicators = []
        fraud_type = alert_data.get("fraud_type", "")
        evidence = alert_data.get("evidence", {})

        if fraud_type == "circular_ring":
            indicators.append("Döngüsel para transferi tespit edildi")
            indicators.append("Kısa sürede birden fazla hesap arasında para akışı")
            if "ring_path" in evidence:
                indicators.append(f"Halka içinde {len(evidence['ring_path'])} hesap")

        elif fraud_type == "impossible_travel":
            indicators.append("Coğrafi olarak imkansız işlem lokasyonu")
            if "distance_km" in evidence:
                indicators.append(f"İmkansız seyahat: {evidence['distance_km']} km")

        elif fraud_type == "blacklist_keyword":
            indicators.append("Şüpheli anahtar kelime tespit edildi")
            if "keywords_found" in evidence:
                indicators.append(f"Kelimeler: {', '.join(evidence['keywords_found'])}")

        elif fraud_type in ("ai_detected_anomaly", "ml_ensemble"):
            indicators.append("Yapay zeka anomali tespiti")
            indicators.append("Normal işlem kalıplarından sapma")
            if "ensemble_score" in evidence:
                indicators.append(f"ML Risk Skoru: {evidence['ensemble_score']:.2f}")

        # Amount-based indicators
        amount = float(alert_data.get("amount", 0))
        if amount >= MASAK_THRESHOLDS["high_risk"]:
            indicators.append(f"Yüksek tutarlı işlem: {amount:,.2f} TL")

        return indicators

    def _generate_description(self, alert_data: dict[str, Any]) -> str:
        """Generate human-readable suspicion description."""
        fraud_type = alert_data.get("fraud_type", "")
        description = alert_data.get("description", "")
        confidence = float(alert_data.get("confidence", 0)) * 100

        base = f"SentinelFlow otomatik tespit sistemi tarafından "

        type_descriptions = {
            "circular_ring": "para aklama şüphesi içeren döngüsel işlem halkası",
            "impossible_travel": "coğrafi olarak imkansız konumdan yapılan şüpheli işlem",
            "blacklist_keyword": "şüpheli anahtar kelimeler içeren işlem",
            "ai_detected_anomaly": "yapay zeka tarafından anomali olarak işaretlenen işlem",
            "ml_ensemble": "çoklu ML modeli ile dolandırıcılık olarak sınıflandırılan işlem",
        }

        type_desc = type_descriptions.get(fraud_type, "şüpheli işlem")

        return (
            f"{base}{type_desc} tespit edilmiştir. "
            f"Güven skoru: %{confidence:.1f}. "
            f"Detay: {description}"
        )

    def _archive_report(self, report: STRReport) -> None:
        """Archive report to disk."""
        # Create year/month subdirectory
        now = datetime.now()
        subdir = self.output_dir / f"{now.year}" / f"{now.month:02d}"
        subdir.mkdir(parents=True, exist_ok=True)

        # Save JSON
        json_path = subdir / f"{report.report_id}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(report.to_json())

        # Save XML
        xml_path = subdir / f"{report.report_id}.xml"
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(report.to_xml())

        logger.debug(f"Report archived: {json_path}")

    def submit_report(self, report: STRReport) -> bool:
        """
        Submit report to MASAK (simulated).

        In production, this would integrate with MASAK's electronic
        reporting system.

        Args:
            report: STRReport to submit

        Returns:
            True if submission successful
        """
        # Validate report
        if not self._validate_report(report):
            logger.error(f"Report validation failed: {report.report_id}")
            return False

        # Simulate submission
        report.status = ReportStatus.SUBMITTED
        report.submitted_at = datetime.now(timezone.utc).isoformat()

        # Update archive
        if self.auto_archive:
            self._archive_report(report)

        logger.info(f"Report submitted to MASAK: {report.report_id}")

        return True

    def _validate_report(self, report: STRReport) -> bool:
        """Validate report completeness for MASAK submission."""
        errors = []

        # Required fields
        if not report.sender.iban:
            errors.append("Gönderen IBAN eksik")
        if not report.receiver.iban:
            errors.append("Alıcı IBAN eksik")
        if report.transaction.amount <= 0:
            errors.append("İşlem tutarı geçersiz")
        if not report.suspicion_description:
            errors.append("Şüphe açıklaması eksik")

        if errors:
            for error in errors:
                logger.warning(f"Validation error: {error}")
            return False

        return True

    def get_pending_reports(self) -> list[STRReport]:
        """Get all pending (unsubmitted) reports."""
        return [
            r
            for r in self._reports.values()
            if r.status in (ReportStatus.DRAFT, ReportStatus.PENDING)
        ]

    def get_report(self, report_id: str) -> STRReport | None:
        """Get a specific report by ID."""
        return self._reports.get(report_id)

    def get_report_statistics(self) -> dict[str, Any]:
        """Get statistics about generated reports."""
        total = len(self._reports)
        by_status = {}
        by_category = {}
        total_amount = 0.0

        for report in self._reports.values():
            status = report.status.value
            by_status[status] = by_status.get(status, 0) + 1

            category = report.suspicion_category.value
            by_category[category] = by_category.get(category, 0) + 1

            total_amount += report.transaction.amount

        return {
            "total_reports": total,
            "by_status": by_status,
            "by_category": by_category,
            "total_amount": total_amount,
            "pending_count": len(self.get_pending_reports()),
        }

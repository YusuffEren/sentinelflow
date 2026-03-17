# =============================================================================
# SentinelFlow - Customer Risk Scorer
# =============================================================================
"""
ML-based customer risk scoring for KYC/AML compliance.

Implements a multi-factor risk scoring model that considers:
- Customer demographics
- Transaction behavior
- Geographic risk
- Business type risk
- Relationship duration
- Account activity patterns

The scoring model follows FATF risk-based approach guidelines.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
import math

from loguru import logger


# =============================================================================
# Enums and Constants
# =============================================================================

class RiskCategory(str, Enum):
    """Customer risk categories."""
    
    LOW = "DUSUK"
    MEDIUM = "ORTA"
    HIGH = "YUKSEK"
    PROHIBITED = "YASAK"


class CustomerType(str, Enum):
    """Customer types."""
    
    INDIVIDUAL = "BIREYSEL"
    CORPORATE = "KURUMSAL"
    SME = "KOBI"
    GOVERNMENT = "KAMU"
    NGO = "STK"
    FINANCIAL = "FINANSAL"


# Risk weights for different factors
RISK_WEIGHTS = {
    "country": 0.20,
    "occupation": 0.15,
    "business_type": 0.15,
    "transaction_volume": 0.15,
    "account_age": 0.10,
    "pep_status": 0.15,
    "source_of_funds": 0.10,
}

# High-risk countries (FATF grey/black list)
HIGH_RISK_COUNTRIES = {
    "North Korea": 10,
    "Iran": 10,
    "Myanmar": 8,
    "Syria": 9,
    "Yemen": 7,
    "Afghanistan": 8,
    "South Sudan": 7,
    "Pakistan": 5,
    "Nigeria": 5,
    "Libya": 7,
}

# Medium-risk countries
MEDIUM_RISK_COUNTRIES = {
    "UAE": 4,
    "Panama": 4,
    "Cayman Islands": 4,
    "Switzerland": 3,
    "Malta": 3,
    "Cyprus": 3,
}

# High-risk occupations
HIGH_RISK_OCCUPATIONS = {
    "Politikacı": 8,
    "Kamu Görevlisi (Üst Düzey)": 7,
    "Silah Tüccarı": 9,
    "Kuyumcu": 6,
    "Döviz Bürosu": 7,
    "Casino İşletmecisi": 8,
    "Kripto Para Tüccarı": 6,
    "İnşaat": 5,
    "Emlak": 5,
    "Avukat": 4,
    "Muhasebeci": 4,
}

# High-risk business types
HIGH_RISK_BUSINESS_TYPES = {
    "Money Services Business": 8,
    "Virtual Asset Service Provider": 7,
    "Casino / Gaming": 8,
    "Precious Metals / Jewelry": 6,
    "Real Estate": 5,
    "Import / Export": 5,
    "Non-Profit": 4,
    "Defense / Arms": 9,
}


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class CustomerProfile:
    """Customer profile for risk assessment."""
    
    # Identity
    customer_id: str = ""
    customer_type: CustomerType = CustomerType.INDIVIDUAL
    full_name: str = ""
    nationality: str = "TC"
    country_of_residence: str = "Türkiye"
    
    # Demographics
    date_of_birth: str = ""
    occupation: str = ""
    employer: str = ""
    
    # Business (for corporate)
    business_type: str = ""
    industry: str = ""
    annual_revenue: float = 0.0
    employee_count: int = 0
    
    # Account
    account_open_date: str = ""
    declared_monthly_income: float = 0.0
    source_of_funds: str = ""
    purpose_of_account: str = ""
    
    # Flags
    is_pep: bool = False
    is_pep_relative: bool = False
    has_adverse_media: bool = False
    previous_str_filed: bool = False
    
    # Transaction patterns (last 12 months)
    monthly_transaction_count: float = 0.0
    monthly_transaction_volume: float = 0.0
    international_transaction_ratio: float = 0.0
    cash_transaction_ratio: float = 0.0
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "customer_type": self.customer_type.value,
            "full_name": self.full_name,
            "nationality": self.nationality,
            "country_of_residence": self.country_of_residence,
            "occupation": self.occupation,
            "business_type": self.business_type,
            "is_pep": self.is_pep,
            "is_pep_relative": self.is_pep_relative,
            "declared_monthly_income": self.declared_monthly_income,
            "source_of_funds": self.source_of_funds,
        }


@dataclass
class RiskFactor:
    """Individual risk factor assessment."""
    
    name: str
    score: float  # 0-10
    weight: float
    weighted_score: float
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskAssessment:
    """Complete customer risk assessment result."""
    
    customer_id: str
    overall_score: float  # 0-100
    risk_category: RiskCategory
    risk_factors: list[RiskFactor] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    required_actions: list[str] = field(default_factory=list)
    assessment_date: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    next_review_date: str = ""
    assessed_by: str = "SentinelFlow AI"
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "overall_score": round(self.overall_score, 2),
            "risk_category": self.risk_category.value,
            "risk_factors": [
                {
                    "name": f.name,
                    "score": round(f.score, 2),
                    "weight": f.weight,
                    "weighted_score": round(f.weighted_score, 2),
                    "reason": f.reason,
                }
                for f in self.risk_factors
            ],
            "recommendations": self.recommendations,
            "required_actions": self.required_actions,
            "assessment_date": self.assessment_date,
            "next_review_date": self.next_review_date,
        }
    
    def summary(self) -> str:
        """Generate summary string."""
        top_factors = sorted(self.risk_factors, key=lambda x: x.weighted_score, reverse=True)[:3]
        factors_str = ", ".join(f"{f.name}({f.score:.1f})" for f in top_factors)
        
        return (
            f"Customer: {self.customer_id[:12]}... | "
            f"Score: {self.overall_score:.1f}/100 | "
            f"Category: {self.risk_category.value} | "
            f"Top Factors: {factors_str}"
        )


# =============================================================================
# Customer Risk Scorer
# =============================================================================

class CustomerRiskScorer:
    """
    Multi-factor customer risk scoring engine.
    
    Calculates a risk score (0-100) based on multiple factors
    weighted according to regulatory best practices.
    
    Example:
        >>> scorer = CustomerRiskScorer()
        >>> profile = CustomerProfile(
        ...     customer_id="C12345",
        ...     full_name="Test Customer",
        ...     occupation="Kuyumcu",
        ...     is_pep=True,
        ... )
        >>> result = scorer.assess(profile)
        >>> print(result.summary())
    """
    
    def __init__(
        self,
        weights: dict[str, float] | None = None,
        high_threshold: float = 60.0,
        medium_threshold: float = 30.0,
    ):
        """
        Initialize risk scorer.
        
        Args:
            weights: Custom risk factor weights
            high_threshold: Score threshold for HIGH risk
            medium_threshold: Score threshold for MEDIUM risk
        """
        self._weights = {**RISK_WEIGHTS, **(weights or {})}
        self._high_threshold = high_threshold
        self._medium_threshold = medium_threshold
        
        # Normalize weights to sum to 1.0
        total_weight = sum(self._weights.values())
        self._weights = {k: v / total_weight for k, v in self._weights.items()}
        
        logger.info("CustomerRiskScorer initialized")
    
    def assess(self, profile: CustomerProfile) -> RiskAssessment:
        """
        Perform comprehensive risk assessment.
        
        Args:
            profile: Customer profile to assess
        
        Returns:
            RiskAssessment with scores and recommendations
        """
        factors: list[RiskFactor] = []
        
        # =====================================================================
        # Factor 1: Country Risk
        # =====================================================================
        country_score, country_reason = self._assess_country_risk(profile)
        factors.append(RiskFactor(
            name="Ülke Riski",
            score=country_score,
            weight=self._weights["country"],
            weighted_score=country_score * self._weights["country"],
            reason=country_reason,
            details={
                "nationality": profile.nationality,
                "residence": profile.country_of_residence,
            },
        ))
        
        # =====================================================================
        # Factor 2: Occupation Risk
        # =====================================================================
        occ_score, occ_reason = self._assess_occupation_risk(profile)
        factors.append(RiskFactor(
            name="Meslek Riski",
            score=occ_score,
            weight=self._weights["occupation"],
            weighted_score=occ_score * self._weights["occupation"],
            reason=occ_reason,
            details={"occupation": profile.occupation},
        ))
        
        # =====================================================================
        # Factor 3: Business Type Risk (Corporate)
        # =====================================================================
        biz_score, biz_reason = self._assess_business_risk(profile)
        factors.append(RiskFactor(
            name="İş Türü Riski",
            score=biz_score,
            weight=self._weights["business_type"],
            weighted_score=biz_score * self._weights["business_type"],
            reason=biz_reason,
            details={"business_type": profile.business_type},
        ))
        
        # =====================================================================
        # Factor 4: Transaction Volume Risk
        # =====================================================================
        vol_score, vol_reason = self._assess_volume_risk(profile)
        factors.append(RiskFactor(
            name="İşlem Hacmi Riski",
            score=vol_score,
            weight=self._weights["transaction_volume"],
            weighted_score=vol_score * self._weights["transaction_volume"],
            reason=vol_reason,
            details={
                "monthly_volume": profile.monthly_transaction_volume,
                "declared_income": profile.declared_monthly_income,
            },
        ))
        
        # =====================================================================
        # Factor 5: Account Age Risk
        # =====================================================================
        age_score, age_reason = self._assess_account_age_risk(profile)
        factors.append(RiskFactor(
            name="Hesap Yaşı Riski",
            score=age_score,
            weight=self._weights["account_age"],
            weighted_score=age_score * self._weights["account_age"],
            reason=age_reason,
        ))
        
        # =====================================================================
        # Factor 6: PEP Status Risk
        # =====================================================================
        pep_score, pep_reason = self._assess_pep_risk(profile)
        factors.append(RiskFactor(
            name="PEP Durumu",
            score=pep_score,
            weight=self._weights["pep_status"],
            weighted_score=pep_score * self._weights["pep_status"],
            reason=pep_reason,
            details={
                "is_pep": profile.is_pep,
                "is_pep_relative": profile.is_pep_relative,
            },
        ))
        
        # =====================================================================
        # Factor 7: Source of Funds Risk
        # =====================================================================
        sof_score, sof_reason = self._assess_source_of_funds_risk(profile)
        factors.append(RiskFactor(
            name="Fon Kaynağı Riski",
            score=sof_score,
            weight=self._weights["source_of_funds"],
            weighted_score=sof_score * self._weights["source_of_funds"],
            reason=sof_reason,
            details={"source_of_funds": profile.source_of_funds},
        ))
        
        # =====================================================================
        # Calculate Overall Score
        # =====================================================================
        overall_score = sum(f.weighted_score for f in factors) * 10  # Scale to 0-100
        
        # Additional penalty factors
        if profile.has_adverse_media:
            overall_score += 15
        if profile.previous_str_filed:
            overall_score += 20
        
        overall_score = min(100, overall_score)
        
        # =====================================================================
        # Determine Risk Category
        # =====================================================================
        if overall_score >= 90:
            risk_category = RiskCategory.PROHIBITED
        elif overall_score >= self._high_threshold:
            risk_category = RiskCategory.HIGH
        elif overall_score >= self._medium_threshold:
            risk_category = RiskCategory.MEDIUM
        else:
            risk_category = RiskCategory.LOW
        
        # =====================================================================
        # Generate Recommendations
        # =====================================================================
        recommendations = self._generate_recommendations(profile, factors, risk_category)
        required_actions = self._generate_required_actions(profile, risk_category)
        
        # Calculate next review date
        review_months = {
            RiskCategory.LOW: 24,
            RiskCategory.MEDIUM: 12,
            RiskCategory.HIGH: 6,
            RiskCategory.PROHIBITED: 0,
        }
        
        from datetime import timedelta
        review_date = datetime.now() + timedelta(days=30 * review_months.get(risk_category, 12))
        
        assessment = RiskAssessment(
            customer_id=profile.customer_id,
            overall_score=overall_score,
            risk_category=risk_category,
            risk_factors=factors,
            recommendations=recommendations,
            required_actions=required_actions,
            next_review_date=review_date.isoformat()[:10],
        )
        
        logger.info(f"Risk assessment completed: {assessment.summary()}")
        
        return assessment
    
    def _assess_country_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess country risk."""
        score = 0.0
        reasons = []
        
        # Check nationality
        if profile.nationality in HIGH_RISK_COUNTRIES:
            score = max(score, HIGH_RISK_COUNTRIES[profile.nationality])
            reasons.append(f"Yüksek riskli ülke vatandaşı: {profile.nationality}")
        elif profile.nationality in MEDIUM_RISK_COUNTRIES:
            score = max(score, MEDIUM_RISK_COUNTRIES[profile.nationality])
            reasons.append(f"Orta riskli ülke vatandaşı: {profile.nationality}")
        
        # Check residence
        if profile.country_of_residence in HIGH_RISK_COUNTRIES:
            score = max(score, HIGH_RISK_COUNTRIES[profile.country_of_residence])
            reasons.append(f"Yüksek riskli ülkede ikamet: {profile.country_of_residence}")
        
        if not reasons:
            reasons.append("Düşük riskli ülke")
        
        return score, "; ".join(reasons)
    
    def _assess_occupation_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess occupation risk."""
        occupation = profile.occupation
        
        if occupation in HIGH_RISK_OCCUPATIONS:
            return (
                HIGH_RISK_OCCUPATIONS[occupation],
                f"Yüksek riskli meslek: {occupation}",
            )
        
        # Check partial matches
        for occ, risk in HIGH_RISK_OCCUPATIONS.items():
            if occ.lower() in occupation.lower() or occupation.lower() in occ.lower():
                return risk * 0.8, f"Potansiyel yüksek riskli meslek: {occupation}"
        
        return 2.0, "Standart risk mesleği"
    
    def _assess_business_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess business type risk."""
        if profile.customer_type == CustomerType.INDIVIDUAL:
            return 0.0, "Bireysel müşteri - iş türü riski yok"
        
        business = profile.business_type
        
        if business in HIGH_RISK_BUSINESS_TYPES:
            return (
                HIGH_RISK_BUSINESS_TYPES[business],
                f"Yüksek riskli iş türü: {business}",
            )
        
        # Check partial matches
        for biz, risk in HIGH_RISK_BUSINESS_TYPES.items():
            if biz.lower() in business.lower():
                return risk * 0.8, f"Potansiyel yüksek riskli iş: {business}"
        
        return 2.0, "Standart iş türü"
    
    def _assess_volume_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess transaction volume vs declared income."""
        if profile.declared_monthly_income <= 0:
            return 5.0, "Beyan edilen gelir bilgisi yok"
        
        volume = profile.monthly_transaction_volume
        income = profile.declared_monthly_income
        
        ratio = volume / income
        
        if ratio > 5:
            return 8.0, f"İşlem hacmi gelirin {ratio:.1f} katı - çok yüksek"
        elif ratio > 3:
            return 6.0, f"İşlem hacmi gelirin {ratio:.1f} katı - yüksek"
        elif ratio > 2:
            return 4.0, f"İşlem hacmi gelirin {ratio:.1f} katı - orta"
        else:
            return 1.0, "İşlem hacmi gelir ile uyumlu"
    
    def _assess_account_age_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess account age risk."""
        if not profile.account_open_date:
            return 5.0, "Hesap açılış tarihi bilinmiyor"
        
        try:
            open_date = datetime.fromisoformat(profile.account_open_date)
            age_days = (datetime.now() - open_date).days
            age_months = age_days / 30
            
            if age_months < 3:
                return 7.0, f"Yeni hesap: {age_months:.0f} aylık"
            elif age_months < 12:
                return 4.0, f"Genç hesap: {age_months:.0f} aylık"
            elif age_months < 24:
                return 2.0, f"Orta yaşlı hesap: {age_months:.0f} aylık"
            else:
                return 1.0, f"Köklü hesap: {age_months/12:.1f} yıllık"
        except:
            return 5.0, "Hesap yaşı hesaplanamadı"
    
    def _assess_pep_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess PEP status risk."""
        if profile.is_pep:
            return 9.0, "Siyasi açıdan maruz kişi (PEP)"
        elif profile.is_pep_relative:
            return 7.0, "PEP yakını veya iş ortağı"
        else:
            return 0.0, "PEP değil"
    
    def _assess_source_of_funds_risk(self, profile: CustomerProfile) -> tuple[float, str]:
        """Assess source of funds risk."""
        sof = profile.source_of_funds.lower()
        
        high_risk_sources = ["miras", "piyango", "kripto", "yatırım", "bağış"]
        medium_risk_sources = ["kira", "satış"]
        low_risk_sources = ["maaş", "emekli", "nafaka"]
        
        if any(s in sof for s in high_risk_sources):
            return 6.0, f"Yüksek riskli fon kaynağı: {profile.source_of_funds}"
        elif any(s in sof for s in medium_risk_sources):
            return 3.0, f"Orta riskli fon kaynağı: {profile.source_of_funds}"
        elif any(s in sof for s in low_risk_sources):
            return 1.0, f"Düşük riskli fon kaynağı: {profile.source_of_funds}"
        else:
            return 4.0, "Fon kaynağı belirsiz veya standart"
    
    def _generate_recommendations(
        self,
        profile: CustomerProfile,
        factors: list[RiskFactor],
        category: RiskCategory,
    ) -> list[str]:
        """Generate recommendations based on assessment."""
        recommendations = []
        
        if category == RiskCategory.HIGH or category == RiskCategory.PROHIBITED:
            recommendations.append("Güçlendirilmiş Müşteri İncelemesi (EDD) gerekli")
            recommendations.append("İşlem izleme sıklığını artırın")
            recommendations.append("Üst yönetim onayı alın")
        
        if category == RiskCategory.MEDIUM:
            recommendations.append("Standart Müşteri İncelemesi (CDD) güncellemesi yapın")
            recommendations.append("Yıllık gözden geçirme planlayın")
        
        # Factor-specific recommendations
        for factor in factors:
            if factor.score >= 7:
                if "PEP" in factor.name:
                    recommendations.append("PEP ek belge talep edin")
                elif "Ülke" in factor.name:
                    recommendations.append("Coğrafi risk değerlendirmesi yapın")
                elif "Hacim" in factor.name:
                    recommendations.append("İşlem hacmi kaynağını doğrulayın")
        
        return recommendations
    
    def _generate_required_actions(
        self,
        profile: CustomerProfile,
        category: RiskCategory,
    ) -> list[str]:
        """Generate required actions."""
        actions = []
        
        if category == RiskCategory.PROHIBITED:
            actions.append("MÜŞTERİ İLİŞKİSİ SONLANDIRILMALI")
            actions.append("MASAK'a bildirim yapılmalı")
        elif category == RiskCategory.HIGH:
            actions.append("EDD süreci başlatılmalı")
            actions.append("Ek kimlik doğrulama gerekli")
            actions.append("Fon kaynağı belgesi talep edilmeli")
        elif category == RiskCategory.MEDIUM:
            actions.append("CDD güncelleme zamanı kontrol edilmeli")
        
        return actions

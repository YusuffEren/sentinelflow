# =============================================================================
# SentinelFlow - Customer Due Diligence Engine
# =============================================================================
"""
Customer Due Diligence (CDD) workflow engine.

Implements the regulatory CDD requirements including:
- Simplified Due Diligence (SDD) for low-risk customers
- Standard Due Diligence (CDD) for medium-risk customers
- Enhanced Due Diligence (EDD) for high-risk customers

Integrates with risk scoring, PEP screening, and sanctions checking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
import uuid

from loguru import logger

from sentinelflow.kyc.risk_scorer import CustomerRiskScorer, CustomerProfile, RiskAssessment, RiskCategory


# =============================================================================
# Enums
# =============================================================================

class DDLevel(str, Enum):
    """Due Diligence level."""
    
    SDD = "BASITLESTIRILMIS"  # Simplified Due Diligence
    CDD = "STANDART"  # Standard Due Diligence
    EDD = "GUCLENDIRILMIS"  # Enhanced Due Diligence


class CDDStatus(str, Enum):
    """CDD process status."""
    
    PENDING = "BEKLEMEDE"
    IN_PROGRESS = "DEVAM_EDIYOR"
    DOCUMENTS_REQUIRED = "BELGE_GEREKLI"
    UNDER_REVIEW = "INCELEMEDE"
    APPROVED = "ONAYLANDI"
    REJECTED = "REDDEDILDI"
    ESCALATED = "UST_YONETIME_AKTARILDI"


class DocumentType(str, Enum):
    """Required document types."""
    
    ID_CARD = "KIMLIK_KARTI"
    PASSPORT = "PASAPORT"
    PROOF_OF_ADDRESS = "ADRES_BELGESI"
    INCOME_PROOF = "GELIR_BELGESI"
    SOURCE_OF_FUNDS = "FON_KAYNAGI_BELGESI"
    COMPANY_DOCS = "SIRKET_BELGELERI"
    BENEFICIAL_OWNER = "GERCEK_FAYDALANICI"
    TAX_DECLARATION = "VERGI_BEYANNAMESI"
    BANK_STATEMENT = "BANKA_EKSTRESI"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class DocumentRequirement:
    """A required document for CDD."""
    
    document_type: DocumentType
    required: bool = True
    provided: bool = False
    verified: bool = False
    notes: str = ""
    provided_date: str | None = None
    verified_date: str | None = None


@dataclass
class CDDResult:
    """Result of CDD process."""
    
    cdd_id: str = field(default_factory=lambda: f"CDD-{uuid.uuid4().hex[:12].upper()}")
    customer_id: str = ""
    dd_level: DDLevel = DDLevel.CDD
    status: CDDStatus = CDDStatus.PENDING
    
    # Risk assessment
    risk_assessment: RiskAssessment | None = None
    risk_score: float = 0.0
    risk_category: RiskCategory = RiskCategory.MEDIUM
    
    # Documents
    required_documents: list[DocumentRequirement] = field(default_factory=list)
    documents_complete: bool = False
    
    # Verification
    identity_verified: bool = False
    address_verified: bool = False
    source_of_funds_verified: bool = False
    beneficial_owner_verified: bool = False
    
    # Screening results
    pep_screening_passed: bool = True
    sanctions_screening_passed: bool = True
    adverse_media_check_passed: bool = True
    
    # Workflow
    assigned_to: str = ""
    approval_by: str = ""
    rejection_reason: str = ""
    
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: str | None = None
    next_review_date: str | None = None
    
    # Notes
    notes: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "cdd_id": self.cdd_id,
            "customer_id": self.customer_id,
            "dd_level": self.dd_level.value,
            "status": self.status.value,
            "risk_score": round(self.risk_score, 2),
            "risk_category": self.risk_category.value,
            "documents_complete": self.documents_complete,
            "verifications": {
                "identity": self.identity_verified,
                "address": self.address_verified,
                "source_of_funds": self.source_of_funds_verified,
                "beneficial_owner": self.beneficial_owner_verified,
            },
            "screenings": {
                "pep": self.pep_screening_passed,
                "sanctions": self.sanctions_screening_passed,
                "adverse_media": self.adverse_media_check_passed,
            },
            "created_at": self.created_at,
            "status": self.status.value,
        }
    
    def summary(self) -> str:
        """Generate summary string."""
        return (
            f"CDD {self.cdd_id[:12]} | "
            f"Customer: {self.customer_id[:8]}... | "
            f"Level: {self.dd_level.value} | "
            f"Status: {self.status.value} | "
            f"Risk: {self.risk_score:.0f}/100 ({self.risk_category.value})"
        )


# =============================================================================
# CDD Engine
# =============================================================================

class CDDEngine:
    """
    Customer Due Diligence workflow engine.
    
    Orchestrates the complete CDD process including:
    1. Risk assessment
    2. DD level determination
    3. Document requirements
    4. Verification steps
    5. Approval workflow
    
    Example:
        >>> engine = CDDEngine()
        >>> result = engine.perform_cdd(customer_profile)
        >>> print(result.summary())
    """
    
    def __init__(
        self,
        auto_approve_low_risk: bool = False,
        require_manager_approval_high_risk: bool = True,
    ):
        """
        Initialize CDD engine.
        
        Args:
            auto_approve_low_risk: Automatically approve low-risk customers
            require_manager_approval_high_risk: Require manager approval for high-risk
        """
        self._risk_scorer = CustomerRiskScorer()
        self._auto_approve_low = auto_approve_low_risk
        self._require_manager_high = require_manager_approval_high_risk
        
        self._active_cdds: dict[str, CDDResult] = {}
        
        logger.info("CDDEngine initialized")
    
    def perform_cdd(
        self,
        profile: CustomerProfile,
        skip_risk_assessment: bool = False,
    ) -> CDDResult:
        """
        Perform Customer Due Diligence on a customer.
        
        Args:
            profile: Customer profile
            skip_risk_assessment: Use existing risk data
        
        Returns:
            CDDResult with workflow status
        """
        result = CDDResult(customer_id=profile.customer_id)
        
        # =====================================================================
        # Step 1: Risk Assessment
        # =====================================================================
        if not skip_risk_assessment:
            risk_assessment = self._risk_scorer.assess(profile)
            result.risk_assessment = risk_assessment
            result.risk_score = risk_assessment.overall_score
            result.risk_category = risk_assessment.risk_category
        
        # =====================================================================
        # Step 2: Determine DD Level
        # =====================================================================
        result.dd_level = self._determine_dd_level(result.risk_category, profile)
        
        # =====================================================================
        # Step 3: Generate Document Requirements
        # =====================================================================
        result.required_documents = self._get_required_documents(
            result.dd_level, 
            profile.customer_type.value,
        )
        
        # =====================================================================
        # Step 4: Initial Status
        # =====================================================================
        if result.risk_category == RiskCategory.PROHIBITED:
            result.status = CDDStatus.REJECTED
            result.rejection_reason = "Yasaklı risk kategorisi"
            result.completed_at = datetime.now(timezone.utc).isoformat()
            
        elif self._auto_approve_low and result.risk_category == RiskCategory.LOW:
            result.status = CDDStatus.APPROVED
            result.approval_by = "SentinelFlow Auto-Approval"
            result.completed_at = datetime.now(timezone.utc).isoformat()
            result.notes.append("Düşük risk - otomatik onay")
            
        elif result.dd_level == DDLevel.EDD:
            result.status = CDDStatus.DOCUMENTS_REQUIRED
            result.notes.append("EDD süreci başlatıldı - ek belgeler gerekli")
            if self._require_manager_high:
                result.notes.append("Üst yönetim onayı gerekecek")
                
        else:
            result.status = CDDStatus.IN_PROGRESS
            result.notes.append(f"{result.dd_level.value} süreci başlatıldı")
        
        # =====================================================================
        # Step 5: Set Review Date
        # =====================================================================
        result.next_review_date = self._calculate_review_date(result.risk_category)
        
        # Store active CDD
        self._active_cdds[result.cdd_id] = result
        
        logger.info(f"CDD initiated: {result.summary()}")
        
        return result
    
    def _determine_dd_level(
        self,
        risk_category: RiskCategory,
        profile: CustomerProfile,
    ) -> DDLevel:
        """Determine required DD level based on risk."""
        # EDD triggers
        if risk_category in (RiskCategory.HIGH, RiskCategory.PROHIBITED):
            return DDLevel.EDD
        
        if profile.is_pep or profile.is_pep_relative:
            return DDLevel.EDD
        
        if profile.has_adverse_media:
            return DDLevel.EDD
        
        # SDD eligibility
        if risk_category == RiskCategory.LOW:
            # Additional SDD eligibility checks
            if profile.monthly_transaction_volume < 50000:
                return DDLevel.SDD
        
        return DDLevel.CDD
    
    def _get_required_documents(
        self,
        dd_level: DDLevel,
        customer_type: str,
    ) -> list[DocumentRequirement]:
        """Get required documents based on DD level."""
        documents = []
        
        # Basic identity - always required
        documents.append(DocumentRequirement(
            document_type=DocumentType.ID_CARD,
            required=True,
        ))
        
        if dd_level == DDLevel.SDD:
            # Minimal requirements
            pass
        
        elif dd_level == DDLevel.CDD:
            # Standard requirements
            documents.append(DocumentRequirement(
                document_type=DocumentType.PROOF_OF_ADDRESS,
                required=True,
            ))
            
            if customer_type == "KURUMSAL":
                documents.append(DocumentRequirement(
                    document_type=DocumentType.COMPANY_DOCS,
                    required=True,
                ))
        
        elif dd_level == DDLevel.EDD:
            # Enhanced requirements
            documents.append(DocumentRequirement(
                document_type=DocumentType.PROOF_OF_ADDRESS,
                required=True,
            ))
            documents.append(DocumentRequirement(
                document_type=DocumentType.SOURCE_OF_FUNDS,
                required=True,
            ))
            documents.append(DocumentRequirement(
                document_type=DocumentType.INCOME_PROOF,
                required=True,
            ))
            documents.append(DocumentRequirement(
                document_type=DocumentType.BANK_STATEMENT,
                required=True,
            ))
            
            if customer_type == "KURUMSAL":
                documents.append(DocumentRequirement(
                    document_type=DocumentType.COMPANY_DOCS,
                    required=True,
                ))
                documents.append(DocumentRequirement(
                    document_type=DocumentType.BENEFICIAL_OWNER,
                    required=True,
                ))
        
        return documents
    
    def _calculate_review_date(self, risk_category: RiskCategory) -> str:
        """Calculate next review date based on risk."""
        from datetime import timedelta
        
        review_months = {
            RiskCategory.LOW: 36,
            RiskCategory.MEDIUM: 24,
            RiskCategory.HIGH: 12,
            RiskCategory.PROHIBITED: 0,
        }
        
        months = review_months.get(risk_category, 24)
        review_date = datetime.now() + timedelta(days=30 * months)
        
        return review_date.isoformat()[:10]
    
    def update_status(
        self,
        cdd_id: str,
        new_status: CDDStatus,
        notes: str = "",
        user: str = "system",
    ) -> CDDResult | None:
        """Update CDD status."""
        if cdd_id not in self._active_cdds:
            logger.warning(f"CDD not found: {cdd_id}")
            return None
        
        result = self._active_cdds[cdd_id]
        result.status = new_status
        result.last_updated = datetime.now(timezone.utc).isoformat()
        
        if notes:
            result.notes.append(f"[{user}] {notes}")
        
        if new_status in (CDDStatus.APPROVED, CDDStatus.REJECTED):
            result.completed_at = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"CDD updated: {result.summary()}")
        
        return result
    
    def submit_document(
        self,
        cdd_id: str,
        document_type: DocumentType,
        verified: bool = False,
        notes: str = "",
    ) -> CDDResult | None:
        """Submit a document for CDD."""
        if cdd_id not in self._active_cdds:
            return None
        
        result = self._active_cdds[cdd_id]
        
        for doc in result.required_documents:
            if doc.document_type == document_type:
                doc.provided = True
                doc.provided_date = datetime.now(timezone.utc).isoformat()
                doc.verified = verified
                if verified:
                    doc.verified_date = datetime.now(timezone.utc).isoformat()
                if notes:
                    doc.notes = notes
                break
        
        # Check if all documents complete
        required_docs = [d for d in result.required_documents if d.required]
        result.documents_complete = all(d.provided for d in required_docs)
        
        if result.documents_complete and result.status == CDDStatus.DOCUMENTS_REQUIRED:
            result.status = CDDStatus.UNDER_REVIEW
            result.notes.append("Tüm belgeler tamamlandı - incelemeye alındı")
        
        result.last_updated = datetime.now(timezone.utc).isoformat()
        
        return result
    
    def approve(
        self,
        cdd_id: str,
        approved_by: str,
        notes: str = "",
    ) -> CDDResult | None:
        """Approve CDD."""
        result = self.update_status(cdd_id, CDDStatus.APPROVED, notes, approved_by)
        if result:
            result.approval_by = approved_by
        return result
    
    def reject(
        self,
        cdd_id: str,
        rejected_by: str,
        reason: str,
    ) -> CDDResult | None:
        """Reject CDD."""
        result = self.update_status(cdd_id, CDDStatus.REJECTED, reason, rejected_by)
        if result:
            result.rejection_reason = reason
        return result
    
    def get_cdd(self, cdd_id: str) -> CDDResult | None:
        """Get CDD by ID."""
        return self._active_cdds.get(cdd_id)
    
    def get_pending_cdds(self) -> list[CDDResult]:
        """Get all pending CDDs."""
        return [
            cdd for cdd in self._active_cdds.values()
            if cdd.status not in (CDDStatus.APPROVED, CDDStatus.REJECTED)
        ]
    
    def get_statistics(self) -> dict[str, Any]:
        """Get CDD statistics."""
        total = len(self._active_cdds)
        by_status = {}
        by_level = {}
        by_risk = {}
        
        for cdd in self._active_cdds.values():
            status = cdd.status.value
            by_status[status] = by_status.get(status, 0) + 1
            
            level = cdd.dd_level.value
            by_level[level] = by_level.get(level, 0) + 1
            
            risk = cdd.risk_category.value
            by_risk[risk] = by_risk.get(risk, 0) + 1
        
        return {
            "total_cdds": total,
            "by_status": by_status,
            "by_level": by_level,
            "by_risk_category": by_risk,
            "pending_count": len(self.get_pending_cdds()),
        }

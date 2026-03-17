# =============================================================================
# SentinelFlow - KYC/AML Module
# =============================================================================
"""
Know Your Customer (KYC) and Anti-Money Laundering (AML) module.

Provides comprehensive customer risk assessment including:
- Customer Due Diligence (CDD)
- Enhanced Due Diligence (EDD)
- PEP (Politically Exposed Persons) screening
- Sanctions list checking
- Customer risk scoring

Components:
    - CustomerRiskScorer: ML-based customer risk assessment
    - PEPScreener: Political exposure screening
    - SanctionsChecker: International sanctions screening
    - CDDEngine: Customer Due Diligence workflow

Usage:
    from sentinelflow.kyc import CustomerRiskScorer, CDDEngine
    
    scorer = CustomerRiskScorer()
    risk_result = scorer.assess_customer(customer_data)
    
    cdd = CDDEngine()
    cdd_result = cdd.perform_cdd(customer_data)
"""

from sentinelflow.kyc.risk_scorer import CustomerRiskScorer, RiskAssessment
from sentinelflow.kyc.cdd import CDDEngine, CDDResult, DDLevel
from sentinelflow.kyc.screening import PEPScreener, SanctionsChecker, ScreeningResult

__all__ = [
    "CustomerRiskScorer",
    "RiskAssessment",
    "CDDEngine",
    "CDDResult",
    "DDLevel",
    "PEPScreener",
    "SanctionsChecker",
    "ScreeningResult",
]

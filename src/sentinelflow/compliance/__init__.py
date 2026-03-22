# =============================================================================
# SentinelFlow - Compliance Module
# =============================================================================
"""
Regulatory compliance module for Turkish financial regulations.

This module provides compliance features for:
- MASAK (Mali Suçları Araştırma Kurulu) reporting
- BDDK (Bankacılık Düzenleme ve Denetleme Kurumu) requirements
- TCMB (Türkiye Cumhuriyet Merkez Bankası) regulations

Components:
    - MASAKReporter: Suspicious Transaction Report (STR) generation
    - ComplianceEngine: Risk thresholds and regulatory checks
    - AuditLogger: Compliance audit trail logging
    - RegulatoryDashboard: Compliance monitoring interface

Usage:
    from sentinelflow.compliance import MASAKReporter, ComplianceEngine

    engine = ComplianceEngine()
    engine.check_transaction(tx_data)

    reporter = MASAKReporter()
    reporter.generate_str(fraud_alert)
"""

from sentinelflow.compliance.audit import AuditEvent, AuditLogger
from sentinelflow.compliance.engine import ComplianceEngine, ComplianceResult
from sentinelflow.compliance.masak import MASAKReporter, STRReport

__all__ = [
    "MASAKReporter",
    "STRReport",
    "ComplianceEngine",
    "ComplianceResult",
    "AuditLogger",
    "AuditEvent",
]

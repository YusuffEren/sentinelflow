# =============================================================================
# SentinelFlow - Contract Enums
# =============================================================================
"""
Enumeration types used across all services.
"""

from enum import Enum


class FraudType(str, Enum):
    """Types of fraud detected by the system."""
    
    CIRCULAR_RING = "circular_ring"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    BLACKLIST_KEYWORD = "blacklist_keyword"
    MULE_ACCOUNT = "mule_account"
    STRUCTURING = "structuring"
    VELOCITY_ANOMALY = "velocity_anomaly"
    ML_ENSEMBLE = "ml_ensemble"
    COMPLIANCE_VIOLATION = "compliance_violation"
    

class Severity(str, Enum):
    """Alert severity levels (aligned with SOC standards)."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    
    @property
    def priority_order(self) -> int:
        """Numeric priority for sorting (higher = more severe)."""
        return {"low": 1, "medium": 2, "high": 3, "critical": 4}[self.value]


class CaseStatus(str, Enum):
    """Case lifecycle statuses."""
    
    NEW = "new"
    TRIAGE = "triage"
    INVESTIGATING = "investigating"
    ESCALATED = "escalated"
    PENDING_INFO = "pending_info"
    RESOLVED_TRUE_POSITIVE = "resolved_true_positive"
    RESOLVED_FALSE_POSITIVE = "resolved_false_positive"
    CLOSED = "closed"
    
    @property
    def is_open(self) -> bool:
        """Whether case is still open for action."""
        return self in (
            CaseStatus.NEW,
            CaseStatus.TRIAGE,
            CaseStatus.INVESTIGATING,
            CaseStatus.ESCALATED,
            CaseStatus.PENDING_INFO,
        )


class CasePriority(str, Enum):
    """Case priority levels."""
    
    P1_CRITICAL = "P1"
    P2_HIGH = "P2"
    P3_MEDIUM = "P3"
    P4_LOW = "P4"


class EventType(str, Enum):
    """Audit event types for case_events table."""
    
    # Case lifecycle
    CASE_CREATED = "case_created"
    CASE_ASSIGNED = "case_assigned"
    CASE_STATUS_CHANGED = "case_status_changed"
    CASE_PRIORITY_CHANGED = "case_priority_changed"
    CASE_ESCALATED = "case_escalated"
    CASE_CLOSED = "case_closed"
    
    # Alert events
    ALERT_CREATED = "alert_created"
    ALERT_LINKED_TO_CASE = "alert_linked_to_case"
    ALERT_DISMISSED = "alert_dismissed"
    
    # User actions
    NOTE_ADDED = "note_added"
    TAG_ADDED = "tag_added"
    TAG_REMOVED = "tag_removed"
    EVIDENCE_ADDED = "evidence_added"
    
    # System events
    STR_GENERATED = "str_generated"
    STR_SUBMITTED = "str_submitted"


class DetectorType(str, Enum):
    """Types of detection engines."""
    
    RULE_ENGINE = "rule_engine"
    GRAPH_ANALYSIS = "graph_analysis"
    GEO_ANALYSIS = "geo_analysis"
    NLP_ANALYSIS = "nlp_analysis"
    ML_ENSEMBLE = "ml_ensemble"
    COMPLIANCE_ENGINE = "compliance_engine"

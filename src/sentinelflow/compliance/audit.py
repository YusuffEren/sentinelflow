# =============================================================================
# SentinelFlow - Compliance Audit Logger
# =============================================================================
"""
Immutable audit logging for compliance requirements.

MASAK and BDDK require financial institutions to maintain
comprehensive audit trails for at least 8 years. This module
provides:
- Tamper-evident logging
- Structured audit events
- Search and export capabilities
- Compliance reporting

All compliance-related actions are logged with timestamps,
user information, and full details for regulatory review.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Generator
import uuid

from loguru import logger


# =============================================================================
# Enums
# =============================================================================

class AuditCategory(str, Enum):
    """Categories of audit events."""
    
    TRANSACTION = "ISLEM"
    COMPLIANCE_CHECK = "UYUM_KONTROLU"
    ALERT_GENERATED = "ALARM_URETILDI"
    STR_CREATED = "SIB_OLUSTURULDU"
    STR_SUBMITTED = "SIB_GONDERILDI"
    USER_ACTION = "KULLANICI_ISLEMI"
    SYSTEM_EVENT = "SISTEM_OLAYI"
    CONFIGURATION = "KONFIGÜRASYON"
    ACCESS = "ERISIM"
    EXPORT = "DISARI_AKTARIM"


class AuditSeverity(str, Enum):
    """Audit event severity levels."""
    
    INFO = "BILGI"
    WARNING = "UYARI"
    ERROR = "HATA"
    CRITICAL = "KRITIK"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class AuditEvent:
    """
    An immutable audit event record.
    
    Each event is cryptographically linked to the previous event
    to ensure tamper-evidence.
    """
    
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    category: AuditCategory = AuditCategory.SYSTEM_EVENT
    severity: AuditSeverity = AuditSeverity.INFO
    
    # Event details
    action: str = ""
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    
    # Actor information
    user_id: str = "system"
    user_name: str = "SentinelFlow System"
    user_ip: str = ""
    
    # Related entities
    transaction_id: str | None = None
    alert_id: str | None = None
    report_id: str | None = None
    
    # Chain integrity
    previous_hash: str = ""
    event_hash: str = ""
    
    def calculate_hash(self) -> str:
        """Calculate cryptographic hash of event data."""
        data = {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "category": self.category.value,
            "severity": self.severity.value,
            "action": self.action,
            "description": self.description,
            "details": self.details,
            "user_id": self.user_id,
            "transaction_id": self.transaction_id,
            "alert_id": self.alert_id,
            "report_id": self.report_id,
            "previous_hash": self.previous_hash,
        }
        json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(json_str.encode("utf-8")).hexdigest()
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "category": self.category.value,
            "severity": self.severity.value,
            "action": self.action,
            "description": self.description,
            "details": self.details,
            "actor": {
                "user_id": self.user_id,
                "user_name": self.user_name,
                "user_ip": self.user_ip,
            },
            "related": {
                "transaction_id": self.transaction_id,
                "alert_id": self.alert_id,
                "report_id": self.report_id,
            },
            "integrity": {
                "previous_hash": self.previous_hash,
                "event_hash": self.event_hash,
            },
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


@dataclass
class AuditStatistics:
    """Statistics about the audit log."""
    
    total_events: int = 0
    events_by_category: dict[str, int] = field(default_factory=dict)
    events_by_severity: dict[str, int] = field(default_factory=dict)
    first_event: str = ""
    last_event: str = ""
    chain_valid: bool = True


# =============================================================================
# Audit Logger
# =============================================================================

class AuditLogger:
    """
    Tamper-evident audit logging system.
    
    Maintains a cryptographically linked chain of audit events
    for regulatory compliance. Events cannot be modified or deleted
    without breaking the chain.
    
    Example:
        >>> logger = AuditLogger()
        >>> logger.log_transaction(tx_data)
        >>> logger.log_compliance_check(result)
        >>> logger.export_report("2024-01", "2024-12")
    """
    
    def __init__(
        self,
        log_dir: str = "logs/audit",
        max_memory_events: int = 10000,
        persist_immediately: bool = True,
    ):
        """
        Initialize audit logger.
        
        Args:
            log_dir: Directory for audit log files
            max_memory_events: Maximum events to keep in memory
            persist_immediately: Write events to disk immediately
        """
        self.log_dir = Path(log_dir)
        self.max_memory_events = max_memory_events
        self.persist_immediately = persist_immediately
        
        self._events: list[AuditEvent] = []
        self._last_hash: str = "GENESIS"
        self._event_count: int = 0
        
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing events to continue chain
        self._load_last_hash()
        
        logger.info(f"AuditLogger initialized (dir={log_dir})")
    
    def _load_last_hash(self) -> None:
        """Load the last event hash to continue the chain."""
        chain_file = self.log_dir / "chain_state.json"
        
        if chain_file.exists():
            try:
                with open(chain_file, "r", encoding="utf-8") as f:
                    state = json.load(f)
                    self._last_hash = state.get("last_hash", "GENESIS")
                    self._event_count = state.get("event_count", 0)
            except Exception as e:
                logger.error(f"Failed to load chain state: {e}")
    
    def _save_chain_state(self) -> None:
        """Save current chain state."""
        chain_file = self.log_dir / "chain_state.json"
        
        state = {
            "last_hash": self._last_hash,
            "event_count": self._event_count,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        
        with open(chain_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    
    def log(
        self,
        category: AuditCategory,
        action: str,
        description: str,
        severity: AuditSeverity = AuditSeverity.INFO,
        details: dict[str, Any] | None = None,
        user_id: str = "system",
        user_name: str = "SentinelFlow System",
        user_ip: str = "",
        transaction_id: str | None = None,
        alert_id: str | None = None,
        report_id: str | None = None,
    ) -> AuditEvent:
        """
        Log an audit event.
        
        Args:
            category: Event category
            action: Action performed
            description: Human-readable description
            severity: Event severity
            details: Additional details
            user_id: Actor user ID
            user_name: Actor name
            user_ip: Actor IP address
            transaction_id: Related transaction ID
            alert_id: Related alert ID
            report_id: Related report ID
        
        Returns:
            Created AuditEvent
        """
        event = AuditEvent(
            category=category,
            severity=severity,
            action=action,
            description=description,
            details=details or {},
            user_id=user_id,
            user_name=user_name,
            user_ip=user_ip,
            transaction_id=transaction_id,
            alert_id=alert_id,
            report_id=report_id,
            previous_hash=self._last_hash,
        )
        
        # Calculate and set hash
        event.event_hash = event.calculate_hash()
        self._last_hash = event.event_hash
        self._event_count += 1
        
        # Store in memory
        self._events.append(event)
        if len(self._events) > self.max_memory_events:
            self._events = self._events[-self.max_memory_events:]
        
        # Persist
        if self.persist_immediately:
            self._persist_event(event)
            self._save_chain_state()
        
        return event
    
    def _persist_event(self, event: AuditEvent) -> None:
        """Write event to disk."""
        # Organize by date
        ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
        date_dir = self.log_dir / f"{ts.year}" / f"{ts.month:02d}" / f"{ts.day:02d}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Append to daily log file
        log_file = date_dir / "audit.jsonl"
        
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(event.to_json().replace("\n", " ") + "\n")
    
    # =========================================================================
    # Convenience Logging Methods
    # =========================================================================
    
    def log_transaction(
        self,
        tx_data: dict[str, Any],
        user_id: str = "system",
    ) -> AuditEvent:
        """Log a transaction processing event."""
        return self.log(
            category=AuditCategory.TRANSACTION,
            action="TRANSACTION_PROCESSED",
            description=f"İşlem işlendi: {tx_data.get('transaction_id', 'N/A')}",
            details={
                "amount": tx_data.get("amount"),
                "sender_iban": tx_data.get("sender_iban", "")[:12] + "...",
                "receiver_iban": tx_data.get("receiver_iban", "")[:12] + "...",
            },
            user_id=user_id,
            transaction_id=tx_data.get("transaction_id"),
        )
    
    def log_compliance_check(
        self,
        result: Any,  # ComplianceResult
        user_id: str = "system",
    ) -> AuditEvent:
        """Log a compliance check result."""
        return self.log(
            category=AuditCategory.COMPLIANCE_CHECK,
            action="COMPLIANCE_CHECKED",
            description=f"Uyum kontrolü: {'Geçti' if result.is_compliant else 'İhlal var'}",
            severity=AuditSeverity.INFO if result.is_compliant else AuditSeverity.WARNING,
            details={
                "is_compliant": result.is_compliant,
                "risk_level": result.risk_level.value if hasattr(result.risk_level, 'value') else str(result.risk_level),
                "violations_count": len(result.violations),
                "requires_str": result.requires_str,
                "requires_ctr": result.requires_ctr,
            },
            user_id=user_id,
            transaction_id=result.transaction_id,
        )
    
    def log_alert(
        self,
        alert_data: dict[str, Any],
        user_id: str = "system",
    ) -> AuditEvent:
        """Log a fraud alert generation."""
        return self.log(
            category=AuditCategory.ALERT_GENERATED,
            action="ALERT_GENERATED",
            description=f"Dolandırıcılık alarmı: {alert_data.get('fraud_type', 'N/A')}",
            severity=AuditSeverity.WARNING,
            details={
                "fraud_type": alert_data.get("fraud_type"),
                "severity": alert_data.get("severity"),
                "confidence": alert_data.get("confidence"),
            },
            user_id=user_id,
            alert_id=alert_data.get("alert_id"),
            transaction_id=alert_data.get("transaction_id"),
        )
    
    def log_str_created(
        self,
        report_id: str,
        user_id: str = "system",
    ) -> AuditEvent:
        """Log STR creation."""
        return self.log(
            category=AuditCategory.STR_CREATED,
            action="STR_CREATED",
            description=f"Şüpheli İşlem Bildirimi oluşturuldu: {report_id}",
            severity=AuditSeverity.INFO,
            user_id=user_id,
            report_id=report_id,
        )
    
    def log_str_submitted(
        self,
        report_id: str,
        user_id: str = "system",
    ) -> AuditEvent:
        """Log STR submission."""
        return self.log(
            category=AuditCategory.STR_SUBMITTED,
            action="STR_SUBMITTED",
            description=f"Şüpheli İşlem Bildirimi MASAK'a gönderildi: {report_id}",
            severity=AuditSeverity.INFO,
            user_id=user_id,
            report_id=report_id,
        )
    
    def log_user_action(
        self,
        action: str,
        description: str,
        user_id: str,
        user_name: str,
        user_ip: str = "",
        details: dict[str, Any] | None = None,
    ) -> AuditEvent:
        """Log a user action."""
        return self.log(
            category=AuditCategory.USER_ACTION,
            action=action,
            description=description,
            details=details or {},
            user_id=user_id,
            user_name=user_name,
            user_ip=user_ip,
        )
    
    def log_access(
        self,
        resource: str,
        action: str,
        user_id: str,
        user_ip: str = "",
        granted: bool = True,
    ) -> AuditEvent:
        """Log an access event."""
        return self.log(
            category=AuditCategory.ACCESS,
            action=f"ACCESS_{action.upper()}",
            description=f"{'Erişim verildi' if granted else 'Erişim reddedildi'}: {resource}",
            severity=AuditSeverity.INFO if granted else AuditSeverity.WARNING,
            details={
                "resource": resource,
                "granted": granted,
            },
            user_id=user_id,
            user_ip=user_ip,
        )
    
    # =========================================================================
    # Query and Export
    # =========================================================================
    
    def search(
        self,
        category: AuditCategory | None = None,
        severity: AuditSeverity | None = None,
        user_id: str | None = None,
        transaction_id: str | None = None,
        alert_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """
        Search audit events.
        
        Only searches in-memory events. For full search,
        use export_report to generate from disk files.
        """
        results = []
        
        for event in reversed(self._events):
            if category and event.category != category:
                continue
            if severity and event.severity != severity:
                continue
            if user_id and event.user_id != user_id:
                continue
            if transaction_id and event.transaction_id != transaction_id:
                continue
            if alert_id and event.alert_id != alert_id:
                continue
            
            # Date filtering
            if start_date or end_date:
                event_date = event.timestamp[:10]
                if start_date and event_date < start_date:
                    continue
                if end_date and event_date > end_date:
                    continue
            
            results.append(event)
            
            if len(results) >= limit:
                break
        
        return results
    
    def get_statistics(self) -> AuditStatistics:
        """Get statistics about the audit log."""
        stats = AuditStatistics(
            total_events=self._event_count,
        )
        
        for event in self._events:
            cat = event.category.value
            stats.events_by_category[cat] = stats.events_by_category.get(cat, 0) + 1
            
            sev = event.severity.value
            stats.events_by_severity[sev] = stats.events_by_severity.get(sev, 0) + 1
        
        if self._events:
            stats.first_event = self._events[0].timestamp
            stats.last_event = self._events[-1].timestamp
        
        # Verify chain (simplified - checks only in-memory)
        stats.chain_valid = self._verify_chain_in_memory()
        
        return stats
    
    def _verify_chain_in_memory(self) -> bool:
        """Verify the integrity of the in-memory chain."""
        if len(self._events) < 2:
            return True
        
        for i in range(1, len(self._events)):
            if self._events[i].previous_hash != self._events[i-1].event_hash:
                return False
            if self._events[i].event_hash != self._events[i].calculate_hash():
                return False
        
        return True
    
    def export_report(
        self,
        start_month: str,
        end_month: str,
        output_path: str | None = None,
    ) -> str:
        """
        Export audit events for a date range.
        
        Args:
            start_month: Start month (YYYY-MM)
            end_month: End month (YYYY-MM)
            output_path: Output file path
        
        Returns:
            Path to exported file
        """
        events = []
        
        # Parse months
        start_year, start_m = map(int, start_month.split("-"))
        end_year, end_m = map(int, end_month.split("-"))
        
        # Iterate through months
        year, month = start_year, start_m
        while (year, month) <= (end_year, end_m):
            month_dir = self.log_dir / str(year) / f"{month:02d}"
            
            if month_dir.exists():
                for day_dir in sorted(month_dir.iterdir()):
                    log_file = day_dir / "audit.jsonl"
                    if log_file.exists():
                        with open(log_file, "r", encoding="utf-8") as f:
                            for line in f:
                                try:
                                    events.append(json.loads(line))
                                except:
                                    pass
            
            # Next month
            month += 1
            if month > 12:
                month = 1
                year += 1
        
        # Generate output
        output_path = output_path or str(
            self.log_dir / f"export_{start_month}_to_{end_month}.json"
        )
        
        report = {
            "export_info": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "start_month": start_month,
                "end_month": end_month,
                "total_events": len(events),
            },
            "events": events,
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        # Log the export
        self.log(
            category=AuditCategory.EXPORT,
            action="AUDIT_EXPORTED",
            description=f"Denetim raporu dışa aktarıldı: {start_month} - {end_month}",
            details={
                "events_count": len(events),
                "output_path": output_path,
            },
        )
        
        logger.info(f"Audit export complete: {output_path} ({len(events)} events)")
        
        return output_path

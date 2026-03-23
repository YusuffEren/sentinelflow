# SentinelFlow - Compliance Module Tests

from datetime import datetime


class TestComplianceEngine:
    """Tests for ComplianceEngine."""

    def test_compliance_engine_initialization(self):
        """Test engine initializes correctly."""
        from sentinelflow.compliance import ComplianceEngine

        engine = ComplianceEngine()
        assert engine is not None

    def test_low_value_transaction_compliant(self, sample_transaction):
        """Test that low-value transactions pass compliance."""
        from sentinelflow.compliance import ComplianceEngine

        engine = ComplianceEngine()

        result = engine.check_transaction(sample_transaction)

        assert result.is_compliant or len(result.violations) == 0

    def test_high_value_triggers_report(self):
        """Test that high-value transactions trigger reporting."""
        from sentinelflow.compliance import ComplianceEngine

        engine = ComplianceEngine()

        high_value_tx = {
            "transaction_id": "TX-HIGH-001",
            "sender_iban": "TR000000000000000000000001",
            "sender_name": "Test",
            "receiver_iban": "TR000000000000000000000002",
            "receiver_name": "Test",
            "amount": 300000.0,
            "timestamp": datetime.now().isoformat(),
        }

        result = engine.check_transaction(high_value_tx)

        assert result.requires_str or result.requires_ctr

    def test_pep_transaction_flagged(self):
        """Test that transactions with PEP names are flagged."""
        from sentinelflow.compliance import ComplianceEngine

        engine = ComplianceEngine(enable_pep_check=True)

        pep_tx = {
            "transaction_id": "TX-PEP-001",
            "sender_iban": "TR000000000000000000000001",
            "sender_name": "Ali Veli",  # Demo PEP
            "receiver_iban": "TR000000000000000000000002",
            "receiver_name": "Normal Kisi",
            "amount": 50000.0,
            "timestamp": datetime.now().isoformat(),
        }

        result = engine.check_transaction(pep_tx)

        # Should have PEP violation
        pep_violations = [v for v in result.violations if "PEP" in v.rule.value]
        assert len(pep_violations) > 0

    def test_sanctions_check(self):
        """Test that sanctioned names are caught."""
        from sentinelflow.compliance import ComplianceEngine

        engine = ComplianceEngine(enable_sanctions_check=True)

        sanctioned_tx = {
            "transaction_id": "TX-SAN-001",
            "sender_iban": "TR000000000000000000000001",
            "sender_name": "Yasak Kisi",  # Demo sanctioned
            "receiver_iban": "TR000000000000000000000002",
            "receiver_name": "Normal",
            "amount": 5000.0,
            "timestamp": datetime.now().isoformat(),
        }

        result = engine.check_transaction(sanctioned_tx)

        sanctions_violations = [v for v in result.violations if "YAPTIRIM" in v.rule.value]
        assert len(sanctions_violations) > 0


class TestMASAKReporter:
    """Tests for MASAK STR reporting."""

    def test_reporter_initialization(self, tmp_path):
        """Test reporter initializes correctly."""
        from sentinelflow.compliance import MASAKReporter

        reporter = MASAKReporter(output_dir=str(tmp_path / "reports"))
        assert reporter is not None

    def test_create_str_from_alert(self, tmp_path):
        """Test STR creation from fraud alert."""
        from sentinelflow.compliance import MASAKReporter

        reporter = MASAKReporter(
            output_dir=str(tmp_path / "reports"),
            auto_archive=False,
        )

        alert = {
            "alert_id": "ALERT-001",
            "transaction_id": "TX-001",
            "fraud_type": "circular_ring",
            "confidence": 0.9,
            "sender_iban": "TR001",
            "sender_name": "Test Sender",
            "receiver_iban": "TR002",
            "receiver_name": "Test Receiver",
            "amount": 50000.0,
            "description": "Test",
            "detected_at": datetime.now().isoformat(),
        }

        str_report = reporter.create_str_from_alert(alert)

        assert str_report is not None
        assert str_report.report_id.startswith("STR-")
        assert str_report.transaction.amount == 50000.0

    def test_str_to_json(self, tmp_path):
        """Test STR JSON serialization."""
        from sentinelflow.compliance import MASAKReporter

        reporter = MASAKReporter(
            output_dir=str(tmp_path / "reports"),
            auto_archive=False,
        )

        alert = {
            "fraud_type": "ai_detected_anomaly",
            "confidence": 0.85,
            "sender_iban": "TR001",
            "receiver_iban": "TR002",
            "amount": 25000.0,
        }

        str_report = reporter.create_str_from_alert(alert)
        json_str = str_report.to_json()

        assert "bildirim_bilgileri" in json_str
        assert "suphe_bilgileri" in json_str


class TestAuditLogger:
    """Tests for audit logging."""

    def test_audit_logger_initialization(self, tmp_path):
        """Test audit logger initializes correctly."""
        from sentinelflow.compliance import AuditLogger

        logger = AuditLogger(log_dir=str(tmp_path / "audit"))
        assert logger is not None

    def test_log_event(self, tmp_path):
        """Test logging an audit event."""
        from sentinelflow.compliance import AuditLogger
        from sentinelflow.compliance.audit import AuditCategory, AuditSeverity

        audit_logger = AuditLogger(
            log_dir=str(tmp_path / "audit"),
            persist_immediately=False,
        )

        event = audit_logger.log(
            category=AuditCategory.TRANSACTION,
            action="TEST_ACTION",
            description="Test event",
            severity=AuditSeverity.INFO,
        )

        assert event is not None
        assert event.event_id is not None
        assert event.event_hash != ""

    def test_chain_integrity(self, tmp_path):
        """Test audit chain integrity."""
        from sentinelflow.compliance import AuditLogger
        from sentinelflow.compliance.audit import AuditCategory

        audit_logger = AuditLogger(
            log_dir=str(tmp_path / "audit"),
            persist_immediately=False,
        )

        # Log multiple events
        event1 = audit_logger.log(
            category=AuditCategory.SYSTEM_EVENT,
            action="EVENT_1",
            description="First event",
        )

        event2 = audit_logger.log(
            category=AuditCategory.SYSTEM_EVENT,
            action="EVENT_2",
            description="Second event",
        )

        # Second event should reference first
        assert event2.previous_hash == event1.event_hash

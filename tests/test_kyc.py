# SentinelFlow - KYC Module Tests

from datetime import datetime, timedelta


class TestCustomerRiskScorer:
    """Tests for CustomerRiskScorer."""

    def test_risk_scorer_initialization(self):
        """Test scorer initializes correctly."""
        from sentinelflow.kyc import CustomerRiskScorer

        scorer = CustomerRiskScorer()
        assert scorer is not None

    def test_assess_low_risk_customer(self):
        """Test that low-risk customers get low scores."""
        from sentinelflow.kyc import CustomerRiskScorer
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        scorer = CustomerRiskScorer()

        profile = CustomerProfile(
            customer_id="C-LOW-001",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="Normal Musteri",
            nationality="TC",
            country_of_residence="Turkiye",
            occupation="Muhendis",
            account_open_date=(datetime.now() - timedelta(days=365 * 3)).isoformat()[:10],
            declared_monthly_income=50000.0,
            source_of_funds="Maas",
            is_pep=False,
            monthly_transaction_volume=40000.0,
        )

        result = scorer.assess(profile)

        assert result.overall_score < 40  # Should be low risk
        assert result.risk_category.value in ("DUSUK", "ORTA")

    def test_assess_high_risk_customer(self):
        """Test that high-risk customers get high scores."""
        from sentinelflow.kyc import CustomerRiskScorer
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        scorer = CustomerRiskScorer()

        profile = CustomerProfile(
            customer_id="C-HIGH-001",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="Riskli Musteri",
            nationality="Iran",  # High-risk country
            country_of_residence="Turkiye",
            occupation="Kuyumcu",  # High-risk occupation
            account_open_date=(datetime.now() - timedelta(days=30)).isoformat()[:10],
            declared_monthly_income=10000.0,
            source_of_funds="Kripto",  # High-risk source
            is_pep=True,  # PEP
            monthly_transaction_volume=500000.0,  # High volume
        )

        result = scorer.assess(profile)

        assert result.overall_score > 50  # Should be high risk
        assert result.risk_category.value in ("YUKSEK", "KRITIK")

    def test_pep_increases_risk(self):
        """Test that PEP status increases risk score."""
        from sentinelflow.kyc import CustomerRiskScorer
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        scorer = CustomerRiskScorer()

        base_profile = CustomerProfile(
            customer_id="C-TEST-001",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="Test",
            is_pep=False,
        )

        pep_profile = CustomerProfile(
            customer_id="C-TEST-002",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="Test",
            is_pep=True,
        )

        base_result = scorer.assess(base_profile)
        pep_result = scorer.assess(pep_profile)

        assert pep_result.overall_score > base_result.overall_score


class TestCDDEngine:
    """Tests for CDDEngine."""

    def test_cdd_engine_initialization(self):
        """Test engine initializes correctly."""
        from sentinelflow.kyc import CDDEngine

        engine = CDDEngine()
        assert engine is not None

    def test_perform_cdd_low_risk(self):
        """Test CDD for low-risk customer."""
        from sentinelflow.kyc import CDDEngine, DDLevel
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        engine = CDDEngine(auto_approve_low_risk=False)

        profile = CustomerProfile(
            customer_id="C-LOW-001",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="Low Risk",
            is_pep=False,
            monthly_transaction_volume=10000.0,
        )

        result = engine.perform_cdd(profile)

        assert result is not None
        assert result.dd_level in (DDLevel.SDD, DDLevel.CDD)

    def test_perform_cdd_high_risk_requires_edd(self):
        """Test that high-risk customers require EDD."""
        from sentinelflow.kyc import CDDEngine, DDLevel
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        engine = CDDEngine()

        profile = CustomerProfile(
            customer_id="C-HIGH-001",
            customer_type=CustomerType.INDIVIDUAL,
            full_name="High Risk",
            is_pep=True,  # PEP triggers EDD
        )

        result = engine.perform_cdd(profile)

        assert result.dd_level == DDLevel.EDD

    def test_cdd_document_requirements(self):
        """Test that EDD requires more documents."""
        from sentinelflow.kyc import CDDEngine
        from sentinelflow.kyc.risk_scorer import CustomerProfile, CustomerType

        engine = CDDEngine()

        high_risk_profile = CustomerProfile(
            customer_id="C-HR-001",
            customer_type=CustomerType.CORPORATE,
            full_name="High Risk Corp",
            is_pep=True,
        )

        result = engine.perform_cdd(high_risk_profile)

        # EDD should require many documents
        assert len(result.required_documents) >= 4


class TestPEPScreener:
    """Tests for PEP screening."""

    def test_pep_screener_initialization(self):
        """Test screener initializes correctly."""
        from sentinelflow.kyc import PEPScreener

        screener = PEPScreener()
        assert screener is not None

    def test_screen_pep_name(self):
        """Test screening a known PEP."""
        from sentinelflow.kyc import PEPScreener

        screener = PEPScreener()

        result = screener.screen("Ahmet Politikaci")  # Demo PEP

        assert result.has_matches
        assert len(result.matches) > 0

    def test_screen_non_pep_name(self):
        """Test screening a non-PEP name."""
        from sentinelflow.kyc import PEPScreener

        screener = PEPScreener()

        result = screener.screen("Siradan Vatandas")

        assert not result.has_matches
        assert result.risk_score == 0.0


class TestSanctionsChecker:
    """Tests for sanctions checking."""

    def test_sanctions_checker_initialization(self):
        """Test checker initializes correctly."""
        from sentinelflow.kyc import SanctionsChecker

        checker = SanctionsChecker()
        assert checker is not None

    def test_check_sanctioned_name(self):
        """Test checking a sanctioned name."""
        from sentinelflow.kyc import SanctionsChecker

        checker = SanctionsChecker()

        result = checker.check("Yasak Kisi")  # Demo sanctioned

        assert result.has_matches
        assert result.risk_score == 100.0  # Sanctions are critical

    def test_check_clean_name(self):
        """Test checking a clean name."""
        from sentinelflow.kyc import SanctionsChecker

        checker = SanctionsChecker()

        result = checker.check("Normal Insan")

        assert not result.has_matches
        assert result.risk_score == 0.0

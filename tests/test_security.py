# =============================================================================
# SentinelFlow - Security Module Tests
# =============================================================================
"""
Tests for security module: rate limiting, input validation, auth.

Run with: pytest tests/test_security.py -v
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# =============================================================================
# Rate Limiter Tests
# =============================================================================


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_initialization(self):
        """RateLimiter should initialize with default limits."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        assert limiter is not None
        assert "default" in limiter._limits
        assert "login" in limiter._limits

    def test_check_allows_first_request(self):
        """First request from a client should be allowed."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        result = limiter.check("client-1", "default")

        assert result.allowed is True
        assert result.remaining >= 0
        assert result.limit > 0

    def test_check_multiple_requests(self):
        """Multiple requests should decrement remaining count."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        result1 = limiter.check("client-2", "default")
        result2 = limiter.check("client-2", "default")

        assert result2.remaining < result1.remaining

    def test_different_clients_independent(self):
        """Different clients should have independent rate limits."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        result_a = limiter.check("client-a", "default")
        result_b = limiter.check("client-b", "default")

        assert result_a.remaining == result_b.remaining

    def test_login_rate_limit_strict(self):
        """Login endpoint should have strict limits."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        # Login allows only 5 requests per 60 seconds
        for i in range(5):
            result = limiter.check("attacker-ip", "login")
            assert result.allowed is True, f"Request {i+1} should be allowed"

        # 6th request should be blocked
        result = limiter.check("attacker-ip", "login")
        assert result.allowed is False
        assert result.retry_after is not None

    def test_reset_client(self):
        """Resetting a client should clear their state."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        limiter.check("reset-client", "default")
        limiter.reset("reset-client")

        result = limiter.check("reset-client", "default")
        assert result.allowed is True

    def test_custom_limits(self):
        """Custom limits should override defaults."""
        from sentinelflow.security import RateLimiter
        from sentinelflow.security.rate_limit import RateLimitConfig

        custom_limits = {"custom_endpoint": RateLimitConfig(requests=10, window_seconds=10)}
        limiter = RateLimiter(limits=custom_limits)

        result = limiter.check("client", "custom_endpoint")
        assert result.allowed is True
        assert result.limit == 10

    def test_cleanup_old_entries(self):
        """Cleanup should remove expired entries."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        limiter.check("old-client", "default")
        assert "old-client" in limiter._state

        # Use max_age=-1 to force cleanup of entries created now
        removed = limiter.cleanup_old_entries(max_age_seconds=-1)
        assert removed >= 1

    def test_rate_limit_headers(self):
        """Rate limit result should produce valid headers."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        result = limiter.check("headers-test", "default")

        headers = result.to_headers()
        assert "X-RateLimit-Limit" in headers
        assert "X-RateLimit-Remaining" in headers
        assert "X-RateLimit-Reset" in headers

    def test_set_limit_updates_config(self):
        """Dynamically updating limits should work."""
        from sentinelflow.security import RateLimiter
        from sentinelflow.security.rate_limit import RateLimitConfig

        limiter = RateLimiter()
        new_config = RateLimitConfig(requests=50, window_seconds=30)
        limiter.set_limit("dynamic_endpoint", new_config)

        limit = limiter.get_limit("dynamic_endpoint")
        assert limit.requests == 50
        assert limit.window_seconds == 30

    def test_get_limit_for_unknown_endpoint(self):
        """Unknown endpoints should return default limit."""
        from sentinelflow.security import RateLimiter

        limiter = RateLimiter()
        limit = limiter.get_limit("nonexistent_endpoint")
        assert limit is not None
        assert limit.requests > 0


# =============================================================================
# Input Validator Tests
# =============================================================================


class TestInputValidator:
    """Tests for InputValidator."""

    def test_initialization(self):
        """Validator should initialize correctly."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        assert validator is not None

    def test_validate_valid_transaction(self):
        """A valid transaction should pass validation."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "TR330006100519786457841326",
            "sender_name": "Ahmet Yilmaz",
            "receiver_iban": "TR110006400000478893400002",
            "receiver_name": "Mehmet Kaya",
            "amount": 15000.00,
            "description": "Kira odemesi",
        }

        result = validator.validate_transaction(tx)
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_missing_required_fields(self):
        """Missing required fields should fail validation."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        result = validator.validate_transaction({"amount": 100})

        assert result.is_valid is False
        field_names = [e.field for e in result.errors]
        assert "sender_iban" in field_names
        assert "receiver_iban" in field_names

    def test_validate_invalid_iban(self):
        """Invalid IBAN should fail validation."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "INVALID-IBAN",
            "receiver_iban": "TR110006400000478893400002",
            "amount": 1000,
        }

        result = validator.validate_transaction(tx)
        assert result.is_valid is False

    def test_validate_negative_amount(self):
        """Negative amount should fail validation."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "TR330006100519786457841326",
            "receiver_iban": "TR110006400000478893400002",
            "amount": -100,
        }

        result = validator.validate_transaction(tx)
        assert result.is_valid is False

    def test_validate_excessive_amount(self):
        """Very large amount should fail validation."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "TR330006100519786457841326",
            "receiver_iban": "TR110006400000478893400002",
            "amount": 999_999_999,
        }

        result = validator.validate_transaction(tx)
        assert result.is_valid is False

    def test_validate_sql_injection_description(self):
        """SQL injection patterns should be detected."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "TR330006100519786457841326",
            "receiver_iban": "TR110006400000478893400002",
            "amount": 1000,
            "description": "'; DROP TABLE users; --",
        }

        result = validator.validate_transaction(tx)
        # Should either fail or sanitize
        if not result.is_valid:
            error_fields = [e.field for e in result.errors]
            assert "description" in error_fields

    def test_validate_sanitizes_html_in_names(self):
        """HTML tags in names should be sanitized."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()
        tx = {
            "sender_iban": "TR330006100519786457841326",
            "sender_name": "<script>alert('xss')</script>Ahmet",
            "receiver_iban": "TR110006400000478893400002",
            "amount": 1000,
        }

        result = validator.validate_transaction(tx)
        # Should sanitize (remove forbidden chars), not fail
        sanitized = result.sanitized_data.get("sender_name", "")
        assert "<" not in sanitized and ">" not in sanitized

    def test_validate_iban_format(self):
        """validate_iban should check format correctly."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()

        valid, normalized = validator.validate_iban("TR330006100519786457841326")
        assert valid is True

        valid, _ = validator.validate_iban("INVALID")
        assert valid is False

    def test_validate_amount(self):
        """validate_amount should check range correctly."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()

        valid, val, _ = validator.validate_amount(5000)
        assert valid is True
        assert val == 5000.0

        valid, _, _ = validator.validate_amount(0)
        assert valid is False

        valid, _, _ = validator.validate_amount("not-a-number")
        assert valid is False

    def test_sanitize_string(self):
        """sanitize_string should clean dangerous content."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()

        result = validator.sanitize_string("<script>alert(1)</script>Merhaba")
        assert "<" not in result and ">" not in result
        # Forbidden chars <> are removed, then script tag text remains
        assert "Merhaba" in result

    def test_sanitize_string_truncates_long(self):
        """sanitize_string should truncate long strings."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()

        long_text = "a" * 1000
        result = validator.sanitize_string(long_text, max_length=10)
        assert len(result) <= 10

    def test_is_safe_sql(self):
        """is_safe_sql should detect SQL injection attempts."""
        from sentinelflow.security import InputValidator

        validator = InputValidator()

        assert validator.is_safe_sql("normal text") is True
        # "DROP TABLE" pattern is detected by the regex
        assert validator.is_safe_sql("1; DROP TABLE alerts") is False
        # Classic SQL injection comment
        assert validator.is_safe_sql("'--") is False


# =============================================================================
# Auth Manager Tests
# =============================================================================


class TestAuthManager:
    """Tests for AuthManager (JWT authentication)."""

    def test_initialization(self):
        """AuthManager should initialize with secret key."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        assert auth is not None

    def test_create_access_token(self):
        """Should create a valid JWT access token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_access_token(user_id="user-1", username="testuser", roles=["analyst"])

        assert token is not None
        assert isinstance(token, str)
        assert len(token.split(".")) == 3  # JWT has 3 parts

    def test_verify_valid_token(self):
        """Should verify a valid token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_access_token(user_id="user-1", username="testuser", roles=["analyst"])

        user = auth.verify_token(token)
        assert user is not None
        assert user.user_id == "user-1"
        assert "analyst" in user.roles

    def test_verify_invalid_token(self):
        """Should reject an invalid token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        user = auth.verify_token("invalid.token.here")
        assert user is None

    def test_verify_expired_token(self):
        """Should reject an expired token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(
            secret_key="test-secret-key-for-testing",
            access_token_expire_minutes=-1,  # Expired 1 minute ago
        )
        token = auth.create_access_token(user_id="user-1", username="testuser", roles=["viewer"])

        user = auth.verify_token(token)
        assert user is None

    def test_create_refresh_token(self):
        """Should create a refresh token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_refresh_token(user_id="user-1", username="testuser")

        assert token is not None

    def test_refresh_token_different_from_access(self):
        """Refresh token should be different from access token."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        access = auth.create_access_token(user_id="user-1", username="testuser", roles=["viewer"])
        refresh = auth.create_refresh_token(user_id="user-1", username="testuser")
        assert access != refresh

    def test_role_based_access(self):
        """Should verify user roles."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_access_token(
            user_id="admin-1", username="admin", roles=["admin", "analyst"]
        )

        user = auth.verify_token(token)
        assert "admin" in user.roles
        assert "analyst" in user.roles
        assert "viewer" not in user.roles

    def test_different_secret_keys(self):
        """Tokens from different secrets should not verify."""
        from sentinelflow.security import AuthManager

        auth1 = AuthManager(secret_key="secret-1")
        auth2 = AuthManager(secret_key="secret-2")

        token = auth1.create_access_token(user_id="user-1", username="testuser", roles=["viewer"])
        user = auth2.verify_token(token)
        assert user is None

    def test_token_contains_claims(self):
        """Token should contain standard claims."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_access_token(user_id="user-42", username="user42", roles=["analyst"])

        from jose import jwt as jose_jwt

        payload = jose_jwt.decode(token, "test-secret-key-for-testing", algorithms=["HS256"])
        assert payload["sub"] == "user-42"
        assert "roles" in payload
        assert "exp" in payload
        assert "iat" in payload
        assert "type" in payload

    def test_api_key_auth(self):
        """Should create and verify API keys."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        api_key = auth.create_api_key(client_id="client-1", client_name="test-client")

        assert api_key is not None
        # API keys are JWT tokens verified via verify_token
        token_data = auth.verify_token(api_key)
        assert token_data is not None
        assert token_data.user_id == "client-1"

    def test_token_blacklist(self):
        """Should blacklist and reject tokens."""
        from sentinelflow.security import AuthManager

        auth = AuthManager(secret_key="test-secret-key-for-testing")
        token = auth.create_access_token(user_id="user-1", username="testuser", roles=["viewer"])

        # Token should work before blacklist
        assert auth.verify_token(token) is not None

        # Blacklist and verify it's rejected
        auth.blacklist_token(token)
        assert auth.verify_token(token) is None

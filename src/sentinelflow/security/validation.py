# =============================================================================
# SentinelFlow - Input Validation
# =============================================================================
"""
Input validation and sanitization for SentinelFlow.

Provides:
- Transaction data validation
- IBAN validation
- Amount validation
- Description sanitization
- Pydantic model validation

Example:
    >>> validator = InputValidator()
    >>> result = validator.validate_transaction(tx_data)
    >>> if not result.is_valid:
    ...     print(result.errors)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic import ValidationError as PydanticValidationError

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False


# =============================================================================
# Validation Patterns
# =============================================================================

# Turkish IBAN: TR + 2 check digits + 5 bank code + 1 reserve + 16 account
IBAN_PATTERN = re.compile(r"^TR\d{24}$")

# Generic IBAN (international)
GENERIC_IBAN_PATTERN = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{4,30}$")

# Amount limits
MIN_AMOUNT = 0.01
MAX_AMOUNT = 100_000_000.0  # 100 million TRY

# Description limits
MAX_DESCRIPTION_LENGTH = 500
FORBIDDEN_CHARS = re.compile(r"[<>{}|\[\]\\^`]")
SQL_INJECTION_PATTERN = re.compile(
    r"(union\s+select|insert\s+into|drop\s+table|delete\s+from|update\s+.+\s+set|"
    r"exec\s*\(|execute\s*\(|'--|\*/|/\*)",
    re.IGNORECASE,
)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class ValidationError:
    """A single validation error."""

    field: str
    message: str
    code: str = "invalid"
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    sanitized_data: dict[str, Any] = field(default_factory=dict)

    def add_error(self, field: str, message: str, code: str = "invalid", value: Any = None):
        self.errors.append(ValidationError(field, message, code, value))
        self.is_valid = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "errors": [
                {"field": e.field, "message": e.message, "code": e.code} for e in self.errors
            ],
        }


# =============================================================================
# Pydantic Models
# =============================================================================

if HAS_PYDANTIC:

    class TransactionInput(BaseModel):
        """Validated transaction input model."""

        transaction_id: str = Field(default="", max_length=100)
        sender_iban: str = Field(..., min_length=26, max_length=34)
        sender_name: str = Field(..., min_length=1, max_length=200)
        sender_city: str = Field(default="", max_length=100)
        receiver_iban: str = Field(..., min_length=26, max_length=34)
        receiver_name: str = Field(..., min_length=1, max_length=200)
        receiver_city: str = Field(default="", max_length=100)
        amount: float = Field(..., gt=0, le=MAX_AMOUNT)
        description: str = Field(default="", max_length=MAX_DESCRIPTION_LENGTH)
        timestamp: str = Field(default="")

        @field_validator("sender_iban", "receiver_iban")
        @classmethod
        def validate_iban(cls, v: str) -> str:
            v = v.upper().replace(" ", "")
            if not GENERIC_IBAN_PATTERN.match(v):
                raise ValueError("Invalid IBAN format")
            return v

        @field_validator("sender_name", "receiver_name")
        @classmethod
        def validate_name(cls, v: str) -> str:
            v = v.strip()
            if FORBIDDEN_CHARS.search(v):
                raise ValueError("Name contains forbidden characters")
            return v

        @field_validator("description")
        @classmethod
        def validate_description(cls, v: str) -> str:
            v = v.strip()
            if SQL_INJECTION_PATTERN.search(v):
                raise ValueError("Description contains suspicious patterns")
            v = FORBIDDEN_CHARS.sub("", v)
            return v

        @field_validator("timestamp")
        @classmethod
        def validate_timestamp(cls, v: str) -> str:
            if not v:
                return datetime.now().isoformat()
            try:
                datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("Invalid timestamp format") from exc
            return v


# =============================================================================
# Input Validator
# =============================================================================


class InputValidator:
    """
    Input validation and sanitization engine.

    Example:
        >>> validator = InputValidator()
        >>> result = validator.validate_transaction({
        ...     "sender_iban": "TR123456789012345678901234",
        ...     "receiver_iban": "TR098765432109876543210987",
        ...     "amount": 5000.0,
        ... })
    """

    def __init__(
        self,
        strict_mode: bool = True,
        allow_missing_optional: bool = True,
    ):
        """
        Initialize validator.

        Args:
            strict_mode: Fail on any validation error
            allow_missing_optional: Allow missing optional fields
        """
        self._strict_mode = strict_mode
        self._allow_missing_optional = allow_missing_optional

        logger.info("InputValidator initialized")

    def validate_transaction(
        self,
        data: dict[str, Any],
    ) -> ValidationResult:
        """
        Validate transaction data.

        Args:
            data: Transaction data dictionary

        Returns:
            ValidationResult with errors and sanitized data
        """
        result = ValidationResult(is_valid=True)

        # Required fields
        required_fields = ["sender_iban", "receiver_iban", "amount"]
        for field_name in required_fields:
            if field_name not in data or data[field_name] is None:
                result.add_error(field_name, f"{field_name} is required", "required")

        if not result.is_valid:
            return result

        # Try Pydantic validation if available
        if HAS_PYDANTIC:
            try:
                validated = TransactionInput(**data)
                result.sanitized_data = validated.model_dump()
                return result
            except PydanticValidationError as e:
                for error in e.errors():
                    field_name = ".".join(str(x) for x in error["loc"])
                    result.add_error(
                        field_name,
                        error["msg"],
                        error["type"],
                        data.get(field_name),
                    )
                return result

        # Manual validation fallback
        result = self._validate_manual(data)
        return result

    def _validate_manual(self, data: dict[str, Any]) -> ValidationResult:
        """Manual validation without Pydantic."""
        result = ValidationResult(is_valid=True, sanitized_data={})

        # IBAN validation
        for iban_field in ["sender_iban", "receiver_iban"]:
            iban = data.get(iban_field, "")
            if isinstance(iban, str):
                iban = iban.upper().replace(" ", "")
                if not GENERIC_IBAN_PATTERN.match(iban):
                    result.add_error(iban_field, "Invalid IBAN format", "pattern", iban)
                else:
                    result.sanitized_data[iban_field] = iban

        # Amount validation
        amount = data.get("amount")
        if amount is not None:
            try:
                amount = float(amount)
                if amount <= MIN_AMOUNT:
                    result.add_error("amount", f"Amount must be greater than {MIN_AMOUNT}", "min")
                elif amount > MAX_AMOUNT:
                    result.add_error("amount", f"Amount must be less than {MAX_AMOUNT}", "max")
                else:
                    result.sanitized_data["amount"] = amount
            except (TypeError, ValueError):
                result.add_error("amount", "Amount must be a number", "type")

        # Name validation
        for name_field in ["sender_name", "receiver_name"]:
            name = data.get(name_field, "")
            if isinstance(name, str):
                name = name.strip()
                if FORBIDDEN_CHARS.search(name):
                    name = FORBIDDEN_CHARS.sub("", name)
                result.sanitized_data[name_field] = name

        # Description sanitization
        description = data.get("description", "")
        if isinstance(description, str):
            description = description.strip()
            if SQL_INJECTION_PATTERN.search(description):
                result.add_error(
                    "description", "Description contains suspicious patterns", "security"
                )
            description = FORBIDDEN_CHARS.sub("", description)
            result.sanitized_data["description"] = description[:MAX_DESCRIPTION_LENGTH]

        # Copy other fields
        for field_name in ["transaction_id", "sender_city", "receiver_city", "timestamp"]:
            if field_name in data:
                value = data[field_name]
                if isinstance(value, str):
                    value = value.strip()
                    value = FORBIDDEN_CHARS.sub("", value)
                result.sanitized_data[field_name] = value

        return result

    def validate_iban(self, iban: str) -> tuple[bool, str]:
        """
        Validate IBAN format.

        Args:
            iban: IBAN string

        Returns:
            (is_valid, normalized_iban)
        """
        if not iban:
            return False, ""

        iban = iban.upper().replace(" ", "")

        if IBAN_PATTERN.match(iban):
            return True, iban

        if GENERIC_IBAN_PATTERN.match(iban):
            return True, iban

        return False, iban

    def validate_amount(self, amount: Any) -> tuple[bool, float, str]:
        """
        Validate transaction amount.

        Args:
            amount: Amount value

        Returns:
            (is_valid, amount, error_message)
        """
        try:
            amount = float(amount)

            if amount <= MIN_AMOUNT:
                return False, amount, f"Amount must be greater than {MIN_AMOUNT}"

            if amount > MAX_AMOUNT:
                return False, amount, f"Amount exceeds maximum of {MAX_AMOUNT:,.2f}"

            return True, round(amount, 2), ""

        except (TypeError, ValueError):
            return False, 0.0, "Amount must be a valid number"

    def sanitize_string(
        self,
        value: str,
        max_length: int = 500,
        strip_html: bool = True,
    ) -> str:
        """
        Sanitize a string value.

        Args:
            value: Input string
            max_length: Maximum length
            strip_html: Remove HTML-like content

        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            return str(value)

        # Strip whitespace
        value = value.strip()

        # Remove forbidden characters
        value = FORBIDDEN_CHARS.sub("", value)

        # Strip HTML tags if requested
        if strip_html:
            value = re.sub(r"<[^>]+>", "", value)

        # Truncate
        if len(value) > max_length:
            value = value[:max_length]

        return value

    def is_safe_sql(self, value: str) -> bool:
        """Check if string is safe from SQL injection."""
        return not SQL_INJECTION_PATTERN.search(value)

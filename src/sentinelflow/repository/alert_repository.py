# =============================================================================
# SentinelFlow - Alert Repository
# =============================================================================
"""
Repository for alert database operations.

Features:
- Idempotent writes (no duplicate alerts for same transaction)
- Async and sync interfaces
- Pagination and filtering
- WebSocket notification triggers
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from loguru import logger
from sqlalchemy import and_, desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from sentinelflow.contracts import Alert, AlertCreate, FraudType, Severity
from sentinelflow.database.models import AlertModel


def generate_alert_id() -> str:
    """Generate unique alert ID."""
    return f"ALERT-{uuid4().hex[:12].upper()}"


class AlertRepository:
    """
    Repository for alert CRUD operations.

    Usage:
        repo = AlertRepository(session)
        alert = repo.create(alert_create)
        alerts = repo.list(page=1, severity="high")
    """

    def __init__(self, session: Session | AsyncSession):
        """Initialize with database session."""
        self._session = session
        self._is_async = isinstance(session, AsyncSession)

    # =========================================================================
    # Create Operations
    # =========================================================================

    def create(
        self,
        alert_data: AlertCreate | dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> Alert:
        """
        Create a new alert (sync).

        Args:
            alert_data: Alert data (AlertCreate or dict)
            idempotency_key: Optional key for idempotent writes (default: transaction_id)

        Returns:
            Created Alert
        """
        if isinstance(alert_data, dict):
            alert_data = AlertCreate(**alert_data)

        # Use transaction_id as idempotency key if not provided

        # Check for existing alert with same idempotency key (within recent window)
        existing = self._check_duplicate_sync(alert_data.transaction_id, alert_data.fraud_type)
        if existing:
            logger.debug(f"Duplicate alert skipped: {existing.alert_id}")
            return Alert.model_validate(existing.to_dict())

        # Create new alert
        alert_id = generate_alert_id()
        now = datetime.now(timezone.utc)

        # Serialize evidence
        evidence_data = []
        if alert_data.evidence:
            evidence_data = [
                e.model_dump(mode="json") if hasattr(e, "model_dump") else e
                for e in alert_data.evidence
            ]

        model = AlertModel(
            alert_id=alert_id,
            fraud_type=(
                alert_data.fraud_type.value
                if isinstance(alert_data.fraud_type, FraudType)
                else alert_data.fraud_type
            ),
            severity=(
                alert_data.severity.value
                if isinstance(alert_data.severity, Severity)
                else alert_data.severity
            ),
            confidence=alert_data.confidence,
            transaction_id=alert_data.transaction_id,
            sender_iban=alert_data.sender_iban,
            sender_name=alert_data.sender_name,
            sender_city=alert_data.sender_city or "",
            receiver_iban=alert_data.receiver_iban,
            receiver_name=alert_data.receiver_name,
            receiver_city=alert_data.receiver_city or "",
            amount=alert_data.amount,
            currency=alert_data.currency,
            title=alert_data.title or "",
            description=alert_data.description or "",
            evidence={"items": evidence_data},
            related_transactions=alert_data.related_transactions or [],
            related_accounts=alert_data.related_accounts or [],
            detected_at=now,
            updated_at=now,
        )

        self._session.add(model)
        self._session.flush()

        logger.info(f"Alert created: {alert_id} | {alert_data.fraud_type} | {alert_data.severity}")

        return Alert.model_validate(model.to_dict())

    async def create_async(
        self,
        alert_data: AlertCreate | dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> Alert:
        """Create a new alert (async)."""
        if isinstance(alert_data, dict):
            alert_data = AlertCreate(**alert_data)

        # Check for existing alert
        existing = await self._check_duplicate_async(
            alert_data.transaction_id, alert_data.fraud_type
        )
        if existing:
            logger.debug(f"Duplicate alert skipped: {existing.alert_id}")
            return Alert.model_validate(existing.to_dict())

        # Create new alert
        alert_id = generate_alert_id()
        now = datetime.now(timezone.utc)

        evidence_data = []
        if alert_data.evidence:
            evidence_data = [
                e.model_dump(mode="json") if hasattr(e, "model_dump") else e
                for e in alert_data.evidence
            ]

        model = AlertModel(
            alert_id=alert_id,
            fraud_type=(
                alert_data.fraud_type.value
                if isinstance(alert_data.fraud_type, FraudType)
                else alert_data.fraud_type
            ),
            severity=(
                alert_data.severity.value
                if isinstance(alert_data.severity, Severity)
                else alert_data.severity
            ),
            confidence=alert_data.confidence,
            transaction_id=alert_data.transaction_id,
            sender_iban=alert_data.sender_iban,
            sender_name=alert_data.sender_name,
            sender_city=alert_data.sender_city or "",
            receiver_iban=alert_data.receiver_iban,
            receiver_name=alert_data.receiver_name,
            receiver_city=alert_data.receiver_city or "",
            amount=alert_data.amount,
            currency=alert_data.currency,
            title=alert_data.title or "",
            description=alert_data.description or "",
            evidence={"items": evidence_data},
            related_transactions=alert_data.related_transactions or [],
            related_accounts=alert_data.related_accounts or [],
            detected_at=now,
            updated_at=now,
        )

        self._session.add(model)
        await self._session.flush()

        logger.info(f"Alert created: {alert_id} | {alert_data.fraud_type} | {alert_data.severity}")

        return Alert.model_validate(model.to_dict())

    # =========================================================================
    # Read Operations
    # =========================================================================

    def get_by_id(self, alert_id: str) -> Alert | None:
        """Get alert by ID (sync)."""
        stmt = select(AlertModel).where(AlertModel.alert_id == alert_id)
        result = self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return Alert.model_validate(model.to_dict()) if model else None

    async def get_by_id_async(self, alert_id: str) -> Alert | None:
        """Get alert by ID (async)."""
        stmt = select(AlertModel).where(AlertModel.alert_id == alert_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return Alert.model_validate(model.to_dict()) if model else None

    def list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        fraud_type: str | None = None,
        severity: str | None = None,
        case_id: str | None = None,
        is_dismissed: bool | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sender_iban: str | None = None,
        receiver_iban: str | None = None,
    ) -> tuple[list[Alert], int]:
        """
        List alerts with pagination and filtering (sync).

        Returns:
            (alerts, total_count)
        """
        stmt = select(AlertModel)
        count_stmt = select(func.count(AlertModel.alert_id))

        # Apply filters
        conditions = []
        if fraud_type:
            conditions.append(AlertModel.fraud_type == fraud_type)
        if severity:
            conditions.append(AlertModel.severity == severity)
        if case_id:
            conditions.append(AlertModel.case_id == case_id)
        if is_dismissed is not None:
            conditions.append(AlertModel.is_dismissed == is_dismissed)
        if start_date:
            conditions.append(AlertModel.detected_at >= start_date)
        if end_date:
            conditions.append(AlertModel.detected_at <= end_date)
        if sender_iban:
            conditions.append(AlertModel.sender_iban == sender_iban)
        if receiver_iban:
            conditions.append(AlertModel.receiver_iban == receiver_iban)

        if conditions:
            stmt = stmt.where(and_(*conditions))
            count_stmt = count_stmt.where(and_(*conditions))

        # Get total count
        total = self._session.execute(count_stmt).scalar() or 0

        # Apply ordering and pagination
        stmt = stmt.order_by(desc(AlertModel.detected_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        result = self._session.execute(stmt)
        models = result.scalars().all()

        alerts = [Alert.model_validate(m.to_dict()) for m in models]
        return alerts, total

    async def list_async(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        fraud_type: str | None = None,
        severity: str | None = None,
        case_id: str | None = None,
        is_dismissed: bool | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> tuple[list[Alert], int]:
        """List alerts with pagination and filtering (async)."""
        stmt = select(AlertModel)
        count_stmt = select(func.count(AlertModel.alert_id))

        conditions = []
        if fraud_type:
            conditions.append(AlertModel.fraud_type == fraud_type)
        if severity:
            conditions.append(AlertModel.severity == severity)
        if case_id:
            conditions.append(AlertModel.case_id == case_id)
        if is_dismissed is not None:
            conditions.append(AlertModel.is_dismissed == is_dismissed)
        if start_date:
            conditions.append(AlertModel.detected_at >= start_date)
        if end_date:
            conditions.append(AlertModel.detected_at <= end_date)

        if conditions:
            stmt = stmt.where(and_(*conditions))
            count_stmt = count_stmt.where(and_(*conditions))

        total_result = await self._session.execute(count_stmt)
        total = total_result.scalar() or 0

        stmt = stmt.order_by(desc(AlertModel.detected_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        result = await self._session.execute(stmt)
        models = result.scalars().all()

        alerts = [Alert.model_validate(m.to_dict()) for m in models]
        return alerts, total

    # =========================================================================
    # Update Operations
    # =========================================================================

    def dismiss(
        self,
        alert_id: str,
        dismissed_by: str,
        reason: str | None = None,
    ) -> Alert | None:
        """Dismiss an alert (sync)."""
        stmt = (
            update(AlertModel)
            .where(AlertModel.alert_id == alert_id)
            .values(
                is_dismissed=True,
                dismissed_by=dismissed_by,
                dismissed_at=datetime.now(timezone.utc),
                dismissed_reason=reason,
                updated_at=datetime.now(timezone.utc),
            )
            .returning(AlertModel)
        )
        result = self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return Alert.model_validate(model.to_dict()) if model else None

    def link_to_case(self, alert_id: str, case_id: str) -> bool:
        """Link alert to a case (sync)."""
        stmt = (
            update(AlertModel)
            .where(AlertModel.alert_id == alert_id)
            .values(case_id=case_id, updated_at=datetime.now(timezone.utc))
        )
        result = self._session.execute(stmt)
        return result.rowcount > 0

    # =========================================================================
    # Stats Operations
    # =========================================================================

    def get_stats(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Get alert statistics (sync)."""
        conditions = []
        if start_date:
            conditions.append(AlertModel.detected_at >= start_date)
        if end_date:
            conditions.append(AlertModel.detected_at <= end_date)

        base_query = select(AlertModel)
        if conditions:
            base_query = base_query.where(and_(*conditions))

        # Total count
        total = (
            self._session.execute(
                select(func.count(AlertModel.alert_id)).where(and_(*conditions))
                if conditions
                else select(func.count(AlertModel.alert_id))
            ).scalar()
            or 0
        )

        # By fraud type
        by_type_query = select(AlertModel.fraud_type, func.count(AlertModel.alert_id)).group_by(
            AlertModel.fraud_type
        )
        if conditions:
            by_type_query = by_type_query.where(and_(*conditions))
        by_type = dict(self._session.execute(by_type_query).all())

        # By severity
        by_severity_query = select(AlertModel.severity, func.count(AlertModel.alert_id)).group_by(
            AlertModel.severity
        )
        if conditions:
            by_severity_query = by_severity_query.where(and_(*conditions))
        by_severity = dict(self._session.execute(by_severity_query).all())

        return {
            "total": total,
            "by_fraud_type": by_type,
            "by_severity": by_severity,
        }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _check_duplicate_sync(
        self, transaction_id: str, fraud_type: FraudType | str
    ) -> AlertModel | None:
        """Check for duplicate alert (sync)."""
        ft = fraud_type.value if isinstance(fraud_type, FraudType) else fraud_type
        stmt = select(AlertModel).where(
            and_(
                AlertModel.transaction_id == transaction_id,
                AlertModel.fraud_type == ft,
            )
        )
        result = self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def _check_duplicate_async(
        self, transaction_id: str, fraud_type: FraudType | str
    ) -> AlertModel | None:
        """Check for duplicate alert (async)."""
        ft = fraud_type.value if isinstance(fraud_type, FraudType) else fraud_type
        stmt = select(AlertModel).where(
            and_(
                AlertModel.transaction_id == transaction_id,
                AlertModel.fraud_type == ft,
            )
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

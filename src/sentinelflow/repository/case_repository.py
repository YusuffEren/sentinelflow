# =============================================================================
# SentinelFlow - Case Repository
# =============================================================================
"""
Repository for case database operations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select, func, and_, desc, update
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from sentinelflow.database.models import CaseModel, AlertModel
from sentinelflow.contracts import Case, CaseCreate, CaseStatus, CasePriority, Severity


def generate_case_id() -> str:
    """Generate unique case ID."""
    return f"CASE-{uuid4().hex[:12].upper()}"


class CaseRepository:
    """Repository for case CRUD operations."""
    
    def __init__(self, session: Session | AsyncSession):
        self._session = session
        self._is_async = isinstance(session, AsyncSession)
    
    # =========================================================================
    # Create Operations
    # =========================================================================
    
    def create(self, case_data: CaseCreate | dict[str, Any]) -> Case:
        """Create a new case (sync)."""
        if isinstance(case_data, dict):
            case_data = CaseCreate(**case_data)
        
        case_id = generate_case_id()
        now = datetime.now(timezone.utc)
        
        model = CaseModel(
            case_id=case_id,
            title=case_data.title,
            description=case_data.description or "",
            status="new",
            priority=case_data.priority.value if isinstance(case_data.priority, CasePriority) else case_data.priority,
            primary_fraud_type=case_data.primary_fraud_type.value if case_data.primary_fraud_type else None,
            tags=case_data.tags or [],
            assigned_to=case_data.assigned_to,
            assigned_team=case_data.assigned_team,
            alert_count=len(case_data.alert_ids),
            created_at=now,
            updated_at=now,
        )
        
        self._session.add(model)
        self._session.flush()
        
        # Link alerts to case
        if case_data.alert_ids:
            self._link_alerts_sync(case_id, case_data.alert_ids)
            self._update_case_aggregates_sync(case_id)
        
        # Refresh to get aggregated values
        self._session.refresh(model)
        
        logger.info(f"Case created: {case_id} | {len(case_data.alert_ids)} alerts")
        
        return Case.model_validate(model.to_dict())
    
    async def create_async(self, case_data: CaseCreate | dict[str, Any]) -> Case:
        """Create a new case (async)."""
        if isinstance(case_data, dict):
            case_data = CaseCreate(**case_data)
        
        case_id = generate_case_id()
        now = datetime.now(timezone.utc)
        
        model = CaseModel(
            case_id=case_id,
            title=case_data.title,
            description=case_data.description or "",
            status="new",
            priority=case_data.priority.value if isinstance(case_data.priority, CasePriority) else case_data.priority,
            primary_fraud_type=case_data.primary_fraud_type.value if case_data.primary_fraud_type else None,
            tags=case_data.tags or [],
            assigned_to=case_data.assigned_to,
            assigned_team=case_data.assigned_team,
            alert_count=len(case_data.alert_ids),
            created_at=now,
            updated_at=now,
        )
        
        self._session.add(model)
        await self._session.flush()
        
        if case_data.alert_ids:
            await self._link_alerts_async(case_id, case_data.alert_ids)
            await self._update_case_aggregates_async(case_id)
        
        await self._session.refresh(model)
        
        logger.info(f"Case created: {case_id} | {len(case_data.alert_ids)} alerts")
        
        return Case.model_validate(model.to_dict())
    
    # =========================================================================
    # Read Operations
    # =========================================================================
    
    def get_by_id(self, case_id: str) -> Case | None:
        """Get case by ID (sync)."""
        stmt = select(CaseModel).where(CaseModel.case_id == case_id)
        result = self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return Case.model_validate(model.to_dict()) if model else None
    
    async def get_by_id_async(self, case_id: str) -> Case | None:
        """Get case by ID (async)."""
        stmt = select(CaseModel).where(CaseModel.case_id == case_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return Case.model_validate(model.to_dict()) if model else None
    
    def list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
        priority: str | None = None,
        assigned_to: str | None = None,
        is_open: bool | None = None,
    ) -> tuple[list[Case], int]:
        """List cases with pagination and filtering (sync)."""
        stmt = select(CaseModel)
        count_stmt = select(func.count(CaseModel.case_id))
        
        conditions = []
        if status:
            conditions.append(CaseModel.status == status)
        if priority:
            conditions.append(CaseModel.priority == priority)
        if assigned_to:
            conditions.append(CaseModel.assigned_to == assigned_to)
        if is_open is not None:
            open_statuses = ["new", "triage", "investigating", "escalated", "pending_info"]
            if is_open:
                conditions.append(CaseModel.status.in_(open_statuses))
            else:
                conditions.append(~CaseModel.status.in_(open_statuses))
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
            count_stmt = count_stmt.where(and_(*conditions))
        
        total = self._session.execute(count_stmt).scalar() or 0
        
        stmt = stmt.order_by(desc(CaseModel.created_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        
        result = self._session.execute(stmt)
        models = result.scalars().all()
        
        cases = [Case.model_validate(m.to_dict()) for m in models]
        return cases, total
    
    async def list_async(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
        priority: str | None = None,
        assigned_to: str | None = None,
    ) -> tuple[list[Case], int]:
        """List cases with pagination and filtering (async)."""
        stmt = select(CaseModel)
        count_stmt = select(func.count(CaseModel.case_id))
        
        conditions = []
        if status:
            conditions.append(CaseModel.status == status)
        if priority:
            conditions.append(CaseModel.priority == priority)
        if assigned_to:
            conditions.append(CaseModel.assigned_to == assigned_to)
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
            count_stmt = count_stmt.where(and_(*conditions))
        
        total_result = await self._session.execute(count_stmt)
        total = total_result.scalar() or 0
        
        stmt = stmt.order_by(desc(CaseModel.created_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        
        result = await self._session.execute(stmt)
        models = result.scalars().all()
        
        cases = [Case.model_validate(m.to_dict()) for m in models]
        return cases, total
    
    # =========================================================================
    # Update Operations
    # =========================================================================
    
    def update_status(
        self,
        case_id: str,
        new_status: CaseStatus | str,
        resolution: str | None = None,
        resolved_by: str | None = None,
    ) -> Case | None:
        """Update case status (sync)."""
        status_val = new_status.value if isinstance(new_status, CaseStatus) else new_status
        now = datetime.now(timezone.utc)
        
        values = {
            "status": status_val,
            "updated_at": now,
        }
        
        if resolution:
            values["resolution"] = resolution
        if resolved_by:
            values["resolved_by"] = resolved_by
        if status_val in ["resolved_true_positive", "resolved_false_positive", "closed"]:
            values["resolved_at"] = now
        
        stmt = (
            update(CaseModel)
            .where(CaseModel.case_id == case_id)
            .values(**values)
        )
        self._session.execute(stmt)
        
        return self.get_by_id(case_id)
    
    def assign(
        self,
        case_id: str,
        assigned_to: str | None,
        assigned_team: str | None = None,
    ) -> Case | None:
        """Assign case to analyst/team (sync)."""
        stmt = (
            update(CaseModel)
            .where(CaseModel.case_id == case_id)
            .values(
                assigned_to=assigned_to,
                assigned_team=assigned_team,
                updated_at=datetime.now(timezone.utc),
            )
        )
        self._session.execute(stmt)
        return self.get_by_id(case_id)
    
    def add_alert(self, case_id: str, alert_id: str) -> bool:
        """Add alert to existing case (sync)."""
        result = self._link_alerts_sync(case_id, [alert_id])
        if result:
            self._update_case_aggregates_sync(case_id)
        return result
    
    # =========================================================================
    # Stats Operations
    # =========================================================================
    
    def get_stats(self) -> dict[str, Any]:
        """Get case statistics (sync)."""
        total = self._session.execute(select(func.count(CaseModel.case_id))).scalar() or 0
        
        # By status
        by_status = dict(
            self._session.execute(
                select(CaseModel.status, func.count(CaseModel.case_id))
                .group_by(CaseModel.status)
            ).all()
        )
        
        # By priority
        by_priority = dict(
            self._session.execute(
                select(CaseModel.priority, func.count(CaseModel.case_id))
                .group_by(CaseModel.priority)
            ).all()
        )
        
        # Open vs closed
        open_statuses = ["new", "triage", "investigating", "escalated", "pending_info"]
        open_count = self._session.execute(
            select(func.count(CaseModel.case_id))
            .where(CaseModel.status.in_(open_statuses))
        ).scalar() or 0
        
        return {
            "total": total,
            "open": open_count,
            "closed": total - open_count,
            "by_status": by_status,
            "by_priority": by_priority,
        }
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _link_alerts_sync(self, case_id: str, alert_ids: list[str]) -> bool:
        """Link alerts to case (sync)."""
        if not alert_ids:
            return False
        
        stmt = (
            update(AlertModel)
            .where(AlertModel.alert_id.in_(alert_ids))
            .values(case_id=case_id, updated_at=datetime.now(timezone.utc))
        )
        result = self._session.execute(stmt)
        return result.rowcount > 0
    
    async def _link_alerts_async(self, case_id: str, alert_ids: list[str]) -> bool:
        """Link alerts to case (async)."""
        if not alert_ids:
            return False
        
        stmt = (
            update(AlertModel)
            .where(AlertModel.alert_id.in_(alert_ids))
            .values(case_id=case_id, updated_at=datetime.now(timezone.utc))
        )
        result = await self._session.execute(stmt)
        return result.rowcount > 0
    
    def _update_case_aggregates_sync(self, case_id: str) -> None:
        """Update case aggregated values from alerts (sync)."""
        # Get linked alerts
        alerts_stmt = select(AlertModel).where(AlertModel.case_id == case_id)
        result = self._session.execute(alerts_stmt)
        alerts = result.scalars().all()
        
        if not alerts:
            return
        
        # Calculate aggregates
        total_amount = sum(a.amount for a in alerts)
        fraud_types = list(set(a.fraud_type for a in alerts))
        involved_accounts = list(set(
            [a.sender_iban for a in alerts] + [a.receiver_iban for a in alerts]
        ))
        involved_txns = list(set(a.transaction_id for a in alerts))
        
        # Determine max severity
        severity_order = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        max_sev = max(alerts, key=lambda a: severity_order.get(a.severity, 0)).severity
        
        avg_conf = sum(a.confidence for a in alerts) / len(alerts)
        
        first_alert = min(alerts, key=lambda a: a.detected_at).detected_at
        last_alert = max(alerts, key=lambda a: a.detected_at).detected_at
        
        # Update case
        stmt = (
            update(CaseModel)
            .where(CaseModel.case_id == case_id)
            .values(
                alert_count=len(alerts),
                total_amount=total_amount,
                max_severity=max_sev,
                avg_confidence=avg_conf,
                fraud_types=fraud_types,
                involved_accounts=involved_accounts,
                involved_transactions=involved_txns,
                first_alert_at=first_alert,
                last_alert_at=last_alert,
                primary_fraud_type=fraud_types[0] if fraud_types else None,
                updated_at=datetime.now(timezone.utc),
            )
        )
        self._session.execute(stmt)
    
    async def _update_case_aggregates_async(self, case_id: str) -> None:
        """Update case aggregated values from alerts (async)."""
        alerts_stmt = select(AlertModel).where(AlertModel.case_id == case_id)
        result = await self._session.execute(alerts_stmt)
        alerts = result.scalars().all()
        
        if not alerts:
            return
        
        total_amount = sum(a.amount for a in alerts)
        fraud_types = list(set(a.fraud_type for a in alerts))
        involved_accounts = list(set(
            [a.sender_iban for a in alerts] + [a.receiver_iban for a in alerts]
        ))
        involved_txns = list(set(a.transaction_id for a in alerts))
        
        severity_order = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        max_sev = max(alerts, key=lambda a: severity_order.get(a.severity, 0)).severity
        
        avg_conf = sum(a.confidence for a in alerts) / len(alerts)
        
        first_alert = min(alerts, key=lambda a: a.detected_at).detected_at
        last_alert = max(alerts, key=lambda a: a.detected_at).detected_at
        
        stmt = (
            update(CaseModel)
            .where(CaseModel.case_id == case_id)
            .values(
                alert_count=len(alerts),
                total_amount=total_amount,
                max_severity=max_sev,
                avg_confidence=avg_conf,
                fraud_types=fraud_types,
                involved_accounts=involved_accounts,
                involved_transactions=involved_txns,
                first_alert_at=first_alert,
                last_alert_at=last_alert,
                primary_fraud_type=fraud_types[0] if fraud_types else None,
                updated_at=datetime.now(timezone.utc),
            )
        )
        await self._session.execute(stmt)

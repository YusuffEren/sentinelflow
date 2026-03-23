# =============================================================================
# SentinelFlow - Event Repository (Audit Log)
# =============================================================================
"""
Repository for case event (audit log) operations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select, func, and_, desc
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from sentinelflow.database.models import CaseEventModel
from sentinelflow.contracts import CaseEvent, EventType


def generate_event_id() -> str:
    """Generate unique event ID."""
    return f"EVT-{uuid4().hex[:12].upper()}"


class EventRepository:
    """Repository for audit event operations."""

    def __init__(self, session: Session | AsyncSession):
        self._session = session
        self._is_async = isinstance(session, AsyncSession)

    def create(
        self,
        case_id: str,
        event_type: EventType | str,
        description: str = "",
        *,
        actor: str = "system",
        actor_type: str = "system",
        previous_value: str | None = None,
        new_value: str | None = None,
        alert_id: str | None = None,
        transaction_id: str | None = None,
        extra_data: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> CaseEvent:
        """Create a new audit event (sync)."""
        event_id = generate_event_id()
        now = datetime.now(timezone.utc)

        event_type_val = event_type.value if isinstance(event_type, EventType) else event_type

        model = CaseEventModel(
            event_id=event_id,
            case_id=case_id,
            event_type=event_type_val,
            actor=actor,
            actor_type=actor_type,
            description=description,
            previous_value=previous_value,
            new_value=new_value,
            extra_data=extra_data or {},
            alert_id=alert_id,
            transaction_id=transaction_id,
            created_at=now,
            ip_address=ip_address,
            user_agent=user_agent,
        )

        self._session.add(model)
        self._session.flush()

        logger.debug(f"Event created: {event_id} | {event_type_val} | {case_id}")

        return CaseEvent(
            event_id=event_id,
            case_id=case_id,
            event_type=EventType(event_type_val),
            actor=actor,
            actor_type=actor_type,
            description=description,
            previous_value=previous_value,
            new_value=new_value,
            extra_data=extra_data or {},
            alert_id=alert_id,
            transaction_id=transaction_id,
            created_at=now,
            ip_address=ip_address,
            user_agent=user_agent,
        )

    async def create_async(
        self,
        case_id: str,
        event_type: EventType | str,
        description: str = "",
        *,
        actor: str = "system",
        actor_type: str = "system",
        previous_value: str | None = None,
        new_value: str | None = None,
        alert_id: str | None = None,
        transaction_id: str | None = None,
        extra_data: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> CaseEvent:
        """Create a new audit event (async)."""
        event_id = generate_event_id()
        now = datetime.now(timezone.utc)

        event_type_val = event_type.value if isinstance(event_type, EventType) else event_type

        model = CaseEventModel(
            event_id=event_id,
            case_id=case_id,
            event_type=event_type_val,
            actor=actor,
            actor_type=actor_type,
            description=description,
            previous_value=previous_value,
            new_value=new_value,
            extra_data=extra_data or {},
            alert_id=alert_id,
            transaction_id=transaction_id,
            created_at=now,
            ip_address=ip_address,
            user_agent=user_agent,
        )

        self._session.add(model)
        await self._session.flush()

        logger.debug(f"Event created: {event_id} | {event_type_val} | {case_id}")

        return CaseEvent(
            event_id=event_id,
            case_id=case_id,
            event_type=EventType(event_type_val),
            actor=actor,
            actor_type=actor_type,
            description=description,
            previous_value=previous_value,
            new_value=new_value,
            extra_data=extra_data or {},
            alert_id=alert_id,
            transaction_id=transaction_id,
            created_at=now,
            ip_address=ip_address,
            user_agent=user_agent,
        )

    def list_by_case(
        self,
        case_id: str,
        *,
        page: int = 1,
        page_size: int = 50,
        event_type: str | None = None,
    ) -> tuple[list[CaseEvent], int]:
        """List events for a case (sync)."""
        stmt = select(CaseEventModel).where(CaseEventModel.case_id == case_id)
        count_stmt = select(func.count(CaseEventModel.event_id)).where(
            CaseEventModel.case_id == case_id
        )

        if event_type:
            stmt = stmt.where(CaseEventModel.event_type == event_type)
            count_stmt = count_stmt.where(CaseEventModel.event_type == event_type)

        total = self._session.execute(count_stmt).scalar() or 0

        stmt = stmt.order_by(desc(CaseEventModel.created_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        result = self._session.execute(stmt)
        models = result.scalars().all()

        events = []
        for m in models:
            events.append(
                CaseEvent(
                    event_id=m.event_id,
                    case_id=m.case_id,
                    event_type=(
                        EventType(m.event_type)
                        if m.event_type in [e.value for e in EventType]
                        else m.event_type
                    ),
                    actor=m.actor,
                    actor_type=m.actor_type,
                    description=m.description,
                    previous_value=m.previous_value,
                    new_value=m.new_value,
                    extra_data=m.extra_data,
                    alert_id=m.alert_id,
                    transaction_id=m.transaction_id,
                    created_at=m.created_at,
                    ip_address=m.ip_address,
                    user_agent=m.user_agent,
                )
            )

        return events, total

    async def list_by_case_async(
        self,
        case_id: str,
        *,
        page: int = 1,
        page_size: int = 50,
        event_type: str | None = None,
    ) -> tuple[list[CaseEvent], int]:
        """List events for a case (async)."""
        stmt = select(CaseEventModel).where(CaseEventModel.case_id == case_id)
        count_stmt = select(func.count(CaseEventModel.event_id)).where(
            CaseEventModel.case_id == case_id
        )

        if event_type:
            stmt = stmt.where(CaseEventModel.event_type == event_type)
            count_stmt = count_stmt.where(CaseEventModel.event_type == event_type)

        total_result = await self._session.execute(count_stmt)
        total = total_result.scalar() or 0

        stmt = stmt.order_by(desc(CaseEventModel.created_at))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        result = await self._session.execute(stmt)
        models = result.scalars().all()

        events = []
        for m in models:
            events.append(
                CaseEvent(
                    event_id=m.event_id,
                    case_id=m.case_id,
                    event_type=(
                        EventType(m.event_type)
                        if m.event_type in [e.value for e in EventType]
                        else m.event_type
                    ),
                    actor=m.actor,
                    actor_type=m.actor_type,
                    description=m.description,
                    previous_value=m.previous_value,
                    new_value=m.new_value,
                    extra_data=m.extra_data,
                    alert_id=m.alert_id,
                    transaction_id=m.transaction_id,
                    created_at=m.created_at,
                    ip_address=m.ip_address,
                    user_agent=m.user_agent,
                )
            )

        return events, total

    # =========================================================================
    # Convenience Methods for Common Events
    # =========================================================================

    def log_case_created(self, case_id: str, actor: str = "system") -> CaseEvent:
        """Log case creation event."""
        return self.create(
            case_id=case_id,
            event_type=EventType.CASE_CREATED,
            description="Case created",
            actor=actor,
        )

    def log_status_change(
        self,
        case_id: str,
        old_status: str,
        new_status: str,
        actor: str = "system",
    ) -> CaseEvent:
        """Log status change event."""
        return self.create(
            case_id=case_id,
            event_type=EventType.CASE_STATUS_CHANGED,
            description=f"Status changed from {old_status} to {new_status}",
            previous_value=old_status,
            new_value=new_status,
            actor=actor,
        )

    def log_assignment(
        self,
        case_id: str,
        old_assignee: str | None,
        new_assignee: str | None,
        actor: str = "system",
    ) -> CaseEvent:
        """Log assignment change event."""
        return self.create(
            case_id=case_id,
            event_type=EventType.CASE_ASSIGNED,
            description=f"Case assigned to {new_assignee or 'unassigned'}",
            previous_value=old_assignee,
            new_value=new_assignee,
            actor=actor,
        )

    def log_note_added(
        self,
        case_id: str,
        note_preview: str,
        actor: str,
    ) -> CaseEvent:
        """Log note added event."""
        return self.create(
            case_id=case_id,
            event_type=EventType.NOTE_ADDED,
            description=note_preview[:200] + "..." if len(note_preview) > 200 else note_preview,
            actor=actor,
            actor_type="user",
        )

    def log_alert_linked(
        self,
        case_id: str,
        alert_id: str,
        actor: str = "system",
    ) -> CaseEvent:
        """Log alert linked event."""
        return self.create(
            case_id=case_id,
            event_type=EventType.ALERT_LINKED_TO_CASE,
            description=f"Alert {alert_id} linked to case",
            alert_id=alert_id,
            actor=actor,
        )

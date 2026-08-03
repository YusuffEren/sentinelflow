# =============================================================================
# SentinelFlow API - Case Routes
# =============================================================================
"""
Case management endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from sentinelflow.api.deps import get_db_session
from sentinelflow.api.schemas import (
    Case,
    CaseCreate,
    CaseListResponse,
    CaseUpdate,
)

router = APIRouter(prefix="/cases", tags=["Cases"])


@router.get(
    "",
    response_model=CaseListResponse,
    summary="List cases",
    description="Returns a paginated list of investigation cases.",
)
async def list_cases(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, description="Filter by status"),
    priority: str | None = Query(None, description="Filter by priority"),
    assigned_to: str | None = Query(None, description="Filter by assignee"),
    is_open: bool | None = Query(None, description="Filter open/closed cases"),
    session=Depends(get_db_session),
):
    """List cases with pagination and filtering."""
    from sentinelflow.repository import CaseRepository

    repo = CaseRepository(session)

    cases, total = repo.list(
        page=page,
        page_size=page_size,
        status=status,
        priority=priority,
        assigned_to=assigned_to,
        is_open=is_open,
    )

    return CaseListResponse(
        total=total,
        page=page,
        page_size=page_size,
        cases=cases,
        filters={"status": status, "priority": priority, "assigned_to": assigned_to},
    )


@router.get(
    "/stats",
    summary="Get case statistics",
)
async def get_case_stats(
    session=Depends(get_db_session),
):
    """Get case statistics."""
    from sentinelflow.repository import CaseRepository

    repo = CaseRepository(session)
    return repo.get_stats()


@router.post(
    "",
    response_model=Case,
    summary="Create a new case",
    description="Create a new investigation case from alerts.",
)
async def create_case(
    case_data: CaseCreate,
    session=Depends(get_db_session),
):
    """Create a new case."""
    from sentinelflow.repository import CaseRepository, EventRepository

    case_repo = CaseRepository(session)
    event_repo = EventRepository(session)

    case = case_repo.create(case_data)

    # Log creation event
    event_repo.log_case_created(case.case_id)

    session.commit()

    return case


@router.get(
    "/{case_id}",
    response_model=Case,
    summary="Get case details",
)
async def get_case(
    case_id: str,
    session=Depends(get_db_session),
):
    """Get a specific case by ID."""
    from sentinelflow.repository import CaseRepository

    repo = CaseRepository(session)
    case = repo.get_by_id(case_id)

    if not case:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")

    return case


@router.patch(
    "/{case_id}",
    response_model=Case,
    summary="Update case",
)
async def update_case(
    case_id: str,
    update_data: CaseUpdate,
    session=Depends(get_db_session),
):
    """Update case fields."""
    from sentinelflow.repository import CaseRepository, EventRepository

    case_repo = CaseRepository(session)
    event_repo = EventRepository(session)

    # Get current case
    current = case_repo.get_by_id(case_id)
    if not current:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")

    # Update status if changed
    if update_data.status and update_data.status != current.status:
        old_status = current.status
        case_repo.update_status(
            case_id,
            update_data.status,
            resolution=update_data.resolution,
        )
        event_repo.log_status_change(case_id, old_status, update_data.status.value)

    # Update assignment if changed
    if update_data.assigned_to is not None:
        old_assignee = current.assigned_to
        case_repo.assign(case_id, update_data.assigned_to, update_data.assigned_team)
        event_repo.log_assignment(case_id, old_assignee, update_data.assigned_to)

    # Add note if provided
    if update_data.note:
        event_repo.log_note_added(case_id, update_data.note, actor="system")  # TODO: actual user

    session.commit()

    return case_repo.get_by_id(case_id)


@router.post(
    "/{case_id}/assign",
    response_model=Case,
    summary="Assign case",
)
async def assign_case(
    case_id: str,
    assigned_to: str = Query(..., description="Username to assign to"),
    assigned_team: str | None = Query(None, description="Team name"),
    session=Depends(get_db_session),
):
    """Assign case to analyst/team."""
    from sentinelflow.repository import CaseRepository, EventRepository

    case_repo = CaseRepository(session)
    event_repo = EventRepository(session)

    current = case_repo.get_by_id(case_id)
    if not current:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")

    case = case_repo.assign(case_id, assigned_to, assigned_team)
    event_repo.log_assignment(case_id, current.assigned_to, assigned_to)

    session.commit()

    return case


@router.post(
    "/{case_id}/add-alert/{alert_id}",
    summary="Add alert to case",
)
async def add_alert_to_case(
    case_id: str,
    alert_id: str,
    session=Depends(get_db_session),
):
    """Add an alert to an existing case."""
    from sentinelflow.repository import CaseRepository, EventRepository

    case_repo = CaseRepository(session)
    event_repo = EventRepository(session)

    success = case_repo.add_alert(case_id, alert_id)

    if not success:
        raise HTTPException(status_code=404, detail="Case or alert not found")

    event_repo.log_alert_linked(case_id, alert_id)

    session.commit()

    return {"status": "added", "case_id": case_id, "alert_id": alert_id}


@router.get(
    "/{case_id}/events",
    summary="Get case events (audit log)",
)
async def get_case_events(
    case_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    event_type: str | None = Query(None),
    session=Depends(get_db_session),
):
    """Get audit log events for a case."""
    from sentinelflow.repository import EventRepository

    repo = EventRepository(session)
    events, total = repo.list_by_case(
        case_id,
        page=page,
        page_size=page_size,
        event_type=event_type,
    )

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "events": [e.model_dump() for e in events],
    }

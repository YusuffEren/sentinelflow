# =============================================================================
# SentinelFlow API - Alert Routes
# =============================================================================
"""
Alert endpoints - read alerts from PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Depends
from loguru import logger

from sentinelflow.api.schemas import (
    Alert,
    AlertListResponse,
    FraudType,
    Severity,
)
from sentinelflow.api.deps import get_db_session

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get(
    "",
    response_model=AlertListResponse,
    summary="List fraud alerts",
    description="Returns a paginated list of fraud alerts from the database.",
)
async def list_alerts(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    fraud_type: str | None = Query(None, description="Filter by fraud type"),
    severity: str | None = Query(None, description="Filter by severity"),
    is_dismissed: bool | None = Query(None, description="Filter by dismissed status"),
    start_date: datetime | None = Query(None, description="Filter by start date"),
    end_date: datetime | None = Query(None, description="Filter by end date"),
    sender_iban: str | None = Query(None, description="Filter by sender IBAN"),
    receiver_iban: str | None = Query(None, description="Filter by receiver IBAN"),
    session=Depends(get_db_session),
):
    """List alerts with pagination and filtering."""
    from sentinelflow.repository import AlertRepository
    
    repo = AlertRepository(session)
    
    alerts, total = repo.list(
        page=page,
        page_size=page_size,
        fraud_type=fraud_type,
        severity=severity,
        is_dismissed=is_dismissed,
        start_date=start_date,
        end_date=end_date,
        sender_iban=sender_iban,
        receiver_iban=receiver_iban,
    )
    
    return AlertListResponse(
        total=total,
        page=page,
        page_size=page_size,
        alerts=alerts,
        filters={
            "fraud_type": fraud_type,
            "severity": severity,
            "is_dismissed": is_dismissed,
        },
    )


@router.get(
    "/stats",
    summary="Get alert statistics",
    description="Returns aggregate statistics for alerts.",
)
async def get_alert_stats(
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
    session=Depends(get_db_session),
):
    """Get alert statistics."""
    from sentinelflow.repository import AlertRepository
    
    repo = AlertRepository(session)
    stats = repo.get_stats(start_date=start_date, end_date=end_date)
    
    return stats


@router.get(
    "/{alert_id}",
    response_model=Alert,
    summary="Get alert details",
    description="Returns details for a specific alert.",
)
async def get_alert(
    alert_id: str,
    session=Depends(get_db_session),
):
    """Get a specific alert by ID."""
    from sentinelflow.repository import AlertRepository
    
    repo = AlertRepository(session)
    alert = repo.get_by_id(alert_id)
    
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    
    return alert


@router.post(
    "/{alert_id}/dismiss",
    response_model=Alert,
    summary="Dismiss an alert",
    description="Mark an alert as dismissed (false positive).",
)
async def dismiss_alert(
    alert_id: str,
    reason: str = Query(None, description="Dismissal reason"),
    session=Depends(get_db_session),
):
    """Dismiss an alert."""
    from sentinelflow.repository import AlertRepository
    
    repo = AlertRepository(session)
    
    # TODO: Get actual user from JWT
    alert = repo.dismiss(alert_id, dismissed_by="system", reason=reason)
    
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    
    session.commit()
    
    return alert


@router.post(
    "/{alert_id}/link-case/{case_id}",
    summary="Link alert to case",
    description="Link an alert to an existing case.",
)
async def link_alert_to_case(
    alert_id: str,
    case_id: str,
    session=Depends(get_db_session),
):
    """Link an alert to a case."""
    from sentinelflow.repository import AlertRepository, EventRepository
    from sentinelflow.contracts import EventType
    
    alert_repo = AlertRepository(session)
    event_repo = EventRepository(session)
    
    success = alert_repo.link_to_case(alert_id, case_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    
    # Log event
    event_repo.log_alert_linked(case_id, alert_id)
    
    session.commit()
    
    return {"status": "linked", "alert_id": alert_id, "case_id": case_id}

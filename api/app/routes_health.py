from __future__ import annotations

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_db_session, get_session_factory
from .db_errors import raise_for_db_error
from .observability.health import build_health_payload
from .observability.metrics import CONTENT_TYPE_LATEST, generate_metrics, refresh_db_gauges

router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health(session: AsyncSession = Depends(get_db_session)) -> dict:
    """Overall health: service is running and DB is reachable."""
    db_ready = True
    try:
        await session.execute(text("SELECT 1"))
    except OperationalError:
        db_ready = False

    return build_health_payload(
        component="qonos.api",
        status="ok" if db_ready else "degraded",
        extra={
            "live": True,
            "db_ready": db_ready,
        },
    )


@router.get("/health/live", status_code=status.HTTP_200_OK)
async def live() -> dict:
    """Liveness: process is up and can handle requests."""
    return build_health_payload(
        component="qonos.api",
        status="live",
        extra={"live": True},
    )


@router.get("/health/ready", status_code=status.HTTP_200_OK)
async def ready(session: AsyncSession = Depends(get_db_session)) -> dict:
    """Readiness: verifies that the database connection is working."""
    try:
        await session.execute(text("SELECT 1"))
    except OperationalError as e:
        raise_for_db_error(e)

    return build_health_payload(
        component="qonos.api",
        status="ready",
        extra={"db_ready": True},
    )


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus-compatible operational metrics."""
    await refresh_db_gauges(get_session_factory())
    return Response(content=generate_metrics(), media_type=CONTENT_TYPE_LATEST)

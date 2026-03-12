from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from .auth.context import AuthContext
from .auth.rbac import require_policy
from .db_errors import raise_for_db_error
from .db import get_db_session
from .error_constants import ErrorMessages
from .schemas import Schedule, ScheduleCreate, ScheduleList, ScheduleUpdate
from db.src.models import ActionType as ActionTypeModel, Schedule as ScheduleModel


router = APIRouter(prefix="/v2", tags=["schedules"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@router.get(
    "/schedules",
    response_model=ScheduleList,
)
async def list_schedules(
    marker: Optional[str] = Query(default=None, description="Pagination marker."),
    limit: int = Query(default=100, ge=1, le=1000),
    auth: AuthContext = Depends(require_policy("schedule:read")),
    session: AsyncSession = Depends(get_db_session),
) -> ScheduleList:
    stmt = select(ScheduleModel).where(ScheduleModel.deleted_at.is_(None))
    if not auth.is_admin:
        stmt = stmt.where(ScheduleModel.project_id == auth.project_id)

    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    rows = result.scalars().all()

    schedules = [Schedule.model_validate(row) for row in rows]
    return ScheduleList(schedules=schedules, links=[])


@router.post(
    "/schedules",
    status_code=status.HTTP_201_CREATED,
    response_model=Schedule,
)
async def create_schedule(
    payload: ScheduleCreate,
    auth: AuthContext = Depends(require_policy("schedule:create")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    try:
        action_result = await session.execute(
            select(ActionTypeModel).where(ActionTypeModel.action_type == payload.action_type)
        )
        action = action_result.scalar_one_or_none()
    except OperationalError as e:
        raise_for_db_error(e)

    if action is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown action_type: {payload.action_type}",
        )

    schedule_id = str(uuid4())
    now = _utcnow()
    schedule_row = ScheduleModel(
        id=schedule_id,
        project_id=auth.project_id,
        name=payload.name,
        description=payload.description,
        action_type=payload.action_type,
        cron_expression=payload.cron_expression,
        action_parameters=payload.action_parameters,
        retention_policy=payload.retention_policy.model_dump() if payload.retention_policy else None,
        enabled=payload.enabled,
        next_run_at=None,
        last_run_at=None,
        created_at=now,
        updated_at=now,
        deleted_at=None,
    )
    session.add(schedule_row)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)
    await session.refresh(schedule_row)

    return Schedule.model_validate(schedule_row)


async def get_schedule_or_404(
    schedule_id: str,
    session: AsyncSession,
) -> ScheduleModel:
    result = await session.execute(
        select(ScheduleModel).where(
            ScheduleModel.id == schedule_id,
            ScheduleModel.deleted_at.is_(None),
        )
    )
    schedule = result.scalar_one_or_none()
    if schedule is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorMessages.SCHEDULE_NOT_FOUND,
        )
    return schedule


def ensure_schedule_access_allowed(schedule: ScheduleModel, auth: AuthContext) -> None:
    if not auth.is_admin and schedule.project_id != auth.project_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=ErrorMessages.FORBIDDEN,
        )


@router.get(
    "/schedules/{schedule_id}",
    response_model=Schedule,
)
async def show_schedule(
    schedule_id: str = Path(..., description="Schedule ID"),
    auth: AuthContext = Depends(require_policy("schedule:read")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)
    return Schedule.model_validate(schedule)


@router.put(
    "/schedules/{schedule_id}",
    response_model=Schedule,
)
async def update_schedule(
    schedule_id: str = Path(..., description="Schedule ID"),
    payload: ScheduleUpdate | None = None,
    auth: AuthContext = Depends(require_policy("schedule:update")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)

    if payload is not None:
        update_data = payload.model_dump(exclude_unset=True)
        if "action_parameters" in update_data and update_data["action_parameters"] is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.ACTION_PARAMETERS_NULL,
            )
        for field, value in update_data.items():
            if field == "retention_policy" and value is not None:
                setattr(schedule, field, value)
            else:
                setattr(schedule, field, value)
        schedule.updated_at = _utcnow()

        session.add(schedule)
        try:
            await session.commit()
        except (IntegrityError, OperationalError) as e:
            await session.rollback()
            raise_for_db_error(e)
        await session.refresh(schedule)

    return Schedule.model_validate(schedule)


@router.delete(
    "/schedules/{schedule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_schedule(
    schedule_id: str = Path(..., description="Schedule ID"),
    auth: AuthContext = Depends(require_policy("schedule:delete")),
    session: AsyncSession = Depends(get_db_session),
) -> None:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)

    schedule.deleted_at = _utcnow()
    session.add(schedule)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)


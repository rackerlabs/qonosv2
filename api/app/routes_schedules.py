from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from .auth.keystone_client import HttpKeystoneClient
from .auth.context import AuthContext
from .auth.rbac import require_policy
from .config import get_config
from .utils.cron import compute_next_run_at
from .utils.time import utc_now
from .db_errors import raise_for_db_error
from .db import get_db_session
from .error_constants import ErrorMessages
from .schemas import Schedule, ScheduleCreate, ScheduleList, ScheduleUpdate
from db.src.models import (
    ActionType as ActionTypeModel,
    ExecutionProfile as ExecutionProfileModel,
    Schedule as ScheduleModel,
)

router = APIRouter(prefix="/v2", tags=["schedules"])


async def _validate_trust_for_project(*, project_id: str, trust_id: Optional[str]) -> None:
    normalized = (trust_id or "").strip()
    if not normalized:
        return
    cfg = get_config()
    if not cfg.keystone_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Keystone URL must be configured when trust_id is provided.",
        )
    if not cfg.keystone_service_username or not cfg.keystone_service_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "trust_id requires service_username/service_password credentials "
                "for delegated execution."
            ),
        )
    client = HttpKeystoneClient(
        base_url=cfg.keystone_url,
        auth_mode="password",
        service_app_credential_id=cfg.keystone_service_app_credential_id,
        service_app_credential_secret=cfg.keystone_service_app_credential_secret,
        service_username=cfg.keystone_service_username,
        service_password=cfg.keystone_service_password,
        service_user_domain_name=cfg.keystone_service_user_domain_name,
        verify_ssl=cfg.keystone_verify_ssl,
    )
    try:
        await client.validate_trust(normalized, expected_project_id=project_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid trust_id: {str(exc)}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to validate trust_id with Keystone: {str(exc)}",
        ) from exc


async def _resolve_delegated_trust_id(
    *,
    session: AsyncSession,
    project_id: str,
    execution_profile_id: Optional[str],
) -> str:
    if not execution_profile_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution_profile_id is required.",
        )
    result = await session.execute(
        select(ExecutionProfileModel).where(
            ExecutionProfileModel.id == execution_profile_id,
            ExecutionProfileModel.project_id == project_id,
            ExecutionProfileModel.deleted_at.is_(None),
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid execution_profile_id: profile not found for project.",
        )
    if not profile.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid execution_profile_id: profile is disabled.",
        )
    if profile.auth_type != "trust":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid execution_profile_id: unsupported auth_type.",
        )
    resolved_trust_id = (profile.trust_id or "").strip()
    if not resolved_trust_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid execution_profile_id: trust_id missing in profile.",
        )
    await _validate_trust_for_project(project_id=project_id, trust_id=resolved_trust_id)
    return execution_profile_id


def _compute_next_run_at(cron_expression: str, *, now: Optional[datetime] = None) -> datetime:
    """Compute next run for API handlers; maps invalid cron to HTTP 400."""
    try:
        return compute_next_run_at(cron_expression, now=now)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


def _validate_action_parameters(params: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """Validate action_parameters against a minimal JSON-schema-like definition."""
    if not isinstance(params, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="action_parameters must be an object.",
        )

    if schema.get("type") == "object":
        required = schema.get("required", [])
        missing = [name for name in required if name not in params]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required action_parameters fields: {', '.join(missing)}",
            )

        properties = schema.get("properties", {})
        for name, prop_schema in properties.items():
            if name not in params or "type" not in prop_schema:
                continue
            expected_type = prop_schema["type"]
            value = params[name]
            if expected_type == "string" and not isinstance(value, str):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"action_parameters.{name} must be a string.",
                )
            if expected_type == "integer" and not isinstance(value, int):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"action_parameters.{name} must be an integer.",
                )
            if expected_type == "boolean" and not isinstance(value, bool):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"action_parameters.{name} must be a boolean.",
                )
            if expected_type == "number" and not isinstance(value, (int, float)):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"action_parameters.{name} must be a number.",
                )


def _normalize_retention_policy_for_storage(
    payload: ScheduleCreate | ScheduleUpdate,
) -> Optional[Dict[str, Any]]:
    """Serialize retention policy without null optional fields."""
    if payload.retention_policy is None:
        return None
    return payload.retention_policy.model_dump(exclude_none=True)


@router.get(
    "/schedules",
    response_model=ScheduleList,
)
async def list_schedules(
    marker: Optional[str] = Query(
        default=None, description="Schedule ID to use as pagination cursor."
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    action_type: Optional[str] = Query(default=None, description="Filter by action_type."),
    enabled: Optional[bool] = Query(default=None, description="Filter by enabled flag."),
    next_run_before: Optional[datetime] = Query(
        default=None, description="Filter by next_run_at before this time."
    ),
    next_run_after: Optional[datetime] = Query(
        default=None, description="Filter by next_run_at after this time."
    ),
    auth: AuthContext = Depends(require_policy("schedule:read")),
    session: AsyncSession = Depends(get_db_session),
) -> ScheduleList:
    stmt = select(ScheduleModel).where(ScheduleModel.deleted_at.is_(None))
    if not auth.is_admin:
        stmt = stmt.where(ScheduleModel.project_id == auth.project_id)

    if action_type is not None:
        stmt = stmt.where(ScheduleModel.action_type == action_type)
    if enabled is not None:
        stmt = stmt.where(ScheduleModel.enabled == enabled)
    if next_run_before is not None:
        stmt = stmt.where(ScheduleModel.next_run_at < next_run_before)
    if next_run_after is not None:
        stmt = stmt.where(ScheduleModel.next_run_at > next_run_after)

    if marker is not None:
        stmt = stmt.where(ScheduleModel.id > marker)

    stmt = stmt.order_by(ScheduleModel.id).limit(limit)
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

    # Validate action_parameters against the registered schema for this action_type.
    _validate_action_parameters(payload.action_parameters, action.action_parameters_schema)
    resolved_execution_profile_id = await _resolve_delegated_trust_id(
        session=session,
        project_id=auth.project_id,
        execution_profile_id=payload.execution_profile_id,
    )

    schedule_id = str(uuid4())
    now = utc_now()
    next_run_at = _compute_next_run_at(payload.cron_expression, now=now)
    schedule_row = ScheduleModel(
        id=schedule_id,
        project_id=auth.project_id,
        name=payload.name,
        description=payload.description,
        action_type=payload.action_type,
        cron_expression=payload.cron_expression,
        action_parameters=payload.action_parameters,
        webhook_url=payload.webhook_url,
        retention_policy=_normalize_retention_policy_for_storage(payload),
        execution_profile_id=resolved_execution_profile_id,
        enabled=payload.enabled,
        next_run_at=next_run_at,
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
    request: Request = None,
    payload: ScheduleUpdate | None = None,
    auth: AuthContext = Depends(require_policy("schedule:update")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)

    if payload is not None:
        try:
            raw_body = await request.json()
        except Exception:
            raw_body = {}
        readonly_attempts = [
            field for field in ("id", "project_id", "created_at", "deleted_at") if field in raw_body
        ]
        if readonly_attempts:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Read-only fields may not be updated: {', '.join(readonly_attempts)}",
            )

        update_data = payload.model_dump(exclude_unset=True)
        if (
            "execution_profile_id" not in update_data
            and not schedule.execution_profile_id
            and schedule.action_type == "server_snapshot"
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "execution_profile_id is required for server_snapshot schedules. "
                    "Provide a valid execution profile."
                ),
            )
        if "action_parameters" in update_data and update_data["action_parameters"] is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.ACTION_PARAMETERS_NULL,
            )
        if "retention_policy" in update_data:
            update_data["retention_policy"] = _normalize_retention_policy_for_storage(payload)
        if "execution_profile_id" in update_data:
            resolved_execution_profile_id = await _resolve_delegated_trust_id(
                session=session,
                project_id=schedule.project_id,
                execution_profile_id=payload.execution_profile_id,
            )
            update_data["execution_profile_id"] = resolved_execution_profile_id

        # If cron_expression is updated, recompute next_run_at.
        if "cron_expression" in update_data and update_data["cron_expression"] is not None:
            schedule.cron_expression = update_data["cron_expression"]
            schedule.next_run_at = _compute_next_run_at(schedule.cron_expression, now=utc_now())
            update_data.pop("cron_expression")

        # If action_parameters are updated, validate against the action schema.
        if "action_parameters" in update_data and update_data["action_parameters"] is not None:
            # Look up the action schema based on the existing schedule.action_type.
            try:
                action_result = await session.execute(
                    select(ActionTypeModel).where(
                        ActionTypeModel.action_type == schedule.action_type
                    )
                )
                action = action_result.scalar_one_or_none()
            except OperationalError as e:
                raise_for_db_error(e)

            if action is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unknown action_type: {schedule.action_type}",
                )

            _validate_action_parameters(
                update_data["action_parameters"], action.action_parameters_schema
            )

        for field, value in update_data.items():
            setattr(schedule, field, value)

        schedule.updated_at = utc_now()

        session.add(schedule)
        try:
            await session.commit()
        except (IntegrityError, OperationalError) as e:
            await session.rollback()
            raise_for_db_error(e)
        await session.refresh(schedule)

    return Schedule.model_validate(schedule)


@router.patch(
    "/schedules/{schedule_id}/enable",
    response_model=Schedule,
)
async def enable_schedule(
    schedule_id: str = Path(..., description="Schedule ID"),
    auth: AuthContext = Depends(require_policy("schedule:update")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)

    if not schedule.enabled:
        schedule.enabled = True
        # If enabling and next_run_at is empty, compute from current cron.
        if schedule.next_run_at is None:
            schedule.next_run_at = _compute_next_run_at(schedule.cron_expression, now=utc_now())
        schedule.updated_at = utc_now()

        session.add(schedule)
        try:
            await session.commit()
        except (IntegrityError, OperationalError) as e:
            await session.rollback()
            raise_for_db_error(e)
        await session.refresh(schedule)

    return Schedule.model_validate(schedule)


@router.patch(
    "/schedules/{schedule_id}/disable",
    response_model=Schedule,
)
async def disable_schedule(
    schedule_id: str = Path(..., description="Schedule ID"),
    auth: AuthContext = Depends(require_policy("schedule:update")),
    session: AsyncSession = Depends(get_db_session),
) -> Schedule:
    schedule = await get_schedule_or_404(schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)

    if schedule.enabled:
        schedule.enabled = False
        schedule.updated_at = utc_now()
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

    schedule.deleted_at = utc_now()
    session.add(schedule)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)

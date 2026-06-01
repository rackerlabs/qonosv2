from __future__ import annotations

from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from .auth.context import AuthContext
from .auth.rbac import require_policy
from .db import get_db_session
from .db_errors import raise_for_db_error
from .error_constants import ErrorMessages
from .routes_schedules import _validate_trust_for_project
from .schemas import (
    ExecutionProfile,
    ExecutionProfileCreate,
    ExecutionProfileList,
    ExecutionProfileUpdate,
)
from .utils.time import utc_now
from db.src.models import ExecutionProfile as ExecutionProfileModel

router = APIRouter(prefix="/v2", tags=["execution_profiles"])


async def get_execution_profile_or_404(
    execution_profile_id: str,
    session: AsyncSession,
) -> ExecutionProfileModel:
    result = await session.execute(
        select(ExecutionProfileModel).where(
            ExecutionProfileModel.id == execution_profile_id,
            ExecutionProfileModel.deleted_at.is_(None),
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution profile not found",
        )
    return profile


def ensure_execution_profile_access_allowed(
    profile: ExecutionProfileModel,
    auth: AuthContext,
) -> None:
    if not auth.is_admin and profile.project_id != auth.project_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=ErrorMessages.FORBIDDEN,
        )


@router.get(
    "/execution-profiles",
    response_model=ExecutionProfileList,
)
async def list_execution_profiles(
    marker: Optional[str] = Query(
        default=None, description="Execution profile ID to use as pagination cursor."
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    enabled: Optional[bool] = Query(default=None, description="Filter by enabled flag."),
    auth_type: Optional[str] = Query(default=None, description="Filter by auth_type."),
    auth: AuthContext = Depends(require_policy("execution_profile:read")),
    session: AsyncSession = Depends(get_db_session),
) -> ExecutionProfileList:
    stmt = select(ExecutionProfileModel).where(ExecutionProfileModel.deleted_at.is_(None))
    if not auth.is_admin:
        stmt = stmt.where(ExecutionProfileModel.project_id == auth.project_id)
    if enabled is not None:
        stmt = stmt.where(ExecutionProfileModel.enabled == enabled)
    if auth_type is not None:
        stmt = stmt.where(ExecutionProfileModel.auth_type == auth_type.strip().lower())
    if marker is not None:
        stmt = stmt.where(ExecutionProfileModel.id > marker)
    stmt = stmt.order_by(ExecutionProfileModel.id).limit(limit)
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return ExecutionProfileList(
        execution_profiles=[ExecutionProfile.model_validate(row) for row in rows],
        links=[],
    )


@router.post(
    "/execution-profiles",
    status_code=status.HTTP_201_CREATED,
    response_model=ExecutionProfile,
)
async def create_execution_profile(
    payload: ExecutionProfileCreate,
    auth: AuthContext = Depends(require_policy("execution_profile:create")),
    session: AsyncSession = Depends(get_db_session),
) -> ExecutionProfile:
    await _validate_trust_for_project(project_id=auth.project_id, trust_id=payload.trust_id)

    now = utc_now()
    row = ExecutionProfileModel(
        id=str(uuid4()),
        project_id=auth.project_id,
        name=payload.name,
        description=payload.description,
        auth_type=payload.auth_type,
        trust_id=payload.trust_id,
        created_by_user_id=auth.user_id,
        enabled=payload.enabled,
        created_at=now,
        updated_at=now,
        deleted_at=None,
    )
    session.add(row)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)
    await session.refresh(row)
    return ExecutionProfile.model_validate(row)


@router.get(
    "/execution-profiles/{execution_profile_id}",
    response_model=ExecutionProfile,
)
async def show_execution_profile(
    execution_profile_id: str = Path(..., description="Execution profile ID"),
    auth: AuthContext = Depends(require_policy("execution_profile:read")),
    session: AsyncSession = Depends(get_db_session),
) -> ExecutionProfile:
    profile = await get_execution_profile_or_404(execution_profile_id, session)
    ensure_execution_profile_access_allowed(profile, auth)
    return ExecutionProfile.model_validate(profile)


@router.put(
    "/execution-profiles/{execution_profile_id}",
    response_model=ExecutionProfile,
)
async def update_execution_profile(
    execution_profile_id: str = Path(..., description="Execution profile ID"),
    request: Request = None,
    payload: ExecutionProfileUpdate | None = None,
    auth: AuthContext = Depends(require_policy("execution_profile:update")),
    session: AsyncSession = Depends(get_db_session),
) -> ExecutionProfile:
    profile = await get_execution_profile_or_404(execution_profile_id, session)
    ensure_execution_profile_access_allowed(profile, auth)

    if payload is not None:
        try:
            raw_body = await request.json()
        except Exception:
            raw_body = {}
        readonly_attempts = [
            field
            for field in (
                "id",
                "project_id",
                "created_by_user_id",
                "created_at",
                "deleted_at",
            )
            if field in raw_body
        ]
        if readonly_attempts:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Read-only fields may not be updated: {', '.join(readonly_attempts)}",
            )

        update_data = payload.model_dump(exclude_unset=True)

        effective_auth_type = (
            str(update_data.get("auth_type", profile.auth_type or "")).strip().lower()
        )
        if effective_auth_type != "trust":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid auth_type: only 'trust' is supported.",
            )

        if "trust_id" in update_data:
            effective_trust_id = (update_data["trust_id"] or "").strip()
            if not effective_trust_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="trust_id is required when auth_type='trust'.",
                )
        else:
            effective_trust_id = (profile.trust_id or "").strip()

        if not effective_trust_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="trust_id is required when auth_type='trust'.",
            )

        await _validate_trust_for_project(
            project_id=profile.project_id, trust_id=effective_trust_id
        )
        update_data["auth_type"] = effective_auth_type
        update_data["trust_id"] = effective_trust_id

        for field, value in update_data.items():
            setattr(profile, field, value)
        profile.updated_at = utc_now()
        session.add(profile)
        try:
            await session.commit()
        except (IntegrityError, OperationalError) as e:
            await session.rollback()
            raise_for_db_error(e)
        await session.refresh(profile)

    return ExecutionProfile.model_validate(profile)


@router.delete(
    "/execution-profiles/{execution_profile_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_execution_profile(
    execution_profile_id: str = Path(..., description="Execution profile ID"),
    auth: AuthContext = Depends(require_policy("execution_profile:delete")),
    session: AsyncSession = Depends(get_db_session),
) -> None:
    profile = await get_execution_profile_or_404(execution_profile_id, session)
    ensure_execution_profile_access_allowed(profile, auth)
    profile.deleted_at = utc_now()
    profile.updated_at = utc_now()
    session.add(profile)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)

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
from .db import get_db_session
from .db_errors import raise_for_db_error
from .error_constants import ErrorMessages
from .schemas import Job, JobCreate, JobDetail, JobFault, JobList, JobStatus, JobUpdate
from .notifications import get_job_event_notifier
from .observability.metrics import observe_job_completed, observe_job_created, observe_job_duration
from .routes_schedules import ensure_schedule_access_allowed, get_schedule_or_404
from db.src.models import (
    ExecutionProfile as ExecutionProfileModel,
    Job as JobModel,
    JobFault as JobFaultModel,
    JobStatus as JobStatusModel,
    Schedule as ScheduleModel,
)

router = APIRouter(prefix="/v2", tags=["jobs"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@router.get(
    "/jobs",
    response_model=JobList,
)
async def list_jobs(
    marker: Optional[str] = Query(default=None, description="Pagination marker."),
    limit: int = Query(default=100, ge=1, le=1000),
    schedule_id: Optional[str] = Query(default=None),
    status_filter: Optional[JobStatus] = Query(default=None, alias="status"),
    action_type: Optional[str] = Query(default=None),
    created_after: Optional[datetime] = Query(default=None),
    created_before: Optional[datetime] = Query(default=None),
    auth: AuthContext = Depends(require_policy("job:read")),
    session: AsyncSession = Depends(get_db_session),
) -> JobList:
    stmt = select(JobModel)

    if not auth.is_admin:
        stmt = stmt.where(JobModel.project_id == auth.project_id)

    if schedule_id is not None:
        stmt = stmt.where(JobModel.schedule_id == schedule_id)

    if status_filter is not None:
        stmt = stmt.where(JobModel.status == JobStatusModel(status_filter.value))
    if action_type is not None:
        stmt = stmt.where(JobModel.action_type == action_type)
    if created_after is not None:
        stmt = stmt.where(JobModel.created_at > created_after)
    if created_before is not None:
        stmt = stmt.where(JobModel.created_at < created_before)
    if marker is not None:
        stmt = stmt.where(JobModel.id > marker)

    stmt = stmt.order_by(JobModel.id).limit(limit)
    result = await session.execute(stmt)
    rows = result.scalars().all()

    jobs = [Job.model_validate(row) for row in rows]
    return JobList(jobs=jobs, links=[])


@router.post(
    "/jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=Job,
)
async def create_job(
    payload: JobCreate,
    auth: AuthContext = Depends(require_policy("job:create")),
    session: AsyncSession = Depends(get_db_session),
) -> Job:
    schedule = await get_schedule_or_404(payload.schedule_id, session)
    ensure_schedule_access_allowed(schedule, auth)
    if not schedule.execution_profile_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Schedule is missing execution_profile_id required for delegated execution.",
        )

    job_id = str(uuid4())
    now = _utcnow()
    action_parameters = dict(schedule.action_parameters or {})
    if schedule.execution_profile_id:
        profile_result = await session.execute(
            select(ExecutionProfileModel).where(
                ExecutionProfileModel.id == schedule.execution_profile_id,
                ExecutionProfileModel.project_id == schedule.project_id,
                ExecutionProfileModel.deleted_at.is_(None),
                ExecutionProfileModel.enabled.is_(True),
                ExecutionProfileModel.auth_type == "trust",
            )
        )
        profile = profile_result.scalar_one_or_none()
        if profile is None or not profile.trust_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Schedule execution profile is invalid or missing trust configuration.",
            )
        action_parameters["qonos_trust_id"] = str(profile.trust_id).strip()

    job_row = JobModel(
        id=job_id,
        schedule_id=payload.schedule_id,
        project_id=schedule.project_id,
        action_type=schedule.action_type,
        action_parameters=action_parameters,
        status=JobStatusModel.QUEUED,
        run_at=now,
        worker_id=None,
        started_at=None,
        ended_at=None,
        retry_count=0,
        error_message=None,
        created_at=now,
        updated_at=now,
        cancelled_at=None,
        completed_at=None,
    )
    session.add(job_row)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)
    await session.refresh(job_row)
    observe_job_created(action_type=job_row.action_type)
    get_job_event_notifier().emit_non_blocking(
        event_type="job.created",
        job=job_row,
        schedule_webhook_url=schedule.webhook_url,
        error_message=job_row.error_message,
    )

    return Job.model_validate(job_row)


async def get_job_or_404(
    job_id: str,
    session: AsyncSession,
) -> JobModel:
    result = await session.execute(select(JobModel).where(JobModel.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorMessages.JOB_NOT_FOUND,
        )
    return job


def ensure_job_access_allowed(job: JobModel, auth: AuthContext) -> None:
    if not auth.is_admin and job.project_id != auth.project_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=ErrorMessages.FORBIDDEN,
        )


@router.get(
    "/jobs/{job_id}",
    response_model=JobDetail,
)
async def show_job(
    job_id: str = Path(..., description="Job ID"),
    auth: AuthContext = Depends(require_policy("job:read")),
    session: AsyncSession = Depends(get_db_session),
) -> JobDetail:
    job = await get_job_or_404(job_id, session)
    ensure_job_access_allowed(job, auth)
    result = await session.execute(
        select(JobFaultModel)
        .where(JobFaultModel.job_id == job.id)
        .order_by(JobFaultModel.occurred_at.asc(), JobFaultModel.id.asc())
    )
    faults = [JobFault.model_validate(row) for row in result.scalars().all()]
    job_data = Job.model_validate(job).model_dump()
    return JobDetail(
        **job_data,
        version=job.version,
        cancelled_at=job.cancelled_at,
        fault_history=faults,
    )


@router.patch(
    "/jobs/{job_id}",
    response_model=Job,
)
async def update_job(
    payload: JobUpdate,
    job_id: str = Path(..., description="Job ID"),
    auth: AuthContext = Depends(require_policy("job:update")),
    session: AsyncSession = Depends(get_db_session),
) -> Job:
    job = await get_job_or_404(job_id, session)
    ensure_job_access_allowed(job, auth)

    update_data = payload.model_dump(exclude_unset=True)
    if "status" in update_data and update_data["status"] is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorMessages.STATUS_NULL,
        )
    if "retry_count" in update_data and update_data["retry_count"] is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorMessages.RETRY_COUNT_NULL,
        )
    if "status" in update_data and update_data["status"] is not None:
        update_data["status"] = JobStatusModel(update_data["status"].value)

    for field, value in update_data.items():
        setattr(job, field, value)
    job.updated_at = _utcnow()

    session.add(job)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)
    await session.refresh(job)

    return Job.model_validate(job)


async def _cancel_job(
    job_id: str,
    auth: AuthContext = Depends(require_policy("job:cancel")),
    session: AsyncSession = Depends(get_db_session),
) -> JobModel:
    job = await get_job_or_404(job_id, session)
    ensure_job_access_allowed(job, auth)

    if job.status not in {JobStatusModel.QUEUED, JobStatusModel.PROCESSING}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job cannot be cancelled in its current state",
        )

    now = _utcnow()
    job.status = JobStatusModel.CANCELLED
    job.cancelled_at = now
    job.updated_at = now

    session.add(job)
    try:
        await session.commit()
    except (IntegrityError, OperationalError) as e:
        await session.rollback()
        raise_for_db_error(e)
    await session.refresh(job)
    observe_job_completed(action_type=job.action_type, status=JobStatusModel.CANCELLED.value)
    if job.started_at is not None:
        started_at = job.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        observe_job_duration(
            action_type=job.action_type,
            duration_seconds=(now - started_at).total_seconds(),
        )
    return job


@router.post(
    "/jobs/{job_id}/cancel",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def cancel_job_via_post(
    job_id: str = Path(..., description="Job ID"),
    auth: AuthContext = Depends(require_policy("job:cancel")),
    session: AsyncSession = Depends(get_db_session),
) -> None:
    job = await _cancel_job(job_id=job_id, auth=auth, session=session)
    schedule_result = await session.execute(
        select(ScheduleModel).where(ScheduleModel.id == job.schedule_id)
    )
    schedule = schedule_result.scalar_one_or_none()
    get_job_event_notifier().emit_non_blocking(
        event_type="job.cancelled",
        job=job,
        schedule_webhook_url=(schedule.webhook_url if schedule else None),
        error_message=job.error_message,
        status=job.status.value,
    )

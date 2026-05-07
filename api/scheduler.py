from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from api.app.messaging.amqp_queue import publish_job_queued
from api.app.config import get_config
from api.app.db import get_session_factory
from api.app.logging import configure_logging
from api.app.utils.cron import compute_next_run_at
from api.app.utils.time import utc_now
from db.src.models import (
    ExecutionProfile as ExecutionProfileModel,
    Job as JobModel,
    JobStatus as JobStatusModel,
    Schedule as ScheduleModel,
)

logger = logging.getLogger("qonos.scheduler")


async def _process_due_schedules(session_factory: sessionmaker) -> None:
    """Process due schedules using compare-and-swap (CAS) on next_run_at.

    This is safe to run concurrently across multiple scheduler instances:
    only one instance will win the CAS update and commit a job for a given
    (schedule_id, next_run_at) due window.
    """
    async with session_factory() as session:
        now = utc_now()
        result = await session.execute(
            select(ScheduleModel).where(
                ScheduleModel.enabled.is_(True),
                ScheduleModel.deleted_at.is_(None),
                ScheduleModel.next_run_at <= now,
            )
        )
        schedules = result.scalars().all()

        if not schedules:
            logger.debug(
                "scheduler tick decision=no_due_schedules due_count=0",
            )
            return

        for schedule in schedules:
            await _create_job_for_schedule_cas(session, schedule, now=now)


async def _create_job_for_schedule_cas(
    session: AsyncSession, schedule: ScheduleModel, *, now
) -> None:
    """Create a QUEUED job and advance the schedule using CAS on next_run_at.

    We insert the job and then CAS-update the schedule within the same
    transaction; if the CAS update does not match (another scheduler won),
    the transaction is rolled back so no duplicate job is created.
    """
    old_next_run_at = schedule.next_run_at
    if old_next_run_at is None:
        logger.info(
            "schedule skipped decision=skip_no_next_run_at schedule_id=%s project_id=%s",
            schedule.id,
            schedule.project_id,
        )
        return

    try:
        next_run_at = compute_next_run_at(schedule.cron_expression, now=now)
    except ValueError as exc:
        logger.warning(
            "schedule skipped decision=skip_invalid_cron schedule_id=%s project_id=%s cron_expression=%s error=%s",
            schedule.id,
            schedule.project_id,
            schedule.cron_expression,
            str(exc),
        )
        return

    if not schedule.execution_profile_id:
        logger.warning(
            "schedule skipped decision=skip_missing_execution_profile schedule_id=%s project_id=%s",
            schedule.id,
            schedule.project_id,
        )
        return

    trust_id: str | None = None
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
        logger.warning(
            "schedule skipped decision=skip_invalid_execution_profile schedule_id=%s project_id=%s execution_profile_id=%s",
            schedule.id,
            schedule.project_id,
            schedule.execution_profile_id,
        )
        return
    trust_id = str(profile.trust_id).strip() or None

    job_id = str(uuid4())
    action_parameters = dict(schedule.action_parameters or {})
    if trust_id:
        action_parameters["qonos_trust_id"] = trust_id

    job_row = JobModel(
        id=job_id,
        schedule_id=schedule.id,
        project_id=schedule.project_id,
        action_type=schedule.action_type,
        action_parameters=action_parameters,
        run_at=now,
        status=JobStatusModel.QUEUED,
        version=1,
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

    stmt = (
        update(ScheduleModel)
        .where(
            and_(
                ScheduleModel.id == schedule.id,
                ScheduleModel.deleted_at.is_(None),
                ScheduleModel.enabled.is_(True),
                ScheduleModel.next_run_at == old_next_run_at,
            )
        )
        .values(
            last_run_at=now,
            next_run_at=next_run_at,
            updated_at=now,
        )
    )
    res = await session.execute(stmt)
    if res.rowcount != 1:
        await session.rollback()
        logger.info(
            "schedule skipped decision=cas_lost schedule_id=%s project_id=%s old_next_run_at=%s",
            schedule.id,
            schedule.project_id,
            str(old_next_run_at),
        )
        return

    await session.commit()

    logger.info(
        "schedule decision=job_queued schedule_id=%s job_id=%s project_id=%s cron_expression=%s next_run_at=%s last_run_at=%s run_at=%s",
        schedule.id,
        job_id,
        schedule.project_id,
        schedule.cron_expression,
        str(next_run_at),
        str(now),
        str(now),
    )

    cfg = get_config()
    amqp_url = cfg.amqp_url
    if amqp_url:
        try:
            await publish_job_queued(
                amqp_url=amqp_url,
                exchange=cfg.job_queue_exchange,
                routing_key=cfg.job_queue_routing_key,
                job_id=job_id,
                action_type=schedule.action_type,
                project_id=schedule.project_id,
            )
            logger.info(
                "scheduler event decision=published_job_queued job_id=%s exchange=%s routing_key=%s",
                job_id,
                cfg.job_queue_exchange,
                cfg.job_queue_routing_key,
            )
        except Exception as exc:
            logger.warning(
                "scheduler event decision=publish_failed job_id=%s error=%s",
                job_id,
                str(exc),
            )


def _poll_interval_seconds() -> float:
    cfg = get_config()
    return cfg.poll_interval_seconds


async def run_scheduler_loop(poll_interval_seconds: float | None = None) -> None:
    interval = (
        poll_interval_seconds if poll_interval_seconds is not None else _poll_interval_seconds()
    )
    session_factory = get_session_factory()
    logger.info(
        "scheduler started decision=startup poll_interval_seconds=%s component=qonos.scheduler",
        interval,
    )
    try:
        while True:
            await _process_due_schedules(session_factory)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info(
            "scheduler stopped decision=shutdown component=qonos.scheduler",
        )


def main() -> None:
    configure_logging(log_name="scheduler")
    asyncio.run(run_scheduler_loop())


if __name__ == "__main__":
    main()

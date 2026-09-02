from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from uuid import uuid4

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from uvicorn import Config as UvicornConfig, Server as UvicornServer

from api.app.messaging.amqp_queue import publish_job_queued
from api.app.config import get_config
from api.app.db import get_session_factory
from api.app.logging import configure_logging
from api.app.observability.context import log_context
from api.app.observability.health import mark_process_started
from api.app.observability.metrics import (
    observe_job_created,
    observe_scheduler_cycle,
    refresh_db_gauges,
)
from api.app.observability.server import build_observability_app
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
    """Process due schedules using compare-and-swap (CAS) on next_run_at."""
    cycle_started = time.monotonic()
    try:
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
                logger.debug("scheduler tick decision=no_due_schedules due_count=0")
                return

            for schedule in schedules:
                await _create_job_for_schedule_cas(session, schedule, now=now)
    finally:
        observe_scheduler_cycle(duration_seconds=time.monotonic() - cycle_started)
        try:
            await refresh_db_gauges(session_factory)
        except Exception as exc:
            logger.warning("scheduler metrics decision=refresh_failed error=%s", str(exc))


async def _create_job_for_schedule_cas(
    session: AsyncSession, schedule: ScheduleModel, *, now
) -> None:
    """Create a QUEUED job and advance the schedule using CAS on next_run_at."""
    with log_context(
        correlation_id=str(uuid4()),
        project_id=schedule.project_id,
        schedule_id=schedule.id,
        action_type=schedule.action_type,
        component="qonos.scheduler",
    ):
        old_next_run_at = schedule.next_run_at
        if old_next_run_at is None:
            logger.info("schedule skipped decision=skip_no_next_run_at")
            return

        try:
            next_run_at = compute_next_run_at(schedule.cron_expression, now=now)
        except ValueError as exc:
            logger.warning(
                "schedule skipped decision=skip_invalid_cron cron_expression=%s error=%s",
                schedule.cron_expression,
                str(exc),
            )
            return

        if not schedule.execution_profile_id:
            logger.warning("schedule skipped decision=skip_missing_execution_profile")
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
                "schedule skipped decision=skip_invalid_execution_profile execution_profile_id=%s",
                schedule.execution_profile_id,
            )
            return
        trust_id = str(profile.trust_id).strip() or None

        job_id = str(uuid4())
        with log_context(job_id=job_id, correlation_id=job_id):
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
                    "schedule skipped decision=cas_lost old_next_run_at=%s",
                    str(old_next_run_at),
                )
                return

            await session.commit()
            observe_job_created(action_type=schedule.action_type)

            logger.info(
                "schedule decision=job_queued cron_expression=%s next_run_at=%s last_run_at=%s run_at=%s",
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
                        "scheduler event decision=published_job_queued exchange=%s routing_key=%s",
                        cfg.job_queue_exchange,
                        cfg.job_queue_routing_key,
                    )
                except Exception as exc:
                    logger.warning(
                        "scheduler event decision=publish_failed error=%s",
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
    with log_context(component="qonos.scheduler"):
        logger.info(
            "scheduler started decision=startup poll_interval_seconds=%s",
            interval,
        )
    try:
        while True:
            await _process_due_schedules(session_factory)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        with log_context(component="qonos.scheduler"):
            logger.info("scheduler stopped decision=shutdown")


async def run_scheduler() -> None:
    cfg = get_config()
    session_factory = get_session_factory()

    async def refresh_metrics() -> None:
        await refresh_db_gauges(session_factory)

    app = build_observability_app(
        component="qonos.scheduler",
        refresh_metrics=refresh_metrics,
    )
    uvicorn_server = UvicornServer(
        UvicornConfig(
            app,
            host=cfg.scheduler_health_host,
            port=cfg.scheduler_health_port,
            log_level="warning",
        )
    )

    scheduler_task = asyncio.create_task(run_scheduler_loop())
    health_task = asyncio.create_task(uvicorn_server.serve())
    try:
        await asyncio.gather(scheduler_task, health_task)
    except asyncio.CancelledError:
        uvicorn_server.should_exit = True
        raise
    finally:
        uvicorn_server.should_exit = True
        with suppress(Exception):
            await scheduler_task
        with suppress(Exception):
            await health_task


def main() -> None:
    configure_logging(log_name="scheduler", component="qonos.scheduler")
    mark_process_started()
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()

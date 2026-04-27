from __future__ import annotations

import asyncio
import logging
import math
import random
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import FastAPI
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from uvicorn import Config as UvicornConfig, Server as UvicornServer

from api.app.messaging.redis_stream import consume_job_events
from api.app.config import AppConfig, get_config
from api.app.db import close_db_engine, get_session_factory
from api.app.logging import configure_logging
from api.app.utils.time import as_utc_aware, utc_now
from api.app.worker.builtin_handlers import register_builtin_handlers
from api.app.worker.handlers import ActionHandler, HandlerRegistry
from api.app.worker.retention import run_retention_for_job
from api.app.worker.registry import build_registry
from db.src.models import (
    ExecutionProfile as ExecutionProfileModel,
    Job as JobModel,
    JobFault as JobFaultModel,
    JobStatus as JobStatusModel,
    Schedule as ScheduleModel,
)

logger = logging.getLogger("qonos.worker")


ALLOWED_TRANSITIONS: dict[JobStatusModel, set[JobStatusModel]] = {
    JobStatusModel.QUEUED: {JobStatusModel.PROCESSING, JobStatusModel.CANCELLED},
    JobStatusModel.PROCESSING: {
        JobStatusModel.DONE,
        JobStatusModel.ERROR,
        JobStatusModel.TIMED_OUT,
        JobStatusModel.CANCELLED,
        JobStatusModel.HARD_TIMED_OUT,
        JobStatusModel.MAX_RETRIED,
        JobStatusModel.QUEUED,
    },
    JobStatusModel.ERROR: {JobStatusModel.QUEUED, JobStatusModel.MAX_RETRIED},
    JobStatusModel.TIMED_OUT: {JobStatusModel.QUEUED, JobStatusModel.MAX_RETRIED},
    JobStatusModel.DONE: set(),
    JobStatusModel.CANCELLED: set(),
    JobStatusModel.HARD_TIMED_OUT: set(),
    JobStatusModel.MAX_RETRIED: set(),
}


def _assert_transition(from_status: JobStatusModel, to_status: JobStatusModel) -> None:
    if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
        raise ValueError(f"Invalid job transition: {from_status.value} -> {to_status.value}")


@dataclass
class WorkerMetrics:
    inflight: int = 0
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    hard_timed_out: int = 0
    retried: int = 0
    stale_requeued: int = 0
    stale_max_retried: int = 0


class WorkerRuntime:
    def __init__(self, cfg: AppConfig, registry: HandlerRegistry) -> None:
        self.cfg = cfg
        self.registry = registry
        self.session_factory = get_session_factory()
        self.metrics = WorkerMetrics()
        self._stop = asyncio.Event()
        self._sem = asyncio.Semaphore(cfg.worker_concurrency)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._wake = asyncio.Event()
        self._last_stale_sweep_at: Optional[datetime] = None

    async def run(self) -> None:
        logger.info(
            "worker started decision=startup concurrency=%s supported_action_types=%s stale_after_seconds=%s",
            self.cfg.worker_concurrency,
            ",".join(self.registry.supported_action_types()),
            self.cfg.worker_stale_processing_after_seconds,
        )
        try:
            while not self._stop.is_set():
                await self._dispatch_once()
                now = utc_now()
                interval = self.cfg.worker_stale_processing_sweep_interval_seconds
                if (
                    self._last_stale_sweep_at is None
                    or (now - self._last_stale_sweep_at).total_seconds() >= interval
                ):
                    await self._enforce_hard_timeout_once()
                    await self._reclaim_stale_processing_once()
                    self._last_stale_sweep_at = now
                    self._wake.set()
                try:
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self.cfg.worker_idle_sleep_seconds
                    )
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
        except asyncio.CancelledError:
            pass
        finally:
            await self._drain_tasks()
            logger.info("worker stopped decision=shutdown")

    def stop(self) -> None:
        self._stop.set()

    async def _drain_tasks(self) -> None:
        if not self._tasks:
            return
        for t in list(self._tasks):
            t.cancel()
        with suppress(Exception):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _dispatch_once(self) -> None:
        supported = self.registry.supported_action_types()
        if not supported:
            logger.warning("worker idle decision=no_supported_action_types")
            return

        slots = self.cfg.worker_concurrency - self.metrics.inflight
        if slots <= 0:
            return

        now = utc_now()
        claimed_job_ids = await self._claim_due_jobs_batch(
            supported=supported,
            now=now,
            slots=slots,
        )

        for job_id in claimed_job_ids:
            if self.metrics.inflight >= self.cfg.worker_concurrency:
                break
            self.metrics.claimed += 1
            await self._spawn(self._process_claimed_job(job_id))

    async def _claim_due_jobs_batch(
        self,
        *,
        supported: list[str],
        now: datetime,
        slots: int,
    ) -> list[str]:
        """Claim due QUEUED jobs in one transaction for higher throughput."""
        _assert_transition(JobStatusModel.QUEUED, JobStatusModel.PROCESSING)
        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(JobModel.id)
                    .where(
                        JobModel.status == JobStatusModel.QUEUED,
                        JobModel.action_type.in_(supported),
                        JobModel.run_at <= now,
                    )
                    .order_by(JobModel.run_at.asc())
                    .limit(max(1, slots))
                    .with_for_update(skip_locked=True)
                )
                job_ids = [row[0] for row in result.all()]
                if not job_ids:
                    return []
                await session.execute(
                    update(JobModel)
                    .where(
                        JobModel.id.in_(job_ids),
                        JobModel.status == JobStatusModel.QUEUED,
                    )
                    .values(
                        status=JobStatusModel.PROCESSING,
                        started_at=now,
                        updated_at=now,
                        version=JobModel.version + 1,
                    )
                )
                return job_ids

    async def on_job_queued_event(self, job_id: str) -> None:
        self._wake.set()
        async with self.session_factory() as session:
            job = await self._load_job(session, job_id)
            if not job or job.status != JobStatusModel.QUEUED:
                return
            if job.action_type not in self.registry.supported_action_types():
                return
            claimed = await self._claim_job(job_id=job.id, expected_version=job.version)
            if claimed:
                self.metrics.claimed += 1
                await self._spawn(self._process_claimed_job(job.id))

    async def _run_in_slot(self, coro: Any) -> None:
        await self._sem.acquire()
        self.metrics.inflight += 1
        try:
            await coro
        finally:
            self.metrics.inflight = max(0, self.metrics.inflight - 1)
            self._sem.release()

    async def _spawn(self, coro: Any) -> None:
        await self._sem.acquire()
        self.metrics.inflight += 1
        task = asyncio.create_task(coro)
        self._tasks.add(task)

        def _done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)
            self.metrics.inflight = max(0, self.metrics.inflight - 1)
            self._sem.release()

        task.add_done_callback(_done)

    async def _claim_job(self, *, job_id: str, expected_version: int) -> bool:
        _assert_transition(JobStatusModel.QUEUED, JobStatusModel.PROCESSING)
        now = utc_now()
        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    and_(
                        JobModel.id == job_id,
                        JobModel.status == JobStatusModel.QUEUED,
                        JobModel.version == expected_version,
                    )
                )
                .values(
                    status=JobStatusModel.PROCESSING,
                    started_at=now,
                    updated_at=now,
                    version=JobModel.version + 1,
                )
            )
            res = await session.execute(stmt)
            if res.rowcount != 1:
                await session.rollback()
                return False
            await session.commit()
            return True

    async def _load_job(self, session: AsyncSession, job_id: str) -> Optional[JobModel]:
        result = await session.execute(select(JobModel).where(JobModel.id == job_id))
        return result.scalar_one_or_none()

    async def _load_schedule(
        self, session: AsyncSession, schedule_id: str
    ) -> Optional[ScheduleModel]:
        result = await session.execute(select(ScheduleModel).where(ScheduleModel.id == schedule_id))
        return result.scalar_one_or_none()

    async def _run_retention(self, *, job: JobModel, trigger_status: JobStatusModel) -> None:
        try:
            trust_id: str | None = None
            async with self.session_factory() as session:
                schedule = await self._load_schedule(session, job.schedule_id)
                if schedule is not None and schedule.execution_profile_id:
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
                    trust_id = (
                        str(profile.trust_id).strip() if profile and profile.trust_id else None
                    )
            if schedule is None:
                return
            await run_retention_for_job(
                action_type=job.action_type,
                project_id=job.project_id,
                schedule_id=job.schedule_id,
                retention_policy_raw=schedule.retention_policy,
                trigger_status=trigger_status.value,
                trust_id=trust_id,
            )
        except Exception as exc:
            logger.warning(
                "retention failed decision=non_fatal job_id=%s status=%s error=%s",
                job.id,
                trigger_status.value,
                str(exc),
            )

    async def _transition(
        self,
        *,
        session: AsyncSession,
        job: JobModel,
        from_status: JobStatusModel,
        to_status: JobStatusModel,
        values: dict[str, Any],
    ) -> bool:
        _assert_transition(from_status, to_status)
        stmt = (
            update(JobModel)
            .where(
                and_(
                    JobModel.id == job.id,
                    JobModel.status == from_status,
                    JobModel.version == job.version,
                )
            )
            .values(
                status=to_status,
                version=JobModel.version + 1,
                updated_at=utc_now(),
                **values,
            )
        )
        res = await session.execute(stmt)
        if res.rowcount != 1:
            await session.rollback()
            return False
        await session.commit()
        job.status = to_status
        job.version += 1
        return True

    async def _record_fault(
        self,
        *,
        job: JobModel,
        code: str,
        title: str,
        detail: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        try:
            async with self.session_factory() as session:
                fault = JobFaultModel(
                    job_id=job.id,
                    project_id=job.project_id,
                    code=code,
                    title=title[:255],
                    detail=detail,
                    extra=extra,
                    occurred_at=utc_now(),
                    created_at=utc_now(),
                )
                session.add(fault)
                await session.commit()
        except Exception as exc:
            logger.warning(
                "job fault decision=record_failed job_id=%s error=%s",
                job.id,
                str(exc),
            )

    async def _reclaim_stale_processing_once(self) -> None:
        supported = self.registry.supported_action_types()
        if not supported:
            return
        now = utc_now()
        after = self.cfg.worker_stale_processing_after_seconds
        cutoff = now - timedelta(seconds=after)
        async with self.session_factory() as session:
            result = await session.execute(
                select(JobModel)
                .where(
                    JobModel.status == JobStatusModel.PROCESSING,
                    JobModel.action_type.in_(supported),
                    or_(JobModel.started_at.is_(None), JobModel.started_at < cutoff),
                )
                .order_by(JobModel.started_at.asc())
                .limit(100)
            )
            candidates = result.scalars().all()

        for row in candidates:
            await self._reclaim_one_stale_job(
                job_id=row.id, expected_version=row.version, cutoff=cutoff
            )

    async def _enforce_hard_timeout_once(self) -> None:
        supported = self.registry.supported_action_types()
        if not supported:
            return
        now = utc_now()
        cutoff = now - timedelta(seconds=self.cfg.worker_hard_timeout_seconds)
        async with self.session_factory() as session:
            result = await session.execute(
                select(JobModel.id, JobModel.version)
                .where(
                    JobModel.status == JobStatusModel.PROCESSING,
                    JobModel.action_type.in_(supported),
                    JobModel.started_at.is_not(None),
                    JobModel.started_at < cutoff,
                )
                .order_by(JobModel.started_at.asc())
                .limit(100)
            )
            candidates = result.all()

        for job_id, expected_version in candidates:
            await self._mark_hard_timed_out(
                job_id=job_id, expected_version=expected_version, cutoff=cutoff
            )

    async def _mark_hard_timed_out(
        self, *, job_id: str, expected_version: int, cutoff: datetime
    ) -> None:
        _assert_transition(JobStatusModel.PROCESSING, JobStatusModel.HARD_TIMED_OUT)
        now = utc_now()
        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    and_(
                        JobModel.id == job_id,
                        JobModel.status == JobStatusModel.PROCESSING,
                        JobModel.version == expected_version,
                        JobModel.started_at.is_not(None),
                        JobModel.started_at < cutoff,
                    )
                )
                .values(
                    status=JobStatusModel.HARD_TIMED_OUT,
                    version=JobModel.version + 1,
                    updated_at=now,
                    ended_at=now,
                    error_message="hard timeout exceeded (sweep)",
                )
            )
            res = await session.execute(stmt)
            if res.rowcount != 1:
                await session.rollback()
                return
            await session.commit()
            self.metrics.hard_timed_out += 1
            logger.warning("job timeout decision=hard_timeout_sweep job_id=%s", job_id)
            job = await self._load_job(session, job_id)
            if job is not None:
                await self._record_fault(
                    job=job,
                    code="qonos.job.hard_timeout_sweep",
                    title="Hard timeout exceeded (sweep)",
                    detail="hard timeout exceeded (sweep)",
                    extra={"action_type": job.action_type, "schedule_id": job.schedule_id},
                )
                await self._run_retention(job=job, trigger_status=JobStatusModel.HARD_TIMED_OUT)

    async def _reclaim_one_stale_job(
        self, *, job_id: str, expected_version: int, cutoff: datetime
    ) -> None:
        now = utc_now()
        async with self.session_factory() as session:
            job = await self._load_job(session, job_id)
            if not job or job.status != JobStatusModel.PROCESSING:
                return
            if job.version != expected_version:
                return
            if job.action_type not in self.registry.supported_action_types():
                return
            if job.started_at is not None and as_utc_aware(job.started_at) >= cutoff:
                return

            if job.retry_count >= self.cfg.worker_max_retry:
                _assert_transition(JobStatusModel.PROCESSING, JobStatusModel.MAX_RETRIED)
                stmt = (
                    update(JobModel)
                    .where(
                        and_(
                            JobModel.id == job.id,
                            JobModel.status == JobStatusModel.PROCESSING,
                            JobModel.version == job.version,
                            or_(JobModel.started_at.is_(None), JobModel.started_at < cutoff),
                        )
                    )
                    .values(
                        status=JobStatusModel.MAX_RETRIED,
                        version=JobModel.version + 1,
                        updated_at=now,
                        ended_at=now,
                        error_message="stale processing: max retries exceeded",
                    )
                )
                res = await session.execute(stmt)
                if res.rowcount != 1:
                    await session.rollback()
                    return
                await session.commit()
                self.metrics.stale_max_retried += 1
                logger.info(
                    "job reclaim decision=stale_processing_max_retried job_id=%s project_id=%s",
                    job.id,
                    job.project_id,
                )
                await self._record_fault(
                    job=job,
                    code="qonos.job.stale_processing_max_retried",
                    title="Stale processing: retry limit exceeded",
                    detail="stale processing: max retries exceeded",
                    extra={"action_type": job.action_type, "schedule_id": job.schedule_id},
                )
                await self._run_retention(job=job, trigger_status=JobStatusModel.MAX_RETRIED)
                return

            _assert_transition(JobStatusModel.PROCESSING, JobStatusModel.QUEUED)
            stmt = (
                update(JobModel)
                .where(
                    and_(
                        JobModel.id == job.id,
                        JobModel.status == JobStatusModel.PROCESSING,
                        JobModel.version == job.version,
                        or_(JobModel.started_at.is_(None), JobModel.started_at < cutoff),
                    )
                )
                .values(
                    status=JobStatusModel.QUEUED,
                    version=JobModel.version + 1,
                    updated_at=now,
                    run_at=now,
                    started_at=None,
                    ended_at=None,
                    retry_count=job.retry_count + 1,
                    error_message="stale processing reclaimed",
                )
            )
            res = await session.execute(stmt)
            if res.rowcount != 1:
                await session.rollback()
                return
            await session.commit()
            self.metrics.stale_requeued += 1
            self.metrics.retried += 1
            logger.info(
                "job reclaim decision=stale_processing_requeued job_id=%s project_id=%s retry_count=%s",
                job.id,
                job.project_id,
                job.retry_count + 1,
            )

    async def _process_claimed_job(self, job_id: str) -> None:
        async with self.session_factory() as session:
            job = await self._load_job(session, job_id)
            if not job or job.status != JobStatusModel.PROCESSING:
                return

            handler_cls = self.registry.get(job.action_type)
            if handler_cls is None:
                await self._fail_and_retry_or_max(
                    session=session,
                    job=job,
                    failure_status=JobStatusModel.ERROR,
                    error=RuntimeError(f"No handler registered for action_type={job.action_type}"),
                    call_hook=False,
                )
                return

            handler: ActionHandler = handler_cls()
            try:
                handler.validate_parameters(job.action_parameters)
            except Exception as exc:
                await self._fail_and_retry_or_max(
                    session=session,
                    job=job,
                    failure_status=JobStatusModel.ERROR,
                    error=exc,
                    call_hook=True,
                    handler=handler,
                )
                return

            outcome, err = await self._execute_with_timeouts(handler, job)
            now = utc_now()

            if outcome == "done":
                ok = await self._transition(
                    session=session,
                    job=job,
                    from_status=JobStatusModel.PROCESSING,
                    to_status=JobStatusModel.DONE,
                    values={"ended_at": now, "completed_at": now, "error_message": None},
                )
                if ok:
                    self.metrics.completed += 1
                    with suppress(Exception):
                        await handler.on_success(job)
                    await self._run_retention(job=job, trigger_status=JobStatusModel.DONE)
                return

            if outcome == "hard_timeout":
                ok = await self._transition(
                    session=session,
                    job=job,
                    from_status=JobStatusModel.PROCESSING,
                    to_status=JobStatusModel.HARD_TIMED_OUT,
                    values={"ended_at": now, "error_message": "hard timeout exceeded"},
                )
                if ok:
                    self.metrics.hard_timed_out += 1
                    await self._record_fault(
                        job=job,
                        code="qonos.job.hard_timeout",
                        title="Hard timeout exceeded",
                        detail="hard timeout exceeded",
                        extra={"action_type": job.action_type, "schedule_id": job.schedule_id},
                    )
                    with suppress(Exception):
                        await handler.on_timeout(job)
                    await self._run_retention(job=job, trigger_status=JobStatusModel.HARD_TIMED_OUT)
                return

            if outcome == "soft_timeout":
                self.metrics.timed_out += 1
                await self._fail_and_retry_or_max(
                    session=session,
                    job=job,
                    failure_status=JobStatusModel.TIMED_OUT,
                    error=err or TimeoutError("soft timeout exceeded"),
                    call_hook=True,
                    handler=handler,
                )
                return

            self.metrics.failed += 1
            await self._fail_and_retry_or_max(
                session=session,
                job=job,
                failure_status=JobStatusModel.ERROR,
                error=err or RuntimeError("unknown handler error"),
                call_hook=True,
                handler=handler,
            )

    async def _execute_with_timeouts(
        self, handler: ActionHandler, job: JobModel
    ) -> tuple[str, Optional[Exception]]:
        task = asyncio.create_task(handler.execute(job))
        soft = self.cfg.worker_soft_timeout_seconds
        hard = max(self.cfg.worker_hard_timeout_seconds, soft)
        grace = max(0.0, hard - soft)

        try:
            await asyncio.wait_for(task, timeout=soft)
            return "done", None
        except asyncio.TimeoutError:
            if grace <= 0:
                task.cancel()
                with suppress(Exception):
                    await task
                return "hard_timeout", TimeoutError("hard timeout exceeded")
            try:
                await asyncio.wait_for(task, timeout=grace)
                return "done", None
            except asyncio.TimeoutError:
                task.cancel()
                with suppress(Exception):
                    await task
                return "hard_timeout", TimeoutError("hard timeout exceeded")
            except Exception as exc:
                return "soft_timeout", exc
        except Exception as exc:
            return "error", exc

    async def _fail_and_retry_or_max(
        self,
        *,
        session: AsyncSession,
        job: JobModel,
        failure_status: JobStatusModel,
        error: Exception,
        call_hook: bool,
        handler: Optional[ActionHandler] = None,
    ) -> None:
        now = utc_now()
        err_text = str(error)
        transitioned = await self._transition(
            session=session,
            job=job,
            from_status=JobStatusModel.PROCESSING,
            to_status=failure_status,
            values={"ended_at": now, "error_message": err_text},
        )
        if not transitioned:
            return

        await self._record_fault(
            job=job,
            code=(
                "qonos.job.timeout"
                if failure_status == JobStatusModel.TIMED_OUT
                else "qonos.job.error"
            ),
            title=("Job timed out" if failure_status == JobStatusModel.TIMED_OUT else "Job failed"),
            detail=err_text,
            extra={
                "action_type": job.action_type,
                "schedule_id": job.schedule_id,
                "status": failure_status.value,
                "retry_count": job.retry_count,
            },
        )

        if call_hook and handler is not None:
            with suppress(Exception):
                if failure_status in {JobStatusModel.TIMED_OUT, JobStatusModel.HARD_TIMED_OUT}:
                    await handler.on_timeout(job)
                else:
                    await handler.on_failure(job, error)

        if failure_status == JobStatusModel.HARD_TIMED_OUT:
            return

        if job.retry_count >= self.cfg.worker_max_retry:
            moved = await self._transition(
                session=session,
                job=job,
                from_status=failure_status,
                to_status=JobStatusModel.MAX_RETRIED,
                values={"ended_at": utc_now()},
            )
            if not moved:
                return
            await self._record_fault(
                job=job,
                code="qonos.job.max_retried",
                title="Retry limit exceeded",
                detail=f"retry limit exceeded (max_retry={self.cfg.worker_max_retry})",
                extra={"action_type": job.action_type, "schedule_id": job.schedule_id},
            )
            await self._run_retention(job=job, trigger_status=JobStatusModel.MAX_RETRIED)
            return

        delay = min(
            self.cfg.worker_backoff_max_seconds,
            self.cfg.worker_backoff_base_seconds * math.pow(2, max(0, job.retry_count)),
        )
        if self.cfg.worker_backoff_jitter and delay > 0:
            delay = random.uniform(0.0, delay)
        next_run = utc_now() + timedelta(seconds=delay)
        ok = await self._transition(
            session=session,
            job=job,
            from_status=failure_status,
            to_status=JobStatusModel.QUEUED,
            values={
                "run_at": next_run,
                "retry_count": job.retry_count + 1,
                "started_at": None,
            },
        )
        if ok:
            self.metrics.retried += 1


def _build_health_app(runtime: WorkerRuntime) -> FastAPI:
    app = FastAPI(title="QonoS Worker Health")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "component": "qonos.worker",
            "inflight": runtime.metrics.inflight,
            "concurrency": runtime.cfg.worker_concurrency,
            "supported_action_types": runtime.registry.supported_action_types(),
            "metrics": {
                "claimed": runtime.metrics.claimed,
                "completed": runtime.metrics.completed,
                "failed": runtime.metrics.failed,
                "timed_out": runtime.metrics.timed_out,
                "hard_timed_out": runtime.metrics.hard_timed_out,
                "retried": runtime.metrics.retried,
                "stale_requeued": runtime.metrics.stale_requeued,
                "stale_max_retried": runtime.metrics.stale_max_retried,
            },
        }

    return app


async def run_worker() -> None:
    cfg = get_config()
    registry = build_registry()
    register_builtin_handlers(registry)
    runtime = WorkerRuntime(cfg, registry)

    app = _build_health_app(runtime)
    uvicorn_server = UvicornServer(
        UvicornConfig(
            app,
            host=cfg.worker_health_host,
            port=cfg.worker_health_port,
            log_level="warning",
        )
    )

    worker_task = asyncio.create_task(runtime.run())
    health_task = asyncio.create_task(uvicorn_server.serve())
    redis_task: Optional[asyncio.Task[Any]] = None
    if cfg.redis_enabled and cfg.redis_url:
        redis_task = asyncio.create_task(
            consume_job_events(
                redis_url=cfg.redis_url,
                stream=cfg.redis_stream,
                consumer_group=cfg.redis_consumer_group,
                consumer_name=cfg.redis_consumer_name,
                on_job_queued=runtime.on_job_queued_event,
                block_ms=cfg.redis_block_timeout_ms,
                count=max(1, cfg.worker_concurrency * 2),
            )
        )

    try:
        tasks = [worker_task, health_task]
        if redis_task is not None:
            tasks.append(redis_task)
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        runtime.stop()
        uvicorn_server.should_exit = True
        if redis_task is not None:
            redis_task.cancel()
        raise
    finally:
        runtime.stop()
        uvicorn_server.should_exit = True
        with suppress(Exception):
            await worker_task
        with suppress(Exception):
            await health_task
        if redis_task is not None:
            redis_task.cancel()
            with suppress(Exception):
                await redis_task
        await close_db_engine()


def main() -> None:
    configure_logging(log_name="worker")
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

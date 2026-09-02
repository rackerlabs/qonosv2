from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

SCHEDULES_TOTAL = Gauge(
    "schedules_total",
    "Total number of enabled, non-deleted schedules",
)
JOBS_CREATED_TOTAL = Counter(
    "jobs_created_total",
    "Total jobs created",
    ["action_type"],
)
JOBS_COMPLETED_TOTAL = Counter(
    "jobs_completed_total",
    "Total jobs completed",
    ["action_type", "status"],
)
JOB_DURATION_SECONDS = Histogram(
    "job_duration_seconds",
    "Job execution duration in seconds",
    ["action_type"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
)
JOB_QUEUE_DEPTH = Gauge(
    "job_queue_depth",
    "Number of jobs in QUEUED status",
)
SCHEDULER_CYCLE_DURATION_SECONDS = Histogram(
    "scheduler_cycle_duration_seconds",
    "Scheduler poll cycle duration in seconds",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
WORKER_ACTIVE_JOBS = Gauge(
    "worker_active_jobs",
    "Number of jobs currently being processed by this worker",
)
RETENTION_ARTIFACTS_DELETED_TOTAL = Counter(
    "retention_artifacts_deleted_total",
    "Total artifacts deleted by retention policies",
    ["action_type"],
)


def generate_metrics() -> bytes:
    return generate_latest()


def observe_job_created(*, action_type: str) -> None:
    JOBS_CREATED_TOTAL.labels(action_type=action_type).inc()


def observe_job_completed(*, action_type: str, status: str) -> None:
    JOBS_COMPLETED_TOTAL.labels(action_type=action_type, status=status).inc()


def observe_job_duration(*, action_type: str, duration_seconds: float) -> None:
    if duration_seconds >= 0:
        JOB_DURATION_SECONDS.labels(action_type=action_type).observe(duration_seconds)


def observe_scheduler_cycle(*, duration_seconds: float) -> None:
    if duration_seconds >= 0:
        SCHEDULER_CYCLE_DURATION_SECONDS.observe(duration_seconds)


def observe_retention_deleted(*, action_type: str, count: int) -> None:
    if count > 0:
        RETENTION_ARTIFACTS_DELETED_TOTAL.labels(action_type=action_type).inc(count)


def set_worker_active_jobs(count: int) -> None:
    WORKER_ACTIVE_JOBS.set(max(0, count))


async def refresh_db_gauges(session_factory: async_sessionmaker[AsyncSession]) -> None:
    from sqlalchemy import func, select

    from db.src.models import Job as JobModel
    from db.src.models import JobStatus as JobStatusModel
    from db.src.models import Schedule as ScheduleModel

    async with session_factory() as session:
        schedules_result = await session.execute(
            select(func.count())
            .select_from(ScheduleModel)
            .where(
                ScheduleModel.enabled.is_(True),
                ScheduleModel.deleted_at.is_(None),
            )
        )
        SCHEDULES_TOTAL.set(int(schedules_result.scalar_one() or 0))

        queue_result = await session.execute(
            select(func.count())
            .select_from(JobModel)
            .where(JobModel.status == JobStatusModel.QUEUED)
        )
        JOB_QUEUE_DEPTH.set(int(queue_result.scalar_one() or 0))

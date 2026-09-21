"""Integration tests for HA: scheduler CAS deduplication, worker reassignment, scaling."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from api.app.utils.time import utc_now_naive
from api.scheduler import _process_due_schedules
from db.src.models import Job as JobModel, JobStatus as JobStatusModel

from .harness import IntegrationHarness

pytestmark = pytest.mark.integration

_CHAOS_REPLICAS = 12
_LOAD_JOB_COUNT = 6
_STALE_AFTER_SECONDS = 30.0


async def test_scheduler_cas_chaos_prevents_duplicate_jobs(harness: IntegrationHarness) -> None:
    """Concurrent scheduler ticks (chaos) must create at most one job per due schedule."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        name=f"ha-cas-{uuid4()}",
    )
    await harness.make_schedule_due(schedule["id"])

    before = await harness.get_schedule(schedule["id"])
    await asyncio.gather(
        *(_process_due_schedules(harness.session_factory) for _ in range(_CHAOS_REPLICAS))
    )

    jobs = await harness.list_jobs_for_schedule(schedule["id"])
    assert len(jobs) == 1, f"expected exactly one job, got {len(jobs)}"

    updated = await harness.get_schedule(schedule["id"])
    assert updated.next_run_at != before.next_run_at
    assert updated.last_run_at is not None


async def test_worker_stale_processing_reassigns_after_worker_failure(
    harness: IntegrationHarness,
) -> None:
    """A job left PROCESSING by a dead worker is requeued and completed by another worker."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        name=f"ha-reassign-{uuid4()}",
    )
    job_id = (await harness.create_job(schedule["id"]))["id"]
    await harness.set_jobs_run_at([job_id])

    worker_a = harness.build_ha_worker(
        worker_id="ha-worker-a",
        worker_concurrency=1,
        worker_stale_processing_after_seconds=_STALE_AFTER_SECONDS,
    )
    claimed = await worker_a._claim_due_jobs_batch(
        supported=worker_a.registry.supported_action_types(),
        now=utc_now_naive(),
        slots=1,
    )
    assert job_id in claimed

    stale_started_at = utc_now_naive() - timedelta(seconds=120)
    async with harness.session_factory() as session:
        await session.execute(
            update(JobModel)
            .where(JobModel.id == job_id)
            .values(started_at=stale_started_at, updated_at=stale_started_at)
        )
        await session.commit()

    worker_b = harness.build_ha_worker(
        worker_id="ha-worker-b",
        worker_concurrency=1,
        worker_stale_processing_after_seconds=_STALE_AFTER_SECONDS,
    )
    await worker_b._reclaim_stale_processing_once()

    job = await harness.get_job(job_id)
    assert job.status == JobStatusModel.QUEUED
    assert job.worker_id is None
    assert job.started_at is None

    terminal = await harness.run_worker_until_job_terminal(job_id, timeout_seconds=20.0)
    assert terminal.status == JobStatusModel.DONE


async def test_worker_throughput_scales_with_replica_count(
    harness: IntegrationHarness,
) -> None:
    """More worker replicas drain a job backlog faster (near-linear throughput scaling)."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        name=f"ha-load-{uuid4()}",
    )

    job_ids = [
        (await harness.create_job(schedule["id"]))["id"] for _ in range(_LOAD_JOB_COUNT)
    ]
    await harness.set_jobs_run_at(job_ids)

    harness.openstack.snapshot_statuses = ["QUEUED"] * 10 + ["ACTIVE"]
    load_cfg = {
        "worker_concurrency": 1,
        "worker_idle_sleep_seconds": 0.01,
        "server_snapshot_poll_interval_seconds": 0.05,
        "server_snapshot_poll_timeout_seconds": 30.0,
    }

    async def _drain_with_workers(*, worker_count: int) -> float:
        await harness.reset_jobs_to_queued(job_ids)
        workers = [
            harness.build_ha_worker(worker_id=f"ha-load-worker-{idx}", **load_cfg)
            for idx in range(worker_count)
        ]

        started = time.monotonic()
        pending = set(job_ids)
        while pending and (time.monotonic() - started) < 60.0:
            await asyncio.gather(*(worker._dispatch_once() for worker in workers))
            for worker in workers:
                if worker._tasks:
                    await asyncio.gather(*worker._tasks, return_exceptions=True)
            pending = await harness.pending_job_ids(job_ids)
            if pending:
                await asyncio.sleep(0.01)

        assert not pending, f"jobs did not finish: {pending}"
        return time.monotonic() - started

    single_worker_seconds = await _drain_with_workers(worker_count=1)
    dual_worker_seconds = await _drain_with_workers(worker_count=2)

    assert dual_worker_seconds < single_worker_seconds * 0.85, (
        f"expected dual-worker drain ({dual_worker_seconds:.2f}s) to beat single-worker "
        f"({single_worker_seconds:.2f}s)"
    )

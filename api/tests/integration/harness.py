from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.config import AppConfig
from api.app.utils.cron import compute_next_run_at
from api.app.utils.time import utc_now
from api.scheduler import _process_due_schedules
from api.worker import WorkerRuntime
from api.app.worker.builtin_handlers import register_builtin_handlers
from api.app.worker.registry import build_registry
from db.src.models import Job as JobModel, JobStatus as JobStatusModel, Schedule as ScheduleModel

from .fakes import (
    OpenStackSimulator,
    PROJECT_A,
    SERVER_A,
    TOKEN_MEMBER_A,
    TOKEN_MEMBER_B,
    TOKEN_SERVICE,
    TRUST_A,
    VOLUME_A,
)

TERMINAL_JOB_STATUSES = {
    JobStatusModel.DONE,
    JobStatusModel.CANCELLED,
    JobStatusModel.HARD_TIMED_OUT,
    JobStatusModel.MAX_RETRIED,
}


@dataclass
class IntegrationHarness:
    """Orchestrates API, scheduler, and worker for integration scenarios."""

    client: httpx.AsyncClient
    session_factory: Any
    worker: WorkerRuntime
    openstack: OpenStackSimulator
    member_token: str = TOKEN_MEMBER_A
    project_id: str = PROJECT_A

    def auth_headers(self, token: str | None = None) -> Dict[str, str]:
        return {"X-Auth-Token": token or self.member_token}

    async def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        json: Dict[str, Any] | None = None,
    ) -> httpx.Response:
        return await self.client.request(
            method,
            path,
            headers=self.auth_headers(token),
            json=json,
        )

    async def assert_status(
        self,
        method: str,
        path: str,
        expected: int,
        *,
        token: str | None = None,
        json: Dict[str, Any] | None = None,
    ) -> httpx.Response:
        resp = await self.request(method, path, token=token, json=json)
        assert resp.status_code == expected, resp.text
        return resp

    async def assert_forbidden(
        self,
        method: str,
        path: str,
        *,
        token: str = TOKEN_MEMBER_B,
        json: Dict[str, Any] | None = None,
    ) -> None:
        await self.assert_status(method, path, 403, token=token, json=json)

    def execution_profile_body(
        self,
        *,
        trust_id: str = TRUST_A,
        name: str | None = None,
    ) -> Dict[str, Any]:
        return {
            "name": name or f"profile-{uuid4()}",
            "description": "integration test profile",
            "auth_type": "trust",
            "trust_id": trust_id,
            "enabled": True,
        }

    async def post_execution_profile(
        self,
        *,
        trust_id: str = TRUST_A,
        name: str | None = None,
        token: str | None = None,
    ) -> httpx.Response:
        return await self.request(
            "POST",
            "/v2/execution-profiles",
            token=token,
            json=self.execution_profile_body(trust_id=trust_id, name=name),
        )

    def schedule_body(
        self,
        *,
        execution_profile_id: str,
        cron_expression: str = "0 8 * * *",
        server_id: str = SERVER_A,
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "name": name or f"schedule-{uuid4()}",
            "description": "integration test schedule",
            "action_type": "server_snapshot",
            "cron_expression": cron_expression,
            "action_parameters": {"server_id": server_id},
            "execution_profile_id": execution_profile_id,
            "enabled": True,
        }
        if retention_policy is not None:
            body["retention_policy"] = retention_policy
        return body

    async def post_schedule(
        self,
        *,
        execution_profile_id: str,
        cron_expression: str = "0 8 * * *",
        server_id: str = SERVER_A,
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
        token: str | None = None,
    ) -> httpx.Response:
        return await self.request(
            "POST",
            "/v2/schedules",
            token=token,
            json=self.schedule_body(
                execution_profile_id=execution_profile_id,
                cron_expression=cron_expression,
                server_id=server_id,
                retention_policy=retention_policy,
                name=name,
            ),
        )

    async def create_execution_profile(
        self,
        *,
        name: str | None = None,
        trust_id: str = TRUST_A,
        token: str | None = None,
    ) -> Dict[str, Any]:
        resp = await self.post_execution_profile(trust_id=trust_id, name=name, token=token)
        assert resp.status_code == 201, resp.text
        return resp.json()

    async def create_schedule(
        self,
        *,
        execution_profile_id: str,
        cron_expression: str = "* * * * *",
        server_id: str = SERVER_A,
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
        token: str | None = None,
    ) -> Dict[str, Any]:
        resp = await self.post_schedule(
            execution_profile_id=execution_profile_id,
            cron_expression=cron_expression,
            server_id=server_id,
            retention_policy=retention_policy,
            name=name,
            token=token,
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    def volume_schedule_body(
        self,
        *,
        execution_profile_id: str,
        action_type: str,
        volume_id: str = VOLUME_A,
        cron_expression: str = "* * * * *",
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "name": name or f"{action_type}-{uuid4()}",
            "description": "integration test volume backup schedule",
            "action_type": action_type,
            "cron_expression": cron_expression,
            "action_parameters": {"volume_id": volume_id},
            "execution_profile_id": execution_profile_id,
            "enabled": True,
        }
        if retention_policy is not None:
            body["retention_policy"] = retention_policy
        return body

    async def post_volume_schedule(
        self,
        *,
        execution_profile_id: str,
        action_type: str,
        volume_id: str = VOLUME_A,
        cron_expression: str = "* * * * *",
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
        token: str | None = None,
    ) -> httpx.Response:
        return await self.request(
            "POST",
            "/v2/schedules",
            token=token,
            json=self.volume_schedule_body(
                execution_profile_id=execution_profile_id,
                action_type=action_type,
                volume_id=volume_id,
                cron_expression=cron_expression,
                retention_policy=retention_policy,
                name=name,
            ),
        )

    async def create_volume_schedule(
        self,
        *,
        execution_profile_id: str,
        action_type: str,
        volume_id: str = VOLUME_A,
        cron_expression: str = "* * * * *",
        retention_policy: Optional[Dict[str, Any]] = None,
        name: str | None = None,
        token: str | None = None,
    ) -> Dict[str, Any]:
        resp = await self.post_volume_schedule(
            execution_profile_id=execution_profile_id,
            action_type=action_type,
            volume_id=volume_id,
            cron_expression=cron_expression,
            retention_policy=retention_policy,
            name=name,
            token=token,
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    async def create_job(
        self,
        schedule_id: str,
        *,
        token: str | None = None,
    ) -> Dict[str, Any]:
        resp = await self.request(
            "POST",
            "/v2/jobs",
            token=token or TOKEN_SERVICE,
            json={"schedule_id": schedule_id},
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    async def make_schedule_due(self, schedule_id: str, *, when: Optional[datetime] = None) -> None:
        due_at = when or utc_now()
        async with self.session_factory() as session:
            await session.execute(
                update(ScheduleModel)
                .where(ScheduleModel.id == schedule_id)
                .values(next_run_at=due_at)
            )
            await session.commit()

    async def run_scheduler_once(self) -> None:
        await _process_due_schedules(self.session_factory)

    async def get_schedule(self, schedule_id: str) -> ScheduleModel:
        async with self.session_factory() as session:
            result = await session.execute(
                select(ScheduleModel).where(ScheduleModel.id == schedule_id)
            )
            row = result.scalar_one()
            return row

    async def list_jobs_for_schedule(self, schedule_id: str) -> list[JobModel]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(JobModel)
                .where(JobModel.schedule_id == schedule_id)
                .order_by(JobModel.created_at.asc())
            )
            return list(result.scalars().all())

    async def get_job(self, job_id: str) -> JobModel:
        async with self.session_factory() as session:
            result = await session.execute(select(JobModel).where(JobModel.id == job_id))
            return result.scalar_one()

    def _track_job_status(
        self, statuses_seen: list[JobStatusModel] | None, status: JobStatusModel
    ) -> None:
        if statuses_seen is not None and (not statuses_seen or statuses_seen[-1] != status):
            statuses_seen.append(status)

    async def run_worker_until_job_terminal(
        self,
        job_id: str,
        *,
        timeout_seconds: float = 15.0,
        statuses_seen: list[JobStatusModel] | None = None,
    ) -> JobModel:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        self._track_job_status(statuses_seen, (await self.get_job(job_id)).status)
        while loop.time() < deadline:
            await self.worker._dispatch_once()
            self._track_job_status(statuses_seen, (await self.get_job(job_id)).status)
            if self.worker._tasks:
                await asyncio.gather(*list(self.worker._tasks), return_exceptions=True)
            job = await self.get_job(job_id)
            self._track_job_status(statuses_seen, job.status)
            if job.status in TERMINAL_JOB_STATUSES:
                return job
            if job.status == JobStatusModel.QUEUED:
                async with self.session_factory() as session:
                    await session.execute(
                        update(JobModel).where(JobModel.id == job_id).values(run_at=utc_now())
                    )
                    await session.commit()
            await asyncio.sleep(0.02)
        job = await self.get_job(job_id)
        raise TimeoutError(f"job {job_id} stuck in status {job.status}")

    async def run_full_cycle(
        self,
        schedule: Dict[str, Any],
        *,
        expect_done: bool = True,
    ) -> JobModel:
        await self.make_schedule_due(schedule["id"])
        await self.run_scheduler_once()
        jobs = await self.list_jobs_for_schedule(schedule["id"])
        assert len(jobs) >= 1, "scheduler did not create a job"
        job = await self.run_worker_until_job_terminal(jobs[-1].id)
        if expect_done:
            assert job.status == JobStatusModel.DONE, job.error_message
        return job

    def expected_next_cron(
        self, cron_expression: str, *, now: Optional[datetime] = None
    ) -> datetime:
        return compute_next_run_at(cron_expression, now=now or utc_now())


def build_test_config(db_url: str) -> AppConfig:
    return AppConfig(
        keystone_url="http://keystone.test:5000/v3",
        keystone_verify_ssl=False,
        keystone_username="qonos-service",
        keystone_password="secret",
        keystone_user_domain_name="Default",
        db_url=db_url,
        poll_interval_seconds=0.1,
        worker_concurrency=2,
        worker_idle_sleep_seconds=0.05,
        worker_soft_timeout_seconds=5.0,
        worker_hard_timeout_seconds=10.0,
        worker_max_retry=2,
        worker_backoff_base_seconds=0.01,
        worker_backoff_max_seconds=0.05,
        worker_backoff_jitter=False,
        worker_stale_processing_sweep_interval_seconds=3600.0,
        worker_stale_processing_after_seconds=3600.0,
        job_cleanup_enabled=False,
        server_snapshot_poll_interval_seconds=0.01,
        server_snapshot_poll_timeout_seconds=1.0,
        volume_backup_poll_interval_seconds=0.01,
        volume_backup_poll_timeout_seconds=1.0,
        worker_id="integration-test-worker",
        amqp_url=None,
    )


def build_worker_runtime(cfg: AppConfig) -> WorkerRuntime:
    registry = build_registry()
    register_builtin_handlers(registry)
    return WorkerRuntime(cfg, registry)

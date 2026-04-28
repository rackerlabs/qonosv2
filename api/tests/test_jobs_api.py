from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from api.app.config import AppConfig
from api.app.db import get_session_factory
from api.app.worker.handlers import HandlerRegistry
from api.worker import WorkerRuntime
from api.app.utils.time import utc_now
from api.tests.test_auth_rbac import (
    ADMIN_TOKEN,
    PROJECT1_MEMBER,
    SERVICE_TOKEN,
    _auth,
     async_client
)
from db.src.models import Job as JobModel, JobFault as JobFaultModel, JobStatus as JobStatusModel


@pytest.mark.anyio
async def test_job_list_filters_and_pagination(async_client: httpx.AsyncClient) -> None:
    schedule_body = {
        "name": f"jobs-list-source-{uuid4()}",
        "description": "Job list source schedule",
        "action_type": "server_snapshot",
        "cron_expression": "0 4 * * *",
        "action_parameters": {"server_id": "srv-jobs"},
        "enabled": True,
    }
    schedule_resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=schedule_body,
    )
    assert schedule_resp.status_code == 201, schedule_resp.text
    schedule = schedule_resp.json()
    
    job1_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert job1_resp.status_code == 201, job1_resp.text
    job1 = job1_resp.json()
    assert "worker_id" in job1
    assert "completed_at" in job1

    job2_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert job2_resp.status_code == 201, job2_resp.text
    job2 = job2_resp.json()

    created_at_1 = datetime.fromisoformat(job1["created_at"].replace("Z", "+00:00"))
    after_job1 = (created_at_1 + timedelta(seconds=1)).astimezone(timezone.utc).isoformat()
    before_job1 = (created_at_1 - timedelta(seconds=1)).astimezone(timezone.utc).isoformat()

    list_by_schedule = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"schedule_id": schedule["id"]},
    )
    assert list_by_schedule.status_code == 200
    schedule_ids = {job["id"] for job in list_by_schedule.json()["jobs"]}
    assert job1["id"] in schedule_ids
    assert job2["id"] in schedule_ids

    admin_list_by_schedule = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": ADMIN_TOKEN}),
        params={"schedule_id": schedule["id"]},
    )
    assert admin_list_by_schedule.status_code == 200
    admin_schedule_ids = {job["id"] for job in admin_list_by_schedule.json()["jobs"]}
    assert job1["id"] in admin_schedule_ids
    assert job2["id"] in admin_schedule_ids

    list_by_action_type = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"action_type": "server_snapshot"},
    )
    assert list_by_action_type.status_code == 200
    action_type_ids = {job["id"] for job in list_by_action_type.json()["jobs"]}
    assert job1["id"] in action_type_ids
    assert job2["id"] in action_type_ids

    list_by_status = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"status": "QUEUED"},
    )
    assert list_by_status.status_code == 200
    status_ids = {job["id"] for job in list_by_status.json()["jobs"]}
    assert job1["id"] in status_ids
    assert job2["id"] in status_ids

    list_created_before = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"schedule_id": schedule["id"], "created_before": before_job1},
    )
    assert list_created_before.status_code == 200
    assert all(job["id"] != job1["id"] for job in list_created_before.json()["jobs"])

    list_created_after = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"schedule_id": schedule["id"], "created_after": after_job1},
    )
    assert list_created_after.status_code == 200
    assert all(job["id"] != job1["id"] for job in list_created_after.json()["jobs"])

    list_created_window = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={
            "schedule_id": schedule["id"],
            "created_after": before_job1,
            "created_before": after_job1,
        },
    )
    assert list_created_window.status_code == 200
    window_ids = {job["id"] for job in list_created_window.json()["jobs"]}
    assert job1["id"] in window_ids

    page1 = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"schedule_id": schedule["id"], "limit": 1},
    )
    assert page1.status_code == 200
    page1_jobs = page1.json()["jobs"]
    assert len(page1_jobs) == 1
    first_id = page1_jobs[0]["id"]

    page2 = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"schedule_id": schedule["id"], "marker": first_id, "limit": 10},
    )
    assert page2.status_code == 200
    page2_ids = {job["id"] for job in page2.json()["jobs"]}
    assert first_id not in page2_ids


@pytest.mark.anyio
async def test_show_job_includes_fault_history(async_client: httpx.AsyncClient) -> None:
    schedule_body = {
        "name": f"job-detail-source-{uuid4()}",
        "description": "Job detail source schedule",
        "action_type": "server_snapshot",
        "cron_expression": "0 5 * * *",
        "action_parameters": {"server_id": "srv-fault"},
        "enabled": True,
    }
    schedule_resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=schedule_body,
    )
    assert schedule_resp.status_code == 201, schedule_resp.text
    schedule = schedule_resp.json()

    job_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert job_resp.status_code == 201, job_resp.text
    job = job_resp.json()

    session_factory = get_session_factory()
    async with session_factory() as session:
        fault = JobFaultModel(
            job_id=job["id"],
            project_id=job["project_id"],
            occurred_at=utc_now(),
            worker_id="worker-test-1",
            code="worker.timeout",
            title="Worker timeout",
            detail="The job exceeded the worker timeout threshold.",
            error_message="The job exceeded the worker timeout threshold.",
            action_parameters_at_failure=job["action_parameters"],
            extra={"attempt": 1},
            created_at=utc_now(),
        )
        session.add(fault)
        await session.commit()

    show_resp = await async_client.get(
        f"/v2/jobs/{job['id']}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert show_resp.status_code == 200, show_resp.text
    payload = show_resp.json()
    assert payload["id"] == job["id"]
    assert isinstance(payload["fault_history"], list)
    assert len(payload["fault_history"]) == 1
    assert payload["fault_history"][0]["job_id"] == job["id"]
    assert payload["fault_history"][0]["code"] == "worker.timeout"
    assert payload["fault_history"][0]["title"] == "Worker timeout"
    assert payload["fault_history"][0]["worker_id"] == "worker-test-1"
    assert (
        payload["fault_history"][0]["error_message"]
        == "The job exceeded the worker timeout threshold."
    )
    assert payload["fault_history"][0]["action_parameters_at_failure"] == job["action_parameters"]
    assert "worker_id" in payload

    admin_show_resp = await async_client.get(
        f"/v2/jobs/{job['id']}",
        headers=_auth({"X-Auth-Token": ADMIN_TOKEN}),
    )
    assert admin_show_resp.status_code == 200, admin_show_resp.text
    admin_payload = admin_show_resp.json()
    assert admin_payload["id"] == job["id"]
    assert len(admin_payload["fault_history"]) == 1
    assert admin_payload["fault_history"][0]["job_id"] == job["id"]


@pytest.mark.anyio
async def test_cancel_job_via_post(async_client: httpx.AsyncClient) -> None:
    schedule_body = {
        "name": f"job-cancel-source-{uuid4()}",
        "description": "Job cancel source schedule",
        "action_type": "server_snapshot",
        "cron_expression": "0 6 * * *",
        "action_parameters": {"server_id": "srv-cancel"},
        "enabled": True,
    }
    schedule_resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=schedule_body,
    )
    assert schedule_resp.status_code == 201, schedule_resp.text
    schedule = schedule_resp.json()

    job_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert job_resp.status_code == 201, job_resp.text
    job = job_resp.json()

    cancel_resp = await async_client.post(
        f"/v2/jobs/{job['id']}/cancel",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
    )
    assert cancel_resp.status_code == 204, cancel_resp.text

    show_resp = await async_client.get(
        f"/v2/jobs/{job['id']}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert show_resp.status_code == 200, show_resp.text
    payload = show_resp.json()
    assert payload["status"] == "CANCELLED"
    assert payload["cancelled_at"] is not None


@pytest.mark.anyio
async def test_worker_cleanup_removes_expired_terminal_jobs_and_faults(
    async_client: httpx.AsyncClient,
) -> None:
    schedule_body = {
        "name": f"job-cleanup-source-{uuid4()}",
        "description": "Job cleanup source schedule",
        "action_type": "server_snapshot",
        "cron_expression": "0 7 * * *",
        "action_parameters": {"server_id": "srv-cleanup"},
        "enabled": True,
    }
    schedule_resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=schedule_body,
    )
    assert schedule_resp.status_code == 201, schedule_resp.text
    schedule = schedule_resp.json()

    expired_no_fault_job_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert expired_no_fault_job_resp.status_code == 201, expired_no_fault_job_resp.text
    expired_no_fault_job = expired_no_fault_job_resp.json()

    expired_job_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert expired_job_resp.status_code == 201, expired_job_resp.text
    expired_job = expired_job_resp.json()

    fresh_job_resp = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule["id"]},
    )
    assert fresh_job_resp.status_code == 201, fresh_job_resp.text
    fresh_job = fresh_job_resp.json()

    session_factory = get_session_factory()
    old_done_at = utc_now() - timedelta(days=15)
    async with session_factory() as session:
        expired_no_fault_row = await session.get(JobModel, expired_no_fault_job["id"])
        expired_row = await session.get(JobModel, expired_job["id"])
        fresh_row = await session.get(JobModel, fresh_job["id"])
        assert expired_no_fault_row is not None
        assert expired_row is not None
        assert fresh_row is not None

        expired_no_fault_row.status = JobStatusModel.DONE
        expired_no_fault_row.ended_at = old_done_at
        expired_no_fault_row.completed_at = old_done_at
        expired_no_fault_row.updated_at = old_done_at

        expired_row.status = JobStatusModel.DONE
        expired_row.ended_at = old_done_at
        expired_row.completed_at = old_done_at
        expired_row.updated_at = old_done_at

        fresh_row.status = JobStatusModel.DONE
        fresh_row.ended_at = utc_now()
        fresh_row.completed_at = utc_now()
        fresh_row.updated_at = utc_now()

        session.add(
            JobFaultModel(
                job_id=expired_job["id"],
                project_id=expired_job["project_id"],
                occurred_at=old_done_at,
                worker_id="cleanup-worker",
                code="worker.timeout",
                title="Worker timeout",
                detail="expired job fault",
                error_message="expired job fault",
                action_parameters_at_failure=expired_job["action_parameters"],
                created_at=old_done_at,
            )
        )
        session.add(
            JobFaultModel(
                job_id=fresh_job["id"],
                project_id=fresh_job["project_id"],
                occurred_at=utc_now(),
                worker_id="cleanup-worker",
                code="worker.timeout",
                title="Worker timeout",
                detail="fresh job fault",
                error_message="fresh job fault",
                action_parameters_at_failure=fresh_job["action_parameters"],
                created_at=utc_now(),
            )
        )
        await session.commit()

    runtime = WorkerRuntime(
        AppConfig(
            job_cleanup_enabled=True,
            job_cleanup_ttl_days=14,
            job_cleanup_interval_seconds=3600.0,
        ),
        HandlerRegistry(),
    )
    deleted = await runtime._cleanup_expired_jobs_once()
    assert deleted >= 1

    async with session_factory() as session:
        expired_no_fault_exists = await session.get(JobModel, expired_no_fault_job["id"])
        expired_exists = await session.get(JobModel, expired_job["id"])
        fresh_exists = await session.get(JobModel, fresh_job["id"])
        assert expired_no_fault_exists is None
        assert expired_exists is None
        assert fresh_exists is not None

        expired_no_fault_faults = await session.execute(
            select(JobFaultModel).where(JobFaultModel.job_id == expired_no_fault_job["id"])
        )
        expired_faults = await session.execute(
            select(JobFaultModel).where(JobFaultModel.job_id == expired_job["id"])
        )
        fresh_faults = await session.execute(
            select(JobFaultModel).where(JobFaultModel.job_id == fresh_job["id"])
        )
        assert expired_no_fault_faults.scalars().all() == []
        assert expired_faults.scalars().all() == []
        assert len(fresh_faults.scalars().all()) == 1

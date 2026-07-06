"""End-to-end integration tests: schedule -> job -> worker actions -> retention."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select

from api.app.utils.cron import compute_next_run_at
from api.app.utils.time import as_utc_aware, utc_now
from db.src.models import (
    Job as JobModel,
    JobFault as JobFaultModel,
    JobStatus as JobStatusModel,
)

from .fakes import (
    PROJECT_A,
    SERVER_A,
    SimulatedKeystoneClient,
    TOKEN_ADMIN_A,
    TOKEN_INVALID,
    TOKEN_MEMBER_A,
    TOKEN_MEMBER_B,
    TOKEN_SERVICE,
    TRUST_A,
    TRUST_B,
    VOLUME_A,
)
from .harness import IntegrationHarness

pytestmark = pytest.mark.integration


async def test_full_lifecycle_schedule_job_snapshot_retention(harness: IntegrationHarness) -> None:
    """Schedule -> scheduler job -> worker snapshot -> retention prunes old images."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        cron_expression="0 8 * * *",
        retention_policy={"type": "count", "value": 2},
        name=f"e2e-retention-{uuid4()}",
    )
    harness.openstack.seed_managed_images(
        schedule_id=schedule["id"],
        count=2,
        ages_days=[10, 8],
    )

    job = await harness.run_full_cycle(schedule)
    assert job.status == JobStatusModel.DONE

    managed = harness.openstack.list_managed_for_schedule(schedule["id"], PROJECT_A)
    assert len(managed) <= 2
    assert len(harness.openstack.deleted_image_ids) >= 1

    updated_schedule = await harness.get_schedule(schedule["id"])
    assert updated_schedule.last_run_at is not None
    assert updated_schedule.next_run_at is not None


async def test_schedule_crud_and_cron_evaluation(harness: IntegrationHarness) -> None:
    """Schedule CRUD via API and cron-derived next_run_at."""
    profile = await harness.create_execution_profile()
    created = await harness.post_schedule(
        execution_profile_id=profile["id"],
        cron_expression="30 14 * * *",
        name=f"cron-crud-{uuid4()}",
    )
    assert created.status_code == 201, created.text
    schedule = created.json()
    schedule_id = schedule["id"]

    expected_next = compute_next_run_at("30 14 * * *")
    assert schedule["next_run_at"][:16] == expected_next.isoformat()[:16]

    path = f"/v2/schedules/{schedule_id}"
    await harness.assert_status("GET", path, 200)
    updated = await harness.assert_status(
        "PUT",
        path,
        200,
        json={"description": "updated by integration test"},
    )
    assert updated.json()["description"] == "updated by integration test"

    listed = await harness.assert_status("GET", "/v2/schedules", 200)
    assert any(s["id"] == schedule_id for s in listed.json()["schedules"])
    await harness.assert_status("DELETE", path, 204)


async def test_scheduler_creates_job_and_advances_schedule(harness: IntegrationHarness) -> None:
    """Due schedule produces QUEUED job and advances next_run_at."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        cron_expression="0 8 * * *",
    )
    due_at = utc_now()
    await harness.make_schedule_due(schedule["id"], when=due_at)
    await harness.run_scheduler_once()

    jobs = await harness.list_jobs_for_schedule(schedule["id"])
    assert len(jobs) == 1
    assert jobs[0].status == JobStatusModel.QUEUED
    assert jobs[0].action_parameters.get("qonos_trust_id")

    after = await harness.get_schedule(schedule["id"])
    assert after.last_run_at is not None
    expected_next = harness.expected_next_cron("0 8 * * *", now=due_at)
    next_run_at = as_utc_aware(after.next_run_at)
    assert next_run_at == expected_next
    assert next_run_at > due_at


async def test_job_state_transitions_to_done(harness: IntegrationHarness) -> None:
    """Worker transitions QUEUED -> PROCESSING -> DONE and creates a Glance image."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    await harness.make_schedule_due(schedule["id"])
    await harness.run_scheduler_once()
    jobs = await harness.list_jobs_for_schedule(schedule["id"])
    assert len(jobs) == 1
    assert jobs[0].status == JobStatusModel.QUEUED

    statuses_seen: list[JobStatusModel] = []
    job = await harness.run_worker_until_job_terminal(
        jobs[0].id,
        statuses_seen=statuses_seen,
    )
    assert statuses_seen == [
        JobStatusModel.QUEUED,
        JobStatusModel.PROCESSING,
        JobStatusModel.DONE,
    ]
    assert job.status == JobStatusModel.DONE
    assert job.completed_at is not None

    images = [
        img
        for img in harness.openstack.images.values()
        if img.properties.get("qonos_job_id") == job.id
    ]
    assert len(images) == 1
    assert images[0].status == "ACTIVE"
    assert any(call[1] == TRUST_A for call in harness.openstack.connection_calls)


async def test_retention_count_after_success(harness: IntegrationHarness) -> None:
    """Retention keeps newest N managed images for the schedule."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        retention_policy={"type": "count", "value": 1},
    )
    harness.openstack.seed_managed_images(
        schedule_id=schedule["id"], count=3, ages_days=[30, 20, 10]
    )

    await harness.run_full_cycle(schedule)

    remaining = harness.openstack.list_managed_for_schedule(schedule["id"], PROJECT_A)
    assert len(remaining) == 1
    assert len(harness.openstack.deleted_image_ids) == 3


async def test_retention_age_after_success(harness: IntegrationHarness) -> None:
    """Retention prunes managed snapshots older than max_age_days."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        retention_policy={"type": "age", "max_age_days": 7},
    )
    harness.openstack.seed_managed_images(
        schedule_id=schedule["id"], count=3, ages_days=[15, 10, 3]
    )

    await harness.run_full_cycle(schedule)

    remaining = harness.openstack.list_managed_for_schedule(schedule["id"], PROJECT_A)
    assert len(remaining) == 2
    assert len(harness.openstack.deleted_image_ids) == 2


async def test_snapshot_failure_retries_then_succeeds(harness: IntegrationHarness) -> None:
    """Transient snapshot failure moves job through ERROR/retry to DONE."""
    harness.openstack.create_snapshot_failures_remaining = 1
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    await harness.make_schedule_due(schedule["id"])
    await harness.run_scheduler_once()
    job_id = (await harness.list_jobs_for_schedule(schedule["id"]))[0].id

    job = await harness.run_worker_until_job_terminal(job_id, timeout_seconds=20.0)
    assert job.status == JobStatusModel.DONE
    assert job.retry_count >= 1
    assert job.error_message is None

    async with harness.session_factory() as session:
        result = await session.execute(select(JobFaultModel).where(JobFaultModel.job_id == job_id))
        faults = list(result.scalars().all())
    assert len(faults) >= 1
    assert any(fault.code == "qonos.job.error" for fault in faults)


async def test_snapshot_server_not_found_records_error(harness: IntegrationHarness) -> None:
    """Missing Nova server fails the job after retries are exhausted."""
    harness.worker.cfg.worker_max_retry = 0
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(
        execution_profile_id=profile["id"],
        server_id=str(uuid4()),
    )

    await harness.make_schedule_due(schedule["id"])
    await harness.run_scheduler_once()
    job_id = (await harness.list_jobs_for_schedule(schedule["id"]))[0].id

    job = await harness.run_worker_until_job_terminal(job_id)
    assert job.status == JobStatusModel.MAX_RETRIED
    assert job.error_message


async def test_snapshot_poll_timeout_records_error(harness: IntegrationHarness) -> None:
    """If snapshot never becomes ACTIVE, worker should fail after timeout."""
    harness.worker.cfg.worker_max_retry = 0
    harness.worker.cfg.server_snapshot_poll_timeout_seconds = 0.05
    harness.openstack.snapshot_statuses = ["QUEUED"]
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    await harness.make_schedule_due(schedule["id"])
    await harness.run_scheduler_once()
    job_id = (await harness.list_jobs_for_schedule(schedule["id"]))[0].id

    job = await harness.run_worker_until_job_terminal(job_id, timeout_seconds=20.0)
    assert job.status in {JobStatusModel.MAX_RETRIED, JobStatusModel.HARD_TIMED_OUT}
    assert "timeout" in (job.error_message or "").lower()


@pytest.mark.parametrize("failed_status", ["ERROR", "KILLED"])
async def test_snapshot_failed_image_status_fails_job(
    harness: IntegrationHarness, failed_status: str
) -> None:
    """Failed image statuses should fail the job when retries are exhausted."""
    harness.worker.cfg.worker_max_retry = 0
    harness.openstack.snapshot_statuses = [failed_status]
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    await harness.make_schedule_due(schedule["id"])
    await harness.run_scheduler_once()
    job_id = (await harness.list_jobs_for_schedule(schedule["id"]))[0].id

    job = await harness.run_worker_until_job_terminal(job_id, timeout_seconds=20.0)
    assert job.status == JobStatusModel.MAX_RETRIED
    assert failed_status.lower() in (job.error_message or "").lower()


async def test_authz_cross_project_schedule_forbidden(harness: IntegrationHarness) -> None:
    """Member in another project cannot read or mutate schedules."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])
    path = f"/v2/schedules/{schedule['id']}"

    await harness.assert_forbidden("GET", path)
    await harness.assert_forbidden("PUT", path, json={"description": "nope"})
    await harness.assert_forbidden("DELETE", path)


async def test_authz_cross_project_execution_profile_forbidden(harness: IntegrationHarness) -> None:
    """Member in another project cannot read or mutate execution profiles."""
    profile = await harness.create_execution_profile()
    path = f"/v2/execution-profiles/{profile['id']}"

    await harness.assert_forbidden("GET", path)
    await harness.assert_forbidden("PUT", path, json={"description": "nope"})
    await harness.assert_forbidden("DELETE", path)


async def test_authz_cross_project_job_access(harness: IntegrationHarness) -> None:
    """Cross-project job read, list isolation, update, and cancel are forbidden."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])
    job = await harness.create_job(schedule["id"])
    job_id = job["id"]
    assert job["project_id"] == PROJECT_A

    job_path = f"/v2/jobs/{job_id}"
    await harness.assert_status("GET", job_path, 200, token=TOKEN_MEMBER_A)
    await harness.assert_forbidden("GET", job_path)
    await harness.assert_status("GET", job_path, 200, token=TOKEN_ADMIN_A)

    list_a = await harness.assert_status("GET", "/v2/jobs", 200, token=TOKEN_MEMBER_A)
    assert any(j["id"] == job_id for j in list_a.json()["jobs"])
    list_b = await harness.assert_status("GET", "/v2/jobs", 200, token=TOKEN_MEMBER_B)
    assert all(j["id"] != job_id for j in list_b.json()["jobs"])

    updated = await harness.assert_status(
        "PATCH",
        job_path,
        200,
        token=TOKEN_SERVICE,
        json={"status": "PROCESSING"},
    )
    assert updated.json()["status"] == "PROCESSING"
    await harness.assert_forbidden("PATCH", job_path, json={"status": "DONE"})

    await harness.assert_forbidden("POST", f"{job_path}/cancel")
    job2 = await harness.create_job(schedule["id"])
    await harness.assert_status("POST", f"/v2/jobs/{job2['id']}/cancel", 204, token=TOKEN_SERVICE)


async def test_authz_member_cannot_create_or_cancel_job(harness: IntegrationHarness) -> None:
    """Members lack job:create and job:cancel policies."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    await harness.assert_forbidden(
        "POST",
        "/v2/jobs",
        token=TOKEN_MEMBER_A,
        json={"schedule_id": schedule["id"]},
    )

    job = await harness.create_job(schedule["id"])
    await harness.assert_forbidden(
        "POST",
        f"/v2/jobs/{job['id']}/cancel",
        token=TOKEN_MEMBER_A,
    )


async def test_trust_validation_on_execution_profile_create(
    harness: IntegrationHarness, keystone: SimulatedKeystoneClient
) -> None:
    """Valid trust succeeds; unknown or wrong-project trust returns 400."""
    ok = await harness.post_execution_profile(trust_id=TRUST_A, name=f"trust-ok-{uuid4()}")
    assert ok.status_code == 201, ok.text
    assert any(call[0] == TRUST_A for call in keystone.trust_validation_calls)

    bad_trust = await harness.post_execution_profile(
        trust_id=str(uuid4()),
        name=f"trust-bad-{uuid4()}",
    )
    assert bad_trust.status_code == 400
    assert "Invalid trust_id" in bad_trust.text

    wrong_project = await harness.post_execution_profile(
        trust_id=TRUST_B,
        name=f"trust-wrong-project-{uuid4()}",
    )
    assert wrong_project.status_code == 400
    assert "does not belong" in wrong_project.text


async def test_trust_validation_on_schedule_create(
    harness: IntegrationHarness, keystone: SimulatedKeystoneClient
) -> None:
    """Schedule create re-validates the execution profile trust for the project."""
    profile = await harness.create_execution_profile(name=f"profile-schedule-trust-{uuid4()}")
    keystone.trust_validation_calls.clear()

    schedule_ok = await harness.post_schedule(
        execution_profile_id=profile["id"],
        name=f"schedule-trust-ok-{uuid4()}",
    )
    assert schedule_ok.status_code == 201, schedule_ok.text
    assert any(call[0] == TRUST_A for call in keystone.trust_validation_calls)


async def test_invalid_cron_returns_400(harness: IntegrationHarness) -> None:
    profile = await harness.create_execution_profile()
    resp = await harness.post_schedule(
        execution_profile_id=profile["id"],
        cron_expression="not a cron",
        name=f"bad-cron-{uuid4()}",
    )
    assert resp.status_code == 400
    assert "cron_expression" in resp.text


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param(None, id="missing"),
        pytest.param({"X-Auth-Token": TOKEN_INVALID}, id="invalid"),
    ],
)
async def test_schedule_list_unauthorized(
    harness: IntegrationHarness, headers: dict[str, str] | None
) -> None:
    resp = await harness.client.get("/v2/schedules", headers=headers or {})
    assert resp.status_code == 401


async def test_service_token_can_create_job(harness: IntegrationHarness) -> None:
    """Service role can manually enqueue a job from a schedule."""
    profile = await harness.create_execution_profile()
    schedule = await harness.create_schedule(execution_profile_id=profile["id"])

    job = await harness.create_job(schedule["id"])
    assert job["schedule_id"] == schedule["id"]
    assert job["status"] == "QUEUED"


async def test_service_token_cannot_create_schedule(harness: IntegrationHarness) -> None:
    """Service role cannot create schedules (member/admin role required)."""
    profile = await harness.create_execution_profile()
    resp = await harness.post_schedule(
        execution_profile_id=profile["id"],
        name=f"service-schedule-{uuid4()}",
        token=TOKEN_SERVICE,
    )
    assert resp.status_code == 403


async def test_e2e_full_backup_then_incremental_backup(harness: IntegrationHarness) -> None:
    profile = await harness.create_execution_profile()

    full = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_full",
        name=f"e2e-vol-full-{uuid4()}",
    )
    inc = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_incremental",
        name=f"e2e-vol-inc-{uuid4()}",
    )

    full_job = await harness.run_full_cycle(full)
    assert full_job.status == JobStatusModel.DONE

    inc_job = await harness.run_full_cycle(inc)
    assert inc_job.status == JobStatusModel.DONE

    backups = [
        b
        for b in harness.openstack.backups.values()
        if b.project_id == PROJECT_A and b.volume_id == VOLUME_A
    ]
    assert len(backups) >= 2
    full_backups = [b for b in backups if not b.is_incremental and b.status == "available"]
    inc_backups = [b for b in backups if b.is_incremental and b.status == "available"]
    assert full_backups
    assert inc_backups
    assert inc_backups[-1].parent_id == full_backups[-1].id


async def test_e2e_incremental_without_full_errors(harness: IntegrationHarness) -> None:
    harness.worker.cfg.worker_max_retry = 0
    profile = await harness.create_execution_profile()
    inc = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_incremental",
        name=f"e2e-vol-inc-nofull-{uuid4()}",
    )

    job = await harness.run_full_cycle(inc, expect_done=False)
    assert job.status == JobStatusModel.MAX_RETRIED
    assert "No successful full backup found" in (job.error_message or "")


async def test_e2e_backup_chain_query_returns_ordered_chain(harness: IntegrationHarness) -> None:
    profile = await harness.create_execution_profile()
    full = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_full",
        name=f"e2e-chain-full-{uuid4()}",
    )
    inc = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_incremental",
        name=f"e2e-chain-inc-{uuid4()}",
    )
    await harness.run_full_cycle(full)
    await harness.run_full_cycle(inc)

    resp = await harness.assert_status(
        "GET",
        f"/v2/volumes/{VOLUME_A}/backup-chain?project_id={PROJECT_A}",
        200,
        token=TOKEN_ADMIN_A,
    )
    body = resp.json()
    assert body["volume_id"] == VOLUME_A
    assert body["project_id"] == PROJECT_A
    assert body["has_full_schedule"] is True
    assert body["has_incremental_schedule"] is True
    assert len(body["backups"]) >= 2
    created = [b.get("created_at") for b in body["backups"]]
    assert created == sorted(created)
    inc_items = [b for b in body["backups"] if b["is_incremental"]]
    full_items = [b for b in body["backups"] if not b["is_incremental"]]
    assert inc_items and full_items
    assert inc_items[-1]["parent_id"] == full_items[-1]["id"]


async def test_e2e_cascade_retention_deletes_incrementals_with_full(
    harness: IntegrationHarness,
) -> None:
    profile = await harness.create_execution_profile()
    full = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_full",
        retention_policy={"type": "count", "value": 1},
        name=f"e2e-ret-full-{uuid4()}",
    )
    inc = await harness.create_volume_schedule(
        execution_profile_id=profile["id"],
        action_type="volume_backup_incremental",
        retention_policy={"type": "count", "value": 10},
        name=f"e2e-ret-inc-{uuid4()}",
    )

    await harness.run_full_cycle(full)
    await harness.run_full_cycle(inc)
    backups_after_chain1 = list(harness.openstack.backups.values())
    assert any(not b.is_incremental for b in backups_after_chain1)
    assert any(b.is_incremental for b in backups_after_chain1)

    await harness.run_full_cycle(full)

    remaining = [
        b
        for b in harness.openstack.backups.values()
        if b.project_id == PROJECT_A and b.volume_id == VOLUME_A and b.status == "available"
    ]
    full_remaining = [b for b in remaining if not b.is_incremental]
    assert len(full_remaining) == 1
    for b in remaining:
        if b.is_incremental:
            assert b.parent_id == full_remaining[0].id

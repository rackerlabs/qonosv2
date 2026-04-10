from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict
from uuid import uuid4

import httpx
import pytest

from api.app.config import get_config
from api.app.main import create_app

get_config.cache_clear()
app = create_app()

# Load RBAC test config from JSON (QONOS_TEST_CONFIG_FILE or api/tests/test_config.json)
_config_path = Path(
    os.getenv("QONOS_TEST_CONFIG_FILE", str(Path(__file__).resolve().parent / "test_config.json"))
)
try:
    _config = json.loads(_config_path.read_text()) if _config_path.exists() else None
except (FileNotFoundError, json.JSONDecodeError, OSError):
    _config = None
_required = [
    "admin_token",
    "project1_member_token",
    "project2_member_token",
    "project1_manager_token",
    "service_token",
]
if not _config or not all(_config.get(k) for k in _required):
    _missing = _required if not _config else [k for k in _required if not _config.get(k)]
    pytest.skip(
        f"Test config {_config_path}: missing keys {', '.join(_missing)}. Required: {', '.join(_required)}.",
        allow_module_level=True,
    )

ADMIN_TOKEN = _config["admin_token"]
PROJECT1_MEMBER = _config["project1_member_token"]
PROJECT2_MEMBER = _config["project2_member_token"]
PROJECT1_MANAGER = _config["project1_manager_token"]
SERVICE_TOKEN = _config["service_token"]
PROJECT1_ID = _config.get("project1_project_id")


def _auth(headers: Dict[str, str] | None = None) -> Dict[str, str]:
    base = {}
    if headers:
        base.update(headers)
    return base


@pytest.fixture
async def async_client():
    from api.app.db import close_db_engine

    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await close_db_engine()


@pytest.mark.anyio
async def test_missing_token_returns_401(async_client: httpx.AsyncClient) -> None:
    resp = await async_client.get("/v2/schedules")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_invalid_token_returns_401(async_client: httpx.AsyncClient) -> None:
    resp = await async_client.get("/v2/schedules", headers=_auth({"X-Auth-Token": "totally-wrong"}))
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_schedule_project_scoping_and_cross_tenant_forbidden(
    async_client: httpx.AsyncClient,
) -> None:
    body = {
        "name": f"daily-backup-{uuid4()}",
        "description": "Proj1 backup",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": "srv-1"},
        "enabled": True,
    }
    create = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert create.status_code == 201, create.text
    schedule = create.json()
    schedule_id = schedule["id"]
    if PROJECT1_ID:
        assert schedule["project_id"] == PROJECT1_ID
    else:
        assert schedule["project_id"]

    show_same = await async_client.get(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert show_same.status_code == 200

    show_other = await async_client.get(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert show_other.status_code == 403

    show_admin = await async_client.get(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": ADMIN_TOKEN}),
    )
    assert show_admin.status_code == 200

    list_proj1 = await async_client.get(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert list_proj1.status_code == 200
    assert any(s["id"] == schedule_id for s in list_proj1.json()["schedules"])

    list_proj2 = await async_client.get(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert list_proj2.status_code == 200
    assert all(s["id"] != schedule_id for s in list_proj2.json()["schedules"])

    update_other = await async_client.put(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
        json={"description": "should-fail"},
    )
    assert update_other.status_code == 403

    delete_other = await async_client.delete(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert delete_other.status_code == 403

    delete_same_member = await async_client.delete(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert delete_same_member.status_code == 204

    body2 = {
        "name": f"daily-backup-manager-{uuid4()}",
        "description": "Proj1 backup (manager delete)",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": "srv-1"},
        "enabled": True,
    }
    create2 = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body2,
    )
    assert create2.status_code == 201, create2.text
    schedule2_id = create2.json()["id"]

    delete_same_manager = await async_client.delete(
        f"/v2/schedules/{schedule2_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MANAGER}),
    )
    assert delete_same_manager.status_code == 204


@pytest.mark.anyio
async def test_job_creation_and_cross_tenant_access(async_client: httpx.AsyncClient) -> None:
    body = {
        "name": f"job-source-schedule-{uuid4()}",
        "description": "Proj1 job schedule",
        "action_type": "server_snapshot",
        "cron_expression": "0 3 * * *",
        "action_parameters": {"server_id": "srv-2"},
        "enabled": True,
    }
    schedule_resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert schedule_resp.status_code == 201, schedule_resp.text
    schedule_id = schedule_resp.json()["id"]

    job_create = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule_id},
    )
    assert job_create.status_code == 201, job_create.text
    job = job_create.json()
    job_id = job["id"]
    if PROJECT1_ID:
        assert job["project_id"] == PROJECT1_ID
    else:
        assert job["project_id"]

    show_same = await async_client.get(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert show_same.status_code == 200

    show_other = await async_client.get(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert show_other.status_code == 403

    show_admin = await async_client.get(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": ADMIN_TOKEN}),
    )
    assert show_admin.status_code == 200

    list_proj1 = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert list_proj1.status_code == 200
    assert any(j["id"] == job_id for j in list_proj1.json()["jobs"])

    list_proj2 = await async_client.get(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert list_proj2.status_code == 200
    assert all(j["id"] != job_id for j in list_proj2.json()["jobs"])

    update_ok = await async_client.patch(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"status": "PROCESSING"},
    )
    assert update_ok.status_code == 200, update_ok.text
    assert update_ok.json()["status"] == "PROCESSING"

    update_other = await async_client.patch(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
        json={"status": "DONE"},
    )
    assert update_other.status_code == 403

    # Cross-tenant cancel forbidden
    cancel_other = await async_client.delete(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert cancel_other.status_code == 403

    cancel_same_member = await async_client.delete(
        f"/v2/jobs/{job_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert cancel_same_member.status_code == 403

    # Service account cancel allowed
    job_create2 = await async_client.post(
        "/v2/jobs",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
        json={"schedule_id": schedule_id},
    )
    assert job_create2.status_code == 201, job_create2.text
    job2 = job_create2.json()
    job2_id = job2["id"]

    cancel_service = await async_client.delete(
        f"/v2/jobs/{job2_id}",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
    )
    assert cancel_service.status_code == 204

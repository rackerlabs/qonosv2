from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from api.tests.test_auth_rbac import (
    PROJECT1_MEMBER,
    PROJECT2_MEMBER,
    SERVICE_TOKEN,
    _auth,
    async_client,
)


@pytest.mark.anyio
async def test_create_schedule_invalid_cron_expression_returns_400(
    async_client: httpx.AsyncClient,
) -> None:
    body = {
        "name": f"invalid-cron-{uuid4()}",
        "description": "Invalid cron test",
        "action_type": "server_snapshot",
        "cron_expression": "invalid cron",
        "action_parameters": {"server_id": "srv-1"},
        "enabled": True,
    }
    resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert resp.status_code == 400
    assert "cron_expression" in resp.text


@pytest.mark.anyio
async def test_create_schedule_missing_required_action_parameter_returns_400(
    async_client: httpx.AsyncClient,
) -> None:
    # For action_type=server_snapshot, server_id is required by action_parameters_schema.
    body = {
        "name": f"missing-param-{uuid4()}",
        "description": "Missing server_id",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {},
        "enabled": True,
    }
    resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert resp.status_code == 400
    assert "Missing required action_parameters fields" in resp.text


@pytest.mark.anyio
async def test_create_schedule_invalid_action_parameter_type_returns_400(
    async_client: httpx.AsyncClient,
) -> None:
    # For action_type=server_snapshot, server_id must be a string.
    body = {
        "name": f"bad-param-type-{uuid4()}",
        "description": "server_id wrong type",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": 123},
        "enabled": True,
    }
    resp = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert resp.status_code == 400
    assert "action_parameters.server_id must be a string" in resp.text


@pytest.mark.anyio
async def test_schedule_list_filters_and_pagination(async_client: httpx.AsyncClient) -> None:
    # Create two schedules in project1 with different action_type/enabled.
    body1 = {
        "name": f"list-test-a-{uuid4()}",
        "description": "List filter test A",
        "action_type": "server_snapshot",
        "cron_expression": "0 1 * * *",
        "action_parameters": {"server_id": "srv-a"},
        "enabled": True,
    }
    body2 = {
        "name": f"list-test-b-{uuid4()}",
        "description": "List filter test B",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": "srv-b"},
        "enabled": False,
    }

    create1 = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body1,
    )
    assert create1.status_code == 201, create1.text
    s1 = create1.json()

    create2 = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body2,
    )
    assert create2.status_code == 201, create2.text
    s2 = create2.json()

    # Filter by enabled=True should only return the first schedule.
    list_enabled = await async_client.get(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"enabled": True},
    )
    assert list_enabled.status_code == 200
    ids_enabled = {s["id"] for s in list_enabled.json()["schedules"]}
    assert s1["id"] in ids_enabled
    assert s2["id"] not in ids_enabled

    page1 = await async_client.get(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"limit": 1},
    )
    assert page1.status_code == 200
    page1_items = page1.json()["schedules"]
    assert len(page1_items) == 1
    first_id = page1_items[0]["id"]

    page2 = await async_client.get(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        params={"marker": first_id, "limit": 10},
    )
    assert page2.status_code == 200
    ids_page2 = {s["id"] for s in page2.json()["schedules"]}
    assert first_id not in ids_page2


@pytest.mark.anyio
async def test_update_schedule_readonly_fields_rejected(async_client: httpx.AsyncClient) -> None:
    # Create a schedule in project1.
    body = {
        "name": f"readonly-test-{uuid4()}",
        "description": "Read-only test",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": "srv-ro"},
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

    # Attempt to change read-only fields should be rejected with 400.
    resp = await async_client.put(
        f"/v2/schedules/{schedule_id}",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json={
            "name": "new-name",
            "project_id": "different-project",
        },
    )
    assert resp.status_code == 400
    assert "Read-only fields may not be updated" in resp.text


@pytest.mark.anyio
async def test_enable_disable_schedule_and_authz(async_client: httpx.AsyncClient) -> None:
    # Create a disabled schedule in project1.
    body = {
        "name": f"enable-disable-{uuid4()}",
        "description": "Enable/disable test",
        "action_type": "server_snapshot",
        "cron_expression": "0 2 * * *",
        "action_parameters": {"server_id": "srv-1"},
        "enabled": False,
    }
    create = await async_client.post(
        "/v2/schedules",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
        json=body,
    )
    assert create.status_code == 201, create.text
    schedule = create.json()
    schedule_id = schedule["id"]

    # Cross-tenant member cannot enable the schedule.
    enable_other = await async_client.patch(
        f"/v2/schedules/{schedule_id}/enable",
        headers=_auth({"X-Auth-Token": PROJECT2_MEMBER}),
    )
    assert enable_other.status_code == 403

    # Service account should not have schedule:update; expect 403.
    enable_service = await async_client.patch(
        f"/v2/schedules/{schedule_id}/enable",
        headers=_auth({"X-Auth-Token": SERVICE_TOKEN}),
    )
    assert enable_service.status_code == 403

    # Member in the same project can enable.
    enable_same = await async_client.patch(
        f"/v2/schedules/{schedule_id}/enable",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert enable_same.status_code == 200, enable_same.text
    assert enable_same.json()["enabled"] is True

    # And then disable again.
    disable_same = await async_client.patch(
        f"/v2/schedules/{schedule_id}/disable",
        headers=_auth({"X-Auth-Token": PROJECT1_MEMBER}),
    )
    assert disable_same.status_code == 200, disable_same.text
    assert disable_same.json()["enabled"] is False


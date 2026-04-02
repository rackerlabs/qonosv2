from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.app.worker.server_snapshot_handler import (
    ServerSnapshotActionHandler,
)


class _FakeCompute:
    def __init__(self, *, server_exists: bool = True, create_error: Exception | None = None) -> None:
        self._server_exists = server_exists
        self._create_error = create_error
        self.created_name = None
        self.created_metadata = None

    def get_server(self, server_id: str):
        if not self._server_exists:
            return None
        return SimpleNamespace(id=server_id, name="srv-a")

    def create_server_image(self, *, server, name: str, metadata: dict[str, str]):
        if self._create_error is not None:
            raise self._create_error
        self.created_name = name
        self.created_metadata = metadata
        return "img-123"


class _FakeImageAPI:
    def __init__(self, statuses: list[str]) -> None:
        self._statuses = list(statuses)
        self._idx = 0

    def get_image(self, image_id: str):
        status = self._statuses[min(self._idx, len(self._statuses) - 1)]
        self._idx += 1
        return SimpleNamespace(id=image_id, status=status)


class _FakeConn:
    def __init__(self, compute: _FakeCompute, image_api: _FakeImageAPI) -> None:
        self.compute = compute
        self.image = image_api


def _job(server_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=str(uuid4()),
        schedule_id=str(uuid4()),
        project_id=str(uuid4()),
        action_type="server_snapshot",
        action_parameters={"server_id": server_id},
    )


def _cfg(**kwargs):
    defaults = dict(
        keystone_url="http://keystone:5000/v3",
        keystone_admin_token="svc-token",
        keystone_verify_ssl=False,
        server_snapshot_default_image_name_prefix="qonos-snapshot",
        server_snapshot_poll_interval_seconds=0.01,
        server_snapshot_poll_timeout_seconds=0.2,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


@pytest.mark.anyio
async def test_server_snapshot_success(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    compute = _FakeCompute(server_exists=True)
    conn = _FakeConn(compute, _FakeImageAPI(["QUEUED", "ACTIVE"]))
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(handler, "_build_connection", lambda project_id: conn)

    result = await handler.execute(_job(str(uuid4())))

    assert result["image_id"] == "img-123"
    assert result["status"] == "ACTIVE"
    assert compute.created_name.startswith("qonos-snapshot-srv-a-")
    assert compute.created_metadata["created_by"] == "qonosv2"
    assert compute.created_metadata["qonos_managed"] == "true"


@pytest.mark.anyio
async def test_server_snapshot_server_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(_FakeCompute(server_exists=False), _FakeImageAPI(["QUEUED"]))
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(handler, "_build_connection", lambda project_id: conn)

    with pytest.raises(ValueError, match="Server not found"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_server_snapshot_creation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(
        _FakeCompute(server_exists=True, create_error=RuntimeError("boom")),
        _FakeImageAPI(["QUEUED"]),
    )
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(handler, "_build_connection", lambda project_id: conn)

    with pytest.raises(RuntimeError, match="Failed to create server snapshot"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_server_snapshot_polling_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(_FakeCompute(server_exists=True), _FakeImageAPI(["QUEUED"]))
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.get_config",
        lambda: _cfg(server_snapshot_poll_interval_seconds=0.01, server_snapshot_poll_timeout_seconds=0.05),
    )
    monkeypatch.setattr(handler, "_build_connection", lambda project_id: conn)

    with pytest.raises(TimeoutError, match="Snapshot polling timed out"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_server_snapshot_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())

    def _raise_auth(project_id: str):
        raise RuntimeError("auth failed")

    monkeypatch.setattr(handler, "_build_connection", _raise_auth)

    with pytest.raises(RuntimeError, match="auth failed"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
@pytest.mark.parametrize("failed_status", ["KILLED", "DELETED", "PENDING_DELETE", "ERROR"])
async def test_server_snapshot_failed_image_statuses(
    monkeypatch: pytest.MonkeyPatch, failed_status: str
) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(_FakeCompute(server_exists=True), _FakeImageAPI([failed_status]))
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(handler, "_build_connection", lambda project_id: conn)

    with pytest.raises(RuntimeError, match=failed_status):
        await handler.execute(_job(str(uuid4())))

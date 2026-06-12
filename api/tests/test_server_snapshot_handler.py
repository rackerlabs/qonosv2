from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.app.worker.server_snapshot_handler import (
    ServerSnapshotActionHandler,
)


class _FakeCompute:
    def __init__(
        self, *, server_exists: bool = True, create_error: Exception | None = None
    ) -> None:
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
        self.deleted: list[str] = []

    def get_image(self, image_id: str):
        status = self._statuses[min(self._idx, len(self._statuses) - 1)]
        self._idx += 1
        return SimpleNamespace(id=image_id, status=status)

    def delete_image(self, image_id: str, ignore_missing: bool = True):
        self.deleted.append(image_id)

    def images(self, owner: str | None = None):
        return []


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
        action_parameters={"server_id": server_id, "qonos_trust_id": str(uuid4())},
    )


def _cfg(**kwargs):
    defaults = dict(
        keystone_url="http://keystone:5000/v3",
        keystone_username="qonos",
        keystone_password="qonos-pass",
        keystone_user_domain_name="Default",
        keystone_verify_ssl=False,
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
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    job = _job(str(uuid4()))

    result = await handler.execute(job)

    assert result["image_id"] == "img-123"
    assert result["status"] == "ACTIVE"
    assert compute.created_name.startswith("qonos-server_snapshot-srv-a-")
    assert compute.created_metadata["qonos_managed"] == "true"
    assert compute.created_metadata["qonos_job_id"] == job.id
    assert compute.created_metadata["qonos_schedule_id"] == job.schedule_id


@pytest.mark.anyio
async def test_server_snapshot_server_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(_FakeCompute(server_exists=False), _FakeImageAPI(["QUEUED"]))
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

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
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(RuntimeError, match="Failed to create server snapshot"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_server_snapshot_polling_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    conn = _FakeConn(_FakeCompute(server_exists=True), _FakeImageAPI(["QUEUED"]))
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.get_config",
        lambda: _cfg(
            server_snapshot_poll_interval_seconds=0.01, server_snapshot_poll_timeout_seconds=0.05
        ),
    )
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(TimeoutError, match="Snapshot polling timed out"):
        await handler.execute(_job(str(uuid4())))
    assert conn.image.deleted == ["img-123"]


@pytest.mark.anyio
async def test_server_snapshot_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ServerSnapshotActionHandler()
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())

    def _raise_auth(project_id: str, trust_id: str | None = None):
        raise RuntimeError("auth failed")

    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        _raise_auth,
    )

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
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(RuntimeError, match=failed_status):
        await handler.execute(_job(str(uuid4())))
    assert conn.image.deleted == ["img-123"]


@pytest.mark.anyio
async def test_server_snapshot_reuses_existing_image_for_same_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ReuseImageAPI(_FakeImageAPI):
        def images(self, owner: str | None = None):
            return [
                SimpleNamespace(
                    id="img-existing",
                    name="qonos-snapshot-srv-a-existing",
                    status="QUEUED",
                    properties={"qonos_job_id": self._job_id},
                )
            ]

    handler = ServerSnapshotActionHandler()
    compute = _FakeCompute(server_exists=True)
    image_api = _ReuseImageAPI(["ACTIVE"])
    conn = _FakeConn(compute, image_api)
    job = _job(str(uuid4()))
    image_api._job_id = job.id

    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    result = await handler.execute(job)

    assert result["image_id"] == "img-existing"
    assert result["status"] == "ACTIVE"
    assert compute.created_name is None


@pytest.mark.anyio
async def test_server_snapshot_requires_delegated_trust_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = ServerSnapshotActionHandler()
    compute = _FakeCompute(server_exists=True)
    conn = _FakeConn(compute, _FakeImageAPI(["QUEUED", "ACTIVE"]))
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.server_snapshot_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    job = _job(str(uuid4()))
    job.action_parameters.pop("qonos_trust_id", None)

    with pytest.raises(RuntimeError, match="Missing delegated trust context"):
        await handler.execute(job)

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.app.worker.volume_backup_full_handler import VolumeBackupFullActionHandler


class _FakeBlockStorage:
    def __init__(
        self,
        *,
        volume_exists: bool = True,
        volume_status: str = "available",
        create_error: Exception | None = None,
        backup_statuses: list[str] | None = None,
    ) -> None:
        self._volume_exists = volume_exists
        self._volume_status = volume_status
        self._create_error = create_error
        self._backup_statuses = list(backup_statuses or ["creating", "available"])
        self._poll_idx = 0
        self.created_attrs: dict | None = None
        self.deleted: list[str] = []
        self._backups: dict[str, SimpleNamespace] = {}

    def get_volume(self, volume_id: str):
        if not self._volume_exists:
            return None
        return SimpleNamespace(id=volume_id, name="vol-a", status=self._volume_status)

    def create_backup(self, **attrs):
        if self._create_error is not None:
            raise self._create_error
        self.created_attrs = dict(attrs)
        backup_id = "bkp-123"
        self._backups[backup_id] = SimpleNamespace(
            id=backup_id,
            status=self._backup_statuses[0],
            name=attrs.get("name"),
            metadata=attrs.get("metadata") or {},
            is_incremental=False,
        )
        return self._backups[backup_id]

    def get_backup(self, backup_id: str):
        backup = self._backups.get(backup_id)
        if backup is None:
            return None
        status = self._backup_statuses[min(self._poll_idx, len(self._backup_statuses) - 1)]
        self._poll_idx += 1
        backup.status = status
        return backup

    def delete_backup(self, backup_id: str, ignore_missing: bool = True):
        self.deleted.append(backup_id)
        self._backups.pop(backup_id, None)

    def backups(self, details: bool = True):
        return list(self._backups.values())


class _FakeConn:
    def __init__(self, block_storage: _FakeBlockStorage) -> None:
        self.block_storage = block_storage


def _job(volume_id: str, *, volume_status: str = "available") -> SimpleNamespace:
    return SimpleNamespace(
        id=str(uuid4()),
        schedule_id=str(uuid4()),
        project_id=str(uuid4()),
        action_type="volume_backup_full",
        action_parameters={
            "volume_id": volume_id,
            "qonos_trust_id": str(uuid4()),
        },
    )


def _cfg(**kwargs):
    defaults = dict(
        volume_backup_full_poll_interval_seconds=0.01,
        volume_backup_full_poll_timeout_seconds=0.2,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


@pytest.mark.anyio
async def test_volume_backup_full_success(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = VolumeBackupFullActionHandler()
    block = _FakeBlockStorage(volume_exists=True, backup_statuses=["creating", "available"])
    conn = _FakeConn(block)
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.get_config", lambda: _cfg()
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    job = _job(str(uuid4()))

    result = await handler.execute(job)

    assert result["backup_id"] == "bkp-123"
    assert result["status"] == "available"
    assert block.created_attrs is not None
    assert block.created_attrs["force"] is False
    assert block.created_attrs["is_incremental"] is False
    assert block.created_attrs["metadata"]["qonos_managed"] == "true"
    assert block.created_attrs["metadata"]["qonos_job_id"] == job.id
    assert block.created_attrs["name"].startswith("qonos-volume_backup_full-vol-a-")


@pytest.mark.anyio
async def test_volume_backup_full_in_use_uses_force(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = VolumeBackupFullActionHandler()
    block = _FakeBlockStorage(volume_status="in-use", backup_statuses=["available"])
    conn = _FakeConn(block)
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.get_config", lambda: _cfg()
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    await handler.execute(_job(str(uuid4())))

    assert block.created_attrs is not None
    assert block.created_attrs["force"] is True


@pytest.mark.anyio
async def test_volume_backup_full_volume_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = VolumeBackupFullActionHandler()
    conn = _FakeConn(_FakeBlockStorage(volume_exists=False))
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.get_config", lambda: _cfg()
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(ValueError, match="Volume not found"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_volume_backup_full_creation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = VolumeBackupFullActionHandler()
    conn = _FakeConn(
        _FakeBlockStorage(volume_exists=True, create_error=RuntimeError("boom"))
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.get_config", lambda: _cfg()
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(RuntimeError, match="Failed to create volume backup"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_volume_backup_full_polling_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = VolumeBackupFullActionHandler()
    conn = _FakeConn(_FakeBlockStorage(backup_statuses=["creating"]))
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.get_config",
        lambda: _cfg(
            volume_backup_full_poll_interval_seconds=0.01,
            volume_backup_full_poll_timeout_seconds=0.05,
        ),
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_full_handler.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(TimeoutError, match="Volume backup polling timed out"):
        await handler.execute(_job(str(uuid4())))
    assert conn.block_storage.deleted == ["bkp-123"]

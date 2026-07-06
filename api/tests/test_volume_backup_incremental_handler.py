from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.app.worker.volume_backup_incremental_handler import (
    VolumeBackupIncrementalActionHandler,
)


class _FakeBlockStorage:
    def __init__(
        self,
        *,
        volume_exists: bool = True,
        volume_status: str = "available",
        create_error: Exception | None = None,
        backup_statuses: list[str] | None = None,
        existing_backups: list[SimpleNamespace] | None = None,
    ) -> None:
        self._volume_exists = volume_exists
        self._volume_status = volume_status
        self._create_error = create_error
        self._backup_statuses = list(backup_statuses or ["creating", "available"])
        self._poll_idx = 0
        self.created_attrs: dict | None = None
        self.deleted: list[str] = []
        self._backups: dict[str, SimpleNamespace] = {}
        for backup in existing_backups or []:
            self._backups[str(backup.id)] = backup

    def get_volume(self, volume_id: str):
        if not self._volume_exists:
            return None
        return SimpleNamespace(id=volume_id, name="vol-a", status=self._volume_status)

    def create_backup(self, **attrs):
        if self._create_error is not None:
            raise self._create_error
        self.created_attrs = dict(attrs)
        backup_id = "bkp-inc-123"
        self._backups[backup_id] = SimpleNamespace(
            id=backup_id,
            status=self._backup_statuses[0],
            name=attrs.get("name"),
            metadata=attrs.get("metadata") or {},
            is_incremental=True,
            parent_id=attrs.get("metadata", {}).get("qonos_parent_full_backup_id"),
            created_at=datetime.now(timezone.utc),
        )
        return self._backups[backup_id]

    def get_backup(self, backup_id: str):
        backup = self._backups.get(backup_id)
        if backup is None:
            return None
        status = self._backup_statuses[min(self._poll_idx, len(self._backup_statuses) - 1)]
        backup.status = status
        self._poll_idx += 1
        return backup

    def delete_backup(self, backup_id: str, ignore_missing: bool = True):
        self.deleted.append(backup_id)
        self._backups.pop(backup_id, None)

    def backups(self, details: bool = True, volume_id: str | None = None, **kwargs):
        for backup in self._backups.values():
            status = self._backup_statuses[min(self._poll_idx, len(self._backup_statuses) - 1)]
            backup.status = status
        self._poll_idx += 1
        return list(self._backups.values())


class _FakeConn:
    def __init__(self, block_storage: _FakeBlockStorage) -> None:
        self.block_storage = block_storage


def _full_backup(
    *,
    backup_id: str,
    created_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=backup_id,
        status="available",
        name=f"full-{backup_id}",
        metadata={"qonos_managed": "true", "qonos_action_type": "volume_backup_full"},
        is_incremental=False,
        parent_id=None,
        created_at=created_at or datetime.now(timezone.utc),
    )


def _job(volume_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=str(uuid4()),
        schedule_id=str(uuid4()),
        project_id=str(uuid4()),
        action_type="volume_backup_incremental",
        action_parameters={
            "volume_id": volume_id,
            "qonos_trust_id": str(uuid4()),
        },
    )


def _cfg(**kwargs):
    defaults = dict(
        volume_backup_poll_interval_seconds=0.01,
        volume_backup_poll_timeout_seconds=0.2,
        volume_backup_coordination_wait_timeout_seconds=0.2,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


@pytest.mark.anyio
async def test_volume_backup_incremental_success_with_full_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    now = datetime.now(timezone.utc)
    block = _FakeBlockStorage(
        backup_statuses=["creating", "available"],
        existing_backups=[
            _full_backup(backup_id="full-old", created_at=now - timedelta(days=2)),
            _full_backup(backup_id="full-base", created_at=now - timedelta(days=1)),
        ],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr("api.app.worker.volume_backup_common.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    job = _job(str(uuid4()))

    result = await handler.execute(job)

    assert result["backup_id"] == "bkp-inc-123"
    assert result["status"] == "available"
    assert result["parent_full_backup_id"] == "full-base"
    assert block.created_attrs is not None
    assert block.created_attrs["is_incremental"] is True
    assert block.created_attrs["metadata"]["qonos_job_id"] == job.id
    assert block.created_attrs["metadata"]["qonos_parent_full_backup_id"] == "full-base"


@pytest.mark.anyio
async def test_volume_backup_incremental_fails_without_full_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    block = _FakeBlockStorage(existing_backups=[])
    conn = _FakeConn(block)
    monkeypatch.setattr("api.app.worker.volume_backup_common.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(ValueError, match="No successful full backup found"):
        await handler.execute(_job(str(uuid4())))

    assert block.created_attrs is None


@pytest.mark.anyio
async def test_volume_backup_incremental_reuses_existing_job_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    job = _job(str(uuid4()))
    block = _FakeBlockStorage(
        backup_statuses=["available"],
        existing_backups=[
            _full_backup(backup_id="full-base"),
            SimpleNamespace(
                id="bkp-existing",
                status="available",
                name="existing-inc",
                metadata={
                    "qonos_job_id": job.id,
                    "qonos_parent_full_backup_id": "full-base",
                },
                is_incremental=True,
                parent_id="full-base",
                created_at=datetime.now(timezone.utc),
            ),
        ],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr("api.app.worker.volume_backup_common.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    result = await handler.execute(job)

    assert result["backup_id"] == "bkp-existing"
    assert result["parent_full_backup_id"] == "full-base"
    assert block.created_attrs is None


@pytest.mark.anyio
async def test_volume_backup_incremental_reuses_existing_when_volume_is_backing_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    job = _job(str(uuid4()))
    block = _FakeBlockStorage(
        volume_status="backing-up",
        backup_statuses=["creating", "available"],
        existing_backups=[
            _full_backup(backup_id="full-base"),
            SimpleNamespace(
                id="bkp-inflight",
                status="creating",
                name="inflight-inc",
                metadata={
                    "qonos_job_id": job.id,
                    "qonos_parent_full_backup_id": "full-base",
                },
                is_incremental=True,
                parent_id="full-base",
                created_at=datetime.now(timezone.utc),
            ),
        ],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr("api.app.worker.volume_backup_common.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    result = await handler.execute(job)

    assert result["backup_id"] == "bkp-inflight"
    assert result["status"] == "available"
    assert block.created_attrs is None


@pytest.mark.anyio
async def test_volume_backup_incremental_ignores_incremental_when_finding_full_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    now = datetime.now(timezone.utc)
    block = _FakeBlockStorage(
        backup_statuses=["available"],
        existing_backups=[
            SimpleNamespace(
                id="inc-only",
                status="available",
                name="inc",
                metadata={},
                is_incremental=True,
                parent_id="missing-full",
                created_at=now,
            ),
        ],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr("api.app.worker.volume_backup_common.get_config", lambda: _cfg())
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(ValueError, match="No successful full backup found"):
        await handler.execute(_job(str(uuid4())))


@pytest.mark.anyio
async def test_volume_backup_incremental_waits_for_full_backup_to_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    now = datetime.now(timezone.utc)
    block = _FakeBlockStorage(
        backup_statuses=["creating", "available", "available"],
        existing_backups=[_full_backup(backup_id="full-base", created_at=now)],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.get_config",
        lambda: _cfg(volume_backup_coordination_wait_timeout_seconds=0.3),
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    result = await handler.execute(_job(str(uuid4())))

    assert result["status"] == "available"
    assert result["parent_full_backup_id"] == "full-base"
    assert block.created_attrs is not None


@pytest.mark.anyio
async def test_volume_backup_incremental_times_out_waiting_for_full_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = VolumeBackupIncrementalActionHandler()
    now = datetime.now(timezone.utc)
    block = _FakeBlockStorage(
        backup_statuses=["creating"],
        existing_backups=[_full_backup(backup_id="full-base", created_at=now)],
    )
    conn = _FakeConn(block)
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.get_config",
        lambda: _cfg(
            volume_backup_poll_interval_seconds=0.01,
            volume_backup_coordination_wait_timeout_seconds=0.03,
        ),
    )
    monkeypatch.setattr(
        "api.app.worker.volume_backup_common.build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )

    with pytest.raises(RuntimeError, match="Full backup currently in progress"):
        await handler.execute(_job(str(uuid4())))

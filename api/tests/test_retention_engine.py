from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from api.app.utils.time import utc_now
from api.app.worker import retention


class _FakeImageAPI:
    def __init__(self, images: list[SimpleNamespace]) -> None:
        self._images = list(images)
        self.deleted: list[str] = []

    def images(self, owner: str | None = None):
        return list(self._images)

    def delete_image(self, image_id: str, ignore_missing: bool = True):
        self.deleted.append(image_id)


class _FakeBackupAPI:
    def __init__(self, backups: list[SimpleNamespace]) -> None:
        self._backups = list(backups)
        self.deleted: list[str] = []

    def backups(self, details: bool = True, volume_id: str | None = None, **kwargs):
        return list(self._backups)

    def delete_backup(self, backup_id: str, ignore_missing: bool = True):
        self.deleted.append(backup_id)


class _FakeConn:
    def __init__(
        self,
        image_api: _FakeImageAPI | None = None,
        backup_api: _FakeBackupAPI | None = None,
    ) -> None:
        self.image = image_api
        self.block_storage = backup_api


def _img(
    *,
    image_id: str,
    created_at,
    status: str = "ACTIVE",
    schedule_id: str = "sched-1",
    action_type: str = "server_snapshot",
    managed: str = "true",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=image_id,
        created_at=created_at,
        status=status,
        properties={
            "qonos_managed": managed,
            "qonos_action_type": action_type,
            "qonos_schedule_id": schedule_id,
        },
    )


@pytest.mark.anyio
async def test_retention_count_based(monkeypatch: pytest.MonkeyPatch) -> None:
    now = utc_now()
    imgs = [
        _img(image_id="i1", created_at=now - timedelta(days=10)),
        _img(image_id="i2", created_at=now - timedelta(days=9)),
        _img(image_id="i3", created_at=now - timedelta(days=8)),
    ]
    image_api = _FakeImageAPI(imgs)
    monkeypatch.setattr(
        retention, "_build_connection", lambda project_id, trust_id=None: _FakeConn(image_api)
    )

    await retention.run_retention_for_job(
        action_type="server_snapshot",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 2},
        trigger_status="DONE",
    )

    assert image_api.deleted == ["i1"]


@pytest.mark.anyio
async def test_retention_age_based(monkeypatch: pytest.MonkeyPatch) -> None:
    now = utc_now()
    imgs = [
        _img(image_id="old", created_at=now - timedelta(days=20)),
        _img(image_id="new", created_at=now - timedelta(days=2)),
    ]
    image_api = _FakeImageAPI(imgs)
    monkeypatch.setattr(
        retention, "_build_connection", lambda project_id, trust_id=None: _FakeConn(image_api)
    )

    await retention.run_retention_for_job(
        action_type="server_snapshot",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "age", "max_age_days": 7},
        trigger_status="DONE",
    )

    assert image_api.deleted == ["old"]


@pytest.mark.anyio
async def test_retention_zero_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    now = utc_now()
    image_api = _FakeImageAPI([_img(image_id="i1", created_at=now - timedelta(days=10))])
    monkeypatch.setattr(
        retention, "_build_connection", lambda project_id, trust_id=None: _FakeConn(image_api)
    )

    await retention.run_retention_for_job(
        action_type="server_snapshot",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 0},
        trigger_status="DONE",
    )

    assert image_api.deleted == []


@pytest.mark.anyio
async def test_retention_mixed_artifact_states(monkeypatch: pytest.MonkeyPatch) -> None:
    now = utc_now()
    imgs = [
        _img(image_id="del", created_at=now - timedelta(days=30), status="DELETED"),
        _img(image_id="pending", created_at=now - timedelta(days=25), status="PENDING_DELETE"),
        _img(image_id="other-sched", created_at=now - timedelta(days=20), schedule_id="sched-2"),
        _img(
            image_id="other-action",
            created_at=now - timedelta(days=18),
            action_type="volume_backup",
        ),
        _img(image_id="keep", created_at=now - timedelta(days=2)),
        _img(image_id="drop", created_at=now - timedelta(days=15)),
    ]
    image_api = _FakeImageAPI(imgs)
    monkeypatch.setattr(
        retention, "_build_connection", lambda project_id, trust_id=None: _FakeConn(image_api)
    )

    await retention.run_retention_for_job(
        action_type="server_snapshot",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 1},
        trigger_status="MAX_RETRIED",
    )

    assert image_api.deleted == ["drop"]


def _bkp(
    *,
    backup_id: str,
    created_at,
    status: str = "available",
    schedule_id: str = "sched-1",
    action_type: str = "volume_backup_full",
    managed: str = "true",
    incremental: bool = False,
    project_id: str = "proj-1",
    parent_id: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=backup_id,
        created_at=created_at,
        status=status,
        project_id=project_id,
        is_incremental=incremental,
        parent_id=parent_id,
        metadata={
            "qonos_managed": managed,
            "qonos_action_type": action_type,
            "qonos_schedule_id": schedule_id,
        },
    )


@pytest.mark.anyio
async def test_volume_backup_full_retention_count_based(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = utc_now()
    backups = [
        _bkp(backup_id="b1", created_at=now - timedelta(days=10)),
        _bkp(backup_id="b2", created_at=now - timedelta(days=9)),
        _bkp(backup_id="b3", created_at=now - timedelta(days=8)),
    ]
    backup_api = _FakeBackupAPI(backups)
    monkeypatch.setattr(
        retention,
        "_build_connection",
        lambda project_id, trust_id=None: _FakeConn(backup_api=backup_api),
    )

    await retention.run_retention_for_job(
        action_type="volume_backup_full",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 2},
        trigger_status="DONE",
        volume_id="vol-1",
    )

    assert backup_api.deleted == ["b1"]


@pytest.mark.anyio
async def test_volume_backup_incremental_retention_count_based(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = utc_now()
    backups = [
        _bkp(
            backup_id="inc1",
            created_at=now - timedelta(days=10),
            action_type="volume_backup_incremental",
            incremental=True,
        ),
        _bkp(
            backup_id="inc2",
            created_at=now - timedelta(days=9),
            action_type="volume_backup_incremental",
            incremental=True,
        ),
        _bkp(
            backup_id="inc3",
            created_at=now - timedelta(days=8),
            action_type="volume_backup_incremental",
            incremental=True,
        ),
        _bkp(backup_id="full1", created_at=now - timedelta(days=7)),
    ]
    backup_api = _FakeBackupAPI(backups)
    monkeypatch.setattr(
        retention,
        "_build_connection",
        lambda project_id, trust_id=None: _FakeConn(backup_api=backup_api),
    )

    await retention.run_retention_for_job(
        action_type="volume_backup_incremental",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 2},
        trigger_status="DONE",
        volume_id="vol-1",
    )

    assert backup_api.deleted == ["inc1"]


@pytest.mark.anyio
async def test_volume_backup_full_retention_cascade_deletes_dependent_incrementals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = utc_now()
    backups = [
        _bkp(backup_id="full-old", created_at=now - timedelta(days=10)),
        _bkp(backup_id="full-keep", created_at=now - timedelta(days=5)),
        _bkp(
            backup_id="inc-child",
            created_at=now - timedelta(days=9),
            action_type="volume_backup_incremental",
            schedule_id="sched-inc",
            incremental=True,
            parent_id="full-old",
        ),
        _bkp(
            backup_id="inc-grandchild",
            created_at=now - timedelta(days=8),
            action_type="volume_backup_incremental",
            schedule_id="sched-inc",
            incremental=True,
            parent_id="inc-child",
        ),
    ]
    backup_api = _FakeBackupAPI(backups)
    monkeypatch.setattr(
        retention,
        "_build_connection",
        lambda project_id, trust_id=None: _FakeConn(backup_api=backup_api),
    )

    await retention.run_retention_for_job(
        action_type="volume_backup_full",
        project_id="proj-1",
        schedule_id="sched-1",
        retention_policy_raw={"type": "count", "value": 1},
        trigger_status="DONE",
        volume_id="vol-1",
    )

    assert backup_api.deleted == ["inc-grandchild", "inc-child", "full-old"]

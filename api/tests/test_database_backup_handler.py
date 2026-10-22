from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from api.app.worker import database_backup_handler as db_handler_mod
from api.app.worker.database_backup_handler import DatabaseBackupActionHandler
from api.app.worker.database_backup_common import (
    backup_failure_detail,
    parse_qonos_description,
    qonos_description_metadata,
)


class _FakeDatabase:
    def __init__(
        self,
        *,
        instance_exists: bool = True,
        instance_status: str = "ACTIVE",
        backup_statuses: list[str] | None = None,
    ) -> None:
        self._instance_exists = instance_exists
        self._instance_status = instance_status
        self._backup_statuses = list(backup_statuses or ["BUILDING", "COMPLETED"])
        self._poll_idx = 0
        self.created_attrs: dict | None = None
        self._backups: dict[str, SimpleNamespace] = {}

    def get_instance(self, instance_id: str):
        if not self._instance_exists:
            return None
        return SimpleNamespace(
            id=instance_id,
            name="db-primary",
            status=self._instance_status,
        )

    def _advance_status(self, backup: SimpleNamespace) -> None:
        status = self._backup_statuses[min(self._poll_idx, len(self._backup_statuses) - 1)]
        backup.status = status
        self._poll_idx += 1

    def trove_create(self, *, name: str, instance_id: str, description: str | None = None):
        self.created_attrs = {
            "name": name,
            "instance_id": instance_id,
            "description": description,
        }
        backup_id = "db-bkp-1"
        self._backups[backup_id] = SimpleNamespace(
            id=backup_id,
            status=self._backup_statuses[0],
            name=name,
            description=description,
        )
        return self._backups[backup_id]

    def trove_get(self, backup_id: str):
        backup = self._backups.get(backup_id)
        if backup is None:
            return None
        self._advance_status(backup)
        return backup

    def trove_list(self, instance_id: str):
        for backup in self._backups.values():
            self._advance_status(backup)
        self._poll_idx += 1
        return list(self._backups.values())


class _FakeConn:
    def __init__(self, database: _FakeDatabase) -> None:
        self.database = database


def _patch_trove(monkeypatch: pytest.MonkeyPatch, database: _FakeDatabase) -> None:
    monkeypatch.setattr(
        db_handler_mod,
        "create_instance_backup",
        lambda conn, *, name, instance_id, description=None: database.trove_create(
            name=name, instance_id=instance_id, description=description
        ),
    )
    monkeypatch.setattr(
        db_handler_mod,
        "get_instance_backup",
        lambda conn, backup_id: database.trove_get(backup_id),
    )
    monkeypatch.setattr(
        db_handler_mod,
        "list_instance_backups",
        lambda conn, *, instance_id: database.trove_list(instance_id),
    )


def _job(instance_id: str | None = None) -> SimpleNamespace:
    iid = instance_id or str(uuid4())
    return SimpleNamespace(
        id=str(uuid4()),
        schedule_id=str(uuid4()),
        project_id=str(uuid4()),
        action_type="database_backup",
        action_parameters={
            "database_instance_id": iid,
            "qonos_trust_id": str(uuid4()),
        },
    )


def _cfg(**kwargs):
    defaults = dict(
        volume_backup_poll_interval_seconds=0.01,
        volume_backup_poll_timeout_seconds=0.2,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_validate_parameters_database_instance_id_uuid() -> None:
    handler = DatabaseBackupActionHandler()
    with pytest.raises(ValueError, match="database_instance_id must be a UUID"):
        handler.validate_parameters({"database_instance_id": "not-a-uuid"})


@pytest.mark.anyio
async def test_database_backup_success(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DatabaseBackupActionHandler()
    database = _FakeDatabase(instance_exists=True, backup_statuses=["BUILDING", "COMPLETED"])
    conn = _FakeConn(database)
    monkeypatch.setattr(db_handler_mod, "get_config", lambda: _cfg())
    monkeypatch.setattr(
        db_handler_mod,
        "build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    _patch_trove(monkeypatch, database)
    job = _job()

    result = await handler.execute(job)

    assert result["backup_id"] == "db-bkp-1"
    assert result["status"] == "completed"
    assert database.created_attrs is not None
    assert database.created_attrs["name"].startswith("qonos-database_backup-db-primary-")
    meta = parse_qonos_description(database.created_attrs["description"])
    assert meta["qonos_job_id"] == job.id
    assert meta["qonos_managed"] == "true"


@pytest.mark.anyio
async def test_database_backup_instance_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DatabaseBackupActionHandler()
    database = _FakeDatabase(instance_exists=False)
    conn = _FakeConn(database)
    monkeypatch.setattr(db_handler_mod, "get_config", lambda: _cfg())
    monkeypatch.setattr(
        db_handler_mod,
        "build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    _patch_trove(monkeypatch, database)
    job = _job()

    with pytest.raises(ValueError, match="Database instance not found"):
        await handler.execute(job)


@pytest.mark.anyio
async def test_database_backup_missing_trust(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DatabaseBackupActionHandler()
    job = _job()
    job.action_parameters.pop("qonos_trust_id", None)

    with pytest.raises(RuntimeError, match="Missing delegated trust"):
        await handler.execute(job)


@pytest.mark.anyio
async def test_database_backup_reuses_existing_for_job(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DatabaseBackupActionHandler()
    database = _FakeDatabase(backup_statuses=["COMPLETED"])
    job = _job()
    description = qonos_description_metadata(
        job_id=job.id,
        schedule_id=job.schedule_id,
        project_id=job.project_id,
        action_type=job.action_type,
    )
    database._backups["existing"] = SimpleNamespace(
        id="existing",
        status="COMPLETED",
        name="prior-backup",
        description=description,
    )
    conn = _FakeConn(database)
    monkeypatch.setattr(db_handler_mod, "get_config", lambda: _cfg())
    monkeypatch.setattr(
        db_handler_mod,
        "build_openstack_connection",
        lambda project_id, trust_id=None: conn,
    )
    _patch_trove(monkeypatch, database)

    result = await handler.execute(job)

    assert result["backup_id"] == "existing"
    assert database.created_attrs is None


def test_validate_parameters_accepts_uuid() -> None:
    handler = DatabaseBackupActionHandler()
    iid = str(uuid4())
    handler.validate_parameters({"database_instance_id": iid})
    UUID(iid)


def test_backup_failure_detail_from_fault_dict() -> None:
    backup = SimpleNamespace(
        fault={"message": "Cannot parse backup output"},
        status_description=None,
        message=None,
    )
    assert backup_failure_detail(backup) == "Cannot parse backup output"

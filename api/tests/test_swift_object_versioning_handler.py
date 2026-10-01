from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from openstack.exceptions import ForbiddenException, NotFoundException

from api.app.worker.swift_object_versioning_handler import SwiftObjectVersioningActionHandler


class _FakeObjectStore:
    def __init__(
        self,
        *,
        containers: dict[str, dict[str, str]] | None = None,
        objects: dict[tuple[str, str], dict[str, object]] | None = None,
        copy_error: Exception | None = None,
        create_container_error: Exception | None = None,
    ) -> None:
        self.containers = dict(containers or {})
        self._stored = dict(objects or {})
        self.copy_error = copy_error
        self.create_container_error = create_container_error
        self.deleted: list[tuple[str, str]] = []
        self.uploaded: list[tuple[str, str, dict[str, str] | None]] = []
        self.copied: list[tuple[str, str, str, str]] = []

    def get_container(self, name: str):
        if name not in self.containers:
            raise NotFoundException("container not found")
        return SimpleNamespace(name=name)

    def get_container_metadata(self, container):
        name = container if isinstance(container, str) else getattr(container, "name", container)
        if name not in self.containers:
            raise NotFoundException("container not found")
        return SimpleNamespace(**self.containers[name])

    def create_container(self, *, name: str):
        if self.create_container_error is not None:
            raise self.create_container_error
        self.containers.setdefault(name, {})

    def objects(self, container_name: str, **query):
        prefix = query.get("prefix")
        for (container, object_name), payload in self._stored.items():
            if container != container_name:
                continue
            if prefix and not str(object_name).startswith(str(prefix)):
                continue
            yield SimpleNamespace(name=object_name, last_modified=payload.get("last_modified"))

    def delete_object(self, name: str, *, container: str):
        key = (container, name)
        if key not in self._stored:
            raise NotFoundException("object not found")
        self.deleted.append((container, name))
        del self._stored[key]

    def get_object_metadata(self, name: str, *, container: str):
        key = (container, name)
        if key not in self._stored:
            raise NotFoundException("object not found")
        metadata = self._stored[key].get("metadata") or {}
        return {"metadata": metadata}

    def copy_object(
        self,
        source_name: str,
        *,
        container: str,
        dest_container: str,
        dest_name: str,
        metadata: dict[str, str] | None = None,
    ):
        if self.copy_error is not None:
            raise self.copy_error
        source_key = (container, source_name)
        if source_key not in self._stored:
            raise NotFoundException("source object not found")
        payload = dict(self._stored[source_key])
        payload["metadata"] = dict(metadata or {})
        self._stored[(dest_container, dest_name)] = payload
        self.copied.append((container, source_name, dest_container, dest_name))

    def download_object(self, name: str, *, container: str):
        key = (container, name)
        if key not in self._stored:
            raise NotFoundException("object not found")
        return self._stored[key].get("data", b"")

    def upload_object(
        self,
        *,
        container: str,
        name: str,
        data: bytes,
        metadata: dict[str, str] | None = None,
    ):
        self._stored[(container, name)] = {"data": data, "metadata": dict(metadata or {})}
        self.uploaded.append((container, name, metadata))


class _FakeConn:
    def __init__(self, object_store: _FakeObjectStore) -> None:
        self.object_store = object_store


def _job(**overrides) -> SimpleNamespace:
    defaults = {
        "id": str(uuid4()),
        "schedule_id": str(uuid4()),
        "project_id": str(uuid4()),
        "action_type": "swift_object_versioning",
        "action_parameters": {
            "container": "source",
            "target_container": "archive",
            "qonos_trust_id": str(uuid4()),
        },
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_validate_parameters_requires_target_container() -> None:
    handler = SwiftObjectVersioningActionHandler()
    with pytest.raises(ValueError, match="target_container"):
        handler.validate_parameters({"container": "src"})


def test_validate_parameters_rejects_same_source_and_target() -> None:
    handler = SwiftObjectVersioningActionHandler()
    with pytest.raises(ValueError, match="must differ"):
        handler.validate_parameters({"container": "same", "target_container": "same"})


@pytest.mark.anyio
async def test_snapshot_copies_objects_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()
    schedule_id = str(uuid4())
    job_id = str(uuid4())
    object_store = _FakeObjectStore(
        containers={"source": {}, "archive": {}},
        objects={
            ("source", "docs/readme.txt"): {"data": b"hello"},
            ("source", "docs/notes.txt"): {"data": b"notes"},
            ("source", "skip/other.txt"): {"data": b"skip"},
        },
    )
    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(object_store),
    )
    job = _job(
        id=job_id,
        schedule_id=schedule_id,
        action_parameters={
            "container": "source",
            "target_container": "archive",
            "object_prefix": "docs/",
            "qonos_trust_id": str(uuid4()),
        },
    )

    result = await handler.execute(job)

    assert result["mode"] == "snapshot"
    assert result["objects_copied"] == 2
    assert result["copy_failed"] == 0
    manifest_key = ("archive", f".qonos/snapshots/{schedule_id}/{job_id}/__qonos_snapshot_manifest__")
    assert manifest_key in object_store._stored
    assert object_store._stored[manifest_key]["metadata"]["qonos_objects_copied"] == "2"


@pytest.mark.anyio
async def test_snapshot_container_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()
    object_store = _FakeObjectStore()
    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(object_store),
    )
    job = _job(action_parameters={"container": "missing", "target_container": "archive", "qonos_trust_id": str(uuid4())})

    with pytest.raises(ValueError, match="not found"):
        await handler.execute(job)


@pytest.mark.anyio
async def test_snapshot_permission_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()

    class _DeniedStore(_FakeObjectStore):
        def get_container_metadata(self, name: str):
            raise ForbiddenException("forbidden")

    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(_DeniedStore()),
    )
    job = _job()

    with pytest.raises(PermissionError, match="permission denied"):
        await handler.execute(job)


@pytest.mark.anyio
async def test_snapshot_creates_target_container(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()
    object_store = _FakeObjectStore(
        containers={"source": {}},
        objects={("source", "file.txt"): {"data": b"data"}},
    )
    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(object_store),
    )
    job = _job()

    result = await handler.execute(job)

    assert "archive" in object_store.containers
    assert result["objects_copied"] == 1


@pytest.mark.anyio
async def test_snapshot_reuses_existing_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()
    schedule_id = str(uuid4())
    job_id = str(uuid4())
    manifest_name = f".qonos/snapshots/{schedule_id}/{job_id}/__qonos_snapshot_manifest__"
    object_store = _FakeObjectStore(
        containers={"source": {}, "archive": {}},
        objects={
            ("source", "file.txt"): {"data": b"data"},
            ("archive", manifest_name): {
                "data": b"",
                "metadata": {
                    "qonos_job_id": job_id,
                    "qonos_snapshot_root": f".qonos/snapshots/{schedule_id}/{job_id}",
                    "qonos_objects_copied": "1",
                },
            },
        },
    )
    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(object_store),
    )
    job = _job(id=job_id, schedule_id=schedule_id)

    result = await handler.execute(job)

    assert result["reused"] is True
    assert object_store.copied == []


@pytest.mark.anyio
async def test_snapshot_quota_exceeded(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = SwiftObjectVersioningActionHandler()
    object_store = _FakeObjectStore(
        containers={"source": {}},
        objects={("source", "file.txt"): {"data": b"data"}},
        create_container_error=RuntimeError("507 Insufficient Storage: quota exceeded"),
    )
    monkeypatch.setattr(
        "api.app.worker.swift_object_versioning_handler.build_openstack_connection",
        lambda project_id, trust_id=None: _FakeConn(object_store),
    )
    job = _job(action_parameters={"container": "source", "target_container": "archive", "qonos_trust_id": str(uuid4())})

    with pytest.raises(Exception, match="quota exceeded"):
        await handler.execute(job)


@pytest.mark.anyio
async def test_snapshot_requires_trust_context() -> None:
    handler = SwiftObjectVersioningActionHandler()
    job = _job(action_parameters={"container": "source", "target_container": "archive"})

    with pytest.raises(RuntimeError, match="Missing delegated trust context"):
        await handler.execute(job)

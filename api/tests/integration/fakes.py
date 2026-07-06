from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from uuid import uuid4

from api.app.auth.context import AuthContext
from api.app.auth.keystone_client import KeystoneClient

PROJECT_A = "11111111-1111-4111-8111-111111111111"
PROJECT_B = "22222222-2222-4222-8222-222222222222"
TRUST_A = "33333333-3333-4333-8333-333333333333"
TRUST_B = "55555555-5555-4555-8555-555555555555"
SERVER_A = "44444444-4444-4444-8444-444444444444"
VOLUME_A = "66666666-6666-4666-8666-666666666666"

TOKEN_MEMBER_A = "token-member-project-a"
TOKEN_MEMBER_B = "token-member-project-b"
TOKEN_ADMIN_A = "token-admin-project-a"
TOKEN_SERVICE = "token-service"
TOKEN_INVALID = "token-invalid"


class SimulatedKeystoneClient(KeystoneClient):

    def __init__(self) -> None:
        self._tokens: Dict[str, AuthContext] = {
            TOKEN_MEMBER_A: AuthContext(
                user_id="user-member-a",
                project_id=PROJECT_A,
                roles=["member"],
            ),
            TOKEN_MEMBER_B: AuthContext(
                user_id="user-member-b",
                project_id=PROJECT_B,
                roles=["member"],
            ),
            TOKEN_ADMIN_A: AuthContext(
                user_id="user-admin-a",
                project_id=PROJECT_A,
                roles=["admin"],
            ),
            TOKEN_SERVICE: AuthContext(
                user_id="service-user",
                project_id=PROJECT_A,
                roles=["service"],
            ),
        }
        self._valid_trusts: Dict[str, str] = {TRUST_A: PROJECT_A, TRUST_B: PROJECT_B}
        self.trust_validation_calls: List[tuple[str, Optional[str]]] = []

    async def validate_token(self, token: str) -> AuthContext:
        ctx = self._tokens.get(token)
        if ctx is None:
            raise ValueError("invalid token")
        return ctx

    async def validate_trust(
        self, trust_id: str, *, expected_project_id: str | None = None
    ) -> dict[str, object]:
        self.trust_validation_calls.append((trust_id, expected_project_id))
        project_id = self._valid_trusts.get(trust_id)
        if project_id is None:
            raise ValueError("trust_id not found")
        if expected_project_id and project_id != expected_project_id:
            raise ValueError("trust_id does not belong to the schedule project")
        return {
            "id": trust_id,
            "project_id": project_id,
            "expires_at": "2099-01-01T00:00:00Z",
        }


@dataclass
class SimulatedImage:
    id: str
    name: str
    status: str
    created_at: datetime
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class SimulatedServer:
    id: str
    name: str
    project_id: str


@dataclass
class SimulatedVolume:
    id: str
    name: str
    project_id: str
    status: str = "available"


@dataclass
class SimulatedBackup:
    id: str
    name: str
    project_id: str
    volume_id: str
    status: str
    created_at: datetime
    is_incremental: bool
    parent_id: str | None = None
    size: int | None = 10
    metadata: Dict[str, str] = field(default_factory=dict)


class OpenStackSimulator:
    def __init__(self) -> None:
        self.servers: Dict[str, SimulatedServer] = {}
        self.images: Dict[str, SimulatedImage] = {}
        self.volumes: Dict[str, SimulatedVolume] = {}
        self.backups: Dict[str, SimulatedBackup] = {}
        self._poll_counts: Dict[str, int] = {}
        self.snapshot_statuses: List[str] = ["QUEUED", "ACTIVE"]
        self.backup_statuses: List[str] = ["creating", "available"]
        self.create_snapshot_failures_remaining: int = 0
        self.create_backup_failures_remaining: int = 0
        self.deleted_image_ids: List[str] = []
        self.deleted_backup_ids: List[str] = []
        self.connection_calls: List[tuple[str, str | None]] = []

    def reset(self) -> None:
        self.servers.clear()
        self.images.clear()
        self.volumes.clear()
        self.backups.clear()
        self.snapshot_statuses = ["QUEUED", "ACTIVE"]
        self.backup_statuses = ["creating", "available"]
        self.create_snapshot_failures_remaining = 0
        self.create_backup_failures_remaining = 0
        self.deleted_image_ids.clear()
        self.deleted_backup_ids.clear()
        self.connection_calls.clear()
        self._poll_counts = {}

    def add_server(
        self,
        *,
        server_id: str = SERVER_A,
        name: str = "vm-test",
        project_id: str = PROJECT_A,
    ) -> SimulatedServer:
        server = SimulatedServer(id=server_id, name=name, project_id=project_id)
        self.servers[server_id] = server
        return server

    def add_volume(
        self,
        *,
        volume_id: str = VOLUME_A,
        name: str = "vol-test",
        project_id: str = PROJECT_A,
        status: str = "available",
    ) -> SimulatedVolume:
        vol = SimulatedVolume(
            id=volume_id,
            name=name,
            project_id=project_id,
            status=status,
        )
        self.volumes[volume_id] = vol
        return vol

    def seed_managed_images(
        self,
        *,
        schedule_id: str,
        project_id: str = PROJECT_A,
        count: int,
        ages_days: Optional[List[int]] = None,
    ) -> List[SimulatedImage]:
        now = datetime.now(timezone.utc)
        created: List[SimulatedImage] = []
        for idx in range(count):
            image_id = str(uuid4())
            age = ages_days[idx] if ages_days and idx < len(ages_days) else idx * 2 + 1
            img = SimulatedImage(
                id=image_id,
                name=f"qonos-seed-{idx}",
                status="ACTIVE",
                created_at=now - timedelta(days=age),
                properties={
                    "qonos_managed": "true",
                    "qonos_action_type": "server_snapshot",
                    "qonos_schedule_id": schedule_id,
                    "qonos_project_id": project_id,
                    "qonos_job_id": str(uuid4()),
                },
            )
            self.images[image_id] = img
            created.append(img)
        return created

    def list_managed_for_schedule(self, schedule_id: str, project_id: str) -> List[SimulatedImage]:
        out: List[SimulatedImage] = []
        for img in self.images.values():
            if img.properties.get("qonos_schedule_id") != schedule_id:
                continue
            if img.properties.get("qonos_project_id") != project_id:
                continue
            if img.status.upper() in {"DELETED", "KILLED", "PENDING_DELETE", "ERROR"}:
                continue
            out.append(img)
        out.sort(key=lambda i: i.created_at)
        return out

    def connection_for(
        self, *, project_id: str, trust_id: str | None = None
    ) -> _SimulatedConnection:
        self.connection_calls.append((project_id, trust_id))
        return _SimulatedConnection(self, project_id=project_id, trust_id=trust_id)


class _SimulatedCompute:
    def __init__(self, simulator: OpenStackSimulator, project_id: str) -> None:
        self._sim = simulator
        self._project_id = project_id

    def get_server(self, server_id: str):
        server = self._sim.servers.get(server_id)
        if server is None or server.project_id != self._project_id:
            return None
        return SimpleNamespace(id=server.id, name=server.name)

    def create_server_image(self, *, server, name: str, metadata: dict[str, str]):
        if self._sim.create_snapshot_failures_remaining > 0:
            self._sim.create_snapshot_failures_remaining -= 1
            raise RuntimeError("simulated snapshot creation failure")

        image_id = str(uuid4())
        self._sim.images[image_id] = SimulatedImage(
            id=image_id,
            name=name,
            status=self._sim.snapshot_statuses[0],
            created_at=datetime.now(timezone.utc),
            properties=dict(metadata),
        )
        return image_id


class _SimulatedImageAPI:
    def __init__(self, simulator: OpenStackSimulator, project_id: str) -> None:
        self._sim = simulator
        self._project_id = project_id

    def get_image(self, image_id: str):
        img = self._sim.images.get(image_id)
        if img is None:
            return None
        if img.properties.get("qonos_project_id") != self._project_id:
            return None
        poll_idx = self._sim._poll_counts.get(image_id, 0)
        self._sim._poll_counts[image_id] = poll_idx + 1
        status = self._sim.snapshot_statuses[min(poll_idx, len(self._sim.snapshot_statuses) - 1)]
        img.status = status
        return SimpleNamespace(id=img.id, status=status, name=img.name, properties=img.properties)

    def images(self, owner: str | None = None):
        owner_id = owner or self._project_id
        return [
            SimpleNamespace(
                id=img.id,
                name=img.name,
                status=img.status,
                created_at=img.created_at,
                properties=img.properties,
            )
            for img in self._sim.images.values()
            if img.properties.get("qonos_project_id") == owner_id
        ]

    def delete_image(self, image_id: str, ignore_missing: bool = True):
        img = self._sim.images.get(image_id)
        if img is None:
            if not ignore_missing:
                raise RuntimeError(f"image not found: {image_id}")
            return
        if img.properties.get("qonos_project_id") != self._project_id:
            if not ignore_missing:
                raise RuntimeError(f"image does not belong to project: {image_id}")
            return
        del self._sim.images[image_id]
        self._sim.deleted_image_ids.append(image_id)


class _SimulatedConnection:
    def __init__(
        self, simulator: OpenStackSimulator, *, project_id: str, trust_id: str | None
    ) -> None:
        self.compute = _SimulatedCompute(simulator, project_id)
        self.image = _SimulatedImageAPI(simulator, project_id)
        self.block_storage = _SimulatedBlockStorage(simulator, project_id)

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)


class _SimulatedBlockStorage:
    def __init__(self, simulator: OpenStackSimulator, project_id: str) -> None:
        self._sim = simulator
        self._project_id = project_id

    def get_volume(self, volume_id: str):
        vol = self._sim.volumes.get(volume_id)
        if vol is None or vol.project_id != self._project_id:
            return None
        return SimpleNamespace(id=vol.id, name=vol.name, status=vol.status)

    def create_backup(self, **attrs):
        if self._sim.create_backup_failures_remaining > 0:
            self._sim.create_backup_failures_remaining -= 1
            raise RuntimeError("simulated backup creation failure")

        volume_id = str(attrs.get("volume_id") or "")
        vol = self._sim.volumes.get(volume_id)
        if vol is None or vol.project_id != self._project_id:
            raise RuntimeError(f"volume not found: {volume_id}")

        backup_id = str(uuid4())
        is_incremental = bool(attrs.get("is_incremental", False))
        metadata = attrs.get("metadata") if isinstance(attrs.get("metadata"), dict) else {}
        parent_id = None
        if is_incremental:
            raw_parent = metadata.get("qonos_parent_full_backup_id")
            if isinstance(raw_parent, str) and raw_parent.strip():
                parent_id = raw_parent.strip()

        b = SimulatedBackup(
            id=backup_id,
            name=str(attrs.get("name") or backup_id),
            project_id=self._project_id,
            volume_id=volume_id,
            status=self._sim.backup_statuses[0],
            created_at=datetime.now(timezone.utc),
            is_incremental=is_incremental,
            parent_id=parent_id,
            size=int(attrs.get("size") or 10) if attrs.get("size") is not None else 10,
            metadata={str(k): str(v) for k, v in (metadata or {}).items()},
        )
        self._sim.backups[backup_id] = b
        return backup_id

    def get_backup(self, backup_id: str):
        b = self._sim.backups.get(backup_id)
        if b is None or b.project_id != self._project_id:
            return None
        poll_idx = self._sim._poll_counts.get(backup_id, 0)
        self._sim._poll_counts[backup_id] = poll_idx + 1
        status = self._sim.backup_statuses[min(poll_idx, len(self._sim.backup_statuses) - 1)]
        b.status = status
        return SimpleNamespace(
            id=b.id,
            name=b.name,
            status=b.status,
            created_at=b.created_at,
            is_incremental=b.is_incremental,
            parent_id=b.parent_id,
            project_id=b.project_id,
            volume_id=b.volume_id,
            size=b.size,
            metadata=b.metadata,
        )

    def backups(self, *, details: bool = True, volume_id: str | None = None):
        _ = details
        out: list[Any] = []
        for b in self._sim.backups.values():
            if b.project_id != self._project_id:
                continue
            if volume_id and b.volume_id != volume_id:
                continue
            out.append(
                SimpleNamespace(
                    id=b.id,
                    name=b.name,
                    status=b.status,
                    created_at=b.created_at,
                    is_incremental=b.is_incremental,
                    parent_id=b.parent_id,
                    project_id=b.project_id,
                    volume_id=b.volume_id,
                    size=b.size,
                    metadata=b.metadata,
                )
            )
        return out

    def delete_backup(self, backup_id: str, ignore_missing: bool = True):
        b = self._sim.backups.get(backup_id)
        if b is None:
            if not ignore_missing:
                raise RuntimeError(f"backup not found: {backup_id}")
            return
        if b.project_id != self._project_id:
            if not ignore_missing:
                raise RuntimeError(f"backup does not belong to project: {backup_id}")
            return
        del self._sim.backups[backup_id]
        self._sim.deleted_backup_ids.append(backup_id)

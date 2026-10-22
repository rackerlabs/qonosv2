from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from api.app.openstack_connection import build_openstack_connection
from api.app.observability.metrics import observe_retention_deleted
from api.app.utils.time import as_utc_aware, utc_now
from api.app.worker.database_backup_common import (
    SKIP_TROVE_BACKUP_STATUSES,
    TROVE_API_TIMEOUT_SECONDS,
    backup_status,
    delete_database_backup,
    list_instance_backups,
    parse_backup_created_at,
    parse_qonos_description,
)
from api.app.worker.swift_common import (
    MANIFEST_OBJECT_NAME,
    extract_object_metadata,
    list_container_objects,
    parse_timestamp,
)

logger = logging.getLogger("qonos.worker.retention")

CINDER_API_TIMEOUT_SECONDS = 60.0
_SKIP_IMAGE_STATUSES = {"DELETED", "PENDING_DELETE"}
_SKIP_BACKUP_STATUSES = frozenset({"creating", "deleting", "deleted"})


@dataclass
class RetentionPolicy:
    count: Optional[int] = None
    max_age_days: Optional[int] = None

    def is_noop(self) -> bool:
        count_disabled = self.count is None or self.count <= 0
        age_disabled = self.max_age_days is None or self.max_age_days <= 0
        return count_disabled and age_disabled


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def parse_retention_policy(raw: Any) -> Optional[RetentionPolicy]:
    """Parse retention policy from stored schedule JSON."""
    if not isinstance(raw, dict):
        return None

    policy_type = str(raw.get("type", "")).strip().lower()
    if policy_type == "count":
        count = _to_int(raw.get("value"))
        if count is None:
            return None
        return RetentionPolicy(count=count)
    if policy_type == "age":
        max_age_days = _to_int(raw.get("max_age_days"))
        if max_age_days is None:
            return None
        return RetentionPolicy(max_age_days=max_age_days)

    return None


def _build_connection(*, project_id: str, trust_id: str | None = None):
    return build_openstack_connection(project_id=project_id, trust_id=trust_id)


def _parse_created_at(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return as_utc_aware(value)
    if isinstance(value, str):
        try:
            return as_utc_aware(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except Exception:
            return None
    return None


def _apply_server_snapshot_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    trust_id: str | None = None,
) -> dict[str, int]:
    conn = _build_connection(project_id=project_id, trust_id=trust_id)
    images = list(conn.image.images(owner=project_id))

    candidates: list[tuple[str, datetime]] = []
    for img in images:
        props = getattr(img, "properties", None) or {}
        if not isinstance(props, dict):
            continue
        if str(props.get("qonos_managed", "")).lower() != "true":
            continue
        if props.get("qonos_action_type") != action_type:
            continue
        if props.get("qonos_schedule_id") != schedule_id:
            continue
        status = str(getattr(img, "status", "") or "").upper()
        if status in _SKIP_IMAGE_STATUSES:
            continue
        created_at = _parse_created_at(getattr(img, "created_at", None))
        if created_at is None:
            continue
        candidates.append((str(getattr(img, "id")), created_at))

    ids_to_delete: set[str] = set()
    if policy.count is not None and policy.count > 0:
        newest_first = sorted(candidates, key=lambda x: x[1], reverse=True)
        for image_id, _ in newest_first[policy.count :]:
            ids_to_delete.add(image_id)

    if policy.max_age_days is not None and policy.max_age_days > 0:
        cutoff = now - timedelta(days=policy.max_age_days)
        for image_id, created_at in candidates:
            if created_at < cutoff:
                ids_to_delete.add(image_id)

    deleted = 0
    failed = 0
    for image_id in sorted(ids_to_delete):
        try:
            conn.image.delete_image(image_id, ignore_missing=True)
            deleted += 1
        except TypeError:
            try:
                conn.image.delete_image(image_id)
                deleted += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    return {
        "scanned": len(images),
        "eligible": len(candidates),
        "delete_candidates": len(ids_to_delete),
        "deleted": deleted,
        "delete_failed": failed,
    }


def _is_incremental_backup(backup: Any) -> bool:
    value = getattr(backup, "is_incremental", False)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _backup_status(backup: Any) -> str:
    return str(getattr(backup, "status", "") or "").lower()


def _cinder_call_with_timeout(
    pool: ThreadPoolExecutor,
    fn: Callable[[], Any],
    *,
    timeout: float,
    operation: str,
    subject_id: str,
) -> Any | None:
    future = pool.submit(fn)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        logger.warning(
            "retention %s timed out subject_id=%s timeout=%.0fs",
            operation,
            subject_id,
            timeout,
        )
        return None
    except Exception as exc:
        logger.warning(
            "retention %s failed subject_id=%s error=%s",
            operation,
            subject_id,
            exc,
        )
        return None


def _list_volume_backups(pool: ThreadPoolExecutor, conn: Any, volume_id: str) -> list[Any]:
    backups = _cinder_call_with_timeout(
        pool,
        lambda: list(conn.block_storage.backups(details=True, volume_id=volume_id)),
        timeout=CINDER_API_TIMEOUT_SECONDS,
        operation="backups_list",
        subject_id=volume_id,
    )
    return backups or []


def _empty_volume_retention_stats() -> dict[str, int]:
    return {
        "scanned": 0,
        "eligible": 0,
        "delete_candidates": 0,
        "deleted": 0,
        "delete_failed": 0,
    }


def _delete_backup(conn: Any, backup_id: str) -> bool:
    try:
        conn.block_storage.delete_backup(backup_id, ignore_missing=True)
        return True
    except TypeError:
        try:
            conn.block_storage.delete_backup(backup_id)
            return True
        except Exception:
            return False
    except Exception:
        return False


def _backup_parent_id(backup: Any) -> str | None:
    for attr in ("parent_id", "parentId"):
        value = getattr(backup, attr, None)
        if value:
            return str(value)
    meta = getattr(backup, "metadata", None) or {}
    if isinstance(meta, dict):
        value = meta.get("qonos_parent_full_backup_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _build_children_by_parent(backups: list[Any]) -> dict[str, list[str]]:
    children: dict[str, list[str]] = {}
    for backup in backups:
        backup_id = str(getattr(backup, "id", "") or "")
        if not backup_id:
            continue
        parent_id = _backup_parent_id(backup)
        if parent_id:
            children.setdefault(parent_id, []).append(backup_id)
    return children


def _cascade_delete_order(
    root_ids: set[str], children_by_parent: dict[str, list[str]]
) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()

    def add_subtree(backup_id: str) -> None:
        for child_id in sorted(children_by_parent.get(backup_id, [])):
            if child_id in seen:
                continue
            add_subtree(child_id)
            order.append(child_id)
            seen.add(child_id)

    for root_id in sorted(root_ids):
        add_subtree(root_id)
        if root_id not in seen:
            order.append(root_id)
            seen.add(root_id)
    return order


def _volume_backup_candidates(
    backups: list[Any],
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    incremental_only: bool,
) -> list[tuple[str, datetime]]:
    candidates: list[tuple[str, datetime]] = []
    for backup in backups:
        backup_project = str(getattr(backup, "project_id", "") or "")
        if backup_project and backup_project != project_id:
            continue
        is_incremental = _is_incremental_backup(backup)
        if incremental_only and not is_incremental:
            continue
        if not incremental_only and is_incremental:
            continue
        meta = getattr(backup, "metadata", None) or {}
        if not isinstance(meta, dict):
            continue
        if str(meta.get("qonos_managed", "")).lower() != "true":
            continue
        if meta.get("qonos_action_type") != action_type:
            continue
        if meta.get("qonos_schedule_id") != schedule_id:
            continue
        if _backup_status(backup) in _SKIP_BACKUP_STATUSES:
            continue
        created_at = _parse_created_at(getattr(backup, "created_at", None))
        if created_at is None:
            continue
        candidates.append((str(getattr(backup, "id")), created_at))
    return candidates


def _apply_volume_backup_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    volume_id: str | None = None,
    trust_id: str | None = None,
    incremental_only: bool,
    cascade_on_full_delete: bool = False,
) -> dict[str, int]:
    if not volume_id:
        logger.warning(
            "retention skipped decision=missing_volume_id schedule_id=%s",
            schedule_id,
        )
        return _empty_volume_retention_stats()

    conn = _build_connection(project_id=project_id, trust_id=trust_id)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="retention-cinder") as pool:
        backups = _list_volume_backups(pool, conn, volume_id)

    candidates = _volume_backup_candidates(
        backups,
        project_id=project_id,
        schedule_id=schedule_id,
        action_type=action_type,
        incremental_only=incremental_only,
    )

    ids_to_delete: set[str] = set()
    if policy.count is not None and policy.count > 0:
        newest_first = sorted(candidates, key=lambda x: x[1], reverse=True)
        for backup_id, _ in newest_first[policy.count :]:
            ids_to_delete.add(backup_id)

    if policy.max_age_days is not None and policy.max_age_days > 0:
        cutoff = now - timedelta(days=policy.max_age_days)
        for backup_id, created_at in candidates:
            if created_at < cutoff:
                ids_to_delete.add(backup_id)

    if cascade_on_full_delete and ids_to_delete:
        children_by_parent = _build_children_by_parent(backups)
        delete_sequence = _cascade_delete_order(ids_to_delete, children_by_parent)
    else:
        delete_sequence = sorted(ids_to_delete)

    deleted = 0
    failed = 0
    for backup_id in delete_sequence:
        if _delete_backup(conn, backup_id):
            deleted += 1
        else:
            failed += 1

    return {
        "scanned": len(backups),
        "eligible": len(candidates),
        "delete_candidates": len(delete_sequence),
        "deleted": deleted,
        "delete_failed": failed,
    }


def _apply_volume_backup_full_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    volume_id: str | None = None,
    trust_id: str | None = None,
) -> dict[str, int]:
    return _apply_volume_backup_retention(
        project_id=project_id,
        schedule_id=schedule_id,
        action_type=action_type,
        policy=policy,
        now=now,
        volume_id=volume_id,
        trust_id=trust_id,
        incremental_only=False,
        cascade_on_full_delete=True,
    )


def _apply_swift_object_versioning_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    target_container: str,
    trust_id: str | None = None,
) -> dict[str, int]:
    conn = _build_connection(project_id=project_id, trust_id=trust_id)
    object_store = conn.object_store

    listed = list_container_objects(object_store, target_container)
    scanned = len(listed)
    object_names = [str(getattr(obj, "name", "") or "") for obj in listed]
    objects_by_name = dict(zip(object_names, listed, strict=False))

    candidates: list[tuple[str, datetime, str]] = []
    for name in object_names:
        if not name.endswith(f"/{MANIFEST_OBJECT_NAME}"):
            continue
        obj = objects_by_name.get(name)
        try:
            metadata = object_store.get_object_metadata(name, container=target_container)
        except Exception:
            continue
        props = extract_object_metadata(metadata)
        if str(props.get("qonos_managed", "")).lower() != "true":
            continue
        if props.get("qonos_action_type") != action_type:
            continue
        if props.get("qonos_schedule_id") != schedule_id:
            continue
        created_at = parse_timestamp(props.get("qonos_created_at")) or parse_timestamp(
            getattr(obj, "last_modified", None) if obj is not None else None
        )
        if created_at is None:
            continue
        snapshot_root = str(props.get("qonos_snapshot_root") or name.rsplit("/", 1)[0])
        candidates.append((name, created_at, snapshot_root))

    manifests_to_delete: dict[str, str] = {}
    if policy.count is not None and policy.count > 0:
        newest_first = sorted(candidates, key=lambda item: item[1], reverse=True)
        for manifest_name, _, snapshot_root in newest_first[policy.count :]:
            manifests_to_delete[manifest_name] = snapshot_root

    if policy.max_age_days is not None and policy.max_age_days > 0:
        cutoff = now - timedelta(days=policy.max_age_days)
        for manifest_name, created_at, snapshot_root in candidates:
            if created_at < cutoff:
                manifests_to_delete[manifest_name] = snapshot_root

    names_to_delete: set[str] = set()
    for manifest_name, snapshot_root in manifests_to_delete.items():
        prefix = f"{snapshot_root}/"
        names_to_delete.add(manifest_name)
        for object_name in object_names:
            if object_name.startswith(prefix):
                names_to_delete.add(object_name)

    deleted_objects = 0
    delete_failed = 0
    for object_name in sorted(names_to_delete):
        try:
            object_store.delete_object(object_name, container=target_container)
            deleted_objects += 1
        except Exception as exc:
            delete_failed += 1
            logger.warning(
                "swift retention decision=delete_failed container=%s object=%s error=%s",
                target_container,
                object_name,
                str(exc),
            )

    return {
        "scanned": scanned,
        "eligible": len(candidates),
        "delete_candidates": len(manifests_to_delete),
        "deleted": deleted_objects,
        "delete_failed": delete_failed,
    }


def _list_database_backups(
    pool: ThreadPoolExecutor, conn: Any, database_instance_id: str
) -> list[Any]:
    future = pool.submit(
        lambda: list_instance_backups(conn, instance_id=database_instance_id)
    )
    try:
        return future.result(timeout=TROVE_API_TIMEOUT_SECONDS) or []
    except FuturesTimeoutError:
        logger.warning(
            "database retention backups_list timed out database_instance_id=%s",
            database_instance_id,
        )
        return []
    except Exception as exc:
        logger.warning(
            "database retention backups_list failed database_instance_id=%s error=%s",
            database_instance_id,
            exc,
        )
        return []


def _database_backup_candidates(
    backups: list[Any],
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
) -> list[tuple[str, datetime]]:
    candidates: list[tuple[str, datetime]] = []
    for backup in backups:
        backup_project = str(getattr(backup, "project_id", "") or "")
        if backup_project and backup_project != project_id:
            continue
        meta = parse_qonos_description(getattr(backup, "description", None))
        if meta.get("qonos_managed") != "true":
            continue
        if meta.get("qonos_action_type") != action_type:
            continue
        if meta.get("qonos_schedule_id") != schedule_id:
            continue
        if backup_status(backup) in SKIP_TROVE_BACKUP_STATUSES:
            continue
        created_at = parse_backup_created_at(backup)
        if created_at is None:
            continue
        candidates.append((str(getattr(backup, "id")), as_utc_aware(created_at)))
    return candidates


def _apply_database_backup_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    database_instance_id: str | None = None,
    trust_id: str | None = None,
) -> dict[str, int]:
    if not database_instance_id:
        logger.warning(
            "retention skipped decision=missing_database_instance_id schedule_id=%s",
            schedule_id,
        )
        return _empty_volume_retention_stats()

    conn = _build_connection(project_id=project_id, trust_id=trust_id)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="retention-trove") as pool:
        backups = _list_database_backups(pool, conn, database_instance_id)

    candidates = _database_backup_candidates(
        backups,
        project_id=project_id,
        schedule_id=schedule_id,
        action_type=action_type,
    )

    ids_to_delete: set[str] = set()
    if policy.count is not None and policy.count > 0:
        newest_first = sorted(candidates, key=lambda x: x[1], reverse=True)
        for backup_id, _ in newest_first[policy.count :]:
            ids_to_delete.add(backup_id)

    if policy.max_age_days is not None and policy.max_age_days > 0:
        cutoff = now - timedelta(days=policy.max_age_days)
        for backup_id, created_at in candidates:
            if created_at < cutoff:
                ids_to_delete.add(backup_id)

    delete_sequence = sorted(ids_to_delete)
    deleted = 0
    failed = 0
    for backup_id in delete_sequence:
        if delete_database_backup(conn, backup_id):
            deleted += 1
        else:
            failed += 1

    return {
        "scanned": len(backups),
        "eligible": len(candidates),
        "delete_candidates": len(delete_sequence),
        "deleted": deleted,
        "delete_failed": failed,
    }


def _apply_volume_backup_incremental_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    volume_id: str | None = None,
    trust_id: str | None = None,
) -> dict[str, int]:
    return _apply_volume_backup_retention(
        project_id=project_id,
        schedule_id=schedule_id,
        action_type=action_type,
        policy=policy,
        now=now,
        volume_id=volume_id,
        trust_id=trust_id,
        incremental_only=True,
        cascade_on_full_delete=False,
    )


async def run_retention_for_job(
    *,
    action_type: str,
    project_id: str,
    schedule_id: str,
    retention_policy_raw: Any,
    trigger_status: str,
    volume_id: str | None = None,
    database_instance_id: str | None = None,
    target_container: str | None = None,
    trust_id: str | None = None,
) -> None:
    policy = parse_retention_policy(retention_policy_raw)
    if policy is None or policy.is_noop():
        return

    now = utc_now()
    try:
        if action_type == "server_snapshot":
            stats = await asyncio.to_thread(
                _apply_server_snapshot_retention,
                project_id=project_id,
                schedule_id=schedule_id,
                action_type=action_type,
                policy=policy,
                now=now,
                trust_id=trust_id,
            )
        elif action_type == "volume_backup_full":
            stats = await asyncio.to_thread(
                _apply_volume_backup_full_retention,
                project_id=project_id,
                schedule_id=schedule_id,
                action_type=action_type,
                policy=policy,
                now=now,
                volume_id=volume_id,
                trust_id=trust_id,
            )
        elif action_type == "volume_backup_incremental":
            stats = await asyncio.to_thread(
                _apply_volume_backup_incremental_retention,
                project_id=project_id,
                schedule_id=schedule_id,
                action_type=action_type,
                policy=policy,
                now=now,
                volume_id=volume_id,
                trust_id=trust_id,
            )
        elif action_type == "swift_object_versioning":
            if not target_container:
                logger.info(
                    "retention skipped decision=missing_target_container action_type=%s schedule_id=%s",
                    action_type,
                    schedule_id,
                )
                return
            stats = await asyncio.to_thread(
                _apply_swift_object_versioning_retention,
                project_id=project_id,
                schedule_id=schedule_id,
                action_type=action_type,
                policy=policy,
                now=now,
                target_container=target_container,
                trust_id=trust_id,
            )
        elif action_type == "database_backup":
            stats = await asyncio.to_thread(
                _apply_database_backup_retention,
                project_id=project_id,
                schedule_id=schedule_id,
                action_type=action_type,
                policy=policy,
                now=now,
                database_instance_id=database_instance_id,
                trust_id=trust_id,
            )
        else:
            logger.info(
                "retention skipped decision=unsupported_action_type action_type=%s schedule_id=%s",
                action_type,
                schedule_id,
            )
            return
        logger.info(
            "retention completed action_type=%s schedule_id=%s trigger_status=%s scanned=%s eligible=%s delete_candidates=%s deleted=%s delete_failed=%s",
            action_type,
            schedule_id,
            trigger_status,
            stats["scanned"],
            stats["eligible"],
            stats["delete_candidates"],
            stats["deleted"],
            stats["delete_failed"],
        )
        observe_retention_deleted(action_type=action_type, count=int(stats["deleted"]))
    except Exception as exc:
        logger.warning(
            "retention failed action_type=%s schedule_id=%s trigger_status=%s error=%s",
            action_type,
            schedule_id,
            trigger_status,
            str(exc),
        )

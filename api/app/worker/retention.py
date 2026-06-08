from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from api.app.openstack_connection import build_openstack_connection
from api.app.utils.time import as_utc_aware, utc_now

logger = logging.getLogger("qonos.worker.retention")

_SKIP_IMAGE_STATUSES = {"DELETED", "PENDING_DELETE"}
_SKIP_BACKUP_STATUSES = {"deleting", "deleted"}


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


def _apply_volume_backup_full_retention(
    *,
    project_id: str,
    schedule_id: str,
    action_type: str,
    policy: RetentionPolicy,
    now: datetime,
    trust_id: str | None = None,
) -> dict[str, int]:
    conn = _build_connection(project_id=project_id, trust_id=trust_id)
    backups = list(conn.block_storage.backups(details=True))

    candidates: list[tuple[str, datetime]] = []
    for backup in backups:
        backup_project = str(getattr(backup, "project_id", "") or "")
        if backup_project and backup_project != project_id:
            continue
        if _is_incremental_backup(backup):
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
        status = str(getattr(backup, "status", "") or "").lower()
        if status in _SKIP_BACKUP_STATUSES:
            continue
        created_at = _parse_created_at(getattr(backup, "created_at", None))
        if created_at is None:
            continue
        candidates.append((str(getattr(backup, "id")), created_at))

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

    deleted = 0
    failed = 0
    for backup_id in sorted(ids_to_delete):
        try:
            conn.block_storage.delete_backup(backup_id, ignore_missing=True)
            deleted += 1
        except TypeError:
            try:
                conn.block_storage.delete_backup(backup_id)
                deleted += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    return {
        "scanned": len(backups),
        "eligible": len(candidates),
        "delete_candidates": len(ids_to_delete),
        "deleted": deleted,
        "delete_failed": failed,
    }


async def run_retention_for_job(
    *,
    action_type: str,
    project_id: str,
    schedule_id: str,
    retention_policy_raw: Any,
    trigger_status: str,
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
    except Exception as exc:
        logger.warning(
            "retention failed action_type=%s schedule_id=%s trigger_status=%s error=%s",
            action_type,
            schedule_id,
            trigger_status,
            str(exc),
        )

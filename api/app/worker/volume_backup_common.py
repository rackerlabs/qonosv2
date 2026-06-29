from __future__ import annotations

import asyncio
import logging
import time
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import Any, Callable, Mapping
from uuid import UUID

from api.app.config import get_config
from api.app.openstack_connection import build_openstack_connection
from db.src.models import Job as JobModel

from .handlers import ActionHandler

ALLOWED_VOLUME_STATUSES = frozenset({"available", "in-use"})
FAILED_BACKUP_STATUSES = frozenset({"error", "deleting"})
SUCCESSFUL_FULL_BACKUP_STATUSES = frozenset({"available"})
CINDER_API_TIMEOUT_SECONDS = 60.0
CREATE_BACKUP_TIMEOUT_SECONDS = 120.0

logger = logging.getLogger("qonos.worker.volume_backup")


def backup_status(backup: Any) -> str:
    return str(getattr(backup, "status", "") or "").lower()


def is_incremental_backup(backup: Any) -> bool:
    value = getattr(backup, "is_incremental", False)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def parse_backup_created_at(backup: Any) -> datetime | None:
    value = getattr(backup, "created_at", None)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def find_most_recent_full_backup(backups: list[Any]) -> Any | None:
    """Return the newest available non-incremental backup for the volume."""
    candidates: list[tuple[datetime, Any]] = []
    for backup in backups:
        if is_incremental_backup(backup):
            continue
        if backup_status(backup) not in SUCCESSFUL_FULL_BACKUP_STATUSES:
            continue
        created_at = parse_backup_created_at(backup)
        if created_at is None:
            continue
        candidates.append((created_at, backup))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


class CinderVolumeBackupHandler(ActionHandler):
    """Shared Cinder volume backup workflow for full and incremental handlers."""

    incremental: bool

    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ValueError("action_parameters must be an object")
        raw_id = parameters.get("volume_id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise ValueError("action_parameters.volume_id must be a non-empty string")
        volume_id = raw_id.strip()
        try:
            UUID(volume_id)
        except Exception as exc:
            raise ValueError("action_parameters.volume_id must be a UUID") from exc

        for field in ("backup_name_prefix", "description", "container"):
            value = parameters.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"action_parameters.{field} must be a string")

    async def execute(self, job: JobModel) -> Any:
        return await asyncio.to_thread(self._execute_sync, job)

    @abstractmethod
    def _execute_sync(self, job: JobModel) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _missing_trust_message(self) -> str:
        raise NotImplementedError

    def _poll_interval_seconds(self, cfg: Any) -> float:
        return cfg.volume_backup_poll_interval_seconds

    def _poll_timeout_seconds(self, cfg: Any) -> float:
        return cfg.volume_backup_poll_timeout_seconds

    def _backup_metadata(self, job: JobModel, **extra: str) -> dict[str, str]:
        metadata = {
            "qonos_managed": "true",
            "qonos_action_type": job.action_type,
            "qonos_job_id": job.id,
            "qonos_project_id": job.project_id,
            "qonos_schedule_id": job.schedule_id,
        }
        metadata.update(extra)
        return metadata

    def _cleanup_failed_backup(self, conn: Any, backup_id: str) -> None:
        try:
            conn.block_storage.delete_backup(backup_id, ignore_missing=True)
        except TypeError:
            try:
                conn.block_storage.delete_backup(backup_id)
            except Exception as exc:
                logger.warning(
                    "volume backup cleanup decision=delete_failed backup_id=%s error=%s",
                    backup_id,
                    str(exc),
                )
        except Exception as exc:
            logger.warning(
                "volume backup cleanup decision=delete_failed backup_id=%s error=%s",
                backup_id,
                str(exc),
            )

    def _call_with_timeout(
        self,
        pool: ThreadPoolExecutor,
        fn: Callable[[], Any],
        *,
        timeout: float,
        operation: str,
        subject_id: str,
        reraise: bool = False,
    ) -> Any | None:
        future = pool.submit(fn)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError:
            logger.warning(
                "volume backup %s timed out subject_id=%s timeout=%.0fs",
                operation,
                subject_id,
                timeout,
            )
            return None
        except Exception as exc:
            if reraise:
                raise
            logger.warning(
                "volume backup %s failed subject_id=%s error=%s",
                operation,
                subject_id,
                exc,
            )
            return None

    def _list_backups(
        self, pool: ThreadPoolExecutor, conn: Any, volume_id: str
    ) -> list[Any]:
        backups = self._call_with_timeout(
            pool,
            lambda: list(conn.block_storage.backups(details=True, volume_id=volume_id)),
            timeout=CINDER_API_TIMEOUT_SECONDS,
            operation="backups_list",
            subject_id=volume_id,
        )
        return backups or []

    def _lookup_backup(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        backup_id: str,
        volume_id: str,
    ) -> Any | None:
        backup = self._call_with_timeout(
            pool,
            lambda: conn.block_storage.get_backup(backup_id),
            timeout=CINDER_API_TIMEOUT_SECONDS,
            operation="get_backup",
            subject_id=backup_id,
        )
        if backup is not None:
            return backup

        for backup in self._list_backups(pool, conn, volume_id):
            if str(getattr(backup, "id", "") or "") == backup_id:
                return backup
        return None

    def _find_existing_job_backup(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        job: JobModel,
        volume_id: str,
    ) -> Any | None:
        for backup in self._list_backups(pool, conn, volume_id):
            if is_incremental_backup(backup) != self.incremental:
                continue
            meta = getattr(backup, "metadata", None) or {}
            if not isinstance(meta, dict):
                continue
            if meta.get("qonos_job_id") != job.id:
                continue
            if backup_status(backup) not in FAILED_BACKUP_STATUSES:
                return backup
        return None

    def _resolve_trust_id(self, params: Mapping[str, Any]) -> str:
        trust_id = str(params.get("qonos_trust_id") or "").strip() or None
        if not trust_id:
            raise RuntimeError(self._missing_trust_message())
        return trust_id

    def _backup_name_from_existing(self, existing_backup: Any) -> str:
        backup_id = str(getattr(existing_backup, "id"))
        existing_name = getattr(existing_backup, "name", None)
        if isinstance(existing_name, str) and existing_name:
            return existing_name
        return backup_id

    def _validate_volume_for_create(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        volume_id: str,
    ) -> tuple[Any, bool]:
        volume = self._call_with_timeout(
            pool,
            lambda: conn.block_storage.get_volume(volume_id),
            timeout=CINDER_API_TIMEOUT_SECONDS,
            operation="get_volume",
            subject_id=volume_id,
        )
        if volume is None:
            raise ValueError(f"Volume not found: {volume_id}")

        volume_status = str(getattr(volume, "status", "") or "").lower()
        if volume_status not in ALLOWED_VOLUME_STATUSES:
            raise ValueError(
                f"Volume not available for backup: {volume_id} status={volume_status}"
            )
        return volume, volume_status == "in-use"

    def _new_backup_name(self, *, prefix: str, volume: Any, volume_id: str) -> str:
        volume_name = getattr(volume, "name", None) or volume_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"{prefix}-{volume_name}-{ts}"[:255]

    def _create_volume_backup(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        *,
        volume_id: str,
        create_attrs: dict[str, Any],
        failure_prefix: str,
    ) -> str:
        try:
            backup = self._call_with_timeout(
                pool,
                lambda: conn.block_storage.create_backup(**create_attrs),
                timeout=CREATE_BACKUP_TIMEOUT_SECONDS,
                operation="create_backup",
                subject_id=volume_id,
                reraise=True,
            )
        except Exception as exc:
            raise RuntimeError(f"{failure_prefix}: {exc}") from exc
        if backup is None:
            raise RuntimeError(f"{failure_prefix}: timed out volume_id={volume_id}")
        backup_id = str(getattr(backup, "id", None) or backup)
        if not backup_id:
            raise RuntimeError(f"{failure_prefix}: missing backup id")
        return backup_id

    def _poll_until_available(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        *,
        job: JobModel,
        backup_id: str,
        volume_id: str,
        backup_name: str,
        poll_interval: float,
        timeout: float,
        result_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_status = "UNKNOWN"
        while time.monotonic() < deadline:
            backup = self._lookup_backup(pool, conn, backup_id, volume_id)
            if backup is None:
                logger.warning(
                    "volume backup poll decision=not_found_retry job_id=%s backup_id=%s",
                    job.id,
                    backup_id,
                )
                time.sleep(poll_interval)
                continue
            status = backup_status(backup)
            last_status = status or "UNKNOWN"
            logger.info(
                "volume backup poll job_id=%s backup_id=%s status=%s",
                job.id,
                backup_id,
                last_status,
            )
            if status == "available":
                result = {
                    "backup_id": backup_id,
                    "backup_name": backup_name,
                    "status": "available",
                    "volume_id": volume_id,
                }
                if result_extra:
                    result.update(result_extra)
                return result
            if status in FAILED_BACKUP_STATUSES:
                self._cleanup_failed_backup(conn, backup_id)
                raise RuntimeError(
                    f"Volume backup entered failed status: {status} backup_id={backup_id}"
                )
            time.sleep(poll_interval)

        logger.warning(
            "volume backup poll timed out job_id=%s backup_id=%s last_status=%s",
            job.id,
            backup_id,
            last_status,
        )
        raise TimeoutError(
            f"Volume backup polling timed out backup_id={backup_id} last_status={last_status}"
        )

    def _run_backup_job(
        self,
        job: JobModel,
        *,
        create_backup: Callable[
            [ThreadPoolExecutor, Any, str], tuple[str, str, dict[str, Any] | None]
        ],
        reuse_result_extra: Callable[[Any], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        cfg = get_config()
        params = job.action_parameters or {}
        volume_id = str(params["volume_id"]).strip()
        trust_id = self._resolve_trust_id(params)
        conn = build_openstack_connection(project_id=job.project_id, trust_id=trust_id)

        pool = ThreadPoolExecutor(max_workers=1)
        try:
            existing_backup = self._find_existing_job_backup(pool, conn, job, volume_id)
            result_extra: dict[str, Any] | None = None
            if existing_backup is not None:
                backup_id = str(getattr(existing_backup, "id"))
                backup_name = self._backup_name_from_existing(existing_backup)
                if reuse_result_extra is not None:
                    result_extra = reuse_result_extra(existing_backup) or None
                logger.info(
                    "volume backup decision=reuse_existing_backup job_id=%s backup_id=%s",
                    job.id,
                    backup_id,
                )
            else:
                backup_id, backup_name, result_extra = create_backup(pool, conn, volume_id)

            return self._poll_until_available(
                pool,
                conn,
                job=job,
                backup_id=backup_id,
                volume_id=volume_id,
                backup_name=backup_name,
                poll_interval=self._poll_interval_seconds(cfg),
                timeout=self._poll_timeout_seconds(cfg),
                result_extra=result_extra,
            )
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

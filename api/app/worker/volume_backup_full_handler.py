from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, TypeVar
from uuid import UUID

from api.app.config import get_config
from api.app.openstack_connection import build_openstack_connection
from db.src.models import Job as JobModel

from .handlers import ActionHandler

ALLOWED_VOLUME_STATUSES = frozenset({"available", "in-use"})
FAILED_BACKUP_STATUSES = frozenset({"error", "deleting"})
CINDER_API_TIMEOUT_SECONDS = 60.0
CREATE_BACKUP_TIMEOUT_SECONDS = 120.0
logger = logging.getLogger("qonos.worker.volume_backup_full")

T = TypeVar("T")


def _backup_status(backup: Any) -> str:
    return str(getattr(backup, "status", "") or "").lower()


class VolumeBackupFullActionHandler(ActionHandler):
    """Create and monitor full Cinder volume backups via the OpenStack SDK."""

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

    def _backup_metadata(self, job: JobModel) -> dict[str, str]:
        return {
            "qonos_managed": "true",
            "qonos_action_type": job.action_type,
            "qonos_job_id": job.id,
            "qonos_project_id": job.project_id,
            "qonos_schedule_id": job.schedule_id,
        }

    def _call_with_timeout(
        self,
        pool: ThreadPoolExecutor,
        fn: Callable[[], T],
        *,
        timeout: float,
        operation: str,
        subject_id: str,
        reraise: bool = False,
    ) -> T | None:
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
        """Resolve backup state; get_backup first, then volume-scoped list."""
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
        """Find an existing backup for this job to make retries idempotent."""
        for backup in self._list_backups(pool, conn, volume_id):
            meta = getattr(backup, "metadata", None) or {}
            if not isinstance(meta, dict):
                continue
            if meta.get("qonos_job_id") != job.id:
                continue
            if self._is_incremental_backup(backup):
                continue
            if _backup_status(backup) not in FAILED_BACKUP_STATUSES:
                return backup
        return None

    @staticmethod
    def _is_incremental_backup(backup: Any) -> bool:
        value = getattr(backup, "is_incremental", False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes"}
        return bool(value)

    def _execute_sync(self, job: JobModel) -> Any:
        cfg = get_config()
        params = job.action_parameters or {}
        volume_id = str(params["volume_id"]).strip()
        trust_id = str(params.get("qonos_trust_id") or "").strip() or None
        if not trust_id:
            raise RuntimeError(
                "Missing delegated trust context for volume_backup_full job execution."
            )

        default_prefix = f"qonos-{job.action_type}"
        prefix = str(params.get("backup_name_prefix") or default_prefix).strip()
        description = params.get("description")
        container = params.get("container")

        conn = build_openstack_connection(project_id=job.project_id, trust_id=trust_id)
        metadata = self._backup_metadata(job)

        pool = ThreadPoolExecutor(max_workers=1)
        try:
            existing_backup = self._find_existing_job_backup(pool, conn, job, volume_id)
            if existing_backup is not None:
                backup_id = str(getattr(existing_backup, "id"))
                existing_name = getattr(existing_backup, "name", None)
                backup_name = (
                    existing_name
                    if isinstance(existing_name, str) and existing_name
                    else str(backup_id)
                )
                logger.info(
                    "volume backup decision=reuse_existing_backup job_id=%s backup_id=%s",
                    job.id,
                    backup_id,
                )
            else:
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
                force = volume_status == "in-use"

                volume_name = getattr(volume, "name", None) or volume_id
                ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                backup_name = f"{prefix}-{volume_name}-{ts}"[:255]

                create_attrs: dict[str, Any] = {
                    "volume_id": volume_id,
                    "name": backup_name,
                    "force": force,
                    "is_incremental": False,
                    "metadata": metadata,
                }
                if isinstance(description, str) and description.strip():
                    create_attrs["description"] = description.strip()
                if isinstance(container, str) and container.strip():
                    create_attrs["container"] = container.strip()
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
                    raise RuntimeError(f"Failed to create volume backup: {exc}") from exc
                if backup is None:
                    raise RuntimeError(
                        f"Failed to create volume backup: timed out volume_id={volume_id}"
                    )
                backup_id = str(getattr(backup, "id", None) or backup)
                if not backup_id:
                    raise RuntimeError("Failed to create volume backup: missing backup id")
                logger.info(
                    "volume backup decision=created job_id=%s backup_id=%s volume_id=%s",
                    job.id,
                    backup_id,
                    volume_id,
                )

            poll_interval = cfg.volume_backup_full_poll_interval_seconds
            timeout = cfg.volume_backup_full_poll_timeout_seconds
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
                status = _backup_status(backup)
                last_status = status or "UNKNOWN"
                logger.info(
                    "volume backup poll job_id=%s backup_id=%s status=%s",
                    job.id,
                    backup_id,
                    last_status,
                )
                if status == "available":
                    return {
                        "backup_id": backup_id,
                        "backup_name": backup_name,
                        "status": "available",
                        "volume_id": volume_id,
                    }
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
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

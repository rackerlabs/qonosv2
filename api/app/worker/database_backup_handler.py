from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID

from api.app.config import get_config
from api.app.openstack_connection import build_openstack_connection
from db.src.models import Job as JobModel

from .database_backup_common import (
    ALLOWED_INSTANCE_STATUSES,
    FAILED_BACKUP_STATUSES,
    SUCCESSFUL_BACKUP_STATUSES,
    TROVE_API_TIMEOUT_SECONDS,
    backup_failure_detail,
    backup_status,
    create_instance_backup,
    delete_database_backup,
    get_instance_backup,
    list_instance_backups,
    parse_qonos_description,
    qonos_description_metadata,
)
from .handlers import ActionHandler

logger = logging.getLogger("qonos.worker.database_backup")

CREATE_BACKUP_TIMEOUT_SECONDS = 120.0


class DatabaseBackupActionHandler(ActionHandler):
    """Create and monitor Trove database instance backups via the OpenStack SDK."""

    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ValueError("action_parameters must be an object")
        raw_id = parameters.get("database_instance_id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise ValueError(
                "action_parameters.database_instance_id must be a non-empty string"
            )
        database_instance_id = raw_id.strip()
        try:
            UUID(database_instance_id)
        except Exception as exc:
            raise ValueError(
                "action_parameters.database_instance_id must be a UUID"
            ) from exc
        for field in ("backup_name_prefix", "description"):
            value = parameters.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"action_parameters.{field} must be a string")

    async def execute(self, job: JobModel) -> Any:
        return await asyncio.to_thread(self._execute_sync, job)

    def _execute_sync(self, job: JobModel) -> dict[str, Any]:
        cfg = get_config()
        params = job.action_parameters or {}
        database_instance_id = str(params["database_instance_id"]).strip()
        trust_id = str(params.get("qonos_trust_id") or "").strip() or None
        if not trust_id:
            raise RuntimeError(
                "Missing delegated trust context for database_backup job execution."
            )

        default_prefix = f"qonos-{job.action_type}"
        prefix = str(params.get("backup_name_prefix") or default_prefix).strip()
        user_description = params.get("description")
        user_description_str = (
            user_description.strip()
            if isinstance(user_description, str) and user_description.strip()
            else None
        )

        conn = build_openstack_connection(project_id=job.project_id, trust_id=trust_id)
        database = conn.database

        with ThreadPoolExecutor(max_workers=1) as pool:
            instance = self._call_with_timeout(
                pool,
                lambda: database.get_instance(database_instance_id),
                timeout=TROVE_API_TIMEOUT_SECONDS,
                operation="get_instance",
                subject_id=database_instance_id,
            )
            if instance is None:
                raise ValueError(f"Database instance not found: {database_instance_id}")

            instance_status = str(getattr(instance, "status", "") or "").lower()
            if instance_status not in ALLOWED_INSTANCE_STATUSES:
                raise ValueError(
                    f"Database instance not ready for backup: {database_instance_id} "
                    f"status={instance_status}"
                )

            existing = self._find_existing_job_backup(pool, conn, job, database_instance_id)
            if existing is not None:
                backup_id = str(getattr(existing, "id"))
                backup_name = str(getattr(existing, "name", "") or backup_id)
                logger.info(
                    "database backup decision=reuse_existing_backup job_id=%s backup_id=%s",
                    job.id,
                    backup_id,
                )
            else:
                instance_name = str(getattr(instance, "name", "") or database_instance_id)
                ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                backup_name = f"{prefix}-{instance_name}-{ts}"[:255]
                description = qonos_description_metadata(
                    job_id=job.id,
                    schedule_id=job.schedule_id,
                    project_id=job.project_id,
                    action_type=job.action_type,
                    user_description=user_description_str,
                )
                backup = self._call_with_timeout(
                    pool,
                    lambda: create_instance_backup(
                        conn,
                        name=backup_name,
                        instance_id=database_instance_id,
                        description=description,
                    ),
                    timeout=CREATE_BACKUP_TIMEOUT_SECONDS,
                    operation="create_backup",
                    subject_id=database_instance_id,
                    reraise=True,
                )
                if backup is None:
                    raise RuntimeError(
                        f"Failed to create database backup: timed out database_instance_id={database_instance_id}"
                    )
                backup_id = str(getattr(backup, "id", "") or "")
                if not backup_id:
                    raise RuntimeError("Failed to create database backup: missing backup id")

            return self._poll_until_complete(
                pool,
                conn,
                database_instance_id=database_instance_id,
                backup_id=backup_id,
                backup_name=backup_name,
                poll_interval=cfg.volume_backup_poll_interval_seconds,
                timeout=cfg.volume_backup_poll_timeout_seconds,
            )

    def _poll_until_complete(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        *,
        database_instance_id: str,
        backup_id: str,
        backup_name: str,
        poll_interval: float,
        timeout: float,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            backup = self._lookup_backup(pool, conn, backup_id, database_instance_id)
            if backup is None:
                time.sleep(poll_interval)
                continue
            status = backup_status(backup)
            if status in SUCCESSFUL_BACKUP_STATUSES:
                return {
                    "database_instance_id": database_instance_id,
                    "backup_id": backup_id,
                    "backup_name": backup_name,
                    "status": status,
                }
            if status in FAILED_BACKUP_STATUSES:
                detail = backup_failure_detail(backup)
                delete_database_backup(conn, backup_id)
                message = f"Database backup failed: backup_id={backup_id} status={status}"
                if detail:
                    message = f"{message} detail={detail}"
                raise RuntimeError(message)
            time.sleep(poll_interval)

        delete_database_backup(conn, backup_id)
        raise RuntimeError(
            f"Database backup timed out: backup_id={backup_id} database_instance_id={database_instance_id}"
        )

    def _find_existing_job_backup(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        job: JobModel,
        database_instance_id: str,
    ) -> Any | None:
        for backup in self._list_backups(pool, conn, database_instance_id):
            meta = parse_qonos_description(getattr(backup, "description", None))
            if meta.get("qonos_job_id") != job.id:
                continue
            if backup_status(backup) not in FAILED_BACKUP_STATUSES:
                return backup
        return None

    def _list_backups(
        self, pool: ThreadPoolExecutor, conn: Any, database_instance_id: str
    ) -> list[Any]:
        backups = self._call_with_timeout(
            pool,
            lambda: list_instance_backups(conn, instance_id=database_instance_id),
            timeout=TROVE_API_TIMEOUT_SECONDS,
            operation="backups_list",
            subject_id=database_instance_id,
        )
        return backups or []

    def _lookup_backup(
        self,
        pool: ThreadPoolExecutor,
        conn: Any,
        backup_id: str,
        database_instance_id: str,
    ) -> Any | None:
        backup = self._call_with_timeout(
            pool,
            lambda: get_instance_backup(conn, backup_id),
            timeout=TROVE_API_TIMEOUT_SECONDS,
            operation="get_backup",
            subject_id=backup_id,
        )
        if backup is not None:
            return backup
        for item in self._list_backups(pool, conn, database_instance_id):
            if str(getattr(item, "id", "") or "") == backup_id:
                return item
        return None

    def _call_with_timeout(
        self,
        pool: ThreadPoolExecutor,
        fn: Any,
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
                "database backup %s timed out subject_id=%s timeout=%.0fs",
                operation,
                subject_id,
                timeout,
            )
            return None
        except Exception as exc:
            if reraise:
                raise
            logger.warning(
                "database backup %s failed subject_id=%s error=%s",
                operation,
                subject_id,
                exc,
            )
            return None

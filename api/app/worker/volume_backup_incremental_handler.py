from __future__ import annotations

import logging
import time
from typing import Any

from db.src.models import Job as JobModel

from . import volume_backup_common as _vbc
from .volume_backup_common import (
    CinderVolumeBackupHandler,
    backup_status,
    find_most_recent_full_backup,
    is_incremental_backup,
)

logger = logging.getLogger("qonos.worker.volume_backup_incremental")


class VolumeBackupIncrementalActionHandler(CinderVolumeBackupHandler):
    """Create and monitor incremental Cinder volume backups via the OpenStack SDK."""

    incremental = True

    def _missing_trust_message(self) -> str:
        return "Missing delegated trust context for volume_backup_incremental job execution."

    def _execute_sync(self, job: JobModel) -> Any:
        cfg = _vbc.get_config()
        params = job.action_parameters or {}
        default_prefix = f"qonos-{job.action_type}"
        prefix = str(params.get("backup_name_prefix") or default_prefix).strip()
        description = params.get("description")
        container = params.get("container")

        def create_backup(pool, conn, volume_id: str) -> tuple[str, str, dict[str, Any] | None]:
            wait_deadline = time.monotonic() + cfg.volume_backup_coordination_wait_timeout_seconds
            while True:
                volume_backups = self._list_backups(pool, conn, volume_id)
                full_in_progress = any(
                    (not is_incremental_backup(b)) and backup_status(b) == "creating"
                    for b in volume_backups
                )
                if not full_in_progress:
                    break
                if time.monotonic() >= wait_deadline:
                    raise RuntimeError(
                        "Full backup currently in progress for volume "
                        f"{volume_id}; timed out waiting and will retry later"
                    )
                logger.info(
                    "volume backup coordination decision=wait_for_full volume_id=%s",
                    volume_id,
                )
                time.sleep(cfg.volume_backup_poll_interval_seconds)

            parent_full = find_most_recent_full_backup(volume_backups)
            if parent_full is None:
                raise ValueError(
                    "No successful full backup found for volume "
                    f"{volume_id}; create a volume_backup_full backup before "
                    "running volume_backup_incremental"
                )
            parent_full_backup_id = str(getattr(parent_full, "id", "") or "")
            if not parent_full_backup_id:
                raise ValueError(
                    "No successful full backup found for volume "
                    f"{volume_id}; create a volume_backup_full backup before "
                    "running volume_backup_incremental"
                )

            volume, force = self._validate_volume_for_create(
                pool,
                conn,
                volume_id,
                coordination_wait_seconds=cfg.volume_backup_coordination_wait_timeout_seconds,
                poll_interval=cfg.volume_backup_poll_interval_seconds,
            )
            backup_name = self._new_backup_name(prefix=prefix, volume=volume, volume_id=volume_id)
            metadata = self._backup_metadata(job, qonos_parent_full_backup_id=parent_full_backup_id)
            create_attrs: dict[str, Any] = {
                "volume_id": volume_id,
                "name": backup_name,
                "force": force,
                "is_incremental": True,
                "metadata": metadata,
            }
            if isinstance(description, str) and description.strip():
                create_attrs["description"] = description.strip()
            if isinstance(container, str) and container.strip():
                create_attrs["container"] = container.strip()
            backup_id = self._create_volume_backup(
                pool,
                conn,
                volume_id=volume_id,
                create_attrs=create_attrs,
                failure_prefix="Failed to create incremental volume backup",
            )
            logger.info(
                "volume backup decision=created_incremental job_id=%s backup_id=%s "
                "volume_id=%s parent_full_backup_id=%s",
                job.id,
                backup_id,
                volume_id,
                parent_full_backup_id,
            )
            return backup_id, backup_name, {"parent_full_backup_id": parent_full_backup_id}

        def reuse_result_extra(existing_backup: Any) -> dict[str, Any]:
            meta = getattr(existing_backup, "metadata", None) or {}
            if isinstance(meta, dict):
                raw_parent = meta.get("qonos_parent_full_backup_id")
                if isinstance(raw_parent, str) and raw_parent.strip():
                    return {"parent_full_backup_id": raw_parent.strip()}
            return {}

        return self._run_backup_job(
            job,
            create_backup=create_backup,
            reuse_result_extra=reuse_result_extra,
        )

from __future__ import annotations

from typing import Any

from db.src.models import Job as JobModel

from .volume_backup_common import CinderVolumeBackupHandler


class VolumeBackupFullActionHandler(CinderVolumeBackupHandler):
    """Create and monitor full Cinder volume backups via the OpenStack SDK."""

    incremental = False

    def _missing_trust_message(self) -> str:
        return "Missing delegated trust context for volume_backup_full job execution."

    def _execute_sync(self, job: JobModel) -> Any:
        params = job.action_parameters or {}
        default_prefix = f"qonos-{job.action_type}"
        prefix = str(params.get("backup_name_prefix") or default_prefix).strip()
        description = params.get("description")
        container = params.get("container")
        metadata = self._backup_metadata(job)

        def create_backup(pool, conn, volume_id: str) -> tuple[str, str, dict[str, Any] | None]:
            volume, force = self._validate_volume_for_create(pool, conn, volume_id)
            backup_name = self._new_backup_name(prefix=prefix, volume=volume, volume_id=volume_id)
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
            backup_id = self._create_volume_backup(
                pool,
                conn,
                volume_id=volume_id,
                create_attrs=create_attrs,
                failure_prefix="Failed to create volume backup",
            )
            return backup_id, backup_name, None

        return self._run_backup_job(job, create_backup=create_backup)

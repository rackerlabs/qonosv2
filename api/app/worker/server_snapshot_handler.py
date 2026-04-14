from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID

from api.app.config import get_config
from db.src.models import Job as JobModel

from .handlers import ActionHandler

FAILED_IMAGE_STATUSES = {"KILLED", "DELETED", "PENDING_DELETE", "ERROR"}
logger = logging.getLogger("qonos.worker.server_snapshot")


class ServerSnapshotActionHandler(ActionHandler):
    """Action handler that creates and monitors Nova server snapshots via OpenStack SDK."""

    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ValueError("action_parameters must be an object")
        raw_id = parameters.get("server_id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise ValueError("action_parameters.server_id must be a non-empty string")
        server_id = raw_id.strip()
        try:
            UUID(server_id)
        except Exception as exc:
            raise ValueError("action_parameters.server_id must be a UUID") from exc
        prefix = parameters.get("image_name_prefix")
        if prefix is not None and not isinstance(prefix, str):
            raise ValueError("action_parameters.image_name_prefix must be a string")

    async def execute(self, job: JobModel) -> Any:
        return await asyncio.to_thread(self._execute_sync, job)

    def _build_connection(self, *, project_id: str):
        cfg = get_config()
        if not cfg.keystone_url or not cfg.keystone_admin_token:
            raise RuntimeError("Missing Keystone configuration: keystone_url and admin_token")
        from openstack import connection as os_connection

        return os_connection.Connection(
            auth_type="v3token",
            auth={
                "auth_url": cfg.keystone_url,
                "token": cfg.keystone_admin_token,
                "project_id": project_id,
            },
            verify=cfg.keystone_verify_ssl,
            compute_api_version="2",
        )

    def _cleanup_failed_snapshot(self, conn: Any, image_id: str) -> None:
        """Best-effort delete of a snapshot created by a failed job attempt."""
        try:
            conn.image.delete_image(image_id, ignore_missing=True)
        except TypeError:
            try:
                conn.image.delete_image(image_id)
            except Exception as exc:
                logger.warning(
                    "snapshot cleanup decision=delete_failed image_id=%s error=%s",
                    image_id,
                    str(exc),
                )
        except Exception as exc:
            logger.warning(
                "snapshot cleanup decision=delete_failed image_id=%s error=%s",
                image_id,
                str(exc),
            )

    def _find_existing_job_image(self, conn: Any, job: JobModel) -> Any | None:
        """Find an existing image for this job to make retries idempotent."""
        try:
            images = conn.image.images(owner=job.project_id)
        except Exception:
            return None
        for img in images:
            props = getattr(img, "properties", None) or {}
            if not isinstance(props, dict):
                continue
            if props.get("qonos_job_id") != job.id:
                continue
            status = str(getattr(img, "status", "") or "").upper()
            if status not in FAILED_IMAGE_STATUSES and status != "DELETED":
                return img
        return None

    def _execute_sync(self, job: JobModel) -> Any:
        cfg = get_config()
        params = job.action_parameters or {}
        server_id = str(params["server_id"]).strip()
        default_prefix = f"qonos-{job.action_type}"
        prefix = str(params.get("image_name_prefix") or default_prefix).strip()
        conn = self._build_connection(project_id=job.project_id)

        server = conn.compute.get_server(server_id)
        if server is None:
            raise ValueError(f"Server not found: {server_id}")
        server_name = getattr(server, "name", None) or server_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        image_name = f"{prefix}-{server_name}-{ts}"
        image_name = image_name[:255]
        metadata = {
            "qonos_managed": "true",
            "qonos_action_type": job.action_type,
            "qonos_job_id": job.id,
            "qonos_project_id": job.project_id,
            "qonos_schedule_id": job.schedule_id,
        }

        existing_image = self._find_existing_job_image(conn, job)
        if existing_image is not None:
            image_id = str(getattr(existing_image, "id"))
            existing_name = getattr(existing_image, "name", None)
            if isinstance(existing_name, str) and existing_name:
                image_name = existing_name
            logger.info(
                "snapshot decision=reuse_existing_image job_id=%s image_id=%s",
                job.id,
                image_id,
            )
        else:
            try:
                image_id = conn.compute.create_server_image(
                    server=server,
                    name=image_name,
                    metadata=metadata,
                )
            except Exception as exc:
                raise RuntimeError(f"Failed to create server snapshot: {exc}") from exc

        poll_interval = cfg.server_snapshot_poll_interval_seconds
        timeout = cfg.server_snapshot_poll_timeout_seconds
        deadline = time.monotonic() + timeout
        last_status = "UNKNOWN"
        try:
            while time.monotonic() < deadline:
                image = conn.image.get_image(image_id)
                if image is None:
                    raise RuntimeError(f"Snapshot image not found after creation: {image_id}")
                status = str(getattr(image, "status", "") or "").upper()
                last_status = status or "UNKNOWN"
                if status == "ACTIVE":
                    return {
                        "image_id": image_id,
                        "image_name": image_name,
                        "status": "ACTIVE",
                    }
                if status in FAILED_IMAGE_STATUSES:
                    raise RuntimeError(
                        f"Snapshot image entered failed status: {status} image_id={image_id}"
                    )
                time.sleep(poll_interval)

            raise TimeoutError(
                f"Snapshot polling timed out image_id={image_id} last_status={last_status}"
            )
        except Exception:
            self._cleanup_failed_snapshot(conn, image_id)
            raise

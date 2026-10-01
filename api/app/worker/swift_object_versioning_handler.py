from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Mapping

from api.app.openstack_connection import build_openstack_connection
from db.src.models import Job as JobModel

from .handlers import ActionHandler
from .swift_common import (
    MANIFEST_OBJECT_NAME,
    VERSIONING_MODE_SNAPSHOT,
    extract_object_metadata,
    get_container_or_raise,
    list_container_objects,
    map_swift_exception,
    normalize_versioning_mode,
    qonos_object_metadata,
    snapshot_manifest_name,
    snapshot_root,
)

logger = logging.getLogger("qonos.worker.swift_object_versioning")


class SwiftObjectVersioningActionHandler(ActionHandler):
    """Swift object versioning (snapshot mode: copy container into archive)."""

    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ValueError("action_parameters must be an object")

        normalize_versioning_mode(parameters.get("mode"))

        container = parameters.get("container")
        if not isinstance(container, str) or not container.strip():
            raise ValueError("action_parameters.container must be a non-empty string")

        target_container = parameters.get("target_container")
        if not isinstance(target_container, str) or not target_container.strip():
            raise ValueError("action_parameters.target_container must be a non-empty string")

        object_prefix = parameters.get("object_prefix")
        if object_prefix is not None and not isinstance(object_prefix, str):
            raise ValueError("action_parameters.object_prefix must be a string")

        if container.strip() == target_container.strip():
            raise ValueError("action_parameters.container and target_container must differ")

    async def execute(self, job: JobModel) -> Any:
        return await asyncio.to_thread(self._execute_sync, job)

    def _execute_sync(self, job: JobModel) -> dict[str, Any]:
        params = job.action_parameters or {}
        trust_id = str(params.get("qonos_trust_id") or "").strip() or None
        if not trust_id:
            raise RuntimeError(
                "Missing delegated trust context for swift_object_versioning job execution."
            )

        normalize_versioning_mode(params.get("mode"))

        container = str(params["container"]).strip()
        target_container = str(params["target_container"]).strip()
        object_prefix = (
            str(params["object_prefix"]).strip() if params.get("object_prefix") else None
        )

        conn = build_openstack_connection(project_id=job.project_id, trust_id=trust_id)
        return self._snapshot_container(
            object_store=conn.object_store,
            job=job,
            source_container=container,
            target_container=target_container,
            object_prefix=object_prefix,
        )

    def _snapshot_container(
        self,
        *,
        object_store: Any,
        job: JobModel,
        source_container: str,
        target_container: str,
        object_prefix: str | None,
    ) -> dict[str, Any]:
        get_container_or_raise(object_store, source_container)
        try:
            get_container_or_raise(object_store, target_container)
        except ValueError:
            try:
                object_store.create_container(name=target_container)
            except Exception as exc:
                raise map_swift_exception(
                    exc, context=f"creating target container '{target_container}'"
                ) from exc

        root = snapshot_root(schedule_id=job.schedule_id, job_id=job.id)
        manifest_name = snapshot_manifest_name(schedule_id=job.schedule_id, job_id=job.id)

        existing_manifest = self._find_existing_manifest(
            object_store=object_store,
            target_container=target_container,
            manifest_name=manifest_name,
            job_id=job.id,
        )
        if existing_manifest is not None:
            logger.info(
                "swift versioning decision=reuse_existing_manifest job_id=%s manifest=%s",
                job.id,
                manifest_name,
            )
            return existing_manifest

        metadata_extra_base = {
            "qonos_versioning_mode": VERSIONING_MODE_SNAPSHOT,
            "qonos_snapshot_root": root,
            "qonos_source_container": source_container,
        }
        copied = 0
        copy_failed = 0
        for obj in list_container_objects(
            object_store, source_container, prefix=object_prefix or None
        ):
            name = str(getattr(obj, "name", "") or "")
            if name.endswith(f"/{MANIFEST_OBJECT_NAME}"):
                continue
            destination = f"{root}/{name}"
            try:
                self._copy_object(
                    object_store=object_store,
                    source_container=source_container,
                    source_name=name,
                    target_container=target_container,
                    target_name=destination,
                    metadata=qonos_object_metadata(
                        job_id=job.id,
                        schedule_id=job.schedule_id,
                        project_id=job.project_id,
                        action_type=job.action_type,
                        extra={**metadata_extra_base, "qonos_source_object": name},
                    ),
                )
                copied += 1
            except Exception as exc:
                copy_failed += 1
                logger.warning(
                    "swift versioning decision=copy_failed source=%s/%s error=%s",
                    source_container,
                    name,
                    str(map_swift_exception(exc, context="copy object")),
                )

        created_at = datetime.now(timezone.utc).isoformat()
        manifest_metadata = qonos_object_metadata(
            job_id=job.id,
            schedule_id=job.schedule_id,
            project_id=job.project_id,
            action_type=job.action_type,
            extra={
                **metadata_extra_base,
                "qonos_objects_copied": str(copied),
                "qonos_created_at": created_at,
            },
        )
        try:
            object_store.upload_object(
                container=target_container,
                name=manifest_name,
                data=b"",
                metadata=manifest_metadata,
            )
        except Exception as exc:
            raise map_swift_exception(exc, context="writing snapshot manifest") from exc

        if copy_failed > 0:
            raise RuntimeError(
                f"snapshot failed: {copy_failed} object(s) failed to copy "
                f"(copied={copied})"
            )

        return {
            "mode": VERSIONING_MODE_SNAPSHOT,
            "container": source_container,
            "target_container": target_container,
            "snapshot_root": root,
            "manifest_object": manifest_name,
            "objects_copied": copied,
            "copy_failed": copy_failed,
        }

    def _copy_object(
        self,
        *,
        object_store: Any,
        source_container: str,
        source_name: str,
        target_container: str,
        target_name: str,
        metadata: dict[str, str],
    ) -> None:
        try:
            object_store.copy_object(
                source_name,
                container=source_container,
                dest_container=target_container,
                dest_name=target_name,
                metadata=metadata,
            )
            return
        except Exception as exc:
            logger.debug(
                "swift versioning decision=copy_object_fallback source=%s/%s error=%s",
                source_container,
                source_name,
                str(exc),
            )

        downloaded = object_store.download_object(source_name, container=source_container)
        data = downloaded if isinstance(downloaded, (bytes, bytearray)) else b""
        if hasattr(downloaded, "read"):
            data = downloaded.read()
        object_store.upload_object(
            container=target_container,
            name=target_name,
            data=data,
            metadata=metadata,
        )

    def _find_existing_manifest(
        self,
        *,
        object_store: Any,
        target_container: str,
        manifest_name: str,
        job_id: str,
    ) -> dict[str, Any] | None:
        try:
            metadata = object_store.get_object_metadata(manifest_name, container=target_container)
        except Exception:
            return None
        props = extract_object_metadata(metadata)
        if props.get("qonos_job_id") != job_id:
            return None
        return {
            "mode": VERSIONING_MODE_SNAPSHOT,
            "target_container": target_container,
            "manifest_object": manifest_name,
            "snapshot_root": props.get("qonos_snapshot_root"),
            "objects_copied": int(props.get("qonos_objects_copied") or 0),
            "reused": True,
        }

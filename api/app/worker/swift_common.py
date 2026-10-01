from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from openstack.exceptions import ForbiddenException, NotFoundException

MANIFEST_OBJECT_NAME = "__qonos_snapshot_manifest__"
SNAPSHOT_ROOT_PREFIX = ".qonos/snapshots"
VERSIONING_MODE_SNAPSHOT = "snapshot"


def normalize_versioning_mode(value: Any) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return VERSIONING_MODE_SNAPSHOT
    if not isinstance(value, str):
        raise ValueError("action_parameters.mode must be a string")
    mode = value.strip().lower()
    if mode != VERSIONING_MODE_SNAPSHOT:
        raise ValueError("action_parameters.mode must be 'snapshot'")
    return mode


class SwiftOperationError(Exception):
    """Raised for Swift operation failures with a stable error code."""


def map_swift_exception(exc: Exception, *, context: str) -> Exception:
    if isinstance(exc, NotFoundException):
        return ValueError(f"{context}: container or object not found")
    if isinstance(exc, ForbiddenException):
        return PermissionError(f"{context}: permission denied")
    message = str(exc).lower()
    if "quota" in message or "entity too large" in message or "507" in message:
        return SwiftOperationError(f"{context}: quota exceeded")
    if "403" in message or "forbidden" in message or "permission denied" in message:
        return PermissionError(f"{context}: permission denied")
    if "404" in message or "not found" in message:
        return ValueError(f"{context}: container or object not found")
    return exc


def qonos_object_metadata(
    *,
    job_id: str,
    schedule_id: str,
    project_id: str,
    action_type: str,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    metadata = {
        "qonos_managed": "true",
        "qonos_action_type": action_type,
        "qonos_job_id": job_id,
        "qonos_project_id": project_id,
        "qonos_schedule_id": schedule_id,
    }
    if extra:
        metadata.update(extra)
    return metadata


def snapshot_root(*, schedule_id: str, job_id: str) -> str:
    return f"{SNAPSHOT_ROOT_PREFIX}/{schedule_id}/{job_id}"


def snapshot_manifest_name(*, schedule_id: str, job_id: str) -> str:
    return f"{snapshot_root(schedule_id=schedule_id, job_id=job_id)}/{MANIFEST_OBJECT_NAME}"


def extract_object_metadata(metadata: Any) -> dict[str, str]:
    props = metadata if isinstance(metadata, dict) else getattr(metadata, "metadata", {}) or {}
    if not isinstance(props, dict):
        return {}
    nested = props.get("metadata")
    if isinstance(nested, dict):
        props = nested
    result: dict[str, str] = {}
    for key, value in props.items():
        key_str = str(key)
        if key_str.lower().startswith("x-object-meta-"):
            key_str = key_str[len("x-object-meta-") :]
        result[key_str] = str(value)
    return result


def delete_snapshot_tree_best_effort(
    object_store: Any,
    *,
    target_container: str,
    snapshot_root: str,
) -> None:
    """Remove partial snapshot objects under snapshot_root (used on copy failure)."""
    prefix = f"{snapshot_root}/"
    for obj in list_container_objects(object_store, target_container, prefix=prefix):
        name = str(getattr(obj, "name", "") or "")
        if not name:
            continue
        try:
            object_store.delete_object(name, container=target_container)
        except Exception:
            continue


def parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_container_or_raise(object_store: Any, container_name: str) -> Any:
    try:
        get_metadata = getattr(object_store, "get_container_metadata", None)
        if get_metadata is not None:
            container = get_metadata(container_name)
        else:
            container = object_store.get_container(container_name)
    except Exception as exc:
        raise map_swift_exception(exc, context=f"container '{container_name}'") from exc
    if container is None:
        raise ValueError(f"container '{container_name}' not found")
    return container


def list_container_objects(
    object_store: Any,
    container_name: str,
    *,
    prefix: str | None = None,
) -> list[Any]:
    try:
        query: dict[str, str] = {}
        if prefix:
            query["prefix"] = prefix
        return list(object_store.objects(container_name, **query))
    except Exception as exc:
        raise map_swift_exception(exc, context=f"listing objects in '{container_name}'") from exc

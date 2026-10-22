from __future__ import annotations

import json
import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger("qonos.worker.database_backup")

TROVE_API_TIMEOUT_SECONDS = 60.0
TROVE_SERVICE_TYPE = "database"
ALLOWED_INSTANCE_STATUSES = frozenset({"active", "healthy"})
SUCCESSFUL_BACKUP_STATUSES = frozenset({"completed"})
FAILED_BACKUP_STATUSES = frozenset({"failed", "delete_failed"})
SKIP_TROVE_BACKUP_STATUSES = frozenset({"building", "deleting", "deleted"})
TROVE_BACKUP_DESCRIPTION_MAX_LENGTH = 255


def backup_status(backup: Any) -> str:
    return str(getattr(backup, "status", "") or "").lower()


def backup_failure_detail(backup: Any) -> str:
    fault = getattr(backup, "fault", None)
    if isinstance(fault, dict):
        message = fault.get("message") or fault.get("details")
        if isinstance(message, str) and message.strip():
            return message.strip()
        if fault:
            return str(fault)
    for attr in ("status_description", "message"):
        value = getattr(backup, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def parse_backup_created_at(backup: Any) -> datetime | None:
    value = getattr(backup, "created", None) or getattr(backup, "created_at", None)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def qonos_description_metadata(
    *,
    job_id: str,
    schedule_id: str,
    project_id: str,
    action_type: str,
    user_description: str | None = None,
) -> str:
    _ = project_id  # retained for call-site compatibility; not stored in description
    payload: dict[str, str] = {
        "m": "1",
        "a": action_type,
        "j": job_id,
        "s": schedule_id,
    }
    encoded = json.dumps(payload, separators=(",", ":"))
    if user_description:
        note = user_description.strip()
        if note:
            room = TROVE_BACKUP_DESCRIPTION_MAX_LENGTH - len(encoded) - len(',"u":""') - 1
            if room > 0:
                payload["u"] = note[:room]
                encoded = json.dumps(payload, separators=(",", ":"))
    if len(encoded) > TROVE_BACKUP_DESCRIPTION_MAX_LENGTH:
        raise ValueError(
            f"Trove backup description exceeds {TROVE_BACKUP_DESCRIPTION_MAX_LENGTH} characters"
        )
    return encoded


def parse_qonos_description(description: Any) -> dict[str, str]:
    if not isinstance(description, str) or not description.strip():
        return {}
    try:
        data = json.loads(description)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    raw = {str(k): str(v) for k, v in data.items()}
    if raw.get("m") != "1":
        return {}
    return {
        "qonos_managed": "true",
        "qonos_action_type": raw.get("a") or "",
        "qonos_job_id": raw.get("j") or "",
        "qonos_schedule_id": raw.get("s") or "",
        "qonos_user_description": raw.get("u") or "",
    }


def _trove_endpoint_filter(conn: Any) -> dict[str, str]:
    interface = "internal"
    config = getattr(conn, "config", None)
    if config is not None:
        interface = getattr(config, "get_interface", lambda _t: "internal")(TROVE_SERVICE_TYPE)
    return {"service_type": TROVE_SERVICE_TYPE, "interface": interface}


def _trove_session_request(
    conn: Any,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> Any:
    session = conn.session
    endpoint_filter = _trove_endpoint_filter(conn)
    try:
        return session.request(
            path,
            method=method,
            endpoint_filter=endpoint_filter,
            params=params,
            json=json_body,
        )
    except Exception as exc:
        http_status = getattr(exc, "http_status", None)
        resp = getattr(exc, "response", None)
        text = str(getattr(resp, "text", "") or "")[:800] if resp is not None else str(exc)
        if http_status is not None:
            raise RuntimeError(
                f"Trove {method} {path} failed: HTTP {http_status} body={text or '(empty)'}"
            ) from exc
        raise RuntimeError(f"Trove {method} {path} failed: {exc}") from exc


def _trove_request(
    conn: Any,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = _trove_session_request(
        conn, method, path, params=params, json_body=json_body
    )
    status_code = int(getattr(response, "status_code", 0) or 0)
    text = str(getattr(response, "text", "") or "")
    if status_code >= 400:
        snippet = text[:800].replace("\n", " ").strip()
        raise RuntimeError(
            f"Trove {method} {path} failed: HTTP {status_code} body={snippet or '(empty)'}"
        )
    if not text.strip():
        return {}
    try:
        payload = response.json()
    except Exception as exc:
        snippet = text[:800].replace("\n", " ").strip()
        raise RuntimeError(
            f"Trove {method} {path} returned non-JSON: HTTP {status_code} body={snippet}"
        ) from exc
    if isinstance(payload, dict):
        return payload
    raise RuntimeError(f"Trove {method} {path} returned unexpected JSON type")


def _backup_from_dict(data: dict[str, Any]) -> SimpleNamespace:
    fault = data.get("fault")
    if fault is not None and not isinstance(fault, dict):
        fault = {"message": str(fault)}
    return SimpleNamespace(
        id=str(data.get("id") or ""),
        name=data.get("name"),
        status=data.get("status"),
        description=data.get("description"),
        instance_id=data.get("instance_id"),
        project_id=data.get("project_id"),
        created=data.get("created"),
        created_at=data.get("created"),
        fault=fault,
        status_description=data.get("status_description"),
        message=data.get("message"),
    )


def create_instance_backup(
    conn: Any,
    *,
    name: str,
    instance_id: str,
    description: str | None = None,
) -> SimpleNamespace:
    body: dict[str, Any] = {
        "backup": {
            "name": name,
            "instance": instance_id,
        }
    }
    if description:
        body["backup"]["description"] = description
    payload = _trove_request(conn, "POST", "/backups", json_body=body)
    backup_data = payload.get("backup")
    if not isinstance(backup_data, dict):
        raise RuntimeError("Trove create backup returned unexpected response")
    return _backup_from_dict(backup_data)


def list_instance_backups(conn: Any, *, instance_id: str) -> list[SimpleNamespace]:
    payload = _trove_request(
        conn, "GET", "/backups", params={"instance_id": instance_id}
    )
    items = payload.get("backups")
    if not isinstance(items, list):
        return []
    return [_backup_from_dict(item) for item in items if isinstance(item, dict)]


def get_instance_backup(conn: Any, backup_id: str) -> SimpleNamespace | None:
    try:
        response = _trove_session_request(conn, "GET", f"/backups/{backup_id}")
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return None
        return None
    except Exception:
        return None
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 404:
        return None
    if status_code >= 400:
        return None
    text = str(getattr(response, "text", "") or "")
    if not text.strip():
        return None
    try:
        payload = response.json()
    except Exception:
        return None
    backup_data = payload.get("backup") if isinstance(payload, dict) else None
    if not isinstance(backup_data, dict):
        return None
    return _backup_from_dict(backup_data)


def delete_instance_backup(conn: Any, backup_id: str) -> bool:
    try:
        response = _trove_session_request(conn, "DELETE", f"/backups/{backup_id}")
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return True
        logger.warning(
            "database backup delete failed backup_id=%s error=%s",
            backup_id,
            str(exc),
        )
        return False
    except Exception as exc:
        logger.warning(
            "database backup delete failed backup_id=%s error=%s",
            backup_id,
            str(exc),
        )
        return False
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code in (404, 204, 202, 200):
        return True
    if status_code >= 400:
        text = str(getattr(response, "text", "") or "")[:500]
        logger.warning(
            "database backup delete failed backup_id=%s status=%s body=%s",
            backup_id,
            status_code,
            text,
        )
        return False
    return True


def delete_database_backup(conn: Any, backup_id: str) -> bool:
    return delete_instance_backup(conn, backup_id)

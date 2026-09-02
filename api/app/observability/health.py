from __future__ import annotations

import time
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Callable, Optional

_PROCESS_STARTED_AT = time.monotonic()
_PACKAGE_NAME = "qonosv2"
_DEV_VERSION = "2.0.0"


def app_version() -> str:
    try:
        return version(_PACKAGE_NAME)
    except PackageNotFoundError:
        return _DEV_VERSION


def mark_process_started() -> None:
    global _PROCESS_STARTED_AT
    _PROCESS_STARTED_AT = time.monotonic()


def get_uptime_seconds() -> float:
    return max(0.0, time.monotonic() - _PROCESS_STARTED_AT)


def build_health_payload(
    *,
    component: str,
    status: str = "ok",
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "version": app_version(),
        "uptime_seconds": round(get_uptime_seconds(), 3),
        "component": component,
    }
    if extra:
        payload.update(extra)
    return payload


HealthExtraProvider = Callable[[], dict[str, Any]]

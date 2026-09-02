from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from .config import get_config
from .observability.context import get_log_context


class JsonLogFormatter(logging.Formatter):
    """Emit structured JSON logs with correlation and job context."""

    def __init__(self, *, component: str) -> None:
        super().__init__()
        self._component = component

    def format(self, record: logging.LogRecord) -> str:
        ctx = get_log_context()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "component": ctx.get("component") or self._component,
            "correlation_id": ctx.get("correlation_id"),
            "project_id": ctx.get("project_id"),
            "schedule_id": ctx.get("schedule_id"),
            "job_id": ctx.get("job_id"),
            "action_type": ctx.get("action_type"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(
    *,
    log_name: str,
    component: Optional[str] = None,
    default_level: str = "INFO",
) -> None:
    cfg = get_config()
    log_component = component or log_name

    log_level = (cfg.log_level or default_level).upper()
    level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()

    if getattr(root, "_qonos_logging_configured", False):
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)
        return

    root.setLevel(level)
    formatter = JsonLogFormatter(component=log_component)

    def add_stream_handler() -> None:
        if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            handler.setLevel(level)
            root.addHandler(handler)

    try:
        if cfg.log_dir:
            log_path = os.path.join(cfg.log_dir, f"{log_name}.log")
            log_dir = os.path.dirname(log_path) or "."
            os.makedirs(log_dir, exist_ok=True)

            if not any(isinstance(h, logging.FileHandler) for h in root.handlers):
                file_handler = logging.FileHandler(log_path, encoding="utf-8", delay=True)
                file_handler.setFormatter(formatter)
                file_handler.setLevel(level)
                root.addHandler(file_handler)

        add_stream_handler()

    except Exception as exc:
        add_stream_handler()
        root.warning(
            "Failed to configure file logging (log_dir=%s, log_name=%s): %s",
            cfg.log_dir,
            log_name,
            str(exc),
        )

    finally:
        setattr(root, "_qonos_logging_configured", True)

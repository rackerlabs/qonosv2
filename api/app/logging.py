from __future__ import annotations

import logging
import os

from .config import get_config


def configure_logging(*, log_name: str, default_level: str = "INFO") -> None:
    cfg = get_config()

    log_level = (cfg.log_level or default_level).upper()
    level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()

    if getattr(root, "_qonos_logging_configured", False):
        root.setLevel(level)
        return

    root.setLevel(level)

    formatter = logging.Formatter("%(asctime)s %(process)d %(levelname)s %(name)s [-] %(message)s")

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

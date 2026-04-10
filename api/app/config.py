from __future__ import annotations

import configparser
import os
import socket
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


@dataclass
class AppConfig:
    """Configuration for the QonoS API service."""

    keystone_url: Optional[str] = None
    keystone_admin_token: Optional[str] = None
    keystone_verify_ssl: bool = True

    db_url: Optional[str] = None

    policy_file: Optional[str] = None

    poll_interval_seconds: float = 1.0

    log_level: str = "INFO"
    log_dir: Optional[str] = None

    worker_concurrency: int = 10
    worker_idle_sleep_seconds: float = 1.0

    worker_soft_timeout_seconds: float = 30.0
    worker_hard_timeout_seconds: float = 60.0

    worker_max_retry: int = 3
    worker_backoff_base_seconds: float = 2.0
    worker_backoff_max_seconds: float = 300.0
    worker_backoff_jitter: bool = True

    worker_health_host: str = "0.0.0.0"
    worker_health_port: int = 8081

    worker_stale_processing_sweep_interval_seconds: float = 120.0
    worker_stale_processing_after_seconds: Optional[float] = None
    server_snapshot_poll_interval_seconds: float = 5.0
    server_snapshot_poll_timeout_seconds: float = 600.0

    redis_enabled: bool = False
    redis_url: Optional[str] = None

    redis_stream: str = "qonos:jobs"
    redis_consumer_group: str = "qonos-workers"
    redis_consumer_name: str = "auto"

    redis_stream_max_len: Optional[int] = 10000
    redis_block_timeout_ms: int = 5000


def _read_secret_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _load_config_file() -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    path = os.getenv("QONOS_CONFIG_FILE", "/etc/qonosv2/qonos.conf")
    if os.path.exists(path):
        parser.read(path)
    return parser


@lru_cache()
def get_config() -> AppConfig:
    """Load configuration from qonos.conf"""
    parser = _load_config_file()

    def get_str(section: str, option: str, default: Optional[str] = None) -> Optional[str]:
        if parser.has_option(section, option):
            return parser.get(section, option)
        return default

    def get_float(section: str, option: str, default: float) -> float:
        if parser.has_option(section, option):
            try:
                return float(parser.get(section, option))
            except ValueError:
                return default
        return default

    def get_int(section: str, option: str, default: int) -> int:
        if parser.has_option(section, option):
            try:
                return int(parser.get(section, option))
            except ValueError:
                return default
        return default

    def get_bool(section: str, option: str, default: bool) -> bool:
        if parser.has_option(section, option):
            try:
                return parser.getboolean(section, option)
            except ValueError:
                return default
        return default

    cfg = AppConfig(
        keystone_url=get_str("keystone", "url", None),
        keystone_admin_token=get_str("keystone", "admin_token", None),
        keystone_verify_ssl=get_bool("keystone", "verify_ssl", True),
        db_url=get_str("database", "url", None),
        policy_file=get_str("policy", "file", None),
        poll_interval_seconds=max(0.1, get_float("scheduler", "poll_interval_seconds", 1.0)),
        log_level=(get_str("scheduler", "log_level", "INFO") or "INFO").upper(),
        log_dir=get_str("scheduler", "log_dir", None),
        worker_concurrency=max(1, get_int("worker", "concurrency", 10)),
        worker_idle_sleep_seconds=max(0.1, get_float("worker", "idle_sleep_seconds", 1.0)),
        worker_soft_timeout_seconds=max(0.1, get_float("worker", "soft_timeout_seconds", 30.0)),
        worker_hard_timeout_seconds=max(0.1, get_float("worker", "hard_timeout_seconds", 60.0)),
        worker_max_retry=max(0, get_int("worker", "max_retry", 3)),
        worker_backoff_base_seconds=max(0.1, get_float("worker", "backoff_base_seconds", 2.0)),
        worker_backoff_max_seconds=max(0.1, get_float("worker", "backoff_max_seconds", 300.0)),
        worker_backoff_jitter=get_bool("worker", "backoff_jitter", True),
        worker_health_host=get_str("worker", "health_host", "0.0.0.0") or "0.0.0.0",
        worker_health_port=max(1, min(65535, get_int("worker", "health_port", 8081))),
        worker_stale_processing_sweep_interval_seconds=max(
            10.0, get_float("worker", "stale_processing_sweep_interval_seconds", 120.0)
        ),
        worker_stale_processing_after_seconds=(
            None
            if not parser.has_option("worker", "stale_processing_after_seconds")
            else max(0.1, get_float("worker", "stale_processing_after_seconds", 0.0))
        ),
        server_snapshot_poll_interval_seconds=max(
            0.1, get_float("worker", "server_snapshot_poll_interval_seconds", 5.0)
        ),
        server_snapshot_poll_timeout_seconds=max(
            0.1, get_float("worker", "server_snapshot_poll_timeout_seconds", 600.0)
        ),
        redis_url=get_str("redis", "url", None),
        redis_enabled=get_bool("redis", "enabled", False),
        redis_stream=get_str("redis", "stream", "qonos:jobs") or "qonos:jobs",
        redis_stream_max_len=10000,
        redis_consumer_group=(
            get_str("redis", "consumer_group", "qonos-workers") or "qonos-workers"
        ),
        redis_consumer_name=get_str("redis", "consumer_name", "auto") or "auto",
        redis_block_timeout_ms=max(1, get_int("redis", "block_timeout_ms", 5000)),
    )

    keystone_admin_token_file = get_str("keystone", "admin_token_file", None)
    if not cfg.keystone_admin_token and keystone_admin_token_file:
        cfg.keystone_admin_token = _read_secret_file(keystone_admin_token_file)

    if cfg.worker_stale_processing_after_seconds is None:
        h = cfg.worker_hard_timeout_seconds
        cfg.worker_stale_processing_after_seconds = max(h * 2, h + 60.0)

    if (cfg.redis_consumer_name or "").strip().lower() == "auto":
        cfg.redis_consumer_name = f"{socket.gethostname()}-{os.getpid()}"

    if parser.has_option("redis", "stream_max_len"):
        _rmax = get_int("redis", "stream_max_len", 0)
        cfg.redis_stream_max_len = None if _rmax <= 0 else max(1, _rmax)
    else:
        cfg.redis_stream_max_len = 10000

    return cfg

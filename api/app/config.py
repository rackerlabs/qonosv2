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
    keystone_verify_ssl: bool = True
    keystone_username: Optional[str] = None
    keystone_password: Optional[str] = None
    keystone_user_domain_name: str = "Default"
    keystone_project_name: Optional[str] = None
    keystone_project_domain_name: Optional[str] = None

    endpoint_type: Optional[str] = None

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
    job_cleanup_enabled: bool = True
    job_cleanup_ttl_days: int = 14
    job_cleanup_interval_seconds: float = 3600.0
    server_snapshot_poll_interval_seconds: float = 5.0
    server_snapshot_poll_timeout_seconds: float = 600.0
    volume_backup_full_poll_interval_seconds: float = 5.0
    volume_backup_full_poll_timeout_seconds: float = 3600.0

    worker_id: str = "auto"
    amqp_url: Optional[str] = None
    job_queue_exchange: str = "qonos.jobs"
    job_queue_routing_key: str = "qonos.jobs.queued"
    job_queue_name: str = "qonos.jobs.queue"
    job_queue_prefetch_count: int = 50
    notification_exchange: str = "qonos.events"
    notification_routing_key: str = "qonos.jobs.lifecycle"
    notification_webhook_timeout_seconds: float = 3.0


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
        keystone_verify_ssl=get_bool("keystone", "verify_ssl", True),
        keystone_username=get_str("keystone", "username", None),
        keystone_password=get_str("keystone", "password", None),
        keystone_user_domain_name=(
            get_str("keystone", "user_domain_name", "Default") or "Default"
        ),
        keystone_project_name=get_str("keystone", "project_name", None),
        keystone_project_domain_name=get_str("keystone", "project_domain_name", None),
        endpoint_type=get_str("openstack", "endpoint_type", None),
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
        worker_id=get_str("worker", "id", "auto") or "auto",
        worker_stale_processing_sweep_interval_seconds=max(
            10.0, get_float("worker", "stale_processing_sweep_interval_seconds", 120.0)
        ),
        worker_stale_processing_after_seconds=(
            None
            if not parser.has_option("worker", "stale_processing_after_seconds")
            else max(0.1, get_float("worker", "stale_processing_after_seconds", 0.0))
        ),
        job_cleanup_enabled=get_bool("worker", "job_cleanup_enabled", True),
        job_cleanup_ttl_days=max(1, get_int("worker", "job_cleanup_ttl_days", 14)),
        job_cleanup_interval_seconds=max(
            60.0, get_float("worker", "job_cleanup_interval_seconds", 3600.0)
        ),
        server_snapshot_poll_interval_seconds=max(
            0.1, get_float("worker", "server_snapshot_poll_interval_seconds", 5.0)
        ),
        server_snapshot_poll_timeout_seconds=max(
            0.1, get_float("worker", "server_snapshot_poll_timeout_seconds", 600.0)
        ),
        volume_backup_full_poll_interval_seconds=max(
            0.1, get_float("worker", "volume_backup_full_poll_interval_seconds", 5.0)
        ),
        volume_backup_full_poll_timeout_seconds=max(
            0.1, get_float("worker", "volume_backup_full_poll_timeout_seconds", 3600.0)
        ),
        amqp_url=get_str("amqp", "url", None),
        job_queue_exchange=(get_str("amqp", "job_exchange", "qonos.jobs") or "qonos.jobs"),
        job_queue_routing_key=(
            get_str("amqp", "job_routing_key", "qonos.jobs.queued") or "qonos.jobs.queued"
        ),
        job_queue_name=(get_str("amqp", "job_queue", "qonos.jobs.queue") or "qonos.jobs.queue"),
        job_queue_prefetch_count=max(1, get_int("amqp", "job_prefetch_count", 50)),
        notification_exchange=(
            get_str("amqp", "notification_exchange", "qonos.events") or "qonos.events"
        ),
        notification_routing_key=(
            get_str("amqp", "notification_routing_key", "qonos.jobs.lifecycle")
            or "qonos.jobs.lifecycle"
        ),
        notification_webhook_timeout_seconds=max(
            0.1, get_float("notifications", "webhook_timeout_seconds", 3.0)
        ),
    )

    if cfg.worker_stale_processing_after_seconds is None:
        h = cfg.worker_hard_timeout_seconds
        cfg.worker_stale_processing_after_seconds = max(h * 2, h + 60.0)

    if (cfg.worker_id or "").strip().lower() == "auto":
        cfg.worker_id = f"{socket.gethostname()}-{os.getpid()}"

    return cfg

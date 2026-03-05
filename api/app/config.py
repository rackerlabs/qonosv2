from __future__ import annotations

import configparser
import os
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
    )

    keystone_admin_token_file = get_str("keystone", "admin_token_file", None)
    if not cfg.keystone_admin_token and keystone_admin_token_file:
        cfg.keystone_admin_token = _read_secret_file(keystone_admin_token_file)

    return cfg


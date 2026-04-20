from __future__ import annotations

import json
from functools import lru_cache
from typing import Optional

from api.app.config import get_config


@lru_cache()
def _load_trust_map() -> dict[str, str]:
    cfg = get_config()
    path = cfg.keystone_trusts_file
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, str) and k and v:
            out[k] = v
    return out


def get_trust_id_for_project(project_id: str) -> Optional[str]:
    return _load_trust_map().get(project_id)


def build_openstack_connection(*, project_id: str):
    """Build an OpenStackSDK Connection for the target project.

    Use configured Keystone auth mode for service credentials.
    """
    cfg = get_config()
    from openstack import connection as os_connection

    if not cfg.keystone_url:
        raise RuntimeError("Missing Keystone URL configuration")

    auth_mode = (cfg.keystone_auth_mode or "password").lower()

    if auth_mode == "password":
        # Password mode with optional per-project trust delegation.
        if not cfg.keystone_service_username or not cfg.keystone_service_password:
            raise RuntimeError(
                "Missing Keystone password auth credentials: service_username/service_password"
            )
        trust_id = get_trust_id_for_project(project_id)
        if trust_id:
            return os_connection.Connection(
                auth_type="v3trust",
                auth={
                    "auth_url": cfg.keystone_url,
                    "username": cfg.keystone_service_username,
                    "password": cfg.keystone_service_password,
                    "user_domain_name": cfg.keystone_service_user_domain_name,
                    "trust_id": trust_id,
                },
                verify=cfg.keystone_verify_ssl,
                compute_api_version="2",
            )

        return os_connection.Connection(
            auth_type="password",
            auth={
                "auth_url": cfg.keystone_url,
                "username": cfg.keystone_service_username,
                "password": cfg.keystone_service_password,
                "user_domain_name": cfg.keystone_service_user_domain_name,
                "project_id": project_id,
            },
            verify=cfg.keystone_verify_ssl,
            compute_api_version="2",
        )

    if auth_mode == "app_credential":
        if not cfg.keystone_service_app_credential_id or not cfg.keystone_service_app_credential_secret:
            raise RuntimeError(
                "Missing Keystone app credential auth configuration: service_app_credential_id/service_app_credential_secret"
            )
        return os_connection.Connection(
            auth_type="v3applicationcredential",
            auth={
                "auth_url": cfg.keystone_url,
                "application_credential_id": cfg.keystone_service_app_credential_id,
                "application_credential_secret": cfg.keystone_service_app_credential_secret,
            },
            verify=cfg.keystone_verify_ssl,
            compute_api_version="2",
        )

    raise RuntimeError("Invalid keystone auth_mode. Expected 'password' or 'app_credential'.")


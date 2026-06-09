from __future__ import annotations

from uuid import UUID

from api.app.config import get_config


def _is_valid_uuid(value: str) -> bool:
    try:
        UUID(value)
    except Exception:
        return False
    return True


def _connection_kwargs(cfg) -> dict:
    kwargs: dict = {
        "verify": cfg.keystone_verify_ssl,
        "compute_api_version": "2",
    }
    interface = (cfg.endpoint_type or "").strip().lower()
    if interface:
        kwargs["interface"] = interface
    return kwargs


def build_openstack_connection(*, project_id: str, trust_id: str | None = None):
    """Build an OpenStackSDK Connection for the target project.

    Use configured Keystone service username/password (and trust_id when provided).
    """
    cfg = get_config()
    from openstack import connection as os_connection

    if not cfg.keystone_url:
        raise RuntimeError("Missing Keystone URL configuration")

    conn_kwargs = _connection_kwargs(cfg)
    effective_trust_id = (trust_id or "").strip()
    if effective_trust_id and not _is_valid_uuid(effective_trust_id):
        raise RuntimeError("Invalid trust_id configured; expected UUID format")
    if not effective_trust_id and not _is_valid_uuid((project_id or "").strip()):
        raise RuntimeError("Invalid project_id configured; expected UUID format")

    if effective_trust_id:
        if not cfg.keystone_username or not cfg.keystone_password:
            raise RuntimeError(
                "Trust-based delegation requires username and password."
            )
        return os_connection.Connection(
            auth_type="password",
            auth={
                "auth_url": cfg.keystone_url,
                "username": cfg.keystone_username,
                "password": cfg.keystone_password,
                "user_domain_name": cfg.keystone_user_domain_name,
                "trust_id": effective_trust_id,
            },
            **conn_kwargs,
        )

    if not cfg.keystone_username or not cfg.keystone_password:
        raise RuntimeError(
            "Missing Keystone password auth credentials: username/password"
        )
    return os_connection.Connection(
        auth_type="password",
        auth={
            "auth_url": cfg.keystone_url,
            "username": cfg.keystone_username,
            "password": cfg.keystone_password,
            "user_domain_name": cfg.keystone_user_domain_name,
            "project_id": project_id,
        },
        **conn_kwargs,
    )

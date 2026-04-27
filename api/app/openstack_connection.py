from __future__ import annotations

from uuid import UUID

from api.app.config import get_config


def _is_valid_uuid(value: str) -> bool:
    try:
        UUID(value)
    except Exception:
        return False
    return True


def build_openstack_connection(*, project_id: str, trust_id: str | None = None):
    """Build an OpenStackSDK Connection for the target project.

    Use configured Keystone auth mode for service credentials.
    """
    cfg = get_config()
    from openstack import connection as os_connection

    if not cfg.keystone_url:
        raise RuntimeError("Missing Keystone URL configuration")

    effective_trust_id = (trust_id or "").strip()
    if effective_trust_id and not _is_valid_uuid(effective_trust_id):
        raise RuntimeError("Invalid trust_id configured; expected UUID format")
    if not effective_trust_id and not _is_valid_uuid((project_id or "").strip()):
        raise RuntimeError("Invalid project_id configured; expected UUID format")

    auth_mode = (cfg.keystone_auth_mode or "password").lower()

    if effective_trust_id:
        if not cfg.keystone_service_username or not cfg.keystone_service_password:
            raise RuntimeError(
                "Trust-based delegation requires service_username and service_password."
            )
        return os_connection.Connection(
            auth_type="password",
            auth={
                "auth_url": cfg.keystone_url,
                "username": cfg.keystone_service_username,
                "password": cfg.keystone_service_password,
                "user_domain_name": cfg.keystone_service_user_domain_name,
                "trust_id": effective_trust_id,
            },
            verify=cfg.keystone_verify_ssl,
            compute_api_version="2",
        )

    if auth_mode == "password":
        if not cfg.keystone_service_username or not cfg.keystone_service_password:
            raise RuntimeError(
                "Missing Keystone password auth credentials: service_username/service_password"
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
        if (
            not cfg.keystone_service_app_credential_id
            or not cfg.keystone_service_app_credential_secret
        ):
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

    raise RuntimeError(
        f"Invalid keystone auth_mode '{auth_mode}'. Expected 'password' or 'app_credential'."
    )

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import httpx

from .context import AuthContext


class KeystoneClient(ABC):
    """Abstract interface for Keystone token validation."""

    @abstractmethod
    async def validate_token(self, token: str) -> AuthContext:
        """Validate the given token and return an AuthContext.

        Implementations should call Keystone's /v3/auth/tokens API (or equivalent)
        and derive project_id and roles from the token data.
        """


class HttpKeystoneClient(KeystoneClient):
    """Keystone client that validates tokens via HTTP against a Keystone v3 API."""

    def __init__(
        self,
        base_url: str,
        *,
        auth_mode: str = "password",
        service_app_credential_id: str | None = None,
        service_app_credential_secret: str | None = None,
        service_username: str | None = None,
        service_password: str | None = None,
        service_user_domain_name: str = "Default",
        verify_ssl: bool = True,
        timeout: float = 5.0,
    ) -> None:
        normalized = base_url.rstrip("/")
        if normalized.endswith("/v3"):
            normalized = normalized[: -len("/v3")]
        self._base_url = normalized
        self._auth_mode = (auth_mode or "password").lower()
        self._service_app_credential_id = service_app_credential_id
        self._service_app_credential_secret = service_app_credential_secret
        self._service_username = service_username
        self._service_password = service_password
        self._service_user_domain_name = service_user_domain_name
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._cached_validation_token: str | None = None
        self._cached_validation_token_expires_at: datetime | None = None

    def _cached_token_is_valid(self) -> bool:
        if not self._cached_validation_token or not self._cached_validation_token_expires_at:
            return False
        return datetime.now(timezone.utc) < self._cached_validation_token_expires_at

    async def _request_service_token_with_app_credential(self) -> tuple[str, datetime]:
        if not self._service_app_credential_id or not self._service_app_credential_secret:
            raise ValueError("service app credentials are not configured")
        url = f"{self._base_url}/v3/auth/tokens"
        payload = {
            "auth": {
                "identity": {
                    "methods": ["application_credential"],
                    "application_credential": {
                        "id": self._service_app_credential_id,
                        "secret": self._service_app_credential_secret,
                    },
                }
            }
        }
        async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
            resp = await client.post(url, json=payload)
        if resp.status_code != 201:
            raise ValueError(f"App-credential auth failed with status {resp.status_code}")
        token = resp.headers.get("X-Subject-Token")
        body = resp.json()
        expires_at_raw = ((body.get("token") or {}).get("expires_at") or "").strip()
        if not token or not expires_at_raw:
            raise ValueError("Malformed Keystone response during app-credential auth")
        expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
        return token, expires_at

    async def _request_service_token_with_password(self) -> tuple[str, datetime]:
        if not self._service_username or not self._service_password:
            raise ValueError("service username/password are not configured")
        url = f"{self._base_url}/v3/auth/tokens"
        payload = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": self._service_username,
                            "domain": {"name": self._service_user_domain_name},
                            "password": self._service_password,
                        }
                    },
                }
            }
        }
        async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
            resp = await client.post(url, json=payload)
        if resp.status_code != 201:
            raise ValueError(f"Password auth failed with status {resp.status_code}")
        token = resp.headers.get("X-Subject-Token")
        body = resp.json()
        expires_at_raw = ((body.get("token") or {}).get("expires_at") or "").strip()
        if not token or not expires_at_raw:
            raise ValueError("Malformed Keystone response during password auth")
        expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
        return token, expires_at

    async def _get_validation_token(self) -> str:
        if self._cached_token_is_valid():
            return self._cached_validation_token or ""
        if self._auth_mode == "password":
            if not self._service_username or not self._service_password:
                raise ValueError(
                    "Keystone auth_mode=password requires service_username and service_password."
                )
            token, expires_at = await self._request_service_token_with_password()
            self._cached_validation_token = token
            self._cached_validation_token_expires_at = expires_at - timedelta(seconds=30)
            return token

        if self._auth_mode == "app_credential":
            if not self._service_app_credential_id or not self._service_app_credential_secret:
                raise ValueError(
                    "Keystone auth_mode=app_credential requires app credential id and secret."
                )
            token, expires_at = await self._request_service_token_with_app_credential()
            self._cached_validation_token = token
            self._cached_validation_token_expires_at = expires_at - timedelta(seconds=30)
            return token

        raise ValueError("Invalid Keystone auth_mode. Expected 'password' or 'app_credential'.")

    async def validate_token(self, token: str) -> AuthContext:
        """Call Keystone /v3/auth/tokens to validate the token and build AuthContext."""
        url = f"{self._base_url}/v3/auth/tokens"
        validation_token = await self._get_validation_token()
        headers = {
            "X-Auth-Token": validation_token,
            "X-Subject-Token": token,
        }

        async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
            resp = await client.get(url, headers=headers)

        if resp.status_code != 200:
            raise ValueError(f"Token validation failed with status {resp.status_code}")

        body = resp.json()
        token_body: Optional[Dict] = body.get("token")
        if not token_body:
            raise ValueError("Malformed Keystone response: missing 'token' field")

        user = token_body.get("user") or {}
        user_id = user.get("id")
        if not user_id:
            raise ValueError("Token missing user ID")

        project = token_body.get("project") or {}
        project_id = project.get("id") or ""

        # Extract project-scoped roles
        project_roles_list = token_body.get("roles", [])
        project_roles: List[str] = [r.get("name") for r in project_roles_list if r.get("name")]

        # Extract system-scoped roles if present
        system_roles_set: Set[str] = set()
        if token_body.get("system") or token_body.get("domain"):
            for r in token_body.get("roles", []):
                name = r.get("name")
                if name:
                    system_roles_set.add(name)

        return AuthContext(
            user_id=user_id,
            project_id=project_id,
            roles=project_roles,
            system_roles=system_roles_set if system_roles_set else None,
        )

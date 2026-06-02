from __future__ import annotations

import asyncio
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
        self._service_username = service_username
        self._service_password = service_password
        self._service_user_domain_name = service_user_domain_name
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._cached_validation_token: str | None = None
        self._cached_validation_token_expires_at: datetime | None = None
        self._token_refresh_lock = asyncio.Lock()

    @staticmethod
    def _response_snippet(resp: httpx.Response, *, max_len: int = 300) -> str:
        text = (resp.text or "").strip().replace("\n", " ")
        if not text:
            return ""
        if len(text) > max_len:
            return text[:max_len] + "..."
        return text

    def _cached_token_is_valid(self) -> bool:
        if not self._cached_validation_token or not self._cached_validation_token_expires_at:
            return False
        return datetime.now(timezone.utc) < self._cached_validation_token_expires_at

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
        try:
            async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
                resp = await client.post(url, json=payload)
        except httpx.RequestError as exc:
            raise ValueError(f"Keystone password auth request failed: {str(exc)}") from exc
        if resp.status_code != 201:
            body_snippet = self._response_snippet(resp)
            detail = f" body={body_snippet}" if body_snippet else ""
            raise ValueError(f"Password auth failed with status {resp.status_code}.{detail}")
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
        async with self._token_refresh_lock:
            if self._cached_token_is_valid():
                return self._cached_validation_token or ""

            if not self._service_username or not self._service_password:
                raise ValueError(
                    "Keystone service token requires service_username and service_password."
                )
            token, expires_at = await self._request_service_token_with_password()
            self._cached_validation_token = token
            self._cached_validation_token_expires_at = expires_at - timedelta(seconds=30)
            return token

    async def validate_token(self, token: str) -> AuthContext:
        """Call Keystone /v3/auth/tokens to validate the token and build AuthContext."""
        url = f"{self._base_url}/v3/auth/tokens"
        validation_token = await self._get_validation_token()
        headers = {
            "X-Auth-Token": validation_token,
            "X-Subject-Token": token,
        }

        try:
            async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
                resp = await client.get(url, headers=headers)
        except httpx.RequestError as exc:
            raise ValueError(f"Keystone token validation request failed: {str(exc)}") from exc

        if resp.status_code != 200:
            body_snippet = self._response_snippet(resp)
            detail = f" body={body_snippet}" if body_snippet else ""
            raise ValueError(f"Token validation failed with status {resp.status_code}.{detail}")

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

    async def validate_trust(
        self, trust_id: str, *, expected_project_id: str | None = None
    ) -> dict[str, object]:
        normalized_trust_id = (trust_id or "").strip()
        if not normalized_trust_id:
            raise ValueError("trust_id must be a non-empty string")

        validation_token = await self._get_validation_token()
        url = f"{self._base_url}/v3/OS-TRUST/trusts/{normalized_trust_id}"
        headers = {"X-Auth-Token": validation_token}

        try:
            async with httpx.AsyncClient(verify=self._verify_ssl, timeout=self._timeout) as client:
                resp = await client.get(url, headers=headers)
        except httpx.RequestError as exc:
            raise ValueError(f"Keystone trust validation request failed: {str(exc)}") from exc

        if resp.status_code == 404:
            raise ValueError("trust_id not found")
        if resp.status_code == 403:
            raise ValueError("not authorized to read trust_id")
        if resp.status_code != 200:
            body_snippet = self._response_snippet(resp)
            detail = f" body={body_snippet}" if body_snippet else ""
            raise ValueError(f"Trust validation failed with status {resp.status_code}.{detail}")

        body = resp.json()
        trust = body.get("trust")
        if not isinstance(trust, dict):
            raise ValueError("Malformed Keystone trust response")

        trust_project_id = str(trust.get("project_id") or "").strip()
        if expected_project_id and trust_project_id and trust_project_id != expected_project_id:
            raise ValueError("trust_id does not belong to the schedule project")

        expires_at_raw = str(trust.get("expires_at") or "").strip()
        if expires_at_raw:
            try:
                expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
            except Exception as exc:
                raise ValueError("Invalid trust expiration timestamp") from exc
            if datetime.now(timezone.utc) >= expires_at:
                raise ValueError("trust_id is expired")

        return trust

from __future__ import annotations

from abc import ABC, abstractmethod
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
        admin_token: str,
        *,
        verify_ssl: bool = True,
        timeout: float = 5.0,
    ) -> None:
        normalized = base_url.rstrip("/")
        if normalized.endswith("/v3"):
            normalized = normalized[: -len("/v3")]
        self._base_url = normalized
        self._admin_token = admin_token
        self._verify_ssl = verify_ssl
        self._timeout = timeout

    async def validate_token(self, token: str) -> AuthContext:
        """Call Keystone /v3/auth/tokens to validate the token and build AuthContext."""
        url = f"{self._base_url}/v3/auth/tokens"
        headers = {
            "X-Auth-Token": self._admin_token,
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


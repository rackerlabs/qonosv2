from __future__ import annotations

import json
from typing import Dict, Iterable, List

from fastapi import Depends, HTTPException, Request, status

from .context import AuthContext
from ..config import get_config
from ..error_constants import ErrorMessages
from ..policy_constants import DEFAULT_POLICY


def get_auth_context(request: Request) -> AuthContext:
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication context",
        )
    return auth


def require_roles(
    required_roles: Iterable[str],
    allow_admin: bool = True,
):
    """FastAPI dependency enforcing that caller has at least one of the required roles.

    - If allow_admin is True, admin automatically passes.
    """

    async def dependency(auth: AuthContext = Depends(get_auth_context)) -> AuthContext:
        normalized = {r.lower() for r in auth.roles}
        if allow_admin and "admin" in normalized:
            return auth

        lower_required = {r.lower() for r in required_roles}
        if not normalized & lower_required:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=ErrorMessages.FORBIDDEN,
            )
        return auth

    return dependency


def _load_policy_from_file() -> Dict[str, List[str]]:
    """Load policy rules from a JSON file, if configured."""
    cfg = get_config()
    path = cfg.policy_file
    if not path:
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    if not isinstance(data, dict):
        return {}

    normalized: Dict[str, List[str]] = {}
    for action, roles in data.items():
        if not isinstance(action, str) or not isinstance(roles, list):
            continue
        normalized[action] = [str(r) for r in roles]
    return normalized


_FILE_POLICY: Dict[str, List[str]] = _load_policy_from_file()


def require_policy(action: str):
    """Policy-based RBAC helper.

    Looks up allowed roles for the given action from a JSON policy file
    if configured, falling back to built-in defaults.
    """

    allowed_roles = _FILE_POLICY.get(action, DEFAULT_POLICY.get(action, []))
    return require_roles(required_roles=allowed_roles, allow_admin=True)


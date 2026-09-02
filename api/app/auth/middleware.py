from __future__ import annotations

from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from .context import AuthContext
from .keystone_client import KeystoneClient
from ..error_constants import unauthorized_json


class KeystoneAuthMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces X-Auth-Token and populates request.state.auth."""

    def __init__(self, app, keystone_client: KeystoneClient) -> None:
        super().__init__(app)
        self._keystone_client = keystone_client

    async def dispatch(self, request: Request, call_next: Callable):
        path = request.url.path
        if (
            path.startswith("/docs")
            or path.startswith("/openapi")
            or path.startswith("/health")
            or path == "/metrics"
        ):
            return await call_next(request)

        token = request.headers.get("X-Auth-Token")
        if not token:
            return unauthorized_json()

        try:
            auth_ctx: AuthContext = await self._keystone_client.validate_token(token)
        except Exception:
            return unauthorized_json()

        request.state.auth = auth_ctx
        return await call_next(request)

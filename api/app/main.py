"""
Entry point for the QonoS v2 FastAPI application.

Sets up Keystone token authentication, RBAC, database lifecycle management,
and registers routers for schedules, jobs, actions, and health endpoints.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .auth.keystone_client import HttpKeystoneClient, KeystoneClient
from .auth.middleware import KeystoneAuthMiddleware
from .config import get_config
from .routes_actions import router as actions_router
from .routes_execution_profiles import router as execution_profiles_router
from .routes_health import router as health_router
from .routes_jobs import router as jobs_router
from .routes_schedules import router as schedules_router
from .routes_volumes import router as volumes_router
from .db import close_db_engine


def create_app(*, keystone_client: KeystoneClient | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await close_db_engine()

    app = FastAPI(title="QonoS v2 API", lifespan=lifespan)

    if keystone_client is None:
        cfg = get_config()
        if not cfg.keystone_url:
            raise RuntimeError("Keystone URL must be configured for HttpKeystoneClient")
        keystone_client = HttpKeystoneClient(
            base_url=cfg.keystone_url,
            username=cfg.keystone_username,
            password=cfg.keystone_password,
            user_domain_name=cfg.keystone_user_domain_name,
            project_name=cfg.keystone_project_name,
            project_domain_name=cfg.keystone_project_domain_name,
            verify_ssl=cfg.keystone_verify_ssl,
        )
    app.add_middleware(KeystoneAuthMiddleware, keystone_client=keystone_client)

    app.include_router(schedules_router)
    app.include_router(execution_profiles_router)
    app.include_router(jobs_router)
    app.include_router(volumes_router)
    app.include_router(actions_router)
    app.include_router(health_router)

    return app


app = create_app()

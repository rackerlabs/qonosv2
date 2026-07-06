from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, AsyncIterator, Optional

_INTEGRATION_CONF = Path(__file__).resolve().parents[3] / "config" / "qonos.integration.conf"
if not os.getenv("QONOS_CONFIG_FILE"):
    os.environ["QONOS_CONFIG_FILE"] = str(_INTEGRATION_CONF)

import pytest
import pytest_asyncio
from fastapi import HTTPException, status
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.app import routes_execution_profiles, routes_schedules, routes_volumes
from api.app.config import get_config
from api.app.db import close_db_engine
from api.app.main import create_app
from api.app.worker import retention
from api.app.worker import server_snapshot_handler
from api.app.worker import volume_backup_common
from db.src.models import ActionType as ActionTypeModel, Base

from .fakes import OpenStackSimulator, SimulatedKeystoneClient, SERVER_A, VOLUME_A
from .harness import IntegrationHarness, build_test_config, build_worker_runtime

_DEFAULT_MARIADB_URL = "mysql+aiomysql://root:root@127.0.0.1:3306/qonosv2_test"


def _integration_db_url() -> str:
    url = (os.getenv("QONOS_TEST_DB_URL") or _DEFAULT_MARIADB_URL).strip()
    if url.startswith("sqlite"):
        raise RuntimeError(
            "Set QONOS_TEST_DB_URL to a MariaDB URL, e.g. " f"{_DEFAULT_MARIADB_URL}"
        )
    if not url.startswith("mysql"):
        raise RuntimeError(
            "Integration tests require a MySQL/MariaDB URL (mysql+aiomysql://...). "
            f"Got: {url.split('://', 1)[0]}://..."
        )
    return url


def _build_simulated_trust_validator(keystone: SimulatedKeystoneClient):
    async def _validate_trust_for_project(
        *, project_id: str, trust_id: Optional[str] = None
    ) -> None:
        normalized = (trust_id or "").strip()
        if not normalized:
            return
        try:
            await keystone.validate_trust(normalized, expected_project_id=project_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid trust_id: {str(exc)}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to validate trust_id with Keystone: {str(exc)}",
            ) from exc

    return _validate_trust_for_project


_INTEGRATION_DATA_TABLES = ("job_faults", "jobs", "schedules", "execution_profiles")


async def _clear_integration_data(session_factory: sessionmaker) -> None:
    async with session_factory() as session:
        await session.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for table in _INTEGRATION_DATA_TABLES:
            await session.execute(text(f"TRUNCATE TABLE {table}"))
        await session.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        await session.commit()


async def _seed_action_types(session: AsyncSession) -> None:
    existing = await session.get(ActionTypeModel, "server_snapshot")
    if existing is None:
        session.add(
            ActionTypeModel(
                action_type="server_snapshot",
                name="Server snapshot",
                description="Integration test action",
                action_parameters_schema={
                    "type": "object",
                    "properties": {"server_id": {"type": "string"}},
                    "required": ["server_id"],
                    "additionalProperties": True,
                },
                enabled=True,
            )
        )

    if await session.get(ActionTypeModel, "volume_backup_full") is None:
        session.add(
            ActionTypeModel(
                action_type="volume_backup_full",
                name="Volume backup (full)",
                description="Integration test action",
                action_parameters_schema={
                    "type": "object",
                    "properties": {"volume_id": {"type": "string"}},
                    "required": ["volume_id"],
                    "additionalProperties": True,
                },
                enabled=True,
            )
        )
    if await session.get(ActionTypeModel, "volume_backup_incremental") is None:
        session.add(
            ActionTypeModel(
                action_type="volume_backup_incremental",
                name="Volume backup (incremental)",
                description="Integration test action",
                action_parameters_schema={
                    "type": "object",
                    "properties": {"volume_id": {"type": "string"}},
                    "required": ["volume_id"],
                    "additionalProperties": True,
                },
                enabled=True,
            )
        )
    await session.commit()


@pytest.fixture(scope="session")
def db_url() -> str:
    return _integration_db_url()


@pytest_asyncio.fixture(loop_scope="function")
async def integration_env(
    db_url: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[dict[str, Any]]:
    get_config.cache_clear()
    await close_db_engine()

    cfg = build_test_config(db_url)
    monkeypatch.setattr("api.app.config.get_config", lambda: cfg)
    monkeypatch.setattr("api.scheduler.get_config", lambda: cfg)
    monkeypatch.setattr("api.worker.get_config", lambda: cfg)
    monkeypatch.setattr("api.app.worker.server_snapshot_handler.get_config", lambda: cfg)

    keystone = SimulatedKeystoneClient()
    validate_trust = _build_simulated_trust_validator(keystone)
    monkeypatch.setattr(routes_schedules, "_validate_trust_for_project", validate_trust)
    monkeypatch.setattr(routes_execution_profiles, "_validate_trust_for_project", validate_trust)
    openstack = OpenStackSimulator()
    openstack.add_server(server_id=SERVER_A)
    openstack.add_volume(volume_id=VOLUME_A)

    def _fake_connection(*, project_id: str, trust_id: str | None = None):
        return openstack.connection_for(project_id=project_id, trust_id=trust_id)

    monkeypatch.setattr(server_snapshot_handler, "build_openstack_connection", _fake_connection)
    monkeypatch.setattr(retention, "_build_connection", _fake_connection)
    monkeypatch.setattr(volume_backup_common, "build_openstack_connection", _fake_connection)
    monkeypatch.setattr(routes_volumes, "build_openstack_connection", _fake_connection)

    engine = create_async_engine(db_url)
    session_factory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        await _seed_action_types(session)

    await _clear_integration_data(session_factory)

    monkeypatch.setattr("api.app.db._engine", engine)
    monkeypatch.setattr("api.app.db._session_factory", session_factory)

    app = create_app(keystone_client=keystone)
    worker = build_worker_runtime(cfg)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        harness = IntegrationHarness(
            client=client,
            session_factory=session_factory,
            worker=worker,
            openstack=openstack,
        )
        yield {"harness": harness, "keystone": keystone}

    for task in list(worker._tasks):
        task.cancel()
    if worker._tasks:
        await asyncio.gather(*worker._tasks, return_exceptions=True)
    worker._tasks.clear()

    await close_db_engine()
    await engine.dispose()
    get_config.cache_clear()


@pytest.fixture
def harness(integration_env: dict[str, Any]) -> IntegrationHarness:
    h: IntegrationHarness = integration_env["harness"]
    keystone: SimulatedKeystoneClient = integration_env["keystone"]
    keystone.trust_validation_calls.clear()
    h.openstack.reset()
    h.openstack.add_server(server_id=SERVER_A)
    h.openstack.add_volume(volume_id=VOLUME_A)
    return h


@pytest.fixture
def keystone(integration_env: dict[str, Any]) -> SimulatedKeystoneClient:
    return integration_env["keystone"]

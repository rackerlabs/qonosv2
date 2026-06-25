from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from keystoneauth1 import exceptions as keystone_exceptions
from sqlalchemy.ext.asyncio import AsyncSession

from .auth.context import AuthContext
from .auth.rbac import require_policy
from .db import get_db_session
from .openstack_connection import build_openstack_connection
from .schemas import VolumeBackupChain, VolumeBackupChainItem
from .volume_schedule_advisories import (
    resolve_volume_backup_trust_id,
    volume_backup_schedule_flags,
    volume_schedule_advisories_for_volume,
)

router = APIRouter(prefix="/v2", tags=["volumes"])


def _backup_parent_id(backup: Any) -> str | None:
    for attr in ("parent_id", "parentId"):
        value = getattr(backup, attr, None)
        if value:
            return str(value)
    meta = getattr(backup, "metadata", None) or {}
    if isinstance(meta, dict):
        value = meta.get("qonos_parent_full_backup_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _backup_created_at(backup: Any) -> datetime | None:
    value = getattr(backup, "created_at", None)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


@router.get(
    "/volumes/{volume_id}/backup-chain",
    response_model=VolumeBackupChain,
)
async def get_volume_backup_chain(
    volume_id: str,
    project_id: str | None = Query(
        default=None,
        description="Required for admin tokens; ignored for non-admin users.",
    ),
    auth: AuthContext = Depends(require_policy("schedule:read")),
    session: AsyncSession = Depends(get_db_session),
) -> VolumeBackupChain:
    resolved_project_id = auth.project_id
    if auth.is_admin:
        if not project_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="project_id query parameter is required for admin requests.",
            )
        resolved_project_id = project_id

    has_full, has_incremental = await volume_backup_schedule_flags(
        session=session,
        project_id=resolved_project_id,
        volume_id=volume_id,
    )
    advisories = await volume_schedule_advisories_for_volume(
        session=session,
        project_id=resolved_project_id,
        volume_id=volume_id,
    )

    trust_id = await resolve_volume_backup_trust_id(
        session=session,
        project_id=resolved_project_id,
        volume_id=volume_id,
    )
    if not trust_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No enabled execution profile with trust delegation found for "
                "volume backup schedules on this volume."
            ),
        )

    conn = build_openstack_connection(
        project_id=resolved_project_id,
        trust_id=trust_id,
    )
    try:
        backups = await asyncio.to_thread(
            lambda: list(conn.block_storage.backups(details=True, volume_id=volume_id))
        )
    except keystone_exceptions.ClientException as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"OpenStack request failed: {exc}",
        ) from exc
    items: list[VolumeBackupChainItem] = []
    for backup in backups:
        is_incremental = bool(getattr(backup, "is_incremental", False))
        meta = getattr(backup, "metadata", None) or {}
        if not isinstance(meta, dict):
            meta = {}
        items.append(
            VolumeBackupChainItem(
                id=str(getattr(backup, "id", "")),
                name=getattr(backup, "name", None),
                status=str(getattr(backup, "status", "") or ""),
                is_incremental=is_incremental,
                parent_id=_backup_parent_id(backup),
                size=getattr(backup, "size", None),
                created_at=_backup_created_at(backup),
                project_id=str(getattr(backup, "project_id", "") or "") or None,
                metadata=meta,
            )
        )
    items.sort(key=lambda i: i.created_at or datetime.min)

    return VolumeBackupChain(
        volume_id=volume_id,
        project_id=resolved_project_id,
        has_full_schedule=has_full,
        has_incremental_schedule=has_incremental,
        advisories=advisories,
        backups=items,
    )

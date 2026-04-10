from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .auth.context import AuthContext
from .auth.rbac import require_policy
from .db import get_db_session
from .error_constants import ErrorMessages
from .schemas import Action, ActionList
from db.src.models import ActionType as ActionTypeModel

router = APIRouter(prefix="/v2", tags=["actions"])


@router.get("/actions", response_model=ActionList)
async def list_actions(
    marker: Optional[str] = Query(default=None, description="Pagination marker."),
    limit: int = Query(default=100, ge=1, le=1000),
    auth: AuthContext = Depends(require_policy("action:read")),
    session: AsyncSession = Depends(get_db_session),
) -> ActionList:
    stmt = select(ActionTypeModel).limit(limit)
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return ActionList(actions=[Action.model_validate(r) for r in rows])


@router.get("/actions/{action_type}", response_model=Action)
async def show_action(
    action_type: str = Path(..., description="Action type identifier"),
    auth: AuthContext = Depends(require_policy("action:read")),
    session: AsyncSession = Depends(get_db_session),
) -> Action:
    result = await session.execute(
        select(ActionTypeModel).where(ActionTypeModel.action_type == action_type)
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=ErrorMessages.ACTION_TYPE_NOT_FOUND
        )
    return Action.model_validate(row)

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError

from .error_constants import ErrorMessages


def _get_dbapi_errno(err: BaseException) -> Optional[int]:
    args = getattr(err, "args", None)
    if not args:
        return None
    try:
        return int(args[0])
    except Exception:
        return None


def raise_for_db_error(exc: Exception) -> None:
    """Convert common SQLAlchemy DB errors into stable HTTP errors."""
    if isinstance(exc, IntegrityError):
        orig = getattr(exc, "orig", exc)
        errno = _get_dbapi_errno(orig)
        msg = str(orig)

        # Duplicate key
        if errno == 1062 or "Duplicate entry" in msg:
            if "schedules_project_active_name_uniq" in msg:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorMessages.SCHEDULE_NAME_CONFLICT,
                )
            if "execution_profiles_project_active_name_uniq" in msg:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorMessages.EXECUTION_PROFILE_NAME_CONFLICT,
                )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=ErrorMessages.CONFLICT,
            )

        # Foreign key violation
        if errno == 1452:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.INVALID_REFERENCE,
            )

        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=ErrorMessages.CONFLICT)

    if isinstance(exc, OperationalError):
        orig = getattr(exc, "orig", exc)
        errno = _get_dbapi_errno(orig)
        msg = str(orig)

        if errno == 1062 or "Duplicate entry" in msg:
            if "schedules_project_active_name_uniq" in msg:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorMessages.SCHEDULE_NAME_CONFLICT,
                )
            if "execution_profiles_project_active_name_uniq" in msg:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorMessages.EXECUTION_PROFILE_NAME_CONFLICT,
                )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=ErrorMessages.CONFLICT,
            )

        if errno == 1290:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorMessages.DB_READ_ONLY,
            )
        if errno == 2003:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorMessages.DB_UNAVAILABLE,
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=ErrorMessages.DB_ERROR
        )

    if isinstance(exc, DBAPIError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=ErrorMessages.DB_ERROR
        )

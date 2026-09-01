from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse


@dataclass(frozen=True)
class ApiErrorDef:
    code: str
    title: str


class ErrorCodes:
    UNAUTHORIZED = ApiErrorDef(code="qonos.unauthorized", title="Unauthorized")


class ErrorMessages:
    # Generic auth / RBAC
    FORBIDDEN = "Forbidden"
    UNAUTHORIZED = "Unauthorized"

    # Not found
    SCHEDULE_NOT_FOUND = "Schedule not found"
    JOB_NOT_FOUND = "Job not found"
    ACTION_TYPE_NOT_FOUND = "Action type not found"

    # Validation
    ACTION_PARAMETERS_NULL = "action_parameters cannot be null"
    STATUS_NULL = "status cannot be null"
    RETRY_COUNT_NULL = "retry_count cannot be null"

    # DB / conflicts
    CONFLICT = "Conflict"
    INVALID_REFERENCE = "Invalid reference"
    DB_ERROR = "Database error"
    DB_UNAVAILABLE = "Database unavailable"
    DB_READ_ONLY = "Database is read-only"
    SCHEDULE_NAME_CONFLICT = "A schedule with this name already exists in this project"
    EXECUTION_PROFILE_NAME_CONFLICT = (
        "An execution profile with this name already exists in this project"
    )


def jsonapi_error_payload(
    *,
    code: str,
    title: str,
    http_status: int,
    detail: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    err: Dict[str, Any] = {"code": code, "status": http_status, "title": title}
    if detail:
        err["detail"] = detail
    return {"errors": [err]}


def unauthorized_json(detail: Optional[str] = None) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content=jsonapi_error_payload(
            code=ErrorCodes.UNAUTHORIZED.code,
            title=ErrorCodes.UNAUTHORIZED.title,
            http_status=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
        ),
    )


def http_forbidden() -> HTTPException:
    return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=ErrorMessages.FORBIDDEN)

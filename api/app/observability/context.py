from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Iterator, Optional

_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
_project_id: ContextVar[Optional[str]] = ContextVar("project_id", default=None)
_schedule_id: ContextVar[Optional[str]] = ContextVar("schedule_id", default=None)
_job_id: ContextVar[Optional[str]] = ContextVar("job_id", default=None)
_action_type: ContextVar[Optional[str]] = ContextVar("action_type", default=None)
_component: ContextVar[Optional[str]] = ContextVar("component", default=None)


def bind_log_context(
    *,
    correlation_id: Optional[str] = None,
    project_id: Optional[str] = None,
    schedule_id: Optional[str] = None,
    job_id: Optional[str] = None,
    action_type: Optional[str] = None,
    component: Optional[str] = None,
) -> list[Token[Any]]:
    """Bind structured logging fields for the current async/task context."""
    tokens: list[Token[Any]] = []
    if correlation_id is not None:
        tokens.append(_correlation_id.set(correlation_id))
    if project_id is not None:
        tokens.append(_project_id.set(project_id))
    if schedule_id is not None:
        tokens.append(_schedule_id.set(schedule_id))
    if job_id is not None:
        tokens.append(_job_id.set(job_id))
    if action_type is not None:
        tokens.append(_action_type.set(action_type))
    if component is not None:
        tokens.append(_component.set(component))
    return tokens


def clear_log_context(tokens: list[Token[Any]]) -> None:
    for token in reversed(tokens):
        token.var.reset(token)


def get_log_context() -> dict[str, Optional[str]]:
    return {
        "correlation_id": _correlation_id.get(),
        "project_id": _project_id.get(),
        "schedule_id": _schedule_id.get(),
        "job_id": _job_id.get(),
        "action_type": _action_type.get(),
        "component": _component.get(),
    }


@contextmanager
def log_context(**kwargs: Optional[str]) -> Iterator[None]:
    tokens = bind_log_context(**kwargs)
    try:
        yield
    finally:
        clear_log_context(tokens)

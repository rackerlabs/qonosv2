"""Persistence layer exceptions and error codes for QonoS v2."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class PersistenceError(Exception):
    """Base exception for persistence layer errors."""

    message: str
    code: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.code or 'PERSISTENCE_ERROR'}: {self.message}"


class PersistenceErrorCode:
    """Error codes for persistence operations."""

    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    OCC_CONFLICT = "OCC_CONFLICT"
    CAS_CONFLICT = "CAS_CONFLICT"

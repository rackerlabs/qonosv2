from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Set


@dataclass
class AuthContext:
    """Information derived from a validated Keystone token."""

    user_id: str
    project_id: str
    roles: List[str]
    system_roles: Optional[Set[str]] = None

    @property
    def is_admin(self) -> bool:
        """Return True if the user has an admin role in the project."""
        return any(r.lower() == "admin" for r in self.roles)

    @property
    def is_service(self) -> bool:
        """Return True if the token represents a service user."""

        role_set = {r.lower() for r in self.roles}

        if "service" in role_set:
            return True

        if self.system_roles and "admin" in {r.lower() for r in self.system_roles}:
            return True

        return False


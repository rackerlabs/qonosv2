from __future__ import annotations

from typing import Dict, List

# Default RBAC policy mapping (action -> allowed roles).
# Admin is handled separately in `require_roles(..., allow_admin=True)`.
DEFAULT_POLICY: Dict[str, List[str]] = {
    "schedule:create": ["member"],
    "schedule:read": ["reader", "member"],
    "schedule:update": ["member"],
    "schedule:delete": ["member"],
    "execution_profile:create": ["member"],
    "execution_profile:read": ["reader", "member"],
    "execution_profile:update": ["member"],
    "execution_profile:delete": ["member"],
    "job:create": ["service"],
    "job:read": ["reader", "member", "service"],
    "job:update": ["service"],
    "job:cancel": [
        "service",
    ],
}

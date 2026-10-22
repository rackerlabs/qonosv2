"""Add database_backup action type."""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

revision = "0003_database_backup"
down_revision = "0002_swift_object_versioning"

_SQL_PATH = Path(__file__).resolve().parents[2] / "migrations" / "0003_database_backup.sql"


def _run_sql_file(path: Path) -> None:
    content = path.read_text(encoding="utf-8")
    statements: list[str] = []
    buffer: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []

    connection = op.get_bind()
    for statement in statements:
        connection.execute(text(statement))


def upgrade() -> None:
    _run_sql_file(_SQL_PATH)

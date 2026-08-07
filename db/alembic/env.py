from __future__ import annotations

import sys
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.app.config import get_config

config = context.config


def _migration_database_url() -> str:
    db_url = get_config().db_url
    if not db_url:
        raise RuntimeError("Database URL must be configured (database.url in qonos.conf)")

    if db_url.startswith("mysql+aiomysql://"):
        return "mysql+pymysql://" + db_url[len("mysql+aiomysql://") :]
    return db_url


configuration = config.get_section(config.config_ini_section, {})
configuration["sqlalchemy.url"] = _migration_database_url()
connectable = engine_from_config(
    configuration,
    prefix="sqlalchemy.",
    poolclass=pool.NullPool,
)

with connectable.connect() as connection:
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()

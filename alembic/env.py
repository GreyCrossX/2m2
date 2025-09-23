# alembic/env.py
from __future__ import annotations

import os
import sys
import asyncio
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# Load .env first, so env vars are available before we read config
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv is optional; ignore if not installed
    pass

# Alembic Config object, provides access to the .ini file values
config = context.config

# Set up Python logging via the config file, if present
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Ensure project root is on sys.path so `app.*` imports work ---
# This assumes the repo structure:
# project_root/
#   app/
#   alembic/
#     env.py  <-- this file
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- Import your Base that registers all models ---
# app/db/base.py must define ONE Declarative Base and import all models.
from app.db.base import Base  # noqa: E402

import app.db.models

# Expose metadata for autogenerate
target_metadata = Base.metadata

# --- Read database URL from environment and inject into Alembic config ---
db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError(
        "Database URL not set. Define DB_URL or DATABASE_URL in your environment (.env)."
    )

# Make the URL visible to both offline and online migration paths
config.set_main_option("sqlalchemy.url", db_url)


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine.
    Calls to context.execute() emit the given string to the script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,             # detect column type changes
        compare_server_default=True,   # detect server_default changes
        include_schemas=True,          # if you use multiple schemas
    )

    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    """
    Configure Alembic with an existing DBAPI connection and run migrations.
    This is called inside an async connection via run_sync.
    """
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online_async() -> None:
    """
    Run migrations in 'online' mode using an async engine.
    """
    connectable = create_async_engine(
        db_url,
        poolclass=pool.NullPool,   # avoid connection pooling in migrations
        future=True,
    )

    try:
        async with connectable.connect() as connection:
            await connection.run_sync(_do_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_migrations_online_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

# app/db/session.py
from __future__ import annotations

import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)

# Use the SINGLE source of truth (handles DB_URL / DATABASE_URL / POSTGRES_* and encoding)
ASYNC_DB_URL = settings.db_url_async

def _mask(dsn: str) -> str:
    try:
        if "://" in dsn and "@" in dsn:
            i = dsn.index("://") + 3
            j = dsn.index("@")
            return dsn[:i] + "***:***" + dsn[j:]
    except Exception:
        pass
    return dsn

logger.info("DB DSN (masked): %s", _mask(ASYNC_DB_URL))

engine = create_async_engine(
    ASYNC_DB_URL,
    pool_pre_ping=True,   # handles stale connections
    echo=False,
    future=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

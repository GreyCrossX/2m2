from __future__ import annotations

from typing import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def create_session_factory(dsn: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(dsn, echo=False, pool_pre_ping=True, future=True)
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

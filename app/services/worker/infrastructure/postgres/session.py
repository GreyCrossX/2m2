from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def create_session_factory(dsn: str) -> async_sessionmaker[AsyncSession]:
    global _engine, _session_factory
    if _session_factory is not None:
        return _session_factory
    _engine = create_async_engine(dsn, pool_pre_ping=True, pool_size=5, max_overflow=10, future=True)
    _session_factory = async_sessionmaker(bind=_engine, expire_on_commit=False, autoflush=False, autocommit=False)
    return _session_factory


def get_engine():
    return _engine

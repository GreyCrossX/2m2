from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union
from uuid import UUID as _UUID

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.user import User
from app.db.session import get_db
from app.core import config

# Used by FastAPI's security utils & OpenAPI docs
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def _cfg(name: str, default: Any) -> Any:
    """Read setting from config, falling back to env, then default."""
    if hasattr(config, name):
        val = getattr(config, name)
        if val is not None:
            return val
    return os.getenv(name, default)


def _get_secret_key() -> str:
    # env override: SECRET_KEY
    return str(_cfg("SECRET_KEY", "super_secret_key"))


def _get_algorithm() -> str:
    # env override: ALGORITHM
    return str(_cfg("ALGORITHM", "HS256"))


def _get_expire_minutes() -> int:
    # Support both ACCESS_TOKEN_EXPIRE_MINUTES and the historical typo.
    raw = _cfg("ACCESS_TOKEN_EXPIRE_MINUTES", None)
    if raw is None:
        raw = _cfg("ACCESS_TOKE_EXPIRE_MINUTES", 60)  # fallback if typo exists
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 60


def create_access_token(
    subject: Union[str, int],
    expires_delta: Optional[timedelta] = None,
    extra_claims: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Create a signed JWT.
    - `subject` is stored in `sub` (we use user email today).
    - `extra_claims` lets you add roles, scopes, etc.
    """
    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "sub": str(subject),
        "iat": int(now.timestamp()),
    }
    if extra_claims:
        payload.update(extra_claims)

    if expires_delta is None:
        expires_delta = timedelta(minutes=_get_expire_minutes())
    payload["exp"] = int((now + expires_delta).timestamp())

    return jwt.encode(payload, _get_secret_key(), algorithm=_get_algorithm())


def decode_token(token: str) -> Dict[str, Any]:
    """Decode/verify a JWT and return its claims dict. Raises JWTError on failure."""
    if not token or not token.strip():
        raise ValueError("Empty token")
    return jwt.decode(token, _get_secret_key(), algorithms=[_get_algorithm()])


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Resolve the authenticated user from a Bearer JWT.
    - Expects `sub` to be the user's email (current behavior).
    - Falls back to UUID lookup if `sub` is not an email.
    """
    invalid = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = decode_token(token)
    except (JWTError, ValueError):
        raise invalid

    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub.strip():
        raise invalid

    # Primary: email subject (case-insensitive)
    stmt = select(User).where(func.lower(User.email) == sub.strip().lower())
    user = (await db.execute(stmt)).scalar_one_or_none()

    # Optional fallback: subject as UUID (future-proofing)
    if not user:
        try:
            uid = _UUID(sub)
        except Exception:
            raise invalid
        stmt2 = select(User).where(User.id == uid)
        user = (await db.execute(stmt2)).scalar_one_or_none()
        if not user:
            raise invalid

    return user

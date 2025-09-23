from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union

from jose import jwt  # Correct import for python-jose

from app.core import config


def _get_secret_key()-> str:
    return getattr(config, "SECRET_KEY", "super_secret_key")


def _get_algorithm()-> str:
    return getattr(config, "ALGORITHM", "HS256")


def _get_expire_minutes()-> int:
    # Note: config currently defines ACCESS_TOKE_EXPIRE_MINUTES (typo). Handle both.
    minutes = getattr(config, "ACCESS_TOKE_EXPIRE_MINUTES", None)
    if minutes is None:
        minutes = getattr(config, "ACCESS_TOKEN_EXPIRE_MINUTES", 60)
    try:
        return int(minutes)
    except (TypeError, ValueError):
        return 60


def create_access_token(
    subject: Union[str, int],
    expires_delta: Optional[timedelta] = None,
    extra_claims: Optional[Dict[str, Any]] = None,
)-> str:
    """
    Create a JWT access token.

    subject: The principal the token is issued to (e.g., user id or email).
    expires_delta: Optional timedelta for expiration; otherwise uses config minutes.
    extra_claims: Optional additional claims to include in payload.
    """
    to_encode: Dict[str, Any] = {}
    if extra_claims:
        to_encode.update(extra_claims)

    now = datetime.now(timezone.utc)
    to_encode.update(
        {
            "sub": str(subject),
            "iat": int(now.timestamp()),
        }
    )

    if expires_delta is None:
        expires_delta = timedelta(minutes=_get_expire_minutes())

    expire = now + expires_delta
    to_encode["exp"] = int(expire.timestamp())

    secret_key = _get_secret_key()
    algorithm = _get_algorithm()
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algorithm)
    return encoded_jwt


def decode_token(token: str)-> Dict[str, Any]:
    """
    Decode and validate a JWT token.

    Raises:
        JWTError (from jose) for invalid signatures, expired tokens, etc.
        ValueError if token is empty.
    """
    if not token or not token.strip():
        raise ValueError("Empty token")

    secret_key = _get_secret_key()
    algorithm = _get_algorithm()

    # Note: jose.jwt.decode raises JWTError on invalid/expired tokens.
    payload = jwt.decode(token, secret_key, algorithms=[algorithm])
    return payload

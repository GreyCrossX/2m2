from __future__ import annotations

import os
import base64
import hashlib
from typing import Final
from cryptography.fernet import Fernet, InvalidToken

# Derive a Fernet key from a master secret in env.
# Accepts any string; we SHA-256 it and urlsafe_b64encode to 32-byte Fernet key.
_ENV_KEY_NAME: Final[str] = "CREDENTIALS_MASTER_KEY"

_fernet: Fernet | None = None


def _ensure_fernet() -> Fernet:
    global _fernet
    if _fernet is not None:
        return _fernet

    raw = os.getenv(_ENV_KEY_NAME)
    if not raw:
        raise RuntimeError(
            f"{_ENV_KEY_NAME} is not set; cannot (de)crypt API credentials. "
            "Set a strong, unpredictable value in your environment."
        )

    # Normalize to bytes, then derive a 32-byte key (Fernet requires 32 urlsafe base64 bytes)
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    else:
        raw_bytes = raw

    digest = hashlib.sha256(raw_bytes).digest()  # 32 bytes
    fkey = base64.urlsafe_b64encode(digest)  # Fernet-formatted key
    _fernet = Fernet(fkey)
    return _fernet


_PREFIX: Final[str] = "v1:"  # simple versioning for future key/alg rotation


def encrypt_secret(plaintext: str) -> str:
    """
    Encrypt a secret string (e.g., api_key/api_secret).
    Returns a versioned ciphertext string.
    """
    if plaintext is None:
        raise ValueError("plaintext must not be None")
    f = _ensure_fernet()
    token = f.encrypt(plaintext.encode("utf-8"))
    return f"{_PREFIX}{token.decode('utf-8')}"


def decrypt_secret(ciphertext: str) -> str:
    """
    Decrypt a versioned ciphertext string back to plaintext.
    Raises InvalidToken on tampering / wrong key.
    """
    if not isinstance(ciphertext, str) or ":" not in ciphertext:
        raise InvalidToken("invalid ciphertext format")

    prefix, raw = ciphertext.split(":", 1)
    if prefix != "v1":
        # Support future rotations; for now only v1
        raise InvalidToken(f"unsupported ciphertext version: {prefix}")

    f = _ensure_fernet()
    data = f.decrypt(raw.encode("utf-8"))
    return data.decode("utf-8")

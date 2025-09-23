# app/config.py
from __future__ import annotations
from pathlib import Path
from urllib.parse import quote_plus
import re
from pydantic import Field, AliasChoices
from pydantic_settings import SettingsConfigDict, BaseSettings
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)  # load the root .env explicitly
except Exception:
    pass


_PLACEHOLDER_PAT = re.compile(
    r"//\s*(username|user|usr)\s*:\s*(password|pass|pwd)@", re.IGNORECASE
)

def _normalize_to_asyncpg(dsn: str) -> str:
    """Normalize to postgresql+asyncpg:// ..."""
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    if dsn.startswith("postgresql://") and "+asyncpg" not in dsn:
        dsn = "postgresql+asyncpg://" + dsn[len("postgresql://"):]
    return dsn


class Settings(BaseSettings):
    # Full DSN options
    DB_URL: str | None = None
    DATABASE_URL: str | None = None

    # Component options
    POSTGRES_USER: str | None = None
    POSTGRES_PASSWORD: str | None = None
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str | None = None

    # Secrets
    SECRET_KEY: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SECRET_KEY", "secret_key", "jwt_secret", "super_secret_key"),
    )
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    # Config
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    #redis
    REDIS_URL : str | None = None
    PAIRS_1M: str = "BTCUSDT:1m"

    def pairs_1m_list(self) -> List[Tuple[str, str]]:
        pairs: List[Tuple[str, str]] = []
        if not self.PAIRS_1M:
            return pairs
        for token in self.PAIRS_1M.split(","):
            token = token.strip()
            if not token:
                continue
            # tolerate SYMBOL or SYMBOL:1m; default timeframe to 1m
            if ":" in token:
                sym, tf = token.split(":", 1)
            else:
                sym, tf = token, "1m"
            pairs.append((sym.strip(), tf.strip()))
        return pairs

    @property
    def jwt_secret(self) -> str:
        if self.SECRET_KEY:
            return self.SECRET_KEY
        raise RuntimeError("SECRET_KEY (or alias) not set in .env")

    def _build_from_components(self) -> str | None:
        if self.POSTGRES_USER and self.POSTGRES_PASSWORD and self.POSTGRES_DB:
            pwd = quote_plus(self.POSTGRES_PASSWORD)
            return (
                f"postgresql+asyncpg://{self.POSTGRES_USER}:{pwd}"
                f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
        return None

    @property
    def db_url_async(self) -> str:
        """
        Choose a safe async DSN. Prefer DB_URL/DATABASE_URL unless they look like placeholders,
        otherwise build from POSTGRES_* parts. Raise if nothing valid.
        """
        env_dsn = self.DB_URL or self.DATABASE_URL
        if env_dsn:
            norm = _normalize_to_asyncpg(env_dsn.strip())
            # Reject obvious placeholders like //username:password@
            if _PLACEHOLDER_PAT.search(norm) or "://username:" in norm:
                # fall back to components
                built = self._build_from_components()
                if built:
                    return built
                raise RuntimeError(
                    "DATABASE_URL/DB_URL looks like a placeholder and POSTGRES_* is incomplete."
                )
            return norm

        built = self._build_from_components()
        if built:
            return built

        raise RuntimeError("Set DB_URL/DATABASE_URL or complete POSTGRES_* in .env")

    @property
    def db_url_source(self) -> str:
        """Human-readable source for logging."""
        if self.DB_URL:
            return "DB_URL"
        if self.DATABASE_URL:
            return "DATABASE_URL"
        if self.POSTGRES_USER and self.POSTGRES_PASSWORD and self.POSTGRES_DB:
            return "POSTGRES_*"
        return "UNSET"


settings = Settings()

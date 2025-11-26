"""Domain-level exceptions shared across services."""

from __future__ import annotations


class DomainError(Exception):
    """Base exception for domain-level failures."""


class DomainExchangeError(DomainError):
    """Base class for normalized exchange failures."""

    def __init__(self, message: str, *, code: int | str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.message} (code={self.code})" if self.code is not None else self.message


class DomainBadRequest(DomainExchangeError):
    """The exchange rejected the request due to invalid parameters."""


class DomainAuthError(DomainExchangeError):
    """Authentication failed due to invalid credentials or permissions."""


class DomainRateLimit(DomainExchangeError):
    """Exchange rate limits were exceeded; caller should retry with backoff."""


class DomainExchangeDown(DomainExchangeError):
    """Exchange is unavailable due to network/server errors."""

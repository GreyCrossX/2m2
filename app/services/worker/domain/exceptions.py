from __future__ import annotations


class WorkerException(Exception):
    """Base exception for Worker domain errors."""


class InsufficientBalanceException(WorkerException):
    """Raised when available balance is below the minimum required."""


class InvalidSignalException(WorkerException):
    """Raised when an incoming signal payload is invalid or inconsistent."""


class BinanceAPIException(WorkerException):
    """Raised when Binance API returns an error that should surface to the application."""


class DomainExchangeError(WorkerException):
    """Base class for normalized exchange failures from infrastructure adapters."""


class DomainBadRequest(DomainExchangeError):
    """The exchange rejected the request due to invalid parameters."""


class DomainAuthError(DomainExchangeError):
    """Authentication failed (bad key/secret or insufficient permissions)."""


class DomainRateLimit(DomainExchangeError):
    """Exchange rate limits exceeded; caller should retry with backoff."""


class DomainExchangeDown(DomainExchangeError):
    """Exchange is unavailable (5xx, network issues, etc.)."""


class OrderNotFoundException(WorkerException):
    """Raised when an order expected to exist cannot be found."""

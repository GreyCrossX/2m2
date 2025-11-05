from __future__ import annotations

from app.services.domain.exceptions import (
    DomainAuthError,
    DomainBadRequest,
    DomainExchangeDown,
    DomainExchangeError,
    DomainRateLimit,
)


class WorkerException(Exception):
    """Base exception for Worker domain errors."""


class InsufficientBalanceException(WorkerException):
    """Raised when available balance is below the minimum required."""


class InvalidSignalException(WorkerException):
    """Raised when an incoming signal payload is invalid or inconsistent."""


class BinanceAPIException(WorkerException):
    """Raised when Binance API returns an error that should surface to the application."""


class BinanceBadRequestException(BinanceAPIException):
    """Raised when Binance rejects a request due to invalid parameters."""


class BinanceRateLimitException(BinanceAPIException):
    """Raised when Binance signals a rate-limit condition."""


class BinanceExchangeDownException(BinanceAPIException):
    """Raised when Binance is unavailable due to server/network failure."""


class OrderNotFoundException(WorkerException):
    """Raised when an order expected to exist cannot be found."""


__all__ = [
    "WorkerException",
    "InsufficientBalanceException",
    "InvalidSignalException",
    "BinanceAPIException",
    "BinanceBadRequestException",
    "BinanceRateLimitException",
    "BinanceExchangeDownException",
    "OrderNotFoundException",
    "DomainBadRequest",
    "DomainAuthError",
    "DomainRateLimit",
    "DomainExchangeDown",
    "DomainExchangeError",
]

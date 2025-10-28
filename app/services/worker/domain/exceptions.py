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


class OrderNotFoundException(WorkerException):
    """Raised when an order expected to exist cannot be found."""

__all__ = [
    "WorkerException",
    "InsufficientBalanceException",
    "InvalidSignalException",
    "BinanceAPIException",
    "OrderNotFoundException",
    "DomainBadRequest",
    "DomainAuthError",
    "DomainRateLimit",
    "DomainExchangeDown",
    "DomainExchangeError",
]

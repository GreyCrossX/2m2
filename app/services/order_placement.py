"""Order placement orchestration helpers used by the worker OrderExecutor."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Optional, Protocol, Sequence

from app.services.worker.domain.enums import OrderSide, exit_side_for
from app.services.worker.domain.exceptions import (
    BinanceAPIException,
    BinanceBadRequestException,
    BinanceExchangeDownException,
    BinanceRateLimitException,
    DomainBadRequest,
    DomainExchangeDown,
    DomainExchangeError,
    DomainRateLimit,
)


class TradingPort(Protocol):
    async def create_limit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool,
        time_in_force: str,
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    async def create_stop_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
        reduce_only: bool,
        order_type: str,
        time_in_force: str | None = None,
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    async def create_take_profit_limit(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        stop_price: Decimal,
        reduce_only: bool,
        time_in_force: str,
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    async def cancel_order(self, *, symbol: str, order_id: int) -> None: ...


@dataclass(slots=True)
class TrioOrderResult:
    entry_order_id: Optional[int]
    stop_order_id: Optional[int]
    take_profit_order_id: Optional[int]


def _log_context(base: Mapping[str, str] | None) -> Mapping[str, str]:
    return dict(base or {})


def _to_int_or_none(value: object) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _extract_order_id(resp: object) -> Optional[int]:
    if not isinstance(resp, dict):
        return None
    value = resp.get("orderId")
    if value in (None, ""):
        value = resp.get("order_id")
    return _to_int_or_none(value)


def _is_gte_requires_open_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "time in force" in msg and "gte" in msg and "open positions" in msg


class OrderPlacementService:
    """Provide atomic placement and rollback for entry/stop/take-profit orders."""

    def __init__(
        self, trading: TradingPort, *, logger: logging.Logger | None = None
    ) -> None:
        self._trading = trading
        self._log = logger or logging.getLogger(__name__)

    async def place_trio_orders(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        take_profit_price: Decimal,
        reduce_only: bool = False,
        context: Mapping[str, str] | None = None,
        stop_client_order_id: Optional[str] = None,
        tp_client_order_id: Optional[str] = None,
    ) -> TrioOrderResult:
        ctx = _log_context(context)
        exit_side = exit_side_for(side)

        try:
            entry_resp = await self._trading.create_limit_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=entry_price,
                reduce_only=reduce_only,
                time_in_force="GTC",
            )
        except DomainBadRequest as exc:
            raise BinanceBadRequestException(str(exc)) from exc
        except DomainRateLimit as exc:
            raise BinanceRateLimitException(str(exc)) from exc
        except DomainExchangeDown as exc:
            raise BinanceExchangeDownException(str(exc)) from exc
        except DomainExchangeError as exc:
            raise BinanceAPIException(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise BinanceAPIException(
                f"Unexpected create_limit_order error: {exc}"
            ) from exc

        entry_id = _extract_order_id(entry_resp)
        self._log.info(
            "order.entry_placed",
            extra={
                **ctx,
                "symbol": symbol,
                "entry_order_id": entry_id,
                "side": side.value,
            },
        )

        stop_id: Optional[int] = None
        tp_id: Optional[int] = None

        exit_tif: str | None = "GTE_GTC"
        has_pos = getattr(self._trading, "has_open_position", None)
        if callable(has_pos):
            try:
                if not bool(await has_pos(symbol)):
                    exit_tif = None
            except Exception:
                exit_tif = "GTE_GTC"

        try:
            try:
                stop_resp = await self._trading.create_stop_market_order(
                    symbol=symbol,
                    side=exit_side,
                    quantity=quantity,
                    stop_price=stop_price,
                    reduce_only=True,
                    order_type="STOP_MARKET",
                    time_in_force=exit_tif,
                    new_client_order_id=stop_client_order_id,
                )
            except Exception as exc:
                if exit_tif and _is_gte_requires_open_error(exc):
                    stop_resp = await self._trading.create_stop_market_order(
                        symbol=symbol,
                        side=exit_side,
                        quantity=quantity,
                        stop_price=stop_price,
                        reduce_only=True,
                        order_type="STOP_MARKET",
                        time_in_force=None,
                        new_client_order_id=stop_client_order_id,
                    )
                else:
                    raise
            stop_id = _extract_order_id(stop_resp)
            self._log.info(
                "order.stop_placed",
                extra={
                    **ctx,
                    "symbol": symbol,
                    "stop_order_id": stop_id,
                    "side": exit_side.value,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "order.stop_placement_failed",
                extra={
                    **ctx,
                    "symbol": symbol,
                    "entry_order_id": entry_id,
                    "error": str(exc),
                },
            )

        try:
            try:
                tp_resp = await self._trading.create_stop_market_order(
                    symbol=symbol,
                    side=exit_side,
                    quantity=quantity,
                    stop_price=take_profit_price,
                    reduce_only=True,
                    order_type="TAKE_PROFIT_MARKET",
                    time_in_force=exit_tif,
                    new_client_order_id=tp_client_order_id,
                )
            except Exception as exc:
                if exit_tif and _is_gte_requires_open_error(exc):
                    tp_resp = await self._trading.create_stop_market_order(
                        symbol=symbol,
                        side=exit_side,
                        quantity=quantity,
                        stop_price=take_profit_price,
                        reduce_only=True,
                        order_type="TAKE_PROFIT_MARKET",
                        time_in_force=None,
                        new_client_order_id=tp_client_order_id,
                    )
                else:
                    raise
            tp_id = _extract_order_id(tp_resp)
            self._log.info(
                "order.tp_placed",
                extra={
                    **ctx,
                    "symbol": symbol,
                    "tp_order_id": tp_id,
                    "side": exit_side.value,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "order.tp_placement_failed",
                extra={
                    **ctx,
                    "symbol": symbol,
                    "entry_order_id": entry_id,
                    "error": str(exc),
                },
            )

        return TrioOrderResult(
            entry_order_id=entry_id,
            stop_order_id=stop_id,
            take_profit_order_id=tp_id,
        )
        # Note: validation of order ids happens in caller.

    async def rollback_orders(
        self,
        symbol: str,
        order_ids: Sequence[int | str | None],
        *,
        context: Mapping[str, str] | None = None,
    ) -> None:
        ctx = _log_context(context)
        for oid in order_ids:
            if oid in (None, ""):
                continue
            try:
                await self._trading.cancel_order(symbol=symbol, order_id=int(oid))
                self._log.info(
                    "order.rollback_cancelled",
                    extra={**ctx, "symbol": symbol, "order_id": oid},
                )
            except Exception as exc:  # noqa: BLE001
                self._log.warning(
                    "order.rollback_failed",
                    extra={**ctx, "symbol": symbol, "order_id": oid, "error": str(exc)},
                )

"""
Order Payload Builders (pure)

- Consumes: sym, side, qty, trigger, stop, optional tp_ratio, avg_entry_price,
  working type, hedge/one-way flags
- Produces: OrderPayload dicts for Entry, SL, TP (no I/O)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional, Tuple

from .contracts import (
    Side,
    WorkingType,
    OrderPayload,
    BracketPayloads,
    ExchangeFilters,
)
from .exchange_filters import apply_symbol_filters


def build_entry_order(
    *,
    sym: str,
    side: Side,                # "long" | "short"
    qty: Decimal,
    trigger: Decimal,
    working_type: WorkingType = "MARK_PRICE",
    position_side: Optional[str] = None,  # hedge mode: "LONG" | "SHORT"
    client_order_id: Optional[str] = None,
    filters: Optional[ExchangeFilters] = None,
) -> OrderPayload:
    """
    Entry as STOP_MARKET at trigger (breakout-style).
    """
    buy_sell = "BUY" if side == "long" else "SELL"

    payload: OrderPayload = {
        "symbol": sym,
        "side": buy_sell,
        "order_type": "STOP_MARKET",
        "quantity": qty,
        "stop_price": trigger,
        "working_type": working_type,
    }
    if client_order_id:
        payload["client_order_id"] = client_order_id  # mapped later in exchange facade

    # Optional hedge mode: passthrough key (supported by Binance futures)
    if position_side in ("LONG", "SHORT"):
        # type: ignore[typeddict-item]
        payload["positionSide"] = position_side  # runtime-safe; typed dict is total=False

    return apply_symbol_filters(payload, filters or {}) if filters is not None else payload


def build_stop_loss_order(
    *,
    sym: str,
    side: Side,               # side of the OPEN position ("long"|"short")
    qty: Decimal,
    stop: Decimal,
    working_type: WorkingType = "MARK_PRICE",
    position_side: Optional[str] = None,
    client_order_id: Optional[str] = None,
    filters: Optional[ExchangeFilters] = None,
) -> OrderPayload:
    """
    Reduce-only STOP_MARKET to close the position at 'stop'.
    """
    # To close a long, we SELL; to close a short, we BUY.
    close_side = "SELL" if side == "long" else "BUY"

    payload: OrderPayload = {
        "symbol": sym,
        "side": close_side,
        "order_type": "STOP_MARKET",
        "quantity": qty,
        "stop_price": stop,
        "reduce_only": True,
        "close_position": True,
        "working_type": working_type,
    }
    if client_order_id:
        payload["client_order_id"] = client_order_id
    if position_side in ("LONG", "SHORT"):
        # type: ignore[typeddict-item]
        payload["positionSide"] = position_side

    return apply_symbol_filters(payload, filters or {}) if filters is not None else payload


def _tp_price_from_r_multiple(
    *,
    avg_entry: Decimal,
    stop: Decimal,
    side: Side,
    tp_ratio: Decimal,
) -> Decimal:
    """
    Compute TP price using an R-multiple:
      R = |entry - stop|
      long:  tp = entry + tp_ratio * R
      short: tp = entry - tp_ratio * R
    """
    distance = (avg_entry - stop).copy_abs()
    if side == "long":
        return avg_entry + tp_ratio * distance
    else:
        return avg_entry - tp_ratio * distance


def build_take_profit_order(
    *,
    sym: str,
    side: Side,                # side of the OPEN position
    qty: Decimal,
    avg_entry_price: Decimal,
    stop: Decimal,
    tp_ratio: Decimal,         # R-multiple (e.g., 1.8)
    working_type: WorkingType = "MARK_PRICE",
    position_side: Optional[str] = None,
    client_order_id: Optional[str] = None,
    filters: Optional[ExchangeFilters] = None,
) -> Tuple[OrderPayload, Decimal]:
    """
    Reduce-only TAKE_PROFIT_MARKET using R-multiple from avg_entry and stop.
    Returns (payload, tp_price)
    """
    close_side = "SELL" if side == "long" else "BUY"
    tp_price = _tp_price_from_r_multiple(
        avg_entry=avg_entry_price, stop=stop, side=side, tp_ratio=tp_ratio
    )

    payload: OrderPayload = {
        "symbol": sym,
        "side": close_side,
        "order_type": "TAKE_PROFIT_MARKET",
        "quantity": qty,
        "stop_price": tp_price,
        "reduce_only": True,
        "close_position": True,
        "working_type": working_type,
    }
    if client_order_id:
        payload["client_order_id"] = client_order_id
    if position_side in ("LONG", "SHORT"):
        # type: ignore[typeddict-item]
        payload["positionSide"] = position_side

    payload = apply_symbol_filters(payload, filters or {}) if filters is not None else payload
    # Note: tp_price might have been rounded inside payload; return what we sent:
    final_tp = payload["stop_price"] if "stop_price" in payload else tp_price
    return payload, final_tp  # type: ignore[return-value]


def build_brackets(
    *,
    sym: str,
    side: Side,
    qty: Decimal,
    avg_entry_price: Decimal,
    stop: Decimal,
    tp_ratio: Decimal,
    working_type: WorkingType = "MARK_PRICE",
    position_side: Optional[str] = None,
    sl_client_order_id: Optional[str] = None,
    tp_client_order_id: Optional[str] = None,
    filters: Optional[ExchangeFilters] = None,
) -> Tuple[BracketPayloads, Decimal]:
    """
    Convenience: build both SL and TP reduce-only orders.
    Returns (brackets, tp_price_final)
    """
    sl = build_stop_loss_order(
        sym=sym,
        side=side,
        qty=qty,
        stop=stop,
        working_type=working_type,
        position_side=position_side,
        client_order_id=sl_client_order_id,
        filters=filters,
    )
    tp, tp_price = build_take_profit_order(
        sym=sym,
        side=side,
        qty=qty,
        avg_entry_price=avg_entry_price,
        stop=stop,
        tp_ratio=tp_ratio,
        working_type=working_type,
        position_side=position_side,
        client_order_id=tp_client_order_id,
        filters=filters,
    )
    return {"stop_loss": sl, "take_profit": tp}, tp_price

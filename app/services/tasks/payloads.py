from __future__ import annotations

from decimal import Decimal
from typing import Dict, Tuple

from .exchange_filters import apply_symbol_filters


def _side_to_exchange(side: str, *, entry: bool = True) -> str:
    side = str(side).lower()
    if entry:
        return "BUY" if side == "long" else "SELL"
    return "SELL" if side == "long" else "BUY"


def _norm_filters(filters: Dict[str, object]) -> Dict[str, object]:
    return dict(filters or {})


def build_entry_order(
    *,
    sym: str,
    side: str,
    qty: Decimal,
    trigger: Decimal,
    working_type: str,
    client_order_id: str,
    filters: Dict[str, object],
) -> Dict[str, object]:
    payload = {
        "symbol": sym.upper(),
        "side": _side_to_exchange(side, entry=True),
        "order_type": "STOP_MARKET",
        "quantity": qty,
        "stop_price": trigger,
        "working_type": working_type,
        "client_order_id": client_order_id,
    }
    return apply_symbol_filters(payload, _norm_filters(filters))


def build_stop_loss_order(
    *,
    sym: str,
    side: str,
    qty: Decimal,
    stop: Decimal,
    working_type: str,
    client_order_id: str,
    filters: Dict[str, object],
) -> Dict[str, object]:
    payload = {
        "symbol": sym.upper(),
        "side": _side_to_exchange(side, entry=False),
        "order_type": "STOP_MARKET",
        "quantity": qty,
        "stop_price": stop,
        "working_type": working_type,
        "client_order_id": client_order_id,
        "reduce_only": True,
        "close_position": True,
    }
    return apply_symbol_filters(payload, _norm_filters(filters))


def build_take_profit_order(
    *,
    sym: str,
    side: str,
    qty: Decimal,
    avg_entry_price: Decimal,
    stop: Decimal,
    tp_ratio: Decimal,
    working_type: str,
    client_order_id: str,
    filters: Dict[str, object],
) -> Tuple[Dict[str, object], Decimal]:
    entry = Decimal(avg_entry_price)
    stop_price = Decimal(stop)
    ratio = Decimal(tp_ratio)
    if side.lower() == "long":
        distance = entry - stop_price
        tp_price = entry + (distance * ratio)
        order_type = "TAKE_PROFIT_MARKET"
    else:
        distance = stop_price - entry
        tp_price = entry - (distance * ratio)
        order_type = "TAKE_PROFIT_MARKET"

    payload = {
        "symbol": sym.upper(),
        "side": _side_to_exchange(side, entry=False),
        "order_type": order_type,
        "quantity": qty,
        "stop_price": tp_price,
        "working_type": working_type,
        "client_order_id": client_order_id,
        "reduce_only": True,
        "close_position": True,
    }
    payload = apply_symbol_filters(payload, _norm_filters(filters))
    return payload, payload["stop_price"]  # post-filtered tp


def build_brackets(
    *,
    sym: str,
    side: str,
    qty: Decimal,
    avg_entry_price: Decimal,
    stop: Decimal,
    tp_ratio: Decimal,
    working_type: str,
    sl_client_order_id: str,
    tp_client_order_id: str,
    filters: Dict[str, object],
) -> Tuple[Dict[str, Dict[str, object]], Decimal]:
    sl = build_stop_loss_order(
        sym=sym,
        side=side,
        qty=qty,
        stop=stop,
        working_type=working_type,
        client_order_id=sl_client_order_id,
        filters=filters,
    )
    tp_payload, tp_price = build_take_profit_order(
        sym=sym,
        side=side,
        qty=qty,
        avg_entry_price=avg_entry_price,
        stop=stop,
        tp_ratio=tp_ratio,
        working_type=working_type,
        client_order_id=tp_client_order_id,
        filters=filters,
    )
    return {"stop_loss": sl, "take_profit": tp_payload}, tp_price


__all__ = [
    "build_entry_order",
    "build_stop_loss_order",
    "build_take_profit_order",
    "build_brackets",
]

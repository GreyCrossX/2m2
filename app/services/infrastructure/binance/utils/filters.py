"""Helpers for working with Binance symbol filters."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict

SymbolFilters = Dict[str, Dict[str, Any]]


def build_symbol_filters(exchange_info: Dict[str, Any]) -> Dict[str, SymbolFilters]:
    """Map symbol -> filter type -> raw filter dict."""

    symbols = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else []
    result: Dict[str, SymbolFilters] = {}
    for entry in symbols:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).upper()
        if not symbol:
            continue
        filters = entry.get("filters") or []
        by_type: SymbolFilters = {}
        for filt in filters:
            if not isinstance(filt, dict):
                continue
            filter_type = str(filt.get("filterType", ""))
            if not filter_type:
                continue
            by_type[filter_type] = filt
        result[symbol] = by_type
    return result


def quantize_qty(symbol_filters: SymbolFilters, qty: Decimal) -> Decimal:
    """Quantize quantity using LOT_SIZE/MARKET_LOT_SIZE filters."""

    quantity = _ensure_decimal(qty)
    if quantity <= 0:
        return Decimal("0")

    lot = symbol_filters.get("LOT_SIZE") or symbol_filters.get("MARKET_LOT_SIZE") or {}
    step = _ensure_positive_decimal(lot.get("stepSize"))
    min_qty = _ensure_positive_decimal(lot.get("minQty"))
    max_qty = _ensure_positive_decimal(lot.get("maxQty"))

    if step > 0:
        quantizer = step.normalize()
        quantity = quantity.quantize(quantizer, rounding=ROUND_DOWN)

    if max_qty > 0 and quantity > max_qty:
        if step > 0:
            quantizer = step.normalize()
            quantity = max_qty.quantize(quantizer, rounding=ROUND_DOWN)
        else:
            quantity = max_qty

    if min_qty > 0 and quantity < min_qty:
        return Decimal("0")

    return quantity if quantity >= 0 else Decimal("0")


def quantize_price(symbol_filters: SymbolFilters, price: Decimal | None) -> Decimal | None:
    """Quantize price using PRICE_FILTER."""

    if price is None:
        return None
    px = _ensure_decimal(price)
    if px <= 0:
        return Decimal("0")

    price_filter = symbol_filters.get("PRICE_FILTER") or {}
    tick = _ensure_positive_decimal(price_filter.get("tickSize"))
    min_price = _ensure_positive_decimal(price_filter.get("minPrice"))
    max_price = _ensure_positive_decimal(price_filter.get("maxPrice"))

    if tick > 0:
        quantizer = tick.normalize()
        px = px.quantize(quantizer, rounding=ROUND_DOWN)

    if max_price > 0 and px > max_price:
        if tick > 0:
            quantizer = tick.normalize()
            px = max_price.quantize(quantizer, rounding=ROUND_DOWN)
        else:
            px = max_price

    if min_price > 0 and px < min_price:
        if tick > 0:
            quantizer = tick.normalize()
            px = min_price.quantize(quantizer, rounding=ROUND_DOWN)
        else:
            px = min_price

    return px if px >= 0 else Decimal("0")


def _ensure_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _ensure_positive_decimal(value: Any) -> Decimal:
    dec = _ensure_decimal(value)
    return dec if dec > 0 else Decimal("0")

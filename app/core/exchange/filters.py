from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict

SymbolFilters = Dict[str, Dict[str, Any]]


def build_symbol_filters(
    exchange_info: Dict[str, Any],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}

    symbols = exchange_info.get("symbols") or exchange_info.get("data") or []
    for s in symbols:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue

        filt_map: Dict[str, Dict[str, Any]] = {}
        for f in s.get("filters", []):
            ftype = f.get("filterType")
            if ftype:
                # keep all filters, including LOT_SIZE and MARKET_LOT_SIZE
                filt_map[ftype] = f

        # Normalize NOTIONAL name across APIs
        if "NOTIONAL" in filt_map and "MIN_NOTIONAL" not in filt_map:
            filt_map["MIN_NOTIONAL"] = filt_map["NOTIONAL"]

        # Carry meta precisions as a fallback when tick/step omitted by some SDKs
        filt_map["META"] = {
            "pricePrecision": s.get("pricePrecision"),
            "quantityPrecision": s.get("quantityPrecision"),
            "baseAssetPrecision": s.get("baseAssetPrecision"),
            "quotePrecision": s.get("quotePrecision"),
        }

        out[sym] = filt_map

    return out


# ---------- helpers ----------


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


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """
    Floor 'value' to an integer multiple of 'step'.
    Using ROUND_DOWN on (value/step).to_integral_value ensures we respect odd steps (e.g., 0.05).
    Then quantize to 'step' to preserve exponent/format.
    """
    if step <= 0:
        return value
    multiples = (value / step).to_integral_value(rounding=ROUND_DOWN)
    floored = multiples * step
    # Preserve exponent of step (e.g., 0.001 vs 0.01)
    return floored.quantize(step, rounding=ROUND_DOWN)


def _step_from_precision(precision: Any) -> Decimal:
    try:
        p = int(precision)
    except Exception:
        p = 0
    if p < 0:
        p = 0
    # 10**(-p)
    return Decimal(1).scaleb(-p)


# ---------- quantizers ----------


def quantize_qty(symbol_filters: SymbolFilters, qty: Decimal) -> Decimal:
    quantity = _ensure_decimal(qty)
    if quantity <= 0:
        return Decimal("0")

    lot = symbol_filters.get("LOT_SIZE") or {}
    mlot = symbol_filters.get("MARKET_LOT_SIZE") or {}

    # Prefer LOT_SIZE.stepSize; else MARKET_LOT_SIZE.stepSize; else derive from META.quantityPrecision
    step = _ensure_positive_decimal(lot.get("stepSize"))
    if step <= 0:
        step = _ensure_positive_decimal(mlot.get("stepSize"))
    if step <= 0:
        step = _step_from_precision(
            symbol_filters.get("META", {}).get("quantityPrecision")
        )

    # Floor to step (true multiple, not just decimal-place quantize)
    q = _floor_to_step(quantity, step) if step > 0 else quantity

    # Enforce min/max from whichever filter provides it (prefer LOT_SIZE values)
    min_qty = _ensure_positive_decimal(lot.get("minQty") or mlot.get("minQty"))
    max_qty = _ensure_positive_decimal(lot.get("maxQty") or mlot.get("maxQty"))

    if max_qty > 0 and q > max_qty:
        q = _floor_to_step(max_qty, step) if step > 0 else max_qty
    if min_qty > 0 and q < min_qty:
        return Decimal("0")

    return q if q >= 0 else Decimal("0")


def quantize_price(
    symbol_filters: SymbolFilters, price: Decimal | None
) -> Decimal | None:
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
        # True multiple of tick, not just exponent match
        px = _floor_to_step(px, tick)
    else:
        # Fallback to precision if tickSize missing
        tick = _step_from_precision(
            symbol_filters.get("META", {}).get("pricePrecision")
        )
        if tick > 0:
            px = _floor_to_step(px, tick)

    if max_price > 0 and px > max_price:
        px = _floor_to_step(max_price, tick) if tick > 0 else max_price
    if min_price > 0 and px < min_price:
        px = _floor_to_step(min_price, tick) if tick > 0 else min_price

    return px if px >= 0 else Decimal("0")

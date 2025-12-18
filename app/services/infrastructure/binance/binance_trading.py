"""High-level trading helpers built on top of :class:`BinanceClient`."""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Iterable, List, Tuple

from app.services.domain.exceptions import DomainBadRequest, DomainExchangeError
from app.services.worker.domain.enums import OrderSide  # domain side
from .binance_client import BinanceClient

# ---- Validation sets ---------------------------------------------------------

_VALID_SIDES = {"BUY", "SELL"}
_VALID_ORDER_TYPES = {
    "LIMIT",
    "MARKET",
    "STOP_MARKET",
    "TAKE_PROFIT_MARKET",
    "TAKE_PROFIT",  # futures TP with price + stopPrice
}
_VALID_TIME_IN_FORCE = {"GTC", "IOC", "FOK", "GTX", "GTE_GTC"}
_VALID_POSITION_SIDES = {"BOTH", "LONG", "SHORT"}
_VALID_WORKING_TYPES = {"CONTRACT_PRICE", "MARK_PRICE"}


# ---- Utilities ---------------------------------------------------------------


def _normalize_side(side: OrderSide | str) -> str:
    """Map domain side to exchange side string."""
    if isinstance(side, OrderSide):
        return "BUY" if side == OrderSide.LONG else "SELL"
    side_str = str(side).strip().lower()
    if side_str == "long":
        return "BUY"
    if side_str == "short":
        return "SELL"
    normalized = side_str.upper()
    if normalized not in _VALID_SIDES:
        raise DomainBadRequest(f"Invalid side '{side}'")
    return normalized


def _validate_choice(value: str | None, valid: Iterable[str], field: str) -> str:
    if value is None:
        raise DomainBadRequest(f"Missing {field}")
    normalized = str(value).upper()
    if normalized not in {v.upper() for v in valid}:
        raise DomainBadRequest(f"Invalid {field} '{value}'")
    return normalized


def _as_decimal(value: Decimal | str | int | float | None, name: str) -> Decimal:
    if value is None:
        raise DomainBadRequest(f"Missing {name}")
    try:
        return Decimal(str(value))
    except Exception:
        raise DomainBadRequest(f"Invalid {name} '{value}'")


def _ensure_positive_decimal(name: str, value: Decimal | None) -> Decimal:
    v = _as_decimal(value, name)
    if v <= 0:
        raise DomainBadRequest(f"{name} must be > 0 (got {v})")
    return v


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Floor value to integer multiples of step, preserving exponent."""
    if step is None or step <= 0:
        return value
    multiples = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return (multiples * step).quantize(step, rounding=ROUND_DOWN)


def _final_qty_clamp(filters: Dict[str, Dict[str, Any]], q_qty: Decimal) -> Decimal:
    """Final clamp for qty using LOT_SIZE or MARKET_LOT_SIZE step."""
    lot = filters.get("LOT_SIZE") or {}
    mlot = filters.get("MARKET_LOT_SIZE") or {}
    step_raw = (lot.get("stepSize") or mlot.get("stepSize") or "0").strip()
    step = Decimal(step_raw) if step_raw not in ("", "0", None) else Decimal("0")
    if step > 0:
        q_qty = _floor_to_step(q_qty, step)

        # Enforce min/max defensively
        def _d(x: Any) -> Decimal:
            try:
                return Decimal(str(x))
            except Exception:
                return Decimal("0")

        min_qty = _d(lot.get("minQty") or mlot.get("minQty"))
        max_qty = _d(lot.get("maxQty") or mlot.get("maxQty"))
        if max_qty > 0 and q_qty > max_qty:
            q_qty = _floor_to_step(max_qty, step)
        if min_qty > 0 and q_qty < min_qty:
            return Decimal("0")
    return q_qty


# ---- Trading Facade ----------------------------------------------------------


class BinanceTrading:
    """
    Trading facade that performs validation and quantization before submitting orders.

    Notes:
      * Quantity/price rounding is delegated to BinanceClient.quantize_order(...).
      * We additionally apply a final, hard clamp to LOT_SIZE/MARKET_LOT_SIZE step
        right before submit to avoid precision errors.
      * All public helpers accept domain OrderSide (or a string) and map to BUY/SELL.
    """

    def __init__(self, client: BinanceClient) -> None:
        self._client = client
        self._filters_cache: dict[str, dict] = {}

    # --- Account / Config -----------------------------------------------------

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self._client.change_leverage(symbol.upper(), int(leverage))

    # --- Filters cache ---------------------------------------------------------

    async def get_symbol_filters(self, symbol: str) -> dict:
        sym = symbol.upper()
        if sym in self._filters_cache:
            return self._filters_cache[sym]

        info = await self._client.exchange_info()
        entry = next(
            (s for s in info.get("symbols", []) if s.get("symbol") == sym), None
        )
        filters: dict[str, dict] = {}
        if entry:
            for f in entry.get("filters", []):
                t = f.get("filterType")
                if t == "PRICE_FILTER":
                    filters["PRICE_FILTER"] = {
                        "tickSize": f.get("tickSize"),
                        "minPrice": f.get("minPrice"),
                        "maxPrice": f.get("maxPrice"),
                    }
                elif t == "LOT_SIZE":
                    filters["LOT_SIZE"] = {
                        "stepSize": f.get("stepSize"),
                        "minQty": f.get("minQty"),
                        "maxQty": f.get("maxQty"),
                    }
                elif t == "MARKET_LOT_SIZE":
                    filters["MARKET_LOT_SIZE"] = {
                        "stepSize": f.get("stepSize"),
                        "minQty": f.get("minQty"),
                        "maxQty": f.get("maxQty"),
                    }
                elif t in ("NOTIONAL", "MIN_NOTIONAL"):
                    filters[t] = {
                        "notional": f.get("notional")
                        or f.get("minNotional")
                        or f.get("minNotionalValue")
                    }

        # safe defaults
        filters.setdefault("LOT_SIZE", {"stepSize": "0.001", "minQty": "0"})
        filters.setdefault("PRICE_FILTER", {"tickSize": None})
        filters.setdefault("NOTIONAL", {"notional": "20"})
        self._filters_cache[sym] = filters
        return filters

    # --- Quantization ---------------------------------------------------------

    async def quantize_limit_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> Tuple[Decimal, Decimal | None]:
        """Return (q_qty, q_price) using exchange LOT_SIZE/PRICE_FILTER."""
        return await self._client.quantize_order(symbol.upper(), quantity, price)

    async def _quantize_price_only(self, symbol: str, price: Decimal) -> Decimal:
        """Quantize price using quantize_order; ignores qty output."""
        _, q_price = await self._client.quantize_order(
            symbol.upper(), Decimal("1"), price
        )
        if q_price is None or q_price <= 0:
            raise DomainBadRequest("Quantized price invalid")
        return q_price

    # --- Core Orders ----------------------------------------------------------

    async def create_limit_order(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        price: Decimal,
        *,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: str | None = None,
        position_side: str | None = None,  # optional dual-side param
    ) -> Dict[str, Any]:
        """Create a validated LIMIT order (reduceOnly supported for futures)."""
        sym = symbol.upper()
        side_str = _normalize_side(side)
        tif = _validate_choice(time_in_force, _VALID_TIME_IN_FORCE, "time_in_force")
        if position_side:
            position_side = _validate_choice(
                position_side, _VALID_POSITION_SIDES, "position_side"
            )

        filters = await self.get_symbol_filters(sym)

        # Client quantization (primary)
        q_qty, q_price = await self.quantize_limit_order(sym, quantity, price)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            raise DomainBadRequest("Quantized quantity/price invalid for LIMIT order")

        # Final hard clamp by LOT_SIZE step (prevents precision errors)
        q_qty = _final_qty_clamp(filters, q_qty)
        if q_qty <= 0:
            raise DomainBadRequest("Quantity below minimum after final clamp")

        # Min notional check
        mn = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
        mn_val = mn.get("notional") or "0"
        min_notional = (
            Decimal(str(mn_val)) if mn_val not in (None, "") else Decimal("0")
        )
        notional = q_qty * q_price
        if min_notional > 0 and notional < min_notional:
            raise DomainBadRequest(
                f"Notional {notional} below minimum {min_notional} for {sym}"
            )

        payload: Dict[str, Any] = {
            "symbol": sym,
            "side": side_str,
            "type": "LIMIT",
            "quantity": q_qty,
            "price": q_price,
            "timeInForce": tif,
            "reduceOnly": bool(reduce_only),
        }
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id
        if position_side:
            payload["positionSide"] = position_side

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = (
                f"{exc} | symbol={sym} qty={q_qty} price={q_price} "
                f"reduce_only={reduce_only} tif={tif}"
            )
            raise type(exc)(message) from exc

    async def create_market_order(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        *,
        reduce_only: bool = False,
        position_side: str | None = None,
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create MARKET order (price not applicable; still quantize qty)."""
        sym = symbol.upper()
        side_str = _normalize_side(side)
        if position_side:
            position_side = _validate_choice(
                position_side, _VALID_POSITION_SIDES, "position_side"
            )

        filters = await self.get_symbol_filters(sym)
        q_qty, _ = await self._client.quantize_order(sym, quantity, Decimal("1"))
        if q_qty <= 0:
            raise DomainBadRequest("Quantized quantity invalid for MARKET order")

        # Final clamp by LOT_SIZE step
        q_qty = _final_qty_clamp(filters, q_qty)
        if q_qty <= 0:
            raise DomainBadRequest("Quantity below minimum after final clamp")

        payload: Dict[str, Any] = {
            "symbol": sym,
            "side": side_str,
            "type": "MARKET",
            "quantity": q_qty,
            "reduceOnly": bool(reduce_only),
        }
        if position_side:
            payload["positionSide"] = position_side
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = f"{exc} | symbol={sym} qty={q_qty} reduce_only={reduce_only}"
            raise type(exc)(message) from exc

    async def create_stop_market_order(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        stop_price: Decimal,
        *,
        working_type: str | None = None,  # CONTRACT_PRICE / MARK_PRICE
        position_side: str = "BOTH",
        order_type: str = "STOP_MARKET",
        reduce_only: bool = True,
        time_in_force: str | None = None,
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        """
        Create a STOP_MARKET or TAKE_PROFIT_MARKET order (reduceOnly implicit via position mode).
        """
        sym = symbol.upper()
        order_type = _validate_choice(order_type, _VALID_ORDER_TYPES, "type")
        if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise DomainBadRequest(
                "Stop order must be STOP_MARKET or TAKE_PROFIT_MARKET"
            )

        side_str = _normalize_side(side)
        position_side = _validate_choice(
            position_side, _VALID_POSITION_SIDES, "position_side"
        )
        if working_type:
            working_type = _validate_choice(
                working_type, _VALID_WORKING_TYPES, "working_type"
            )

        filters = await self.get_symbol_filters(sym)
        q_qty, q_stop = await self._client.quantize_order(sym, quantity, stop_price)
        if q_qty <= 0 or q_stop is None or q_stop <= 0:
            raise DomainBadRequest(
                "Quantized quantity/stop invalid for stop-market order"
            )

        # Final clamp by LOT_SIZE step
        q_qty = _final_qty_clamp(filters, q_qty)
        if q_qty <= 0:
            raise DomainBadRequest("Quantity below minimum after final clamp")

        payload: Dict[str, Any] = {
            "symbol": sym,
            "side": side_str,
            "type": order_type,
            "quantity": q_qty,
            "stopPrice": q_stop,
            "positionSide": position_side,
        }
        payload["reduceOnly"] = bool(reduce_only)
        if working_type:
            payload["workingType"] = working_type
        if time_in_force:
            payload["timeInForce"] = _validate_choice(
                time_in_force, _VALID_TIME_IN_FORCE, "time_in_force"
            )
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = (
                f"{exc} | symbol={sym} qty={q_qty} stopPrice={q_stop} "
                f"type={order_type} positionSide={position_side}"
            )
            raise type(exc)(message) from exc

    # --- TAKE_PROFIT_LIMIT (reduce-only) --------------------------------------

    async def create_take_profit_limit(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        price: Decimal,  # limit price
        stop_price: Decimal,  # trigger price
        *,
        time_in_force: str = "GTC",
        reduce_only: bool = True,
        working_type: str | None = None,  # CONTRACT_PRICE / MARK_PRICE
        position_side: str | None = None,  # LONG/SHORT/BOTH (if dual-side)
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        """
        Futures TAKE_PROFIT (limit-style):
          - type="TAKE_PROFIT"
          - price      = limit price (quantized)
          - stopPrice  = trigger (quantized)
          - timeInForce= GTC by default
          - reduceOnly = True by default (recommended for exits)
        """
        sym = symbol.upper()
        side_str = _normalize_side(side)
        tif = _validate_choice(time_in_force, _VALID_TIME_IN_FORCE, "time_in_force")
        if working_type:
            working_type = _validate_choice(
                working_type, _VALID_WORKING_TYPES, "working_type"
            )
        else:
            working_type = "CONTRACT_PRICE"
        if position_side:
            position_side = _validate_choice(
                position_side, _VALID_POSITION_SIDES, "position_side"
            )
        else:
            position_side = "BOTH"

        filters = await self.get_symbol_filters(sym)

        # Quantize qty & price, then final clamp on qty; quantize stop separately
        q_qty, q_price = await self.quantize_limit_order(sym, quantity, price)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            raise DomainBadRequest("Quantized qty/price invalid for TAKE_PROFIT")
        q_qty = _final_qty_clamp(filters, q_qty)
        if q_qty <= 0:
            raise DomainBadRequest("Quantity below minimum after final clamp")

        q_stop = await self._quantize_price_only(sym, stop_price)
        if q_stop <= 0:
            raise DomainBadRequest("Quantized stopPrice invalid for TAKE_PROFIT")

        payload: Dict[str, Any] = {
            "symbol": sym,
            "side": side_str,
            "type": "TAKE_PROFIT",
            "quantity": q_qty,
            "price": q_price,
            "stopPrice": q_stop,
            "timeInForce": tif,
            "reduceOnly": bool(reduce_only),
            "workingType": working_type,
            "positionSide": position_side,
        }
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = (
                f"{exc} | symbol={sym} qty={q_qty} price={q_price} stopPrice={q_stop} "
                f"tif={tif} reduce_only={reduce_only}"
            )
            raise type(exc)(message) from exc

    # --- Management -----------------------------------------------------------

    async def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return await self._client.cancel_order(
            symbol=symbol.upper(), orderId=int(order_id)
        )

    async def get_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return await self._client.query_order(
            symbol=symbol.upper(), orderId=int(order_id)
        )

    async def list_open_orders(self, symbol: str | None = None) -> List[Dict[str, Any]]:
        return await self._client.open_orders(symbol=symbol)

    async def has_open_position(
        self, symbol: str, *, position_side: str | None = None
    ) -> bool:
        """
        Return True if the account has any non-zero position for the symbol.

        The underlying connector returns snake_case keys (e.g., ``position_amt``)
        on the position endpoint; older clients may return camelCase.
        """
        sym = symbol.upper()
        side_filter = position_side.upper() if position_side else None
        positions = await self._client.position_information(sym)
        for position in positions:
            if str(position.get("symbol") or "").upper() != sym:
                continue
            if side_filter:
                pos_side = position.get("position_side") or position.get("positionSide")
                if pos_side is not None and str(pos_side).upper() != side_filter:
                    continue

            raw_amt = (
                position.get("position_amt")
                if "position_amt" in position
                else position.get("positionAmt")
            )
            try:
                amt = Decimal(str(raw_amt or "0"))
            except Exception:
                amt = Decimal("0")
            if amt != 0:
                return True

        return False

    async def get_order_status(self, symbol: str, order_id: int) -> str:
        result = await self._client.query_order(
            symbol=symbol.upper(), orderId=int(order_id)
        )
        return str(result.get("status", ""))

    async def close_position_market(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        *,
        position_side: str = "BOTH",
    ) -> Dict[str, Any]:
        side_str = _normalize_side(side)
        position_side = _validate_choice(
            position_side, _VALID_POSITION_SIDES, "position_side"
        )
        return await self._client.close_position_market(
            symbol.upper(),
            side_str,
            quantity,
            position_side=position_side,
        )

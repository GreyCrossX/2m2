#!/usr/bin/env python3
"""
Exchange Facade — Binance USDS-M Futures (MAINNET, ONE-WAY)
Clean Architecture (Ports/Adapters) in a single module for easy adoption.

Highlights
- ONE-WAY position mode only (never probes/uses positionSide).
- Risk-based sizing (risk_percent * equity) with stop-based distance.
- Margin constraint check (shrink qty to fit availableBalance & leverage).
- Exchange filter quantization (tick/step/minQty) with caching.
- Exact "wire" logging of kwargs sent to SDK (toggle via env).
- CamelCase→snake_case normalization based on SDK method signatures.
- Clear separation of: domain (entities/values), use-cases, infra adapter, and facade API.
- Time drift hardening: recvWindow injection + retry on Binance -1021 timestamp errors.

Drop-in public API (compatible with prior facade):
    new_order, cancel_order, cancel_all_orders, get_open_orders,
    get_positions, set_leverage, set_margin_type, get_account_info,
    set_creds_lookup, _selftest_probe

Environment variables:
    BINANCE_USDSF_API_KEY, BINANCE_USDSF_API_SECRET, BINANCE_USDSF_BASE_URL
    EXCHANGE_PRINT_WIRE=1           # print exact kwargs to SDK
    EXCHANGE_PRINT_WIRE_PRE=1       # also print pre-normalization dict
    BINANCE_RECV_WINDOW_MS=10000    # recvWindow value; defaults to 10000ms
    BINANCE_TIME_RETRY_ATTEMPTS=2   # retries on timestamp error
    BINANCE_TIME_RETRY_DELAY_MS=250 # delay between time retries (ms)

Notes
- This file is organized by layers, top-to-bottom: domain → use cases → infra → facade.
- If you later split into packages, each section can be its own module.
"""
from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Stdlib & typing
# ──────────────────────────────────────────────────────────────────────────────
import os
import json
import logging
import inspect
import re
import uuid
import hashlib
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from functools import lru_cache
from typing import Any, Callable, Dict, Optional, Tuple, Protocol, runtime_checkable

# ──────────────────────────────────────────────────────────────────────────────
# External SDK (Binance USDS-M Futures)
# ──────────────────────────────────────────────────────────────────────────────
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
    ConfigurationRestAPI,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    NewOrderSideEnum,
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging & Config
# ──────────────────────────────────────────────────────────────────────────────
LOG = logging.getLogger("exchange")

ENV_KEY = "BINANCE_USDSF_API_KEY"
ENV_SECRET = "BINANCE_USDSF_API_SECRET"
ENV_BASE_URL = "BINANCE_USDSF_BASE_URL"
ENV_PRINT_WIRE = "EXCHANGE_PRINT_WIRE"          # "1"/"true"/"yes"/"on"
ENV_PRINT_WIRE_PRE = "EXCHANGE_PRINT_WIRE_PRE"  # pre-normalization dict

_default_api_key = os.getenv(ENV_KEY, "") or ""
_default_api_secret = os.getenv(ENV_SECRET, "") or ""
_default_base_url = os.getenv(ENV_BASE_URL, "") or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL

_PRINT_WIRE = os.getenv(ENV_PRINT_WIRE, "").strip().lower() in ("1", "true", "yes", "on")
_PRINT_WIRE_PRE = os.getenv(ENV_PRINT_WIRE_PRE, "").strip().lower() in ("1", "true", "yes", "on")

# Timestamp/recvWindow hardening (Binance -1021 prevention)
RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "10000"))  # default 10s
TIME_RETRY_ATTEMPTS = int(os.getenv("BINANCE_TIME_RETRY_ATTEMPTS", "2"))
TIME_RETRY_DELAY_MS = int(os.getenv("BINANCE_TIME_RETRY_DELAY_MS", "250"))

# ──────────────────────────────────────────────────────────────────────────────
# Domain: Value Objects / Entities
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RiskSettings:
    risk_percent: Optional[Decimal] = None  # e.g., Decimal("0.05") for 5%
    fee_buffer: Decimal = Decimal("0.002")  # 0.2% extra safety for fees/slippage
    margin_headroom: Decimal = Decimal("0.98")  # keep 2% headroom vs availableBalance


@dataclass(frozen=True)
class OrderRequest:
    user_id: str
    symbol: str
    side: str  # BUY/SELL (string; translated to enum)
    order_type: str
    quantity: Optional[Decimal] = None
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    working_type: str = "MARK_PRICE"
    close_position: Optional[bool] = None
    time_in_force: Optional[str] = None
    reduce_only: Optional[bool] = None
    client_order_id: Optional[str] = None
    # Free-form extras (kept out of the entity's core fields)
    leverage: int = 5
    risk: RiskSettings = field(default_factory=RiskSettings)


@dataclass
class OrderResult:
    ok: bool
    raw: Any
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    error: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Ports (Interfaces)
# ──────────────────────────────────────────────────────────────────────────────
@runtime_checkable
class ExchangeGateway(Protocol):
    """Port for exchange interaction (adapter will implement with Binance SDK)."""

    # market/ref data
    def fetch_mark_price(self, symbol: str) -> Optional[Decimal]:
        ...

    def fetch_symbol_filters(self, symbol: str) -> Dict[str, Any]:
        ...

    def fetch_available_balance(self) -> Optional[Decimal]:
        ...

    # trading actions
    def send_new_order(self, params: Dict[str, Any]) -> Any:
        ...

    def cancel_order(self, params: Dict[str, Any]) -> Any:
        ...

    def cancel_all_orders(self, params: Dict[str, Any]) -> Any:
        ...

    def get_open_orders(self, params: Dict[str, Any]) -> Any:
        ...

    def get_positions(self, params: Dict[str, Any]) -> Any:
        ...

    def set_leverage(self, params: Dict[str, Any]) -> Any:
        ...

    def set_margin_type(self, params: Dict[str, Any]) -> Any:
        ...

    def get_account_info(self) -> Any:
        ...


# ──────────────────────────────────────────────────────────────────────────────
# Use Case Services
# ──────────────────────────────────────────────────────────────────────────────

# utility conversions

def _to_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _floor_to_step(x: Decimal, step: Any) -> Decimal:
    """Floor value to step size with proper precision."""
    ds = _to_decimal(step)
    if ds is None or ds <= 0:
        return x
    
    # Calculate the floored value
    floored = (x // ds) * ds
    
    # Quantize to match step precision to avoid floating point artifacts
    places = _dec_places_from_step(step)
    if places is not None:
        quantum = Decimal(10) ** -places
        floored = floored.quantize(quantum, rounding=ROUND_DOWN)
    
    return floored

def _validate_precision(value_str: str, field_name: str, max_decimals: int = 8) -> None:
    """Validate that a string number doesn't exceed precision limits."""
    if '.' in value_str:
        decimal_part = value_str.split('.')[1]
        if len(decimal_part) > max_decimals:
            LOG.warning(
                "Precision warning: %s='%s' has %d decimals (max %d)",
                field_name, value_str, len(decimal_part), max_decimals
            )


def _validate_order_params(params: Dict[str, Any]) -> None:
    """Validate order params before sending to exchange."""
    for field in ['quantity', 'price', 'stopPrice', 'activationPrice']:
        if field in params and params[field] is not None:
            value_str = str(params[field])
            _validate_precision(value_str, field)


def _dec_places_from_step(step: Any) -> Optional[int]:
    """Calculate decimal places from step size."""
    ds = _to_decimal(step)
    if ds is None or ds <= 0:
        return None
    # Using Decimal exponent to infer precision, e.g. 0.001 -> 3
    exp = -ds.as_tuple().exponent
    return max(exp, 0)


def _fmt_decimal(d: Decimal, places: Optional[int]) -> str:
    """Format decimal with exact precision, never exceeding places."""
    if places is None:
        # Without explicit places, still normalize but strip trailing zeros
        # This ensures we don't send excessive precision
        normalized = d.normalize()
        # Convert to string without scientific notation
        s = format(normalized, 'f')
        # Ensure we're not sending too many decimals (cap at 8 for safety)
        if '.' in s:
            integer_part, decimal_part = s.split('.')
            if len(decimal_part) > 8:
                s = f"{integer_part}.{decimal_part[:8]}"
        return s.rstrip('0').rstrip('.') if '.' in s else s
    
    if places == 0:
        return str(int(d))
    
    # Quantize to exact precision and format without trailing zeros
    quantum = Decimal(10) ** -places
    quantized = d.quantize(quantum, rounding=ROUND_DOWN)
    
    # Format with exact places, then strip trailing zeros
    formatted = f"{quantized:.{places}f}"
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.')
    
    return formatted


def _risk_position_qty(
    equity_usdt: Decimal,
    risk_percent: Decimal,
    entry_price: Decimal,
    stop_price: Decimal,
    fee_buffer: Decimal,
) -> Optional[Decimal]:
    """qty = (equity * risk_pct) / |entry - stop|  (linear USDT futures)."""
    if equity_usdt <= 0 or risk_percent <= 0:
        return None
    distance = abs(entry_price - stop_price)
    if distance <= 0:
        return None
    risk_usdt = (equity_usdt * risk_percent)
    qty = (risk_usdt / distance) * (Decimal(1) - fee_buffer)
    return qty


def _margin_fits(qty: Decimal, entry_price: Decimal, leverage: int, avail_balance: Decimal, headroom: Decimal) -> bool:
    notional = qty * entry_price
    required = (notional / Decimal(max(leverage, 1))) * headroom
    return required <= avail_balance


@dataclass
class PlaceOrderService:
    """Use case: compute final order params (qty, quantization, guards) and execute."""
    gw: ExchangeGateway

    def _symbol_filters(self, symbol: str) -> Dict[str, Any]:
        return self.gw.fetch_symbol_filters(symbol) or {}

    def _quantize(self, symbol: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Quantize order params to exchange filters. Returns strings."""
        f = self._symbol_filters(symbol)
        tick = f.get("tick_size")
        step = f.get("step_size")
        tick_places = _dec_places_from_step(tick)
        step_places = _dec_places_from_step(step)
        
        out = dict(params)
        
        # Quantities
        if "quantity" in out and out["quantity"] is not None and step is not None:
            q_old = _to_decimal(out["quantity"]) or Decimal("0")
            q_new = _floor_to_step(q_old, step)
            out["quantity"] = _fmt_decimal(q_new, step_places)
        
        # Prices
        for key in ("price", "stopPrice", "activationPrice"):
            if key in out and out[key] is not None and tick is not None:
                p_old = _to_decimal(out[key])
                if p_old is not None:
                    p_new = _floor_to_step(p_old, tick)
                    out[key] = _fmt_decimal(p_new, tick_places)
        
        # minQty bump
        min_qty = _to_decimal(f.get("min_qty"))
        if min_qty is not None and "quantity" in out and out["quantity"] is not None:
            qv = _to_decimal(out["quantity"]) or Decimal("0")
            if qv < min_qty:
                out["quantity"] = _fmt_decimal(min_qty, step_places)
        
        return out

    def _compute_qty_from_risk(self, req: OrderRequest, ref_entry_price: Decimal) -> Optional[Decimal]:
        if req.quantity is not None:
            return req.quantity
        rp = req.risk.risk_percent
        if rp is None or req.stop_price is None:
            return None
        fb = self.gw.fetch_available_balance()
        if fb is None:
            return None
        qty = _risk_position_qty(
            equity_usdt=fb,
            risk_percent=rp,
            entry_price=ref_entry_price,
            stop_price=req.stop_price,
            fee_buffer=req.risk.fee_buffer,
        )
        return qty

    def _enforce_margin(self, qty: Decimal, entry_price: Decimal, req: OrderRequest, symbol_filters: Dict[str, Any]) -> Decimal:
        fb = self.gw.fetch_available_balance()
        if fb is None:
            return qty
        step = symbol_filters.get("step_size")
        # shrink to fit margin
        while qty > 0 and not _margin_fits(qty, entry_price, req.leverage, fb, req.risk.margin_headroom):
            qty = qty * Decimal("0.9")  # reduce by 10% per iteration
            qty = _floor_to_step(qty, step)
            if qty <= 0:
                break
        return qty

    def execute(self, req: OrderRequest) -> OrderResult:
        # Build caller-style map
        raw: Dict[str, Any] = {
            "symbol": req.symbol,
            "side": _side_to_enum(req.side),
            "type": req.order_type,
            "quantity": req.quantity,
            "price": req.price,
            "reduceOnly": req.reduce_only,
            "stopPrice": req.stop_price,
            "workingType": req.working_type,
            "closePosition": req.close_position,
            "timeInForce": req.time_in_force,
            "newClientOrderId": req.client_order_id,
    }
    
        # STOP/TP-MKT rules
        otype = (req.order_type or "").upper()
        if otype in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            raw.pop("price", None)
            raw.pop("timeInForce", None)
            if raw.get("closePosition"):
                raw.pop("quantity", None)
                raw.pop("reduceOnly", None)

        # Determine reference entry price
        ref_entry: Optional[Decimal] = req.price if req.price is not None else None
        if ref_entry is None:
            ref_entry = req.stop_price if req.stop_price is not None else None
        if ref_entry is None:
            mk = self.gw.fetch_mark_price(req.symbol)
            if mk is not None:
                ref_entry = mk
        ref_entry = _to_decimal(ref_entry)

        # Get filters once
        filters = self._symbol_filters(req.symbol)
        step_places = _dec_places_from_step(filters.get("step_size"))

        # ── 1) Risk-based sizing if quantity missing ────────────────────────
        if raw.get("quantity") is None and ref_entry is not None:
            q_risk = self._compute_qty_from_risk(req, ref_entry)
            if q_risk is not None:
                # Pre-quantization margin fit
                q_risk = self._enforce_margin(q_risk, ref_entry, req, filters)
                # ✅ KEEP AS DECIMAL, don't convert to float yet
                raw["quantity"] = q_risk if q_risk > 0 else Decimal("0")

        # ── 2) Enforce margin guard even if caller supplied quantity ─────────
        if (
            raw.get("quantity") is not None
            and ref_entry is not None
            and not raw.get("reduceOnly")
            and not raw.get("closePosition")
        ):
            q0 = _to_decimal(raw["quantity"]) or Decimal("0")
            q_adj = self._enforce_margin(q0, ref_entry, req, filters)
            if q_adj <= 0:
                return OrderResult(
                    ok=False,
                    raw={"would_send": raw},
                    error=(
                        "Insufficient margin to open position at current leverage; "
                        "reduce risk/qty or increase leverage."
                    ),
                )
            if q_adj != q0:
                LOG.warning(
                    "Margin clamp (pre-quantize): qty %s -> %s @ px=%s lev=%s",
                    q0, q_adj, ref_entry, req.leverage
                )
            # ✅ KEEP AS DECIMAL
            raw["quantity"] = q_adj

        # ── 3) Quantize to exchange filters ──────────────────────────────────
        # This converts Decimals to properly formatted strings
        raw = self._quantize(req.symbol, raw)

        # ── 4) Post-quantization margin safety ────────────────────────────────
        if (
            raw.get("quantity") is not None
            and ref_entry is not None
            and not raw.get("reduceOnly")
            and not raw.get("closePosition")
        ):
            fb = self.gw.fetch_available_balance() or Decimal("0")
            qv = _to_decimal(raw["quantity"]) or Decimal("0")
            
            if not _margin_fits(qv, ref_entry, req.leverage, fb, req.risk.margin_headroom):
                # Try one more clamp
                q2 = self._enforce_margin(qv, ref_entry, req, filters)
                if q2 <= 0:
                    return OrderResult(
                        ok=False,
                        raw={"would_send": raw},
                        error=(
                            "Insufficient margin after quantization "
                            "(minQty too high for balance/leverage)."
                        ),
                    )
                if q2 != qv:
                    LOG.warning("Margin clamp (post-quantize): qty %s -> %s", qv, q2)
                
                # Re-quantize the adjusted value
                raw["quantity"] = q2
                raw = self._quantize(req.symbol, raw)

        # ── 5) Send downstream ───────────────────────────────────────────────
        try:
            data = self.gw.send_new_order(raw)
            order_id, coid = _extract_order_ids(data)
            return OrderResult(ok=True, raw=data, order_id=order_id, client_order_id=coid)
        except Exception as e:
            LOG.error(
                "new_order error (user=%s sym=%s side=%s type=%s): %s",
                req.user_id, req.symbol, req.side, req.order_type, e,
            )
            return OrderResult(ok=False, raw=None, error=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Infrastructure Adapter (Binance SDK) — implements ExchangeGateway
# ──────────────────────────────────────────────────────────────────────────────

# wire logging
_DEF_SAFE_COID_RE = re.compile(r"[^A-Za-z0-9_-]")


def _safe_client_order_id(coid: Optional[str]) -> str:
    if not coid:
        return f"{uuid.uuid4().hex[:8]}-{uuid.uuid4().hex[:12]}"
    coid = _DEF_SAFE_COID_RE.sub("-", coid)
    if len(coid) <= 36:
        return coid
    head = coid[:16]
    tail = coid[-6:]
    digest = hashlib.blake2b(coid.encode("utf-8"), digest_size=5).hexdigest()
    return f"{head}-{digest}-{tail}"


def _jsonable(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, bytes):
        try:
            return o.decode()
        except Exception:
            return repr(o)
    return o


def _print_wire(func_name: str, sdk_func_name: str, *, params: Dict[str, Any], pre: Dict[str, Any] | None = None) -> None:
    if not _PRINT_WIRE:
        return
    try:
        pretty = json.dumps({k: _jsonable(v) for k, v in params.items()}, ensure_ascii=False, sort_keys=True, indent=2)
    except Exception:
        pretty = str(params)
    banner = f"[WIRE] {func_name} -> client.rest_api.{sdk_func_name}(**params)"
    print(banner)
    print(pretty)
    print("-" * len(banner))
    LOG.info("%s\n%s", banner, pretty)
    if _PRINT_WIRE_PRE and pre is not None:
        try:
            pre_pretty = json.dumps({k: _jsonable(v) for k, v in pre.items()}, ensure_ascii=False, sort_keys=True, indent=2)
        except Exception:
            pre_pretty = str(pre)
        pre_banner = f"[WIRE:pre] {func_name} (caller dict prior to normalization/quantization)"
        print(pre_banner)
        print(pre_pretty)
        print("-" * len(pre_banner))
        LOG.info("%s\n%s", pre_banner, pre_pretty)


# camel→snake mapping for selected endpoints
_C2S_MAP: Dict[str, Optional[str]] = {
    "timeInForce": "time_in_force",
    "newClientOrderId": "new_client_order_id",
    "reduceOnly": "reduce_only",
    "closePosition": "close_position",
    "stopPrice": "stop_price",
    "workingType": "working_type",
    "priceProtect": "price_protect",
    "activationPrice": "activation_price",
    "callbackRate": "callback_rate",
    "recvWindow": "recv_window",
    "responseType": "response_type",
    "orderId": "order_id",
    "origClientOrderId": "orig_client_order_id",
    "marginType": "margin_type",
    # ONE-WAY: ignore positionSide entirely
    "positionSide": None,
}


def _normalize_kwargs(func, params: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(params, dict):
        return {}
    mapped: Dict[str, Any] = {}
    for k, v in params.items():
        alias = _C2S_MAP.get(k, k)
        if alias is None:
            continue
        mapped[alias] = v
    try:
        sig = inspect.signature(func)
        allowed = set(sig.parameters.keys())
        cleaned = {k: v for k, v in mapped.items() if k in allowed}
        dropped = sorted(set(mapped.keys()) - allowed)
        if dropped:
            LOG.debug("%s(): dropped unsupported kwargs: %s", getattr(func, "__name__", func), dropped)
        return cleaned
    except Exception:
        return mapped


def _maybe_add_recv_window_for_fn(fn, params: Dict[str, Any]) -> Dict[str, Any]:
    """Add recv_window if the SDK function supports it."""
    if RECV_WINDOW_MS <= 0:
        return params
    try:
        sig = inspect.signature(fn)
        if "recv_window" in sig.parameters and "recv_window" not in params:
            p = dict(params)
            p["recv_window"] = RECV_WINDOW_MS
            return p
    except Exception:
        pass
    return params


def _is_time_error(exc: Exception) -> bool:
    m = str(exc)
    return (
        "-1021" in m
        or "ahead of the server's time" in m
        or "outside of the recvWindow" in m
        or "Timestamp for this request was" in m
    )


def _call_with_time_retry(client: DerivativesTradingUsdsFutures, fn, sdk_params: Dict[str, Any], sdk_func_name: str):
    """Call SDK and retry on timestamp/recvWindow errors."""
    import time
    attempt = 0
    while True:
        try:
            return fn(**sdk_params).data()
        except Exception as e:
            if attempt >= TIME_RETRY_ATTEMPTS or not _is_time_error(e):
                raise
            # Touch server time (best-effort) and wait a bit so the SDK re-stamps
            try:
                tfn = getattr(client.rest_api, "time", None)
                if callable(tfn):
                    _ = tfn().data()
            except Exception:
                pass
            attempt += 1
            LOG.warning("Time drift detected; retrying %s (attempt %d): %s", sdk_func_name, attempt, e)
            time.sleep(TIME_RETRY_DELAY_MS / 1000.0)


# Credentials management & client factory
_creds_lookup: Optional[Callable[[str], Optional[Tuple[str, str, Optional[str]]]]] = None


def set_creds_lookup(fn: Callable[[str], Optional[Tuple[str, str, Optional[str]]]]) -> None:
    """Register a creds lookup: (user_id) -> (api_key, api_secret, base_url|None)."""
    global _creds_lookup
    _creds_lookup = fn
    clear = getattr(_client_for_user, "cache_clear", None)
    if callable(clear):
        clear()


def _resolve_creds(user_id: str) -> Tuple[str, str, str]:
    if _creds_lookup:
        try:
            res = _creds_lookup(user_id)
            if res and len(res) >= 2:
                k, s, b = (res + (None,))[:3]
                base = (b or "").strip() or _default_base_url
                return (k or "", s or "", base)
        except Exception as e:
            LOG.error("creds_lookup failed for user_id=%s: %s", user_id, e)
    return (_default_api_key, _default_api_secret, _default_base_url)


@lru_cache(maxsize=256)
def _client_for_user(user_id: str, api_key: str, api_secret: str, base_url: str) -> DerivativesTradingUsdsFutures:
    cfg = ConfigurationRestAPI(api_key=api_key, api_secret=api_secret, base_path=base_url)
    return DerivativesTradingUsdsFutures(config_rest_api=cfg)


def _get_client(user_id: str) -> DerivativesTradingUsdsFutures:
    k, s, b = _resolve_creds(user_id)
    if not k or not s:
        LOG.error(
            "Missing Binance credentials (user_id=%s). Check %s / %s or creds lookup.",
            user_id, ENV_KEY, ENV_SECRET
        )
    return _client_for_user(user_id, k, s, b)


class BinanceGateway(ExchangeGateway):
    """Adapter implementing ExchangeGateway using the Binance USDS-M SDK."""

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.client = _get_client(user_id)

    # ── Market/ref data ──────────────────────────────────────────────────────
    def fetch_mark_price(self, symbol: str) -> Optional[Decimal]:
        fn = getattr(self.client.rest_api, "mark_price", None)
        if not callable(fn):
            return None
        try:
            data = fn(symbol=symbol).data()
            if isinstance(data, dict):
                mp = data.get("markPrice") or data.get("mark_price")
                return _to_decimal(mp)
            if isinstance(data, list) and data:
                return _to_decimal(data[0].get("markPrice"))
        except Exception:
            return None
        return None

    @lru_cache(maxsize=512)
    def fetch_symbol_filters(self, symbol: str) -> Dict[str, Any]:
        try:
            ei = self.client.rest_api.exchange_information().data()
            syms = ei.get("symbols") if isinstance(ei, dict) else None
            if not syms:
                return {}
            sym_u = (symbol or "").upper()
            srec = next((s for s in syms if s.get("symbol") == sym_u), None)
            if not srec:
                return {}
            filters = srec.get("filters", []) or []

            def find(ftype: str) -> Optional[Dict[str, Any]]:
                return next((f for f in filters if f.get("filterType") == ftype), None)

            price_f = find("PRICE_FILTER") or {}
            lot_f = find("LOT_SIZE") or find("MARKET_LOT_SIZE") or {}
            return {
                "tick_size": price_f.get("tickSize"),
                "step_size": lot_f.get("stepSize"),
                "min_qty": lot_f.get("minQty"),
                "min_notional": (find("MIN_NOTIONAL") or {}).get("notional"),
            }
        except Exception as e:
            LOG.warning("exchange_information parse failed for %s: %s", symbol, e)
            return {}

    def fetch_available_balance(self) -> Optional[Decimal]:
        try:
            acct = self.client.rest_api.account_information().data()
            if isinstance(acct, dict):
                ab = acct.get("availableBalance") or acct.get("available_balance")
                return _to_decimal(ab)
        except Exception:
            return None
        return None

    # ── Trading endpoints ────────────────────────────────────────────────────
    def send_new_order(self, params: Dict[str, Any]) -> Any:
        """Fixed version - no duplicates, proper validation."""
        fn = self.client.rest_api.new_order
        pre = dict(params)
        
        # Sanitize client order ID in original params
        if params.get("newClientOrderId"):
            params["newClientOrderId"] = _safe_client_order_id(params["newClientOrderId"])
        
        # Normalize to SDK format
        sdk_params = _normalize_kwargs(fn, params)
        
        # Sanitize in normalized params
        if "new_client_order_id" in sdk_params:
            sdk_params["new_client_order_id"] = _safe_client_order_id(sdk_params["new_client_order_id"])
        
        # Add recv window
        sdk_params = _maybe_add_recv_window_for_fn(fn, sdk_params)
        
        # Validate precision BEFORE sending
        _validate_order_params(sdk_params)
        
        # Wire logging
        _print_wire("new_order", "new_order", params=sdk_params, pre=pre if _PRINT_WIRE_PRE else None)
        
        # Send with retry
        return _call_with_time_retry(self.client, fn, sdk_params, "new_order")

    def cancel_order(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.cancel_order
        sdk_params = _normalize_kwargs(fn, params)
        sdk_params = _maybe_add_recv_window_for_fn(fn, sdk_params)
        _print_wire("cancel_order", "cancel_order", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
        return _call_with_time_retry(self.client, fn, sdk_params, "cancel_order")

    def cancel_all_orders(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.cancel_all_open_orders
        sdk_params = _maybe_add_recv_window_for_fn(fn, dict(params))
        _print_wire("cancel_all_orders", "cancel_all_open_orders", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
        return _call_with_time_retry(self.client, fn, sdk_params, "cancel_all_open_orders")

    def get_open_orders(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.current_all_open_orders
        if params:
            sdk_params = _maybe_add_recv_window_for_fn(fn, dict(params))
            _print_wire("get_open_orders", "current_all_open_orders", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
            return _call_with_time_retry(self.client, fn, sdk_params, "current_all_open_orders")
        _print_wire("get_open_orders", "current_all_open_orders", params={})
        return _call_with_time_retry(self.client, fn, {}, "current_all_open_orders")

    def get_positions(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.position_information
        if params:
            sdk_params = _maybe_add_recv_window_for_fn(fn, dict(params))
            _print_wire("get_positions", "position_information", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
            return _call_with_time_retry(self.client, fn, sdk_params, "position_information")
        _print_wire("get_positions", "position_information", params={})
        return _call_with_time_retry(self.client, fn, {}, "position_information")

    def set_leverage(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.change_initial_leverage
        sdk_params = _maybe_add_recv_window_for_fn(fn, dict(params))
        _print_wire("set_leverage", "change_initial_leverage", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
        return _call_with_time_retry(self.client, fn, sdk_params, "change_initial_leverage")

    def set_margin_type(self, params: Dict[str, Any]) -> Any:
        fn = self.client.rest_api.change_margin_type
        sdk_params = _normalize_kwargs(fn, params)
        sdk_params = _maybe_add_recv_window_for_fn(fn, sdk_params)
        _print_wire("set_margin_type", "change_margin_type", params=sdk_params, pre=params if _PRINT_WIRE_PRE else None)
        return _call_with_time_retry(self.client, fn, sdk_params, "change_margin_type")

    def get_account_info(self) -> Any:
        fn = self.client.rest_api.account_information
        sdk_params = _maybe_add_recv_window_for_fn(fn, {})
        _print_wire("get_account_info", "account_information", params=sdk_params)
        return _call_with_time_retry(self.client, fn, sdk_params, "account_information")


# ──────────────────────────────────────────────────────────────────────────────
# Facade (imperative API) — Backwards-compatible entrypoints
# ──────────────────────────────────────────────────────────────────────────────

# Helpers used by facade

def _wrap_ok(data: Any, **extra) -> Dict[str, Any]:
    return {"ok": True, **extra, "raw": data}


def _wrap_err(err: Exception | str) -> Dict[str, Any]:
    return {"ok": False, "error": str(err)}


def _extract_order_ids(resp_data: Any) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(resp_data, dict):
        oid = resp_data.get("orderId") or resp_data.get("orderID") or resp_data.get("order_id")
        coid = (
            resp_data.get("clientOrderId")
            or resp_data.get("newClientOrderId")
            or resp_data.get("origClientOrderId")
            or resp_data.get("new_client_order_id")
        )
        return (str(oid) if oid is not None else None, str(coid) if coid is not None else None)
    return None, None


def _side_to_enum(side: str) -> str:
    s = (side or "").upper()
    try:
        return NewOrderSideEnum[s].value
    except Exception:
        return s


# Public API

def new_order(
    user_id: str,
    *,
    symbol: str,
    side: str,
    order_type: str,
    quantity: float | None = None,
    price: float | None = None,
    reduce_only: bool | None = None,
    stop_price: float | None = None,
    working_type: str | None = "MARK_PRICE",
    close_position: bool | None = None,
    time_in_force: str | None = None,
    client_order_id: str | None = None,
    **extra_kwargs: Any,
) -> Dict[str, Any]:
    """Place a new order (ONE-WAY). Supports risk-based sizing via extra risk_percent.

    extra kwargs (optional):
        risk_percent: float/Decimal (e.g., 0.05 for 5%)
        leverage: int (default 5)
    """
    try:
        gw = BinanceGateway(user_id)
        risk = RiskSettings(
            risk_percent=_to_decimal(extra_kwargs.get("risk_percent")) if extra_kwargs.get("risk_percent") is not None else None
        )
        req = OrderRequest(
            user_id=user_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=_to_decimal(quantity) if quantity is not None else None,
            price=_to_decimal(price) if price is not None else None,
            stop_price=_to_decimal(stop_price) if stop_price is not None else None,
            working_type=(working_type or "MARK_PRICE"),
            close_position=close_position,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            client_order_id=_safe_client_order_id(client_order_id) if client_order_id else None,
            leverage=int(extra_kwargs.get("leverage") or 5),
            risk=risk,
        )
        svc = PlaceOrderService(gw)
        res = svc.execute(req)
        if res.ok:
            return _wrap_ok(res.raw, order_id=res.order_id, client_order_id=res.client_order_id)
        return _wrap_err(res.error or "unknown_error")
    except Exception as e:
        LOG.error("new_order fatal (user=%s sym=%s side=%s type=%s): %s", user_id, symbol, side, order_type, e)
        return _wrap_err(e)


def cancel_order(
    user_id: str,
    *,
    symbol: str,
    order_id: str | int | None = None,
    orig_client_order_id: str | None = None,
) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        data = gw.cancel_order(params)
        return _wrap_ok(data)
    except Exception as e:
        LOG.error("cancel_order error (user=%s sym=%s order_id=%s orig=%s): %s",
                  user_id, symbol, order_id, orig_client_order_id, e)
        return _wrap_err(e)


def cancel_all_orders(user_id: str, *, symbol: str) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params = {"symbol": symbol}
        data = gw.cancel_all_orders(params)
        return _wrap_ok(data)
    except Exception as e:
        LOG.error("cancel_all_orders error (user=%s sym=%s): %s", user_id, symbol, e)
        return _wrap_err(e)


def get_open_orders(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params = {"symbol": symbol} if symbol else {}
        data = gw.get_open_orders(params)
        orders = data if isinstance(data, list) else (data.get("orders", []) if isinstance(data, dict) else [])
        return _wrap_ok(data, orders=orders)
    except Exception as e:
        LOG.error("get_open_orders error (user=%s sym=%s): %s", user_id, symbol, e)
        return _wrap_err(e)


def get_positions(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params = {"symbol": symbol} if symbol else {}
        data = gw.get_positions(params)
        positions = data if isinstance(data, list) else (data.get("positions", []) if isinstance(data, dict) else [])
        return _wrap_ok(data, positions=positions)
    except Exception as e:
        LOG.error("get_positions error (user=%s sym=%s): %s", user_id, symbol, e)
        return _wrap_err(e)


def set_leverage(user_id: str, *, symbol: str, leverage: int) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params = {"symbol": symbol, "leverage": int(leverage)}
        data = gw.set_leverage(params)
        return _wrap_ok(data)
    except Exception as e:
        LOG.error("set_leverage error (user=%s sym=%s lev=%s): %s", user_id, symbol, leverage, e)
        return _wrap_err(e)


def set_margin_type(user_id: str, *, symbol: str, margin_type: str) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        params = {"symbol": symbol, "marginType": margin_type.upper()}
        data = gw.set_margin_type(params)
        return _wrap_ok(data)
    except Exception as e:
        LOG.error("set_margin_type error (user=%s sym=%s type=%s): %s", user_id, symbol, margin_type, e)
        return _wrap_err(e)


def get_account_info(user_id: str) -> Dict[str, Any]:
    try:
        gw = BinanceGateway(user_id)
        data = gw.get_account_info()
        return _wrap_ok(data)
    except Exception as e:
        LOG.error("get_account_info error (user=%s): %s", user_id, e)
        return _wrap_err(e)


# ──────────────────────────────────────────────────────────────────────────────
# Self-test
# ──────────────────────────────────────────────────────────────────────────────

def _selftest_probe(user_id: str, symbol: str = "BTCUSDT") -> Dict[str, Any]:
    """Quick probe to catch SDK mismatches (ONE-WAY-safe)."""
    out: Dict[str, Any] = {"ok": True, "checks": {}}
    try:
        gw = BinanceGateway(user_id)
        try:
            ei = _get_client(user_id).rest_api.exchange_information().data()
            out["checks"]["exchange_information"] = bool(ei)
        except Exception as e:
            out["ok"] = False
            out["checks"]["exchange_information"] = f"error: {e}"
        # mark/filters
        try:
            mp = gw.fetch_mark_price(symbol)
            out["checks"]["mark_price"] = mp is not None
        except Exception as e:
            out["ok"] = False
            out["checks"]["mark_price"] = f"error: {e}"
        try:
            f = gw.fetch_symbol_filters(symbol)
            out["checks"]["filters"] = bool(f)
        except Exception as e:
            out["ok"] = False
            out["checks"]["filters"] = f"error: {e}"
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    return out

#!/usr/bin/env python3
# app/scripts/debug_sizing.py
from __future__ import annotations

import argparse
import asyncio
import hmac
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# ORM models (DB)
from app.db.models.bots import Bot as ORMBot
from app.db.models.credentials import ApiCredential as ORMCred

# Domain
from app.services.worker.domain.models import BotConfig, ArmSignal
from app.services.worker.domain.enums import OrderSide, SideWhitelist, OrderStatus
from app.services.worker.application.order_executor import OrderExecutor

# ------------- Decimal precision -------------
getcontext().prec = 28

# ------------- CLI -------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Debug sizing and balance using bot credentials (USDT-M Futures)."
    )
    p.add_argument("--bot", required=True, help="Bot UUID")
    p.add_argument("--trigger", required=True, type=Decimal, help="Entry/limit price")
    p.add_argument("--stop", required=True, type=Decimal, help="Protective stop price")
    p.add_argument("--side", required=True, choices=["long", "short"], help="Order side")
    p.add_argument("--timeframe", default="2m", help="Timeframe (for signal)")
    p.add_argument("--quantize", action="store_true", help="Quantize qty/price using exchange filters")
    p.add_argument("--place", action="store_true", help="Place the limit order (otherwise dry-run)")
    p.add_argument("--client-id", default=None, help="Optional newClientOrderId")
    return p.parse_args()

# ------------- DB session factory -------------

def _get_database_url() -> str:
    # Try typical env vars used by the app
    for name in ("DATABASE_URL", "POSTGRES_DSN", "DB_DSN"):
        v = os.getenv(name)
        if v:
            return v
    raise SystemExit("Missing DATABASE_URL/POSTGRES_DSN env")

def _make_session_factory() -> async_sessionmaker[AsyncSession]:
    url = _get_database_url()
    if url.startswith("postgresql://"):
        # Async driver
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(url, future=True, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ------------- Enum helpers -------------

def _map_side_whitelist(value: str | SideWhitelist) -> SideWhitelist:
    if isinstance(value, SideWhitelist):
        return value
    s = str(value).lower()
    if s == "long":
        return SideWhitelist.LONG
    if s == "short":
        return SideWhitelist.SHORT
    return SideWhitelist.BOTH

def _parse_order_side(s: str) -> OrderSide:
    s = s.lower()
    return OrderSide.LONG if s == "long" else OrderSide.SHORT

# ------------- Map ORM -> Domain -------------

def _to_bot_config(row: ORMBot) -> BotConfig:
    return BotConfig(
        id=row.id,
        user_id=row.user_id,
        cred_id=row.cred_id,
        symbol=row.symbol.upper(),
        timeframe=row.timeframe,
        enabled=bool(row.enabled),
        env=str(row.env),  # "testnet" | "prod"
        side_whitelist=_map_side_whitelist(row.side_whitelist),
        leverage=int(row.leverage),
        use_balance_pct=bool(row.use_balance_pct),
        balance_pct=Decimal(row.balance_pct or 0),
        fixed_notional=Decimal(row.fixed_notional or 0),
        max_position_usdt=Decimal(row.max_position_usdt or 0),
    )

async def fetch_bot_and_creds(
    session: AsyncSession, bot_id: str
) -> Tuple[BotConfig, str, str]:
    stmt = (
        select(ORMBot, ORMCred)
        .join(ORMCred, ORMCred.id == ORMBot.cred_id)
        .where(ORMBot.id == bot_id)
    )
    res = await session.execute(stmt)
    r = res.first()
    if not r:
        raise SystemExit(f"Bot {bot_id} not found")
    bot_row, cred_row = r
    api_key, api_secret = cred_row.get_decrypted()
    return _to_bot_config(bot_row), api_key, api_secret

# ------------- Minimal Binance HTTP client (signed) -------------

def _ts_ms() -> int:
    return int(time.time() * 1000)

@dataclass
class HttpBinance:
    api_key: str
    api_secret: str
    env: str  # "testnet" | "prod"

    def _um_base(self) -> str:
        return "https://testnet.binancefuture.com" if self.env == "testnet" else "https://fapi.binance.com"

    def _sign(self, params: Dict[str, Any]) -> str:
        q = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret.encode(), q.encode(), hashlib.sha256).hexdigest()
        return f"{q}&signature={sig}"

    def _request(
        self, method: str, base: str, path: str, *, signed=False, params=None, timeout=20
    ) -> Any:
        params = params or {}
        headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
        url = f"{base}{path}"

        if signed:
            params.setdefault("recvWindow", int(os.getenv("BINANCE_RECV_WINDOW", "5000")))
            params.setdefault("timestamp", _ts_ms())
            body = self._sign(params)
        else:
            body = urllib.parse.urlencode(params, doseq=True)

        data = None
        if method.upper() == "GET":
            if body:
                url = f"{url}?{body}"
        else:
            data = body.encode()

        req = urllib.request.Request(url, data=data, method=method.upper(), headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(txt)
            except Exception:
                return txt

    # ---- Needed endpoints ----
    def um_account(self) -> Dict[str, Any]:
        return self._request("GET", self._um_base(), "/fapi/v2/account", signed=True)

    def um_exchange_info(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", self._um_base(), "/fapi/v1/exchangeInfo", signed=False, params={"symbol": symbol.upper()})

    def um_new_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", self._um_base(), "/fapi/v1/order", signed=True, params=payload)

    def um_change_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return self._request("POST", self._um_base(), "/fapi/v1/leverage", signed=True, params={
            "symbol": symbol.upper(),
            "leverage": int(leverage),
        })

# ------------- BalanceValidator Port impl -------------

class ScriptBalanceCache:
    def __init__(self):
        self._store: Dict[Tuple[str, str], Decimal] = {}

    async def get(self, cred_id, env) -> Decimal | None:
        return self._store.get((str(cred_id), env))

    async def set(self, cred_id, env, value: Decimal) -> None:
        self._store[(str(cred_id), env)] = value

class ScriptBinanceAccount:
    def __init__(self, http: HttpBinance):
        self._http = http

    async def fetch_um_available_balance(self, cred_id, env) -> Decimal:
        data = self._http.um_account()
        # availableBalance is a string number
        ab = data.get("availableBalance", "0")
        try:
            return Decimal(str(ab))
        except Exception:
            return Decimal("0")

# ------------- TradingPort impl (quantize + optional order place) -------------

def _decimal_quantize_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    # floor to step
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return (q * step).quantize(step)

class ScriptTradingPort:
    """
    Implements the minimal TradingPort required by OrderExecutor:
      - set_leverage
      - quantize_limit_order
      - create_limit_order (dry-run by default)
    Quantization is done via /fapi/v1/exchangeInfo.
    """
    def __init__(self, http: HttpBinance, *, place: bool, client_id: Optional[str]):
        self._http = http
        self._place = place
        self._client_id = client_id

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        # Best-effort; ignore failures in debug tool
        try:
            self._http.um_change_leverage(symbol, leverage)
        except Exception:
            pass

    async def quantize_limit_order(
        self, symbol: str, quantity: Decimal, price: Decimal
    ) -> tuple[Decimal, Decimal | None]:
        info = self._http.um_exchange_info(symbol)
        syms = info.get("symbols") or []
        if not syms:
            return Decimal("0"), None
        fdict = {}
        for f in (syms[0].get("filters") or []):
            fdict[f.get("filterType")] = f

        step_size = Decimal(str((fdict.get("LOT_SIZE") or {}).get("stepSize", "0")))
        min_qty = Decimal(str((fdict.get("LOT_SIZE") or {}).get("minQty", "0")))
        tick_size = Decimal(str((fdict.get("PRICE_FILTER") or {}).get("tickSize", "0")))
        min_price = Decimal(str((fdict.get("PRICE_FILTER") or {}).get("minPrice", "0")))

        q_qty = quantity
        q_price = price

        if step_size and step_size > 0:
            q_qty = _decimal_quantize_step(q_qty, step_size)
        if tick_size and tick_size > 0:
            q_price = _decimal_quantize_step(q_price, tick_size)

        if min_qty and q_qty < min_qty:
            q_qty = Decimal("0")
        if min_price and q_price < min_price:
            q_price = Decimal("0")

        return q_qty, q_price

    async def create_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        side_str = "BUY" if side == OrderSide.LONG else "SELL"
        payload = {
            "symbol": symbol.upper(),
            "side": side_str,
            "type": "LIMIT",
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force,
            "reduceOnly": "true" if reduce_only else "false",
        }
        cid = new_client_order_id or self._client_id
        if cid:
            payload["newClientOrderId"] = cid

        if not self._place:
            # Dry run, don't hit new_order
            return {"orderId": 0, "dryRun": True, "payload": payload}

        return self._http.um_new_order(payload)

# ------------- lightweight BalanceValidator wrapper (matches your class) -------------

@dataclass
class ScriptBalanceValidator:
    binance_account: ScriptBinanceAccount
    balance_cache: ScriptBalanceCache

    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
        *,
        available_balance: Decimal | None = None,
    ) -> Tuple[bool, Decimal]:
        available = (
            available_balance
            if available_balance is not None
            else await self.get_available_balance(bot.cred_id, bot.env)
        )
        return (available >= required_margin), available

    async def get_available_balance(self, cred_id, env) -> Decimal:
        cached = await self.balance_cache.get(cred_id, env)
        if cached is not None:
            return cached
        val = await self.binance_account.fetch_um_available_balance(cred_id, env)
        if val < 0:
            val = Decimal("0")
        await self.balance_cache.set(cred_id, env, val)
        return val

# ------------- Main -------------

async def main() -> None:
    args = _parse_args()
    bot_id = args.bot
    trigger = args.trigger
    stop = args.stop
    side = _parse_order_side(args.side)
    quantize = bool(args.quantize)
    place = bool(args.place)
    client_id = args.client_id

    session_factory = _make_session_factory()
    async with session_factory() as session:
        bot, api_key, api_secret = await fetch_bot_and_creds(session, bot_id)

    http = HttpBinance(api_key=api_key, api_secret=api_secret, env=bot.env)
    bal_cache = ScriptBalanceCache()
    bal_port = ScriptBinanceAccount(http)
    validator = ScriptBalanceValidator(bal_port, bal_cache)

    trading = ScriptTradingPort(http, place=place, client_id=client_id)

    # OrderExecutor expects a trading_factory or prewired client
    executor = OrderExecutor(
        balance_validator=validator,
        binance_client=trading,
        trading_factory=None,
    )

    # Build a synthetic ArmSignal (timestamps are not important for this debug)
    now_ms = int(time.time() * 1000)
    signal = ArmSignal(
        version="1",
        side=side,
        symbol=bot.symbol,
        timeframe=args.timeframe,
        ts_ms=now_ms,
        ts=None,             # optional fields in your model may accept None
        ind_ts_ms=now_ms,
        ind_ts=None,
        ind_high=trigger,    # just fill with something reasonable
        ind_low=stop,
        trigger=trigger,
        stop=stop,
    )

    # Debug print header
    print("=== Debug Sizing ===")
    print(f"Bot                 : {bot.id}  ({bot.symbol} {bot.timeframe})  env={bot.env}")
    print(f"API Key (last 8)    : ...{api_key[-8:] if len(api_key) >= 8 else api_key}")
    print(f"Whitelist           : {bot.side_whitelist}")
    print(f"Leverage            : {bot.leverage}x")
    print(f"Config sizing       : use_balance_pct={bot.use_balance_pct}  balance_pct={bot.balance_pct}  fixed_notional={bot.fixed_notional}  max_position_usdt={bot.max_position_usdt}")
    print(f"Signal              : side={side} trigger={trigger} stop={stop}")

    # Raw available balance (directly from UM Futures)
    available = await validator.get_available_balance(bot.cred_id, bot.env)
    print(f"UM availableBalance : {available}")

    # Replicate OrderExecutor sizing path (before calling execute_order)
    # Use the same private logic to preview qty. We'll call execute_order anyway to ensure parity.
    def _calc_qty(b: BotConfig, available_balance: Decimal, entry_price: Decimal) -> Decimal:
        notional = Decimal("0")
        if b.fixed_notional and b.fixed_notional > 0:
            notional = b.fixed_notional
        elif b.use_balance_pct:
            pct = b.balance_pct if b.balance_pct and b.balance_pct > 0 else Decimal("0")
            notional = available_balance * pct
        else:
            return Decimal("0")
        if b.max_position_usdt and b.max_position_usdt > 0 and notional > b.max_position_usdt:
            notional = b.max_position_usdt
        if entry_price <= 0:
            return Decimal("0")
        qty = notional / entry_price
        return qty if qty > 0 else Decimal("0")

    pre_qty = _calc_qty(bot, available, trigger)
    exposure = pre_qty * trigger
    required_margin = exposure / Decimal(bot.leverage if bot.leverage > 0 else 1)
    ok, _ = await validator.validate_balance(bot, required_margin, available_balance=available)

    print(f"Pre-quant qty       : {pre_qty}")
    print(f"Exposure (qty*px)   : {exposure}")
    print(f"Required margin     : {required_margin}")
    print(f"Balance check       : {'OK' if ok else 'SKIP (low balance)'}")

    if pre_qty <= 0 or not ok:
        print("\nSizing yields 0 or balance insufficient. Skipping order placement path.")
        return

    # Optionally quantize before running the executor
    if quantize:
        q_qty, q_price = await trading.quantize_limit_order(bot.symbol, pre_qty, trigger)
        print(f"Quantized qty/price : {q_qty} @ {q_price}")
    else:
        q_qty, q_price = pre_qty, trigger

    # Run through OrderExecutor to see final state & adapter payload
    state = await executor.execute_order(bot, signal)
    status = state.status
    print("\n=== OrderExecutor result ===")
    print(f"status       : {status}")
    print(f"order_id     : {state.order_id}")
    print(f"qty          : {state.quantity}")
    print(f"price        : {state.trigger_price}")
    print(f"stop         : {state.stop_price}")

    if status == OrderStatus.FAILED:
        print("\nResult: FAILED (likely quantization invalid or zero qty after constraints).")
    elif status == OrderStatus.SKIPPED_LOW_BALANCE:
        print("\nResult: SKIPPED_LOW_BALANCE")
    else:
        if state.order_id == 0:
            print("\nNote: dry-run only (no real order created). Use --place to actually send the order.")
        elif state.order_id is None:
            print("\nNote: create_limit_order returned no orderId. (Adapter dry-run?)")
        else:
            print("\nOrder created successfully (real).")


if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Iterable, Tuple
from uuid import UUID

from redis.asyncio import Redis

# --- Align with worker/main.py imports ---
from app.services.worker.config import Config
from app.services.worker.presentation.logging import setup_logging

from app.services.worker.infrastructure.postgres.session import create_session_factory
from app.services.worker.infrastructure.postgres.repositories import (
    BotRepository,
    CredentialRepository,
)
from app.services.worker.infrastructure.postgres.order_states import OrderGateway
from app.services.worker.infrastructure.cache.balance_cache import BalanceCache

from app.services.worker.application.balance_validator import BalanceValidator
from app.services.worker.application.order_executor import OrderExecutor
from app.services.worker.application.position_manager import PositionManager
from app.services.worker.application.signal_processor import SignalProcessor

from app.services.worker.domain.enums import OrderSide
from app.services.worker.domain.models import ArmSignal, BotConfig

from app.services.infrastructure.binance import BinanceAccount, BinanceClient

log = logging.getLogger("scripts.dry_run_signal")


# ------------------------------------------------------------------------------
# Router stub: route only to the chosen bot (keeps SignalProcessor flow intact)
# ------------------------------------------------------------------------------
class _SingleBotRouter:
    def __init__(self, bot_id: UUID, symbol: str, timeframe: str) -> None:
        self._bot_id = bot_id
        self._symbol = symbol
        self._tf = timeframe

    def get_bot_ids(self, symbol: str, timeframe: str) -> Iterable[UUID]:
        if symbol == self._symbol and timeframe == self._tf:
            return [self._bot_id]
        return []


# ------------------------------------------------------------------------------
# Dry-run trading adapter:
# - No-ops for set_* calls your executor uses
# - quantize_limit_order(): applies tick/step rounding (defaults for ETHUSDT)
# - create_limit_order/create_market_order/etc.: PRINTS the payload
# ------------------------------------------------------------------------------
class DryRunTrading:
    # Safe defaults; add overrides in _SYMBOL_FILTERS as needed
    DEFAULT_PRICE_TICK = Decimal("0.01")
    DEFAULT_QTY_STEP = Decimal("0.001")
    DEFAULT_MIN_QTY = Decimal("0.001")

    _SYMBOL_FILTERS: Dict[str, Dict[str, Decimal]] = {
        "ETHUSDT": {
            "price_tick": DEFAULT_PRICE_TICK,
            "qty_step": DEFAULT_QTY_STEP,
            "min_qty": DEFAULT_MIN_QTY,
        }
        # Add BTCUSDT etc. here if needed
    }

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        multiples = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return (multiples * step).quantize(step, rounding=ROUND_DOWN)

    # --- Config / mode no-ops ---
    async def ensure_position_mode(self, *_, **__):
        log.debug("[dry-run] ensure_position_mode noop")

    async def set_position_mode(self, *_, **__):
        log.debug("[dry-run] set_position_mode noop")
        return {"ok": True, "dry_run": True, "op": "set_position_mode"}

    async def set_leverage(self, symbol: str, leverage: int | str, *_, **__):
        log.debug("[dry-run] set_leverage noop | %s -> %s", symbol, leverage)
        return {"ok": True, "dry_run": True, "op": "set_leverage", "symbol": symbol, "leverage": leverage}

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED", *_, **__):
        log.debug("[dry-run] set_margin_type noop | %s -> %s", symbol, margin_type)
        return {"ok": True, "dry_run": True, "op": "set_margin_type", "symbol": symbol, "margin_type": margin_type}

    # --- Helpers used by OrderExecutor ---
    async def quantize_limit_order(self, symbol: str, qty: Decimal, price: Decimal) -> Tuple[Decimal, Decimal]:
        f = self._SYMBOL_FILTERS.get(symbol.upper(), {})
        price_tick = f.get("price_tick", self.DEFAULT_PRICE_TICK)
        qty_step = f.get("qty_step", self.DEFAULT_QTY_STEP)
        min_qty = f.get("min_qty", self.DEFAULT_MIN_QTY)

        q_price = price.quantize(price_tick, rounding=ROUND_DOWN)
        q_qty = self._floor_to_step(qty, qty_step)
        if q_qty < min_qty:
            log.debug("[dry-run] qty < min_qty; bumping | %s -> %s (symbol=%s)", q_qty, min_qty, symbol)
            q_qty = min_qty

        log.debug("[dry-run] quantize | %s qty=%s->%s price=%s->%s step=%s tick=%s",
                  symbol, qty, q_qty, price, q_price, qty_step, price_tick)
        return q_qty, q_price

    # --- Order creation methods (print payloads) ---
    async def create_limit_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("create_limit_order", payload)

    async def create_market_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("create_market_order", payload)

    async def create_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("create_order", payload)

    async def new_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("new_order", payload)

    async def submit_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("submit_order", payload)

    async def place_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("place_order", payload)

    async def cancel_order(self, **payload: Any) -> Dict[str, Any]:
        return await self._print_and_ok("cancel_order", payload)

    async def _print_and_ok(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        print("\n===== DRY RUN: ORDER PAYLOAD ({}) =====".format(method))
        print(json.dumps(payload, indent=2, default=str))
        print("===== END PAYLOAD =====\n")
        return {"ok": True, "dry_run": True, "method": method, "payload": payload}


# ------------------------------------------------------------------------------
# Binance client cache (used by BinanceAccount to fetch REAL balance)
# ------------------------------------------------------------------------------
async def _get_binance_client_cache(cred_repo: CredentialRepository):
    cache: Dict[tuple[str, str], BinanceClient] = {}
    lock = asyncio.Lock()

    async def get_client(cred_id: UUID, env: str) -> BinanceClient:
        key = (str(cred_id), env.lower())
        if key in cache:
            return cache[key]
        async with lock:
            if key in cache:
                return cache[key]
            api_key, api_secret = await cred_repo.get_plaintext_credentials(cred_id)
            client = BinanceClient(
                api_key=api_key,
                api_secret=api_secret,
                testnet=(env.lower() == "testnet"),
                timeout_ms=30_000,
            )
            cache[key] = client
            return client

    return get_client


# ------------------------------------------------------------------------------
# Main dry-run routine
# ------------------------------------------------------------------------------
async def run_dry_run(
    *,
    bot_id_str: str,
    symbol: str,
    side_str: str,
    trigger: Decimal,
    stop: Decimal,
    msg_id: str,
    ts_ms: int | None,
) -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)

    side = OrderSide.LONG if side_str.lower() == "long" else OrderSide.SHORT

    # timestamps for signal + indicator
    now_ms = int(time.time() * 1000)
    s_ts_ms = ts_ms or now_ms
    s_ts = s_ts_ms // 1000
    ind_ts_ms = s_ts_ms - 1000  # 1s earlier; OK for dry-run
    ind_ts = ind_ts_ms // 1000

    log.info(
        "Dry-run ARM | bot_id=%s symbol=%s side=%s trigger=%s stop=%s tf=%s",
        bot_id_str, symbol, side.value, trigger, stop, cfg.timeframe
    )

    # Infra
    redis = Redis.from_url(cfg.redis_url, decode_responses=False)
    session_factory = create_session_factory(cfg.postgres_dsn)
    bot_repo = BotRepository(session_factory)
    cred_repo = CredentialRepository(session_factory)
    order_gateway = OrderGateway(session_factory)

    # Real balance path for accurate sizing
    get_binance_client = await _get_binance_client_cache(cred_repo)
    binance_account = BinanceAccount(get_binance_client)
    balance_cache = BalanceCache(ttl_seconds=cfg.balance_ttl_seconds)
    balance_validator = BalanceValidator(
        binance_account=binance_account,
        balance_cache=balance_cache,
    )

    # Position manager (safe no-op client for dry run)
    position_manager = PositionManager(binance_client=None)

    # Trading factory returns our dry-run printer
    async def trading_factory(_: BotConfig) -> DryRunTrading:
        return DryRunTrading()

    order_executor = OrderExecutor(
        balance_validator=balance_validator,
        position_manager=position_manager,
        trading_factory=trading_factory,
    )

    # Resolve the chosen bot
    async with session_factory() as session:
        bot_id = UUID(bot_id_str)
        bot = await bot_repo.get_bot(bot_id)
        if not bot:
            raise RuntimeError(f"Bot not found: {bot_id_str}")

    # Route only to this bot
    router = _SingleBotRouter(bot_id=bot.id, symbol=symbol, timeframe=cfg.timeframe)

    signal_processor = SignalProcessor(
        router=router,
        bot_repository=bot_repo,
        order_executor=order_executor,
        order_gateway=order_gateway,
        trading_factory=trading_factory,
    )

    # Build full ArmSignal (your model requires these fields)
    arm = ArmSignal(
        side=side,
        symbol=symbol,
        timeframe=cfg.timeframe,
        trigger=trigger,
        stop=stop,
        version=1,
        ts_ms=s_ts_ms,
        ts=s_ts,
        ind_ts_ms=ind_ts_ms,
        ind_ts=ind_ts,
        ind_high=trigger,
        ind_low=stop,
    )

    # Process via the real pipeline:
    # validate balance -> sizing -> set_* no-ops -> quantize -> print payload
    results = await signal_processor.process_arm_signal(
        signal=arm,
        message_id=msg_id,
        log_context={"prev_side": "-"},
    )

    # Summarize resulting OrderState(s)
    print("\n===== DRY RUN RESULT: ORDER STATES =====")
    out = []
    for r in results:
        out.append({
            "bot_id": str(r.bot_id),
            "symbol": r.symbol,
            "side": r.side.value,
            "status": r.status.value,
            "qty": str(r.quantity),
            "order_id": r.order_id,
            "trigger": str(getattr(r, "trigger_price", "")),
            "stop": str(getattr(r, "stop_price", "")),
        })
    print(json.dumps(out, indent=2))
    print("===== END RESULT =====\n")

    await redis.aclose()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Dry-run a fake ARM signal and print the exact order payload (no Binance call)."
    )
    p.add_argument("--bot-id", "--bot", dest="bot_id", required=True,
                   help="Bot UUID to use (must be enabled and configured)")
    p.add_argument("--symbol", default="ETHUSDT")
    p.add_argument("--side", choices=["long", "short"], default="long")
    p.add_argument("--trigger", type=Decimal, required=True)
    p.add_argument("--stop", type=Decimal, required=True)
    p.add_argument("--msg-id", default="dryrun-0001", help="Message ID to tag into the flow")
    p.add_argument("--ms", type=int, default=None, help="Signal timestamp in ms; default=now()")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(
            run_dry_run(
                bot_id_str=args.bot_id,
                symbol=args.symbol,
                side_str=args.side,
                trigger=args.trigger,
                stop=args.stop,
                msg_id=args.msg_id,
                ts_ms=args.ms,
            )
        )
    except Exception:
        log.exception("Dry-run failed")
        raise


if __name__ == "__main__":
    main()

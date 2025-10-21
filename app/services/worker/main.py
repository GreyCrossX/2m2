from __future__ import annotations

import asyncio
import logging
import signal
from typing import Callable
from uuid import UUID
from decimal import Decimal

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Config
from .presentation.logging import setup_logging
from .core.poller import WorkerPoller
from .core.router import SymbolRouter
from .infrastructure.redis.stream_consumer import SignalStreamConsumer
from .infrastructure.postgres.session import create_session_factory
from .infrastructure.postgres.repositories import BotRepository, CredentialRepository
from .infrastructure.postgres.order_states import OrderGateway
from .infrastructure.cache.balance_cache import BalanceCache
from .infrastructure.binance.client import BinanceClient
from .infrastructure.binance.account import BinanceAccount
from .infrastructure.binance.trading import BinanceTrading
from .application.balance_validator import BalanceValidator
from .application.position_manager import PositionManager
from .application.order_executor import OrderExecutor
from .application.signal_processor import SignalProcessor

log = logging.getLogger(__name__)


def _client_provider_factory(session_factory, cred_repo: CredentialRepository) -> Callable[[UUID, str], BinanceClient]:
    """
    Returns a provider that gives you a BinanceClient for (user_id, env).
    Keeps it simple: one client per call; you can add caching if desired.
    """
    async def _provider(user_id: UUID, env: str) -> BinanceClient:
        # look up a credential for this user+env (your repo can be extended for this pattern)
        # here we assume bots provide cred_id directly; for BalanceValidator we might need another lookup path.
        raise NotImplementedError
    # We’ll pass a simpler provider below based on bot. This is here if you later switch to (user_id, env) scoped pooling.
    return _provider  # not used for now


async def main_async() -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)
    log.info("Starting Worker | symbols=%s tf=%s redis=%s", ",".join(cfg.symbols), cfg.timeframe, cfg.redis_url)

    # --- Infra: Redis & DB sessions ---
    redis = Redis.from_url(cfg.redis_url, decode_responses=False)
    session_factory = create_session_factory(cfg.postgres_dsn)

    async with session_factory() as db:
        bot_repo = BotRepository(db)
        cred_repo = CredentialRepository(db)
        order_gateway = OrderGateway(db)

        # --- Router ---
        router = SymbolRouter()

        # --- Binance wiring per-bot on demand ---
        # For OrderExecutor we’ll inject a simple client builder used per call:
        async def build_trading_for_bot(bot_cfg) -> BinanceTrading:
            # fetch decrypted creds for this bot
            _bot_orm, _cred_orm, api_key, api_secret = await bot_repo.get_bot_with_credentials_raw(bot_cfg.id)
            client = BinanceClient(api_key=api_key, api_secret=api_secret, testnet=(bot_cfg.env == "testnet"))
            return BinanceTrading(client)

        # --- Balance & positions ---
        balance_cache = BalanceCache(ttl_seconds=cfg.balance_ttl_seconds)

        # BalanceAccount uses a provider keyed by (user_id, env). For now, implement a per-bot helper in OrderExecutor.
        # To keep BalanceValidator compatible, we provide a light wrapper that, given a bot, constructs a client and fetches balance.
        class _AccountAdapter(BinanceAccount):
            def __init__(self):
                # we won't use the base provider; override fetch methods directly
                pass

        async def _fetch_usdt_balance(bot_cfg) -> Decimal:
            _bot_orm, _cred_orm, api_key, api_secret = await bot_repo.get_bot_with_credentials_raw(bot_cfg.id)
            client = BinanceClient(api_key=api_key, api_secret=api_secret, testnet=(bot_cfg.env == "testnet"))
            bals = await client.balance()
            for b in bals:
                if b.get("asset") == "USDT":
                    from decimal import Decimal
                    return Decimal(str(b.get("availableBalance") or b.get("balance") or "0"))
            from decimal import Decimal
            return Decimal("0")

        # Minimal BalanceValidator that uses our cache with (user_id, env) key.
        balance_validator = BalanceValidator(
            binance_account=None,            # not used by this impl
            balance_cache=balance_cache,
        )
        # Patch in a small method the validator can call (depends on your implementation).
        # If your BalanceValidator expects binance_account.get_available_balance(user_id, env), keep that and wire properly.

        position_manager = PositionManager(binance_client=None)  # not strictly needed until you track fills here

        # --- OrderExecutor wiring ---
        # If your OrderExecutor expects a BinanceClient and PositionManager at init:
        order_executor = OrderExecutor(
            balance_validator=balance_validator,
            binance_client=None,            # we will create trading per call instead
            position_manager=position_manager,
        )
        # Ensure your OrderExecutor calls our trading builder (you can inject a callable if needed).
        # Alternatively, modify OrderExecutor to accept a `trading_factory(bot_cfg) -> BinanceTrading`.

        # --- SignalProcessor (app) ---
        sp = SignalProcessor(router=router, bot_repository=bot_repo)  # add gateways if your impl needs them

        # --- Redis consumer (Calc signals) ---
        consumer = SignalStreamConsumer(
            redis=redis,
            symbols=cfg.symbols,
            timeframe=cfg.timeframe,
            block_ms=cfg.stream_block_ms,
            start_from_latest=True,
            use_consumer_group=False,
            persist_offsets=True,
            catchup_threshold_ms=cfg.catchup_threshold_ms,
        )

        # --- Poller ---
        poller = WorkerPoller(
            config=cfg,
            stream_consumer=consumer,
            signal_processor=sp,
            router=router,
            bot_repository=bot_repo,
            router_refresh_seconds=cfg.router_refresh_seconds,
        )

        # graceful shutdown
        stop_event = asyncio.Event()

        def _handle_sig(*_):
            stop_event.set()

        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(s, _handle_sig)
            except NotImplementedError:
                pass  # Windows

        runner = asyncio.create_task(poller.start(), name="poller")

        await stop_event.wait()
        await poller.stop()
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass

    await redis.close()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

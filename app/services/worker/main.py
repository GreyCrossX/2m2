from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Dict, Tuple
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .application.balance_validator import BalanceValidator
from .application.order_executor import OrderExecutor
from .application.position_manager import PositionManager
from .application.signal_processor import SignalProcessor
from .config import Config
from .core.poller import WorkerPoller
from .core.router import SymbolRouter
from .domain.models import BotConfig
from .infrastructure.binance.account import BinanceAccount
from .infrastructure.binance.client import BinanceClient
from .infrastructure.binance.trading import BinanceTrading
from .infrastructure.cache.balance_cache import BalanceCache
from .infrastructure.postgres.order_states import OrderGateway
from .infrastructure.postgres.repositories import BotRepository, CredentialRepository
from .infrastructure.postgres.session import create_session_factory
from .infrastructure.redis.stream_consumer import SignalStreamConsumer
from .presentation.logging import setup_logging

log = logging.getLogger("worker.main")


async def _ping_redis(redis: Redis, *, retries: int = 60, delay: float = 1.0) -> None:
    """Verify Redis connectivity with retry logic for AOF loading scenarios."""
    for attempt in range(1, retries + 1):
        try:
            pong = await redis.ping()
            log.info("Redis ping OK (attempt %d/%d): %s", attempt, retries, pong)
            return
        except Exception as exc:  # pragma: no cover - connection issues are environment-specific
            msg = str(exc)
            if "LOADING" in msg or "loading the dataset" in msg:
                log.info("Redis loading AOF... waiting (%d/%d)", attempt, retries)
            else:
                log.warning("Redis ping failed (%d/%d): %s", attempt, retries, msg)
        await asyncio.sleep(delay)
    raise RuntimeError("Redis not ready after retries")


async def _ping_db(session: AsyncSession) -> None:
    """Verify PostgreSQL connectivity."""
    log.info("Pinging Postgres (SELECT 1)...")
    await session.execute(text("SELECT 1"))
    log.info("Postgres ping OK")


def _setup_signal_handlers(stop_event: asyncio.Event) -> None:
    """Configure graceful shutdown signal handlers."""

    def handle_signal(*_: object) -> None:
        log.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, handle_signal)


async def main_async() -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)

    log.info("=" * 72)
    log.info("Starting Worker")
    log.info("  Redis: %s", cfg.redis_url)
    log.info("  Postgres: %s", cfg.postgres_dsn)
    log.info("  Symbols: %s", ", ".join(cfg.symbols))
    log.info("  Timeframe: %s", cfg.timeframe)
    log.info("  Stream Block: %dms", cfg.stream_block_ms)
    log.info("  Catchup Threshold: %dms", cfg.catchup_threshold_ms)
    log.info("=" * 72)

    redis = Redis.from_url(cfg.redis_url, decode_responses=False)
    session_factory = create_session_factory(cfg.postgres_dsn)

    await _ping_redis(redis)
    async with session_factory() as db_ping:
        await _ping_db(db_ping)

    bot_repo = BotRepository(session_factory)
    cred_repo = CredentialRepository(session_factory)
    order_gateway = OrderGateway(session_factory)
    router = SymbolRouter()

    balance_cache = BalanceCache(ttl_seconds=cfg.balance_ttl_seconds)

    client_cache: Dict[Tuple[UUID, str], BinanceClient] = {}
    client_lock = asyncio.Lock()

    async def get_binance_client(cred_id: UUID, env: str) -> BinanceClient:
        key = (cred_id, env.lower())
        cached = client_cache.get(key)
        if cached is not None:
            return cached
        async with client_lock:
            cached = client_cache.get(key)
            if cached is not None:
                return cached
            api_key, api_secret = await cred_repo.get_plaintext_credentials(cred_id)
            client = BinanceClient(
                api_key=api_key,
                api_secret=api_secret,
                testnet=(env.lower() == "testnet"),
            )
            client_cache[key] = client
            return client

    binance_account = BinanceAccount(get_binance_client)

    balance_validator = BalanceValidator(
        binance_account=binance_account,
        balance_cache=balance_cache,
    )

    position_manager = PositionManager(binance_client=None)

    async def trading_factory(bot_cfg: BotConfig) -> BinanceTrading:
        client = await get_binance_client(bot_cfg.cred_id, bot_cfg.env)
        return BinanceTrading(client)

    order_executor = OrderExecutor(
        balance_validator=balance_validator,
        position_manager=position_manager,
        trading_factory=trading_factory,
    )

    signal_processor = SignalProcessor(
        router=router,
        bot_repository=bot_repo,
        order_executor=order_executor,
        order_gateway=order_gateway,
        trading_factory=trading_factory,
    )

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

    poller = WorkerPoller(
        config=cfg,
        stream_consumer=consumer,
        signal_processor=signal_processor,
        router=router,
        bot_repository=bot_repo,
        router_refresh_seconds=cfg.router_refresh_seconds,
    )

    stop_event = asyncio.Event()
    _setup_signal_handlers(stop_event)

    runner = asyncio.create_task(poller.start(), name="worker.poller")
    log.info("WorkerPoller task started")

    try:
        await stop_event.wait()
    finally:
        log.info("Stopping poller...")
        await poller.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        await redis.close()
        with contextlib.suppress(Exception):
            await redis.connection_pool.disconnect()
    log.info("Worker stopped cleanly")


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    except Exception:  # pragma: no cover - startup exceptions bubble up
        log.exception("Fatal error during worker startup")
        raise


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Callable
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
    for i in range(retries):
        try:
            pong = await redis.ping()
            log.info("Redis ping OK (attempt %d/%d): %s", i + 1, retries, pong)
            return
        except Exception as e:
            msg = str(e)
            if "LOADING" in msg or "loading the dataset" in msg:
                log.info("Redis loading AOF... waiting (%d/%d)", i + 1, retries)
            else:
                log.warning("Redis ping failed (%d/%d): %s", i + 1, retries, msg)
        await asyncio.sleep(delay)
    raise RuntimeError("Redis not ready after retries")


async def _ping_db(session: AsyncSession) -> None:
    """Verify PostgreSQL connectivity."""
    log.info("Pinging Postgres (SELECT 1)...")
    await session.execute(text("SELECT 1"))
    log.info("Postgres ping OK")


async def _build_trading_client(
    bot_cfg,
    bot_repo: BotRepository
) -> BinanceTrading:
    """Create a BinanceTrading instance for a specific bot configuration."""
    _bot_orm, _cred_orm, api_key, api_secret = await bot_repo.get_bot_with_credentials_raw(bot_cfg.id)
    client = BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        testnet=(bot_cfg.env == "testnet")
    )
    return BinanceTrading(client)


def _setup_signal_handlers(stop_event: asyncio.Event) -> None:
    """Configure graceful shutdown signal handlers."""
    def handle_signal(*_):
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

    async with session_factory() as db:
        bot_repo = BotRepository(db)
        cred_repo = CredentialRepository(db)
        order_gateway = OrderGateway(db)
        router = SymbolRouter()

        balance_cache = BalanceCache(ttl_seconds=cfg.balance_ttl_seconds)
        balance_validator = BalanceValidator(
            binance_account=None,
            balance_cache=balance_cache,
        )
        position_manager = PositionManager(binance_client=None)

        order_executor = OrderExecutor(
            balance_validator=balance_validator,
            binance_client=None,
            position_manager=position_manager,
        )
        
        async def trading_factory(bot_cfg):
            return await _build_trading_client(bot_cfg, bot_repo)
        
        order_executor.trading_factory = trading_factory

        signal_processor = SignalProcessor(
            router=router,
            bot_repository=bot_repo,
            order_executor=order_executor,
            order_gateway=order_gateway,
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

        await stop_event.wait()
        log.info("Stopping poller...")
        await poller.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner

    await redis.close()
    log.info("Worker stopped cleanly")


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("Fatal error during worker startup")
        raise


if __name__ == "__main__":
    main()
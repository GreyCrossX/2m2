from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import os
from typing import Dict, Tuple
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .application.balance_validator import BalanceValidator
from .application.order_executor import OrderExecutor
from .application.order_monitor import BinanceOrderMonitor
from .application.position_manager import PositionManager
from .application.signal_processor import SignalProcessor
from .config import Config
from .core.poller import WorkerPoller
from .core.router import SymbolRouter
from .domain.models import BotConfig
from app.services.infrastructure.binance import (
    BinanceAccount,
    BinanceClient,
    BinanceTrading,
)
from app.services.infrastructure.binance.binance_trading import (
    TradingPort as BinanceTradingPort,
)
from .infrastructure.cache.balance_cache import BalanceCache
from .infrastructure.metrics import WorkerMetrics
from .infrastructure.postgres.order_states import OrderGateway
from .infrastructure.postgres.repositories import BotRepository, CredentialRepository
from .infrastructure.postgres.session import create_session_factory
from .infrastructure.redis.stream_consumer import SignalStreamConsumer
from .presentation.logging import setup_logging
from .infrastructure.dry_run import DryRunTradingAdapter

log = logging.getLogger("worker.main")


async def _ping_redis(redis: Redis, *, retries: int = 60, delay: float = 1.0) -> None:
    """Verify Redis connectivity with retry logic for AOF loading scenarios."""
    for attempt in range(1, retries + 1):
        try:
            pong = await redis.ping()
            log.info("Redis ping OK (attempt %d/%d): %s", attempt, retries, pong)
            return
        except (
            Exception
        ) as exc:  # pragma: no cover - connection issues are environment-specific
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


async def _heartbeat(
    redis: Redis, service_name: str, interval: int = 10, ttl: int = 30
) -> None:
    """Periodic liveness heartbeat stored in Redis."""
    key = f"health:worker:{service_name}"
    while True:
        try:
            await redis.set(key, "alive", ex=ttl)
        except Exception as exc:
            log.warning("Heartbeat set failed | key=%s err=%s", key, exc)
        await asyncio.sleep(interval)


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
    if not os.getenv("CREDENTIALS_MASTER_KEY"):
        raise RuntimeError(
            "CREDENTIALS_MASTER_KEY is required to decrypt API credentials for the worker."
        )

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

    if cfg.dry_run_mode:
        log.warning("DRY_RUN_MODE enabled - orders will not be sent to Binance")

    log.info("Connecting to Redis...")
    redis = Redis.from_url(cfg.redis_url, decode_responses=False)

    log.info("Creating Postgres session factory...")
    session_factory = create_session_factory(cfg.postgres_dsn)

    log.info("Verifying connectivity...")
    await _ping_redis(redis)
    async with session_factory() as db_ping:
        await _ping_db(db_ping)
    log.info("Connectivity verified successfully")

    log.info("Initializing repositories...")
    bot_repo = BotRepository(session_factory)
    cred_repo = CredentialRepository(session_factory)
    order_gateway = OrderGateway(session_factory)
    router = SymbolRouter()
    log.info("Repositories initialized")

    log.info("Initializing balance cache (TTL=%ds)...", cfg.balance_ttl_seconds)
    balance_cache = BalanceCache(ttl_seconds=cfg.balance_ttl_seconds)

    log.info("Setting up Binance client cache...")
    client_cache: Dict[Tuple[UUID, str], BinanceClient] = {}
    client_lock = asyncio.Lock()

    async def get_binance_client(cred_id: UUID, env: str) -> BinanceClient:
        key = (cred_id, env.lower())
        cached = client_cache.get(key)
        if cached is not None:
            log.debug("Using cached Binance client | cred_id=%s env=%s", cred_id, env)
            return cached
        async with client_lock:
            cached = client_cache.get(key)
            if cached is not None:
                return cached
            log.info("Creating new Binance client | cred_id=%s env=%s", cred_id, env)
            api_key, api_secret = await cred_repo.get_plaintext_credentials(cred_id)
            client = BinanceClient(
                api_key=api_key,
                api_secret=api_secret,
                testnet=(env.lower() == "testnet"),
                timeout_ms=30_000,
            )
            client_cache[key] = client
            log.info(
                "Binance client created and cached | cred_id=%s env=%s testnet=%s",
                cred_id,
                env,
                env.lower() == "testnet",
            )
            return client

    log.info("Initializing application services...")
    binance_account = BinanceAccount(get_binance_client)

    balance_validator = BalanceValidator(
        binance_account=binance_account,
        balance_cache=balance_cache,
    )

    position_manager = PositionManager()

    dry_run_adapter = DryRunTradingAdapter() if cfg.dry_run_mode else None

    async def trading_factory(
        bot_cfg: BotConfig,
    ) -> BinanceTradingPort | DryRunTradingAdapter:
        log.debug(
            "Creating trading adapter | bot_id=%s symbol=%s", bot_cfg.id, bot_cfg.symbol
        )
        if cfg.dry_run_mode and dry_run_adapter is not None:
            return dry_run_adapter
        client = await get_binance_client(bot_cfg.cred_id, bot_cfg.env)
        return BinanceTrading(client)

    metrics = WorkerMetrics()

    order_executor = OrderExecutor(
        balance_validator=balance_validator,
        position_manager=position_manager,
        trading_factory=trading_factory,
        metrics=metrics,
    )

    order_monitor = BinanceOrderMonitor(
        bot_repository=bot_repo,
        order_gateway=order_gateway,
        position_manager=position_manager,
        trading_factory=trading_factory,
        poll_interval=float(cfg.order_monitor_interval_seconds),
        metrics=metrics,
    )

    signal_processor = SignalProcessor(
        router=router,
        bot_repository=bot_repo,
        order_executor=order_executor,
        order_gateway=order_gateway,
        trading_factory=trading_factory,
        position_store=position_manager,
    )
    log.info("Application services initialized")

    log.info("Creating signal stream consumer...")
    consumer = SignalStreamConsumer(
        redis=redis,
        symbols=cfg.symbols,
        timeframe=cfg.timeframe,
        block_ms=cfg.stream_block_ms,
        start_from_latest=True,
        use_consumer_group=True,
        consumer_group=f"cg:worker:signal|{cfg.timeframe}",
        consumer_name=cfg.service_name,
        persist_offsets=False,
        catchup_threshold_ms=cfg.catchup_threshold_ms,
    )
    log.info("Signal stream consumer created")

    metrics = WorkerMetrics()

    log.info(
        "Creating worker poller (router refresh: %ds)...", cfg.router_refresh_seconds
    )
    poller = WorkerPoller(
        config=cfg,
        stream_consumer=consumer,
        signal_processor=signal_processor,
        router=router,
        bot_repository=bot_repo,
        redis=redis,
        metrics=metrics,
        router_refresh_seconds=cfg.router_refresh_seconds,
    )

    stop_event = asyncio.Event()
    _setup_signal_handlers(stop_event)
    log.info("Signal handlers configured")

    log.info("Starting order monitor...")
    await order_monitor.start()
    log.info("Order monitor started")

    log.info("Starting heartbeat task...")
    hb_task = asyncio.create_task(
        _heartbeat(redis, cfg.service_name), name="worker.heartbeat"
    )

    log.info("Starting worker poller task...")
    runner = asyncio.create_task(poller.start(), name="worker.poller")
    log.info("WorkerPoller task started - now listening for signals")

    try:
        await stop_event.wait()
    finally:
        log.info("Stopping poller...")
        await poller.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        log.info("Stopping heartbeat...")
        hb_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb_task
        log.info("Stopping order monitor...")
        await order_monitor.stop()
        log.info("Closing Redis connection...")
        await redis.aclose()
        log.info("Redis connection closed")
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

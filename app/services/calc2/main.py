from __future__ import annotations
import asyncio
import logging
from typing import Awaitable, List, cast

from redis.asyncio import Redis

from .config import Config
from .utils.logging import setup_logging
from .processors.symbol_processor import SymbolProcessor

logger = logging.getLogger(__name__)


async def _run_symbol(cfg: Config, r: Redis, sym: str) -> None:
    """Run processor for a single symbol."""
    try:
        logger.info("Starting symbol processor | sym=%s", sym)
        proc = SymbolProcessor(cfg, r, sym)
        await proc.run()
    except asyncio.CancelledError:
        logger.info("Symbol processor cancelled | sym=%s", sym)
        raise
    except Exception as e:
        logger.error("Symbol processor failed | sym=%s error=%s", sym, e, exc_info=True)
        raise


async def main_async() -> None:
    setup_logging()
    log = logging.getLogger("calc.main")
    cfg = Config.from_env()

    log.info("=" * 80)
    log.info("Starting Calc Service")
    log.info("  Symbols: %s", ", ".join(cfg.symbols))
    log.info("  Timeframe: %s", cfg.timeframe)
    log.info("  Redis: %s", cfg.redis_url)
    log.info("  MA Windows: 20=%d, 200=%d", cfg.ma20_window, cfg.ma200_window)
    log.info("  Stream Block: %dms", cfg.stream_block_ms)
    log.info(
        "  Stream Maxlen: ind=%d, signal=%d",
        cfg.stream_maxlen_ind,
        cfg.stream_maxlen_signal,
    )
    log.info("  Tick Size: %s", cfg.tick_size)
    log.info("  Backoff: min=%.2fs, max=%.2fs", cfg.backoff_min_s, cfg.backoff_max_s)
    log.info("  Catchup Threshold: %dms", cfg.catchup_threshold_ms)
    log.info("=" * 80)

    r = Redis.from_url(cfg.redis_url, decode_responses=False)

    try:
        # Test Redis connection
        await cast(Awaitable[bool], r.ping())
        log.info("Redis connection established successfully")
    except Exception as e:
        log.error("Failed to connect to Redis | error=%s", e, exc_info=True)
        await r.close()
        raise

    tasks: List[asyncio.Task] = []
    for sym in cfg.symbols:
        task = asyncio.create_task(_run_symbol(cfg, r, sym), name=f"proc:{sym}")
        tasks.append(task)
        log.info("Created task for symbol | sym=%s task_name=%s", sym, task.get_name())

    log.info("All processor tasks created | count=%d", len(tasks))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Main loop cancelled, shutting down gracefully")
        for task in tasks:
            if not task.done():
                task.cancel()
                log.debug("Cancelled task | name=%s", task.get_name())

        # Wait for all tasks to complete cancellation
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("All tasks cancelled")
    except Exception as e:
        log.error("Main loop error | error=%s", e, exc_info=True)
        raise
    finally:
        log.info("Closing Redis connection")
        await r.close()
        log.info("Calc Service stopped")


def main() -> None:
    """Entry point for the calc service."""
    logger.info("Calc service starting up")

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    except Exception as e:
        logger.error("Fatal error in main | error=%s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()

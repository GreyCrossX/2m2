from __future__ import annotations
import asyncio
import logging
from typing import List

from redis.asyncio import Redis

from .config import Config
from .utils.logging import setup_logging
from .processors.symbol_processor import SymbolProcessor

async def _run_symbol(cfg: Config, r: Redis, sym: str) -> None:
    proc = SymbolProcessor(cfg, r, sym)
    await proc.run()




async def main_async() -> None:
    setup_logging()
    log = logging.getLogger("calc.main")
    cfg = Config.from_env()

    log.info("Starting Calc Service | symbols=%s timeframe=%s redis=%s", cfg.symbols, cfg.timeframe, cfg.redis_url)
    r = Redis.from_url(cfg.redis_url, decode_responses=False)


    tasks: List[asyncio.Task] = []
    for sym in cfg.symbols:
        tasks.append(asyncio.create_task(_run_symbol(cfg, r, sym), name=f"proc:{sym}"))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await r.close()




def main() -> None:
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
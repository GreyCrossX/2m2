# services/ingestor/binance_ws.py
import asyncio
import json
import os
import websockets
import logging

LOG = logging.getLogger("ingestor.ws")

def _base():
    market = (os.getenv("BINANCE_MARKET") or "um_futures").lower()
    if market == "spot":
        return "wss://stream.binance.com:9443"
    if market == "cm_futures":
        return "wss://dstream.binance.com"
    return "wss://fstream.binance.com"  # default: USDⓈ-M futures

BASE = os.getenv("BINANCE_WSS_BASE") or _base()

def ws_url_1m(sym: str) -> str:
    return f"{BASE}/ws/{sym.lower()}@kline_1m"

MAX_RECONNECT_ATTEMPTS = int(os.getenv("BINANCE_WS_MAX_RETRIES", "10"))


async def listen_1m(sym: str, on_message):
    url = ws_url_1m(sym)
    backoff = 1
    attempts = 0
    while True:
        try:
            LOG.info("[WS %s] connecting -> %s", sym, url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                LOG.info("[WS %s] connected", sym)
                backoff = 1
                attempts = 0
                first = True
                async for raw in ws:
                    try:
                        if first:
                            LOG.debug("[WS %s] first frame received", sym)
                            first = False
                        msg = json.loads(raw)
                        await on_message(sym, msg)
                    except Exception as e:
                        # show a short prefix of the raw frame to help debug
                        snippet = raw[:120] if isinstance(raw, (str, bytes)) else str(raw)[:120]
                        LOG.error("[WS %s] on_message error: %s | frame=%r", sym, e, snippet)
        except Exception as e:
            attempts += 1
            LOG.warning(
                "[WS %s] connection error: %s | reconnecting in %ss (attempt %d)",
                sym,
                e,
                backoff,
                attempts,
            )
            if MAX_RECONNECT_ATTEMPTS and attempts >= MAX_RECONNECT_ATTEMPTS:
                LOG.error(
                    "[WS %s] exceeded max reconnect attempts (%d). Giving up.",
                    sym,
                    MAX_RECONNECT_ATTEMPTS,
                )
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

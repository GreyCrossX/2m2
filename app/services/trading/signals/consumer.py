from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass

from celery import Task
from app.services.trading.main import app
from app.services.ingestor.redis_io import r as redis_client
from app.services.ingestor.keys import tag as tag_tf
from app.config import settings

LOG = logging.getLogger("trading.signals")

@dataclass
class SignalData:
    """Structured rep of a trading signal"""
    version: str
    signal_type: str
    side: Optional[str]
    symbol: str
    timeframe: str
    timestamp: int
    indicator_ts: Optional[int] = None
    trigger_price: Optional[float] = None
    stop_price: Optional[float] = None
    prev_side: Optional[str] = None
    reason: Optional[str] = None

    @classmethod
    def from_redis_fields(cls, fields:Dict[str,str],) -> 'SignalData':
        """Create SignalData from Redis stream fields"""
        return cls(
            version=fields.get("v", "1"),
            signal_type=fields["type"],
            side=fields.get("side"),
            symbol=fields["sym"],
            timeframe=fields["tf"],
            timestamp=int(fields["ts"]),
            indicator_ts=int(fields["ind_ts"]) if fields.get("ind_ts") else None,
            trigger_price=float(fields["trigger"]) if fields.get("trigger") else None,
            stop_price=float(fields["stop"]) if fields.get("stop") else None,
            prev_side=fields.get("prev_side"),
            reason=fields.get("reason"),
        )
    
def signal_stream(sym:str, tf:str="2m",) -> str:
    """generate signal stream for specific symbol and timeframe"""
    return f"stream:signal | {{{tag_tf(sym, tf)}}}"

class SignalConsumerTask(Task):
    def __init__(self):
        self.active_orders: Dict[str,str] = {}
        self.consumer_running = False

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        LOG.error(f"Signal consumer task {task_id} failed: {exc}")
        return super().on_failure(exc, task_id, args, kwargs, einfo)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        LOG.warning(f"Signal consumer task {task_id} retrying: {exc}")
        return super().on_retry(exc, task_id, args, kwargs, einfo)
        

@app.task(bind=True, base=SignalConsumerTask, name="consume_signals_for_symbol")
def consume_signals_for_symbol(self, symbol:str):
    stream_key = signal_stream(symbol)
    last_id = "$"

    LOG.info(f"[{symbol}] Starting signal consumer for stream: {stream_key}")

    while True:
        try:
            items = redis_client.xread(
                {stream_key:last_id},
                count=10,
                block=1000
            )
            if not items:
                continue

            stream_name, entries = items[0]

            for message_id, fields in entries:
                last_id = message_id

                try:
                    signal = SignalData.from_redis_fields(fields)

                    LOG.info(f"[{symbol}] Received {signal.signal_type} signal:"
                             f"side={signal.side}, ts={signal.timestamp}"
                             )
                    if signal.signal_type == "arm":
                        process_arm_signal.delay(signal.__dict__)
                    elif signal.signal_type == "disarm":
                        process_disarm_signal.delay(signal.__dict__)
                    else:
                        LOG-Warning(f"[{symbol}] Unknown signal type: {signal.signal_type}. Please review srtream service")

                except Exception as e:
                    LOG.error(f"[{symbol}] Error processing signal. {message_id}: {e}")
                    continue

        except Exception as e:
            LOG.error(f"[{symbol}] Stream consumption error: {e}")
            time.sleep(5)  # Brief pause before retrying
            continue

@app.task(name="process_arm_signal")
def process_arm_signal(signal_data: Dict[str, Any]):
    """
    Process ARM trading signal - triggers trade execution.
    
    Args:
        signal_data: Dictionary containing signal information
    """
    signal = SignalData(**signal_data)
    
    LOG.info(
        f"[{signal.symbol}] Processing ARM signal: {signal.side} "
        f"trigger={signal.trigger_price} stop={signal.stop_price}"
    )
    
    try:
        # Import here to avoid circular imports
        from app.services.trading.trading.executor import execute_trade
        
        # Trigger trade execution asynchronously
        execute_trade.delay(
            symbol=signal.symbol,
            side=signal.side,
            trigger_price=signal.trigger_price,
            stop_price=signal.stop_price,
            indicator_ts=signal.indicator_ts,
            signal_ts=signal.timestamp
        )
        
        LOG.info(f"[{signal.symbol}] ARM signal processed, trade execution initiated")
        
    except Exception as e:
        LOG.error(f"[{signal.symbol}] Error processing ARM signal: {e}")
        raise


@app.task(name="process_disarm_signal")
def process_disarm_signal(signal_data: Dict[str, Any]):
    """
    Process DISARM trading signal - cancels pending orders.
    
    Args:
        signal_data: Dictionary containing signal information
    """
    signal = SignalData(**signal_data)
    
    LOG.info(
        f"[{signal.symbol}] Processing DISARM signal: "
        f"prev_side={signal.prev_side} reason={signal.reason}"
    )
    
    try:
        # Import here to avoid circular imports
        from app.services.trading.trading.orders import cancel_pending_orders
        
        # Cancel any pending orders for this symbol
        cancel_pending_orders.delay(
            symbol=signal.symbol,
            reason=f"DISARM: {signal.reason}"
        )
        
        LOG.info(f"[{signal.symbol}] DISARM signal processed, orders cancelled")
        
    except Exception as e:
        LOG.error(f"[{signal.symbol}] Error processing DISARM signal: {e}")
        raise


@app.task(name="start_signal_consumers")
def start_signal_consumers():
    """
    Start signal consumers for all configured trading pairs.
    
    This is typically called once when the worker starts up.
    """
    try:
        pairs = settings.pairs_1m_list()
        symbols = [sym for sym, tf in pairs if tf.lower() == "1m"]
        
        if not symbols:
            LOG.error("No symbols configured for trading")
            return
        
        LOG.info(f"Starting signal consumers for {len(symbols)} symbols: {symbols}")
        
        # Start consumer task for each symbol
        for symbol in symbols:
            consume_signals_for_symbol.delay(symbol)
            LOG.info(f"Started signal consumer for {symbol}")
        
        return f"Started consumers for {len(symbols)} symbols"
        
    except Exception as e:
        LOG.error(f"Error starting signal consumers: {e}")
        raise


# Health check task
@app.task(name="health_check")
def health_check():
    """Simple health check task"""
    try:
        # Test Redis connection
        redis_client.ping()
        
        # Test configuration
        pairs = settings.pairs_1m_list()
        
        return {
            "status": "healthy",
            "redis": "connected",
            "symbols_count": len(pairs),
            "timestamp": int(time.time())
        }
    except Exception as e:
        LOG.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": int(time.time())
        }


if __name__ == "__main__":
    # For testing individual functions
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    
    # Test signal parsing
    test_fields = {
        "v": "1",
        "type": "arm",
        "side": "long",
        "sym": "BTCUSDT",
        "tf": "2m",
        "ts": "1703000000000",
        "ind_ts": "1703000000000",
        "trigger": "45000.50",
        "stop": "44500.00"
    }
    
    signal = SignalData.from_redis_fields(test_fields)
    print(f"Parsed signal: {signal}")
    

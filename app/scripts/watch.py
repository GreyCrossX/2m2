#!/usr/bin/env python3
"""
Real-time monitoring of calculator service processing.

Usage:
    python watch.py [interval_seconds]
"""

import sys
import time
from services.ingestor.redis_io import r as redis_client


def get_stream_last_ts(stream_key: str) -> tuple[int, str]:
    """Get last timestamp and ID from stream."""
    try:
        last = redis_client.xrevrange(stream_key, count=1)
        if last:
            msg_id, fields = last[0]
            ts = int(fields.get(b'ts', 0))
            # Handle both bytes and str for msg_id
            if isinstance(msg_id, bytes):
                msg_id_str = msg_id.decode()
            else:
                msg_id_str = str(msg_id)
            return ts, msg_id_str
        return 0, "EMPTY"
    except Exception as e:
        return -1, f"ERROR: {str(e)[:30]}"


def clear_screen():
    """Clear terminal screen."""
    print("\033[2J\033[H", end="")


def format_time_ago(ms: int) -> str:
    """Format milliseconds as human-readable time."""
    if ms < 0:
        return "ERROR"
    if ms == 0:
        return "0s"
    
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def watch_processing(interval: int = 2):
    """Watch processing in real-time."""
    print("Starting real-time monitor (Ctrl+C to stop)...")
    time.sleep(1)
    
    symbols = ['BTCUSDT', 'ETHUSDT']
    prev_state = {}
    
    try:
        while True:
            clear_screen()
            
            print("="*90)
            print(f"Calculator Service Monitor - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*90)
            print()
            
            current_time_ms = int(time.time() * 1000)
            
            for symbol in symbols:
                market_stream = f"stream:market|{{{symbol}:2m}}"
                ind_stream = f"stream:ind|{{{symbol}:2m}}"
                sig_stream = f"stream:signal|{{{symbol}:2m}}"
                
                # Get current state
                market_ts, market_id = get_stream_last_ts(market_stream)
                ind_ts, ind_id = get_stream_last_ts(ind_stream)
                sig_ts, sig_id = get_stream_last_ts(sig_stream)
                
                market_len = redis_client.xlen(market_stream) if market_ts >= 0 else 0
                ind_len = redis_client.xlen(ind_stream) if ind_ts >= 0 else 0
                sig_len = redis_client.xlen(sig_stream) if sig_ts >= 0 else 0
                
                # Calculate lags
                market_age = current_time_ms - market_ts if market_ts > 0 else 0
                processing_lag = market_ts - ind_ts if market_ts > 0 and ind_ts > 0 else 0
                signal_lag = ind_ts - sig_ts if ind_ts > 0 and sig_ts > 0 else 0
                
                # Calculate processing rate
                processing_rate = "N/A"
                if symbol in prev_state:
                    prev_ind_len = prev_state[symbol].get('ind_len', ind_len)
                    messages_processed = ind_len - prev_ind_len
                    processing_rate = f"{messages_processed / interval:.1f} msg/s"
                
                # Store current state for next iteration
                prev_state[symbol] = {
                    'ind_len': ind_len,
                    'market_ts': market_ts,
                    'ind_ts': ind_ts,
                }
                
                # Status indicators
                if processing_lag > 120000:  # > 2 minutes
                    status = "🔴 CRITICAL"
                elif processing_lag > 10000:  # > 10 seconds
                    status = "🟡 WARNING"
                elif market_age > 300000:  # > 5 minutes
                    status = "⚪ STALE"
                else:
                    status = "🟢 OK"
                
                print(f"{status} {symbol}")
                print("-" * 90)
                print(f"  Market:      {market_len:6d} msgs | Last: {market_id:20s} | Age: {format_time_ago(market_age):>8s}")
                print(f"  Indicators:  {ind_len:6d} msgs | Last: {ind_id:20s} | Lag: {format_time_ago(processing_lag):>8s}")
                print(f"  Signals:     {sig_len:6d} msgs | Last: {sig_id:20s} | Lag: {format_time_ago(signal_lag):>8s}")
                print(f"  Processing:  {processing_rate:>10s}")
                
                if processing_lag > 120000:
                    missing = (processing_lag / 120000)  # Assuming 2m candles
                    print(f"  ⚠️  Behind by ~{missing:.0f} candles!")
                
                print()
            
            print("="*90)
            print(f"Refreshing every {interval}s... (Ctrl+C to stop)")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")


def main():
    """Main entry point."""
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    
    print(f"Monitoring calculator service every {interval} seconds...")
    watch_processing(interval)


if __name__ == "__main__":
    main()
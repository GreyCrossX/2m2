#!/usr/bin/env python3
"""
Container smoke test for your Binance USDS Futures facade.

What it does (safe by default):
  - Checks that credentials/envs are wired.
  - Calls exchange_information() and parses filters for a symbol.
  - Probes account position mode (one-way vs hedge).
  - OPTIONALLY (only with --smoke-order) places a tiny STOP_MARKET reduce-only order.

Usage:
  python test_exchange.py --user <user_id> --symbol BTCUSDT
  python test_exchange.py --user <user_id> --symbol BTCUSDT --smoke-order

Env it reads (your facade already supports a creds lookup, but we also surface these):
  BINANCE_USDSF_API_KEY
  BINANCE_USDSF_API_SECRET
  BINANCE_USDSF_BASE_URL  (optional; defaults to prod)
"""

import os
import sys
import json
import time
import argparse
import logging

# Adjust this import to match where you placed the drop-in facade file.
# If the facade is in app/services/exchange.py, do:
#   from app.services.exchange import probe_position_mode, _selftest_probe, new_order
# For now we assume it's named `exchange.py` and on PYTHONPATH.
from app.services.tasks.exchange import probe_position_mode, _selftest_probe, new_order  # type: ignore

LOG = logging.getLogger("test_exchange")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
)

def jprint(title, obj):
    LOG.info("%s:\n%s", title, json.dumps(obj, indent=2, sort_keys=True))

def assert_ok(cond, msg):
    if not cond:
        raise SystemExit(f"❌ {msg}")

def tiny_qty_for(symbol: str) -> float:
    # Keep it tiny; you can tweak per-symbol later if desired.
    # For stop-market + reduceOnly + closePosition=False, this qty is only used if you actually have position to reduce.
    return 0.001 if symbol.upper().endswith("USDT") else 1.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", required=True, help="User id passed to facade (for creds lookup).")
    ap.add_argument("--symbol", default="BTCUSDT", help="Trading symbol (default: BTCUSDT).")
    ap.add_argument("--smoke-order", action="store_true",
                    help="Place a tiny STOP_MARKET reduce-only order to verify order path. Default: off.")
    ap.add_argument("--side", default="SELL", choices=["BUY","SELL"],
                    help="Side for the optional smoke order (default: SELL).")
    ap.add_argument("--stop", type=float, default=None,
                    help="Stop trigger price for the optional smoke order. If omitted, auto-choose ~1% away.")
    args = ap.parse_args()

    # 0) Quick env nudge (non-fatal)
    missing_envs = [e for e in ("BINANCE_USDSF_API_KEY","BINANCE_USDSF_API_SECRET") if not os.getenv(e)]
    if missing_envs:
        LOG.warning("Missing envs: %s (ok if you use set_creds_lookup)", ", ".join(missing_envs))

    # 1) Self test probe (exchange_information + position mode)
    LOG.info("Running _selftest_probe(...)")
    selftest = _selftest_probe(args.user, args.symbol)
    jprint("_selftest_probe", selftest)
    assert_ok(selftest.get("ok") is True, "selftest failed (exchange_information/position_mode)")

    # 2) Position mode probe
    LOG.info("Running probe_position_mode(...)")
    pm = probe_position_mode(args.user)
    jprint("probe_position_mode", pm)
    assert_ok(pm.get("ok") is True, "position mode probe failed")
    mode = pm.get("mode")
    assert_ok(mode in ("one_way", "hedge", "unknown"), f"unexpected mode: {mode}")
    if mode == "unknown":
        raise SystemExit("❌ Position mode unknown; refusing to place orders. Fix SDK/permissions and retry.")

    # 3) Optional: place **tiny** reduce-only STOP_MARKET order (safe-ish)
    if args.smoke_order:
        symbol = args.symbol.upper()
        side   = args.side.upper()
        # If no stop given, offset ~1% away from current mark (we don't fetch mark here; use a rough heuristic).
        # For SELL stop (closing LONG), set stop slightly *below* a ballpark price; for BUY stop, slightly *above*.
        # We'll pick a static placeholder around 60k for BTC; adjust if needed in your environment.
        guessed_price = 60000.0 if symbol == "BTCUSDT" else 100.0
        stop = args.stop or (guessed_price * (0.99 if side == "SELL" else 1.01))

        # In hedge mode we need positionSide; infer as in the facade.
        pos_side = None
        if mode == "hedge":
            # reduce-only SELL closes LONG, BUY closes SHORT
            pos_side = "LONG" if side == "SELL" else "SHORT"

        LOG.info("Placing tiny STOP_MARKET reduce-only order (symbol=%s side=%s stop=%.4f mode=%s posSide=%s)",
                 symbol, side, stop, mode, pos_side or "-")

        kwargs = dict(
            user_id=args.user,
            symbol=symbol,
            side=side,
            order_type="STOP_MARKET",
            stop_price=float(stop),
            reduce_only=True,
            client_order_id=f"smoke-{int(time.time())}",
        )
        if pos_side:
            kwargs["positionSide"] = pos_side

        resp = new_order(**kwargs)
        jprint("new_order response", resp)
        assert_ok(resp.get("ok") is True, f"new_order failed: {resp}")

        LOG.info("✅ Smoke order path OK (order_id=%s, client_order_id=%s)",
                 resp.get("order_id"), resp.get("client_order_id"))
    else:
        LOG.info("Skipping order placement (no --smoke-order).")

    LOG.info("✅ All checks passed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

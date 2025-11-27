# Binance USDS Futures Testnet Guide

This document outlines how to run the Binance USDⓈ-M futures adapters against the official Binance Futures testnet.

## Prerequisites
- Export valid API credentials for the testnet (worker/API will read encrypted creds from Postgres, but scripts can read from env):
  ```bash
  export BINANCE_TESTNET_API=your_key
  export BINANCE_TESTNET_SECRET=your_secret
  ```
- Optional: set `DEBUG=1` to enable debug logging for payload redaction checks.

## Testing
- Integration tests (live testnet calls):
  ```bash
  $ BINANCE_TESTNET_API=... BINANCE_TESTNET_SECRET=... pytest -m integration -v
  ```
- Unit tests with debug payloads:
  ```bash
  $ DEBUG=1 pytest -m unit -s
  ```

## Smoke Test
Run the CLI helper that exercises `/fapi/v1/order/test` (no live fills) to validate payload shape:
```bash
python -m app.scripts.binance_order_test --symbol BTCUSDT --side BUY --type LIMIT --qty 0.001 --price 20000 --timeInForce GTC
```

The worker places a trio (entry + stop + TP) on testnet when it receives signals; the CLI above is for conformance without execution.

## Payload conformance tips (from Binance futures docs)
- `/fapi/v1/order/test` echoes validation errors without placing trades; use it to verify field names/casing (`symbol/side/type/timeInForce` uppercase, `stopPrice` required on STOP/TP orders, `positionSide` required in Hedge mode).
- Keep `recvWindow` under 60000 and make sure the local clock is in sync; otherwise the testnet will reject signatures before hitting the gateway.
- `batchOrders` is capped at 5 orders; include only the fields Binance accepts (drop `None` keys) so signatures match.

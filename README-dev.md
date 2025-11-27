# Worker Service Development Guide

## Environment Variables (worker/dev)

- Core: `DATABASE_URL`/`POSTGRES_DSN`, `REDIS_URL`, `CREDENTIALS_MASTER_KEY`, `SECRET_KEY`
- Binance creds for scripts/testnet smoke: `BINANCE_TESTNET_API`, `BINANCE_TESTNET_SECRET`
- Worker flags: `WORKER_SYMBOLS`, `TIMEFRAME`, `DRY_RUN_MODE` (set `true` to avoid live orders)
- Timeout/recv window (optional): `BINANCE_TIMEOUT_MS` (default 5000), `RECV_WINDOW_MS` (default 5000)

## Binance API/SKD guardrails (docs review)
- Futures new orders require specific combos: LIMIT needs `price` + `timeInForce`; STOP/TAKE_PROFIT variants need `stopPrice`; Hedge mode must send `positionSide`; `closePosition` is only valid with STOP_MARKET/TAKE_PROFIT_MARKET; `batchOrders` is capped at 5 (doc: Binance Futures Connector Python).
- Keep payloads uppercase and stringified: `symbol/side/type/timeInForce/positionSide` uppercase, Decimals as strings, booleans as `"true"/"false"` where Binance expects them, and drop `None` fields so the SDK signs the exact server schema.
- Respect `recvWindow` (<= 60000) and ensure local clock skew is small; if skewed, sync before calling signed endpoints or surface a clear error instead of letting the SDK time out.
- Use `/fapi/v1/order/test` for payload validation (no execution) and prefer `newOrderRespType=RESULT` while hardening so we can assert the server parsed the fields we sent.

## Installing Dependencies

```bash
pipenv install --dev
```

## Running Tests

```bash
pytest -q
mypy app
ruff check app
```

### Targeted worker tests

The order pipeline now enforces atomic placement of entry/stop/take-profit
orders through `OrderPlacementService`. To validate the updated behaviour run:

```bash
pytest tests/unit/worker/test_order_executor.py
```

This covers trio placement success and rollback on take-profit failures, while
exercising the new balance re-use and failure-path handling inside
`OrderExecutor`.

## Smoke Test

With `BINANCE_TESTNET=true` and valid credentials in the environment, a quick
end-to-end sanity check can be executed via:

```bash
python app/scripts/smoke_order.py --symbol BTCUSDT --price 10000 --qty 0.001 --side BUY --type LIMIT --timeInForce GTC
```

The command should place, query and cancel an order without triggering precision
errors.

# Worker Service Development Guide

## Environment Variables

The Binance USDS futures adapter expects the following variables when running the
worker locally:

| Variable | Description |
| --- | --- |
| `BINANCE_API_KEY` | API key for the account (testnet or production). |
| `BINANCE_API_SECRET` | Secret for the key above. |
| `BINANCE_TESTNET` | Set to `true` to target the Binance testnet. |
| `BINANCE_TIMEOUT_MS` | Optional timeout for REST calls (defaults to `5000`). |

For testnet smoke testing you will also need to export the key/secret pair above
and set `BINANCE_TESTNET=true`.

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

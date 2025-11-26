# 2m2 Trading Stack

An end-to-end Binance USDS futures trading stack built around FastAPI, Redis Streams, Postgres, and a worker that places atomic entry/stop/take-profit orders from calculated signals.

## Quick Start
1. **Install deps** (Python 3.11+): `pipenv install --dev`  
2. **Configure env**: copy `.env` and set `DATABASE_URL`/`DB_URL`, `REDIS_URL`, `SECRET_KEY`, `CREDENTIALS_MASTER_KEY`, and Binance keys (`BINANCE_API_KEY`/`BINANCE_API_SECRET` or user-specific creds).  
3. **Migrate DB**: `alembic -c alembic.ini upgrade head`  
4. **Run services locally** (separate shells):  
   - API: `uvicorn app.main:app --reload --port 8000`  
   - Ingestor: `python -m services.ingestor.main`  
   - Calc2: `python -m services.calc2.main`  
   - Worker: `python -m services.worker.main`  
5. **Docker Compose** (all services): `docker compose up --build`

## What It Does
- **FastAPI App** (`app/main.py`): Auth (`/auth`), encrypted credential management (`/credentials`), and health checks. JWT signing uses `SECRET_KEY`; API secrets are encrypted with `CREDENTIALS_MASTER_KEY`.
- **Ingestor** (`services/ingestor`): Subscribes to Binance 1m klines, optionally backfills, publishes 1m/2m candles to Redis Streams with caps/retention.
- **Calc2** (`services/calc2`): Consumes candle streams, computes indicators/regime, and emits ARM/DISARM trading signals.
- **Worker** (`services/worker`): Reads signals, validates balances, sets leverage, and places entry/stop/take-profit orders on Binance (or dry-run adapter). Persists order state to Postgres and monitors fills/cancels.

## Environment Variables (core)
- `DATABASE_URL` / `DB_URL`: Postgres DSN (`postgresql+asyncpg://...`).  
- `REDIS_URL`: Redis connection for streams/caching.  
- `SECRET_KEY`: JWT signing key (required).  
- `CREDENTIALS_MASTER_KEY`: Master key for encrypting/decrypting stored API credentials (required).  
- `BINANCE_API_KEY` / `BINANCE_API_SECRET`: Default keys for scripts; per-user keys stored via `/credentials`.  
- `BINANCE_TESTNET`: `true` to target Binance testnet in scripts.  
- Worker-specific: `WORKER_SYMBOLS`, `TIMEFRAME`, `DRY_RUN_MODE`, `POSTGRES_DSN` (typically same as `DATABASE_URL`).  
- Ingestor: `PAIRS_1M`, `BACKFILL_ON_START`, `STREAM_MAXLEN_*`, `STREAM_RETENTION_MS_*`.

## Testing & Smoke Checks
- Unit tests: `pytest -q` (or target worker: `pytest tests/unit/worker`).  
- Static checks: `mypy app` and `ruff check app`.  
- Binance smoke order (testnet-ready):  
  ```bash
  BINANCE_TESTNET=true python app/scripts/smoke_order.py --symbol BTCUSDT --price 10000 --qty 0.001 --side BUY --type LIMIT --timeInForce GTC
  ```

## Project Layout (selected)
- `app/api`: FastAPI routers (auth, credentials, health).
- `app/db/models`: ORM models (users, bots, api_credentials, order_states) with Pydantic schemas in `app/db/schemas`.
- `services/ingestor`: Binance WS ingestion → Redis Streams.
- `services/calc2`: Indicator/regime/signal pipeline consuming Redis streams.
- `services/worker`: Signal processing, order execution, monitoring, Binance adapters, Postgres/Redis gateways.

## Notes
- Ensure `SECRET_KEY` and `CREDENTIALS_MASTER_KEY` are set before starting the API or worker; missing keys will prevent credential storage/decryption and invalidate JWTs.
- Use `DRY_RUN_MODE=true` when testing the worker without hitting live Binance endpoints.

# Architecture

## High-Level Services
- **FastAPI app (`app/main.py`)**: Exposes auth (`/auth`), credential storage (`/credentials`), and health endpoints. Uses Postgres via SQLAlchemy async sessions and issues JWTs for clients.
- **Ingestor (`services.ingestor`)**: Listens to Binance 1m websockets, backfills recent klines, and writes normalized 1m/2m candles to Redis Streams (`market:{sym}:{tf}`) with optional trimming.
- **Calc2 (`services.calc2`)**: Consumes the Redis candle streams, computes indicators/regime, and emits trading signals back into Redis Streams for the worker.
- **Worker (`services.worker`)**: Subscribes to signal streams, loads bot + credential configs from Postgres, validates balance, places entry/stop/take-profit orders on Binance (or dry-run adapter), and persists order lifecycle state back to Postgres. An order monitor polls Binance to transition states and close positions.
- **Infrastructure**: Redis for streams/cache, Postgres for persistence (users, bots, credentials, order_states), Binance adapters under `services/infrastructure/binance`, and celery/utility scripts for smoke tests.

## Data Flow
1) Ingestor writes market data (`market:{sym}:1m` and aggregated `:2m`) to Redis Streams, optionally backfilled on startup.  
2) Calc2 consumes those streams, generates indicators/regime state, and publishes ARM/DISARM signals into Redis.  
3) Worker `SignalStreamConsumer` reads signals per timeframe, routes them to subscribed bots, and uses `OrderExecutor` + `OrderPlacementService` to place entry/stop/TP orders through `BinanceTrading`.  
4) Order states are persisted via `OrderGateway` to Postgres `order_states`; `OrderMonitor` polls Binance for fills/cancels to update state and drive `PositionManager`.  
5) FastAPI endpoints manage users and encrypted API credentials (Fernet key derived from `CREDENTIALS_MASTER_KEY`), which the worker decrypts on demand when building Binance clients.

## Key Modules
- `app/api/*`: HTTP routers for health, auth, and credential CRUD (JWT protected).  
- `app/db/models/*`: Postgres ORM entities (users, bots, api_credentials, order_states) with Pydantic schemas in `app/db/schemas`.  
- `app/core/crypto.py`: Fernet-based encryption/decryption of API secrets; requires `CREDENTIALS_MASTER_KEY`.  
- `services/worker/application/*`: Core use-cases (balance validation, order execution, monitoring, signal processing).  
- `services/worker/infrastructure/*`: Postgres gateways, Redis stream consumer, Binance client/trading adapters, dry-run adapter.  
- `services/ingestor/*`: Redis helpers, Binance WS client, backfill logic, minute aggregation.  
- `services/calc2/*`: Indicator/signal processing pipeline, configs, and regime logic.

## Runtime/Env Notes
- Redis and Postgres are expected (Docker Compose wires both); migrations live under `alembic/`.  
- Secrets: JWT signing key (`SECRET_KEY`) and credential encryption key (`CREDENTIALS_MASTER_KEY`) must be set for API/worker to function.  
- Binance access: API key/secret per user is stored encrypted; worker decrypts per bot credential and instantiates clients keyed by (cred_id, env).  
- Dry run: `DRY_RUN_MODE=true` forces worker to use the dry-run adapter while exercising the full pipeline.

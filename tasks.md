# Task List

- [x] Enforce critical secrets at startup: fail fast if `SECRET_KEY` or `CREDENTIALS_MASTER_KEY` are missing and surface clear errors in the API and worker boot paths.
- [x] Unify the Postgres `env_enum` so `bots` and `api_credentials` share a single definition/migration instead of each model creating it (prevents enum-type collisions during deploys).
- [x] Ship authenticated CRUD endpoints (and schema validations) for Bots so the UI can create/update/delete bot configs without manual DB edits.
- [x] Extend documentation with a production `.env.example`, per-service run commands, and diagrams so new contributors can boot Redis/Postgres/ingestor/calc2/worker without guesswork.
- [x] Add integration coverage that drives a full signal → worker → Binance stub → Postgres order_state round-trip (including cancel/disarm) to guard against regressions.
- [x] Introduce operational checks: worker/ingestor liveness/readiness endpoints plus metrics for Redis lag, order monitor outcomes, and Binance error rates.

# Pyright cleanup
- [x] API models: cast SQLAlchemy columns to runtime types in auth/bots/credentials schemas (pyright complains about Column[...] vs str/UUID and `...` default in BotUpdate).
- [ ] Worker type protocols: relax or cast trading_factory/order_gateway/balance_cache in `worker/main.py` and tests so protocols align with concrete classes (TradingPort/OrderGateway).
- [ ] Worker storage mappers: adjust Decimal/UUID conversions in `infrastructure/postgres/order_states.py` and `repositories.py` to avoid Column[...] to Decimal/UUID assignments.
- [ ] Calc2 signal processor: type narrow signals (`ArmSignal`/`DisarmSignal`) instead of `object` to satisfy `.type`/`.to_stream_map` access.
- [ ] Ingestor Redis helpers: fix synchronous Redis client typings (xadd/xlen/xrange returns Awaitable in stubs) and semicolon issues already fixed; add casts/ignores as needed.
- [ ] Misc warnings: silence optional imports (prometheus_client, binance SDK, celery/kombu) with type: ignore or stub path; address health.py dict bool-set warnings; tidy remaining script typings if kept in scope.

# Binance reliability hardening (API + SDK review)
- [ ] Enforce request shapes per Binance futures docs before POST: LIMIT must send `timeInForce` + `price`; STOP/TAKE_PROFIT variants must send `stopPrice`; Hedge mode must send `positionSide`; `closePosition` only with STOP_MARKET/TAKE_PROFIT_MARKET; reject unknown `workingType`/`timeInForce`/`side`/`type` up front.
- [ ] Normalize payload serialization to the SDK’s expected snake_case and stringified numerics: uppercase symbols/sides, stringify Decimal/float to avoid precision drift, coerce booleans to lowercase strings where Binance expects `"true"/"false"`, and drop `None` keys so signatures match server schema.
- [ ] Add configurable `recvWindow` + timestamp/clock-drift guard on all signed endpoints; surface a clear error when local clock skew would invalidate signatures instead of letting the SDK throw opaque errors.
- [ ] Harden exchangeInfo usage: cache + periodic refresh, detect missing `tickSize/stepSize` from SDK responses, and fall back to documented `pricePrecision/quantityPrecision` before we build orders.
- [ ] Add a testnet conformance harness using `/fapi/v1/order/test` (and optional `batchOrders` <= 5) to validate trio payloads and min-notional handling without placing live orders; wire into CI as a smoke job guarded by env keys.
- [ ] Expand error mapping/metrics: capture Binance error codes/messages in the Domain* exceptions, log redacted payload + base path, and export counters for specific classes (BadRequest, RateLimit, ExchangeDown, Forbidden) to spot formatting/rate issues quickly.

# Task List

- [x] Enforce critical secrets at startup: fail fast if `SECRET_KEY` or `CREDENTIALS_MASTER_KEY` are missing and surface clear errors in the API and worker boot paths.
- [x] Unify the Postgres `env_enum` so `bots` and `api_credentials` share a single definition/migration instead of each model creating it (prevents enum-type collisions during deploys).
- [x] Ship authenticated CRUD endpoints (and schema validations) for Bots so the UI can create/update/delete bot configs without manual DB edits.
- [x] Extend documentation with a production `.env.example`, per-service run commands, and diagrams so new contributors can boot Redis/Postgres/ingestor/calc2/worker without guesswork.
- [x] Add integration coverage that drives a full signal -> worker -> Binance stub -> Postgres order_state round-trip (including cancel/disarm) to guard against regressions.
- [x] Introduce operational checks: worker/ingestor liveness/readiness endpoints plus metrics for Redis lag, order monitor outcomes, and Binance error rates.

# Binance reliability hardening (API + SDK review)
- [x] Enforce request shapes per Binance futures docs before POST: LIMIT must send `timeInForce` + `price`; STOP/TAKE_PROFIT variants must send `stopPrice`; Hedge mode must send `positionSide`; `closePosition` only with STOP_MARKET/TAKE_PROFIT_MARKET; reject unknown `workingType`/`timeInForce`/`side`/`type` up front.
- [x] Normalize payload serialization to the SDK's expected snake_case and stringified numerics: uppercase symbols/sides, stringify Decimal/float to avoid precision drift, coerce booleans to lowercase strings where Binance expects `"true"/"false"`, and drop `None` keys so signatures match server schema.
- [x] Add configurable `recvWindow` + timestamp/clock-drift guard on all signed endpoints; surface a clear error when local clock skew would invalidate signatures instead of letting the SDK throw opaque errors.
- [x] Harden exchangeInfo usage: cache + periodic refresh, detect missing `tickSize/stepSize` from SDK responses, and fall back to documented `pricePrecision/quantityPrecision` before we build orders.
- [x] Add a testnet conformance harness using `/fapi/v1/order/test` (and optional `batchOrders` <= 5) to validate trio payloads and min-notional handling without placing live orders; wire into CI as a smoke job guarded by env keys.
- [x] Add a CLI helper to hit `/fapi/v1/order/test` with trio payloads for local smoke (env-guarded).
- [x] Expand error mapping/metrics: capture Binance error codes/messages in the Domain* exceptions, log redacted payload + base path, and export counters for specific classes (BadRequest, RateLimit, ExchangeDown, Forbidden) to spot formatting/rate issues quickly.

# Futures venue expansion
- [ ] Add USDC-M futures support: separate USDC-M client/gateway (base URL + signing), USDC symbol/filter caching, and config toggle per bot/env/credential so we can trade USDC pairs cleanly.

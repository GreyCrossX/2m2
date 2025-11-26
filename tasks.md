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

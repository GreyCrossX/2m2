# Task List

- [x] Enforce critical secrets at startup: fail fast if `SECRET_KEY` or `CREDENTIALS_MASTER_KEY` are missing and surface clear errors in the API and worker boot paths.
- [x] Unify the Postgres `env_enum` so `bots` and `api_credentials` share a single definition/migration instead of each model creating it (prevents enum-type collisions during deploys).
- [x] Ship authenticated CRUD endpoints (and schema validations) for Bots so the UI can create/update/delete bot configs without manual DB edits.
- [x] Extend documentation with a production `.env.example`, per-service run commands, and diagrams so new contributors can boot Redis/Postgres/ingestor/calc2/worker without guesswork.
- [x] Add integration coverage that drives a full signal → worker → Binance stub → Postgres order_state round-trip (including cancel/disarm) to guard against regressions.
- [x] Introduce operational checks: worker/ingestor liveness/readiness endpoints plus metrics for Redis lag, order monitor outcomes, and Binance error rates.

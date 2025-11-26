# Testing Strategy

- **Unit coverage**: Suites under `tests/unit` cover calc2 indicators/processors, ingestor aggregation, and worker order execution/monitoring/signal processing using in-memory fakes for Redis/Binance and persistence gateways.
- **Async execution**: Async tests run via the anyio runner (`tests/conftest.py` wraps coroutine tests); keep `pytest-asyncio` installed or rely on the wrapper so `@pytest.mark.asyncio` tests execute consistently.
- **Static checks**: Recommended (not auto-run here) `ruff check app` and `mypy app` alongside `pytest`.
- **Smoke/integration**: Manual Binance smoke via `python app/scripts/smoke_order.py` (with testnet creds). Docker Compose can boot Redis/Postgres plus ingestor/calc2/worker for end-to-end signal flow; no live-exchange tests run by default.

# Current Status

- `pytest` passes (43 tests) on Python 3.13 with the anyio harness. No known failures.

# Future Integrations

- **Service-level integration**: Extend the in-memory stub test to include order monitor transitions (fills/cancels) and Redis stream playback.
- **Contract tests for Redis streams**: Validate stream shaping/retention across ingestor and calc2 (1m/2m feeds) to guard against schema drift.
- **Smoke pipelines in CI**: Optional dockerized job that spins up Redis/Postgres and runs ingestor + calc2 briefly to ensure catch-up and signal emission still operate.
- **Binance testnet gate**: Opt-in job hitting Binance testnet with tiny sizes to detect API contract changes.

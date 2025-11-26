from app.services.worker.config import Config


def test_config_reads_dry_run_mode(monkeypatch):
    # Ensure unrelated environment variables do not leak between tests
    monkeypatch.delenv("DRY_RUN_MODE", raising=False)
    monkeypatch.setenv("DRY_RUN_MODE", "true")

    # Provide minimal required env to keep defaults deterministic
    monkeypatch.setenv(
        "POSTGRES_DSN", "postgresql+asyncpg://user:pass@localhost:5432/app"
    )
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    cfg = Config.from_env()

    assert cfg.dry_run_mode is True

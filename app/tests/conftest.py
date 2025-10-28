# app/tests/conftest.py
from __future__ import annotations

import sys
import os
import time
import logging
import asyncio
import types
import importlib
from pathlib import Path
import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Ensure repo root on sys.path
# ──────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ──────────────────────────────────────────────────────────────────────────────
# EARLY STUBS (installed before any module import happens)
#   * Avoids import errors during collection
#   * Keep this section minimal and idempotent
# ──────────────────────────────────────────────────────────────────────────────

# services.ingestor.keys
if "services.ingestor.keys" not in sys.modules:
    kmod = types.ModuleType("services.ingestor.keys")

    def _st_market(sym: str, tf: str) -> str:
        return f"stream:market|{{{sym}|{tf}}}"

    def _tag(sym: str, tf: str) -> str:
        return f"{sym}|{tf}"

    kmod.st_market = _st_market
    kmod.tag = _tag
    sys.modules["services.ingestor.keys"] = kmod

# app.config.settings (minimal surface for tests)
if "app.config" not in sys.modules:
    cfgpkg = types.ModuleType("app.config")

    class _Settings:
        def pairs_1m_list(self):
            return [("BTCUSDT", "1m")]

    cfgpkg.settings = _Settings()
    sys.modules["app.config"] = cfgpkg

# celery_app stub (signal_poller imports this at module import time)
if "celery_app" not in sys.modules:
    cam = types.ModuleType("celery_app")

    class _Conf:
        task_always_eager = False
        task_eager_propagates = True

    class _DummyApp:
        def __init__(self):
            self.conf = _Conf()

        def send_task(self, *args, **kwargs):
            # No-op stub; unit tests that import signal_poller won't break.
            return {"ok": True, "sent": {"args": args, "kwargs": kwargs}}

    # expose as both 'celery' and 'app' (signal_poller accepts either)
    cam.celery = _DummyApp()
    cam.app = cam.celery
    sys.modules["celery_app"] = cam

# ──────────────────────────────────────────────────────────────────────────────
# Pytest config (register markers to silence warnings w/o relying on pytest.ini)
# ──────────────────────────────────────────────────────────────────────────────
def pytest_configure(config: pytest.Config) -> None:
    for mark, help_text in [
        ("unit", "fast, isolated tests of single functions/classes"),
        ("integration", "combine multiple components within/between services"),
        ("e2e", "full pipeline tests (calc -> redis -> poller -> celery handlers)"),
        ("mainnet", "hits real external APIs; opt-in via env creds"),
        ("slow", "tests that may take longer"),
        ("asyncio", "test uses asyncio event loop"),
    ]:
        config.addinivalue_line("markers", f"{mark}: {help_text}")


# ──────────────────────────────────────────────────────────────────────────────
# Redis (fakeredis) — install under BOTH import paths used by code
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def fake_redis():
    """
    Provide a FakeRedis client and install it into:
      - services.ingestor.redis_io.r
      - app.services.ingestor.redis_io.r
    so all modules resolve the same in-memory Redis.
    """
    import fakeredis

    r = fakeredis.FakeRedis()

    # services.ingestor.redis_io
    mod_legacy = "services.ingestor.redis_io"
    if mod_legacy not in sys.modules:
        sys.modules[mod_legacy] = types.ModuleType(mod_legacy)
    sys.modules[mod_legacy].r = r

    # app.services.ingestor.redis_io
    mod_app = "app.services.ingestor.redis_io"
    if mod_app not in sys.modules:
        sys.modules[mod_app] = types.ModuleType(mod_app)
    sys.modules[mod_app].r = r

    try:
        yield r
    finally:
        r.flushall()


# ──────────────────────────────────────────────────────────────────────────────
# Calc main import helper (ensures fakes are in place first)
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def import_main(fake_redis):
    """
    Import calc main AFTER fakes are installed (avoids cached real deps).
    """
    for key in list(sys.modules.keys()):
        if key.startswith("services.calc.main") or key.startswith("app.services.calc.main"):
            del sys.modules[key]
    import app.services.calc.main as main  # noqa: WPS433 (runtime import intentional)
    return main


# ──────────────────────────────────────────────────────────────────────────────
# Asyncio loop (per-test for isolation)
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()


# ──────────────────────────────────────────────────────────────────────────────
# Optional: Celery eager mode toggle for e2e tests
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def celery_eager():
    """
    Switch Celery to eager mode so send_task executes synchronously.
    Use in e2e tests when you want to avoid spinning workers.
    """
    try:
        import celery_app  # our stub or a real one if present
        app = getattr(celery_app, "celery", None) or getattr(celery_app, "app", None)
    except Exception:
        app = None

    if not app:
        yield None
        return

    prev_always = getattr(app.conf, "task_always_eager", False)
    prev_prop = getattr(app.conf, "task_eager_propagates", True)
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    try:
        yield app
    finally:
        app.conf.task_always_eager = prev_always
        app.conf.task_eager_propagates = prev_prop


# ──────────────────────────────────────────────────────────────────────────────
# Utilities / builders commonly used in tests
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def bar():
    """Factory for creating market bar data."""
    def _bar(ts: int, open_: float, high: float, low: float, close: float, color: str):
        return {
            "ts": str(ts),
            "open": str(open_),
            "high": str(high),
            "low": str(low),
            "close": str(close),
            "color": color,
        }
    return _bar


@pytest.fixture()
def now_ts():
    """Current timestamp in milliseconds."""
    return int(time.time() * 1000)


@pytest.fixture()
def regime_patcher():
    """Helper to patch choose_regime with a sequence of regimes."""
    def _patch(regimes_sequence):
        idx = {"i": 0}

        def fake_choose_regime(_close, _ma20, _ma200):
            i = min(idx["i"], len(regimes_sequence) - 1)
            return regimes_sequence[i]

        return fake_choose_regime, idx

    return _patch


@pytest.fixture()
def fake_sma():
    """Fake SMA that returns immediate values (skips warmup)."""
    class FakeSMA:
        def __init__(self, period: int):
            self.period = period
            self._count = 0
            self._value = 100.0 if period == 20 else 95.0

        def update(self, x: float):
            self._count += 1
            return self._value

        @property
        def count(self):
            return self._count

    return FakeSMA


# ──────────────────────────────────────────────────────────────────────────────
# Mainnet env helper (opt-in via @pytest.mark.mainnet)
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def set_env_mainnet(monkeypatch):
    """
    Export mainnet env variables for tests that hit live Binance.
    Reads from the environment you run pytest under.
    """
    for name in (
        "BINANCE_USDSF_API_KEY",
        "BINANCE_USDSF_API_SECRET",
        "BINANCE_USDSF_BASE_URL",  # optional override
    ):
        if name in os.environ:
            monkeypatch.setenv(name, os.environ[name])
    yield


# ──────────────────────────────────────────────────────────────────────────────
# Logging defaults for nicer failure output
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _caplog_level_default(caplog):
    """
    Default INFO level so poller/handlers logs show up in test output
    when a test fails. Override per-test with caplog.set_level(...).
    """
    caplog.set_level(logging.INFO)
    yield

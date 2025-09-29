# app/tests/conftest.py
import sys
import types
import asyncio
import time
import pytest

# --- Ensure repo root is on sys.path (paranoid safety even with pytest.ini) ---
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]  # repo root (…/yourrepo)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

@pytest.fixture(scope="session", autouse=True)
def _install_strategy_alias():
    """
    main.py imports from .strategy, while you pasted that content in indicators.py.
    Alias services.calc.strategy -> services.calc.indicators so imports resolve.
    """
    import app.services.calc.indicators as indicators
    sys.modules['services.calc.strategy'] = indicators
    yield

@pytest.fixture(scope="session", autouse=True)
def _install_keys_stub():
    """
    Provide stubs for services.ingestor.keys.{st_market, tag}
    so main.py builds predictable stream names for tests.
    """
    mod = types.ModuleType("services.ingestor.keys")

    def st_market(sym: str, tf: str) -> str:
        return f"stream:market|{{{sym}|{tf}}}"

    def tag(sym: str, tf: str) -> str:
        return f"{sym}|{tf}"

    mod.st_market = st_market
    mod.tag = tag

    sys.modules["services.ingestor.keys"] = mod
    yield

@pytest.fixture(scope="session", autouse=True)
def _install_settings_stub():
    """
    Stub app.config.settings so pairs_1m_list() returns one symbol.
    """
    cfgpkg = types.ModuleType("app.config")
    class _Settings:
        def pairs_1m_list(self):
            return [("BTCUSDT", "1m")]
    cfgpkg.settings = _Settings()
    sys.modules["app.config"] = cfgpkg
    yield

@pytest.fixture()
def fake_redis(monkeypatch):
    """
    Provide a FakeRedis client and patch it into services.ingestor.redis_io.r
    """
    import fakeredis
    r = fakeredis.FakeRedis()
    modname = "services.ingestor.redis_io"
    if modname not in sys.modules:
        sys.modules[modname] = types.ModuleType(modname)
    sys.modules[modname].r = r
    yield r
    r.flushall()

@pytest.fixture()
def import_main(fake_redis):
    """
    Import main after fakes are installed so it picks them up.
    """
    if "services.calc.main" in sys.modules:
        del sys.modules["services.calc.main"]
    import app.services.calc.main as main
    return main

@pytest.fixture()
def event_loop():
    """If you’re not using pytest-asyncio's loop fixture, provide one."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

# ---- Utility builders for tests ----
def _bar(ts: int, open_: float, high: float, low: float, close: float, color: str):
    return {
        "ts": str(ts),
        "open": str(open_),
        "high": str(high),
        "low": str(low),
        "close": str(close),
        "color": color,
    }

@pytest.fixture()
def bar():
    return _bar

@pytest.fixture()
def now_ts():
    return int(time.time() * 1000)

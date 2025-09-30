# app/tests/conftest.py
import sys
import types
import asyncio
import time
import pytest

# --- Ensure repo root is on sys.path ---
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

@pytest.fixture(scope="session", autouse=True)
def _install_strategy_alias():
    """Alias services.calc.strategy -> app.services.calc.strategy"""
    import app.services.calc.strategy as strategy
    sys.modules['services.calc.strategy'] = strategy
    yield

@pytest.fixture(scope="session", autouse=True)
def _install_keys_stub():
    """Provide stubs for services.ingestor.keys"""
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
    """Stub app.config.settings"""
    cfgpkg = types.ModuleType("app.config")
    class _Settings:
        def pairs_1m_list(self):
            return [("BTCUSDT", "1m")]
    cfgpkg.settings = _Settings()
    sys.modules["app.config"] = cfgpkg
    yield

@pytest.fixture()
def fake_redis():
    """Provide a FakeRedis client"""
    import fakeredis
    r = fakeredis.FakeRedis()
    
    # Install into the module system
    modname = "services.ingestor.redis_io"
    if modname not in sys.modules:
        sys.modules[modname] = types.ModuleType(modname)
    sys.modules[modname].r = r
    
    yield r
    r.flushall()

@pytest.fixture()
def import_main(fake_redis):
    """Import main after fakes are installed"""
    # Clear any cached import
    for key in list(sys.modules.keys()):
        if 'services.calc.main' in key or 'app.services.calc.main' in key:
            del sys.modules[key]
    
    import app.services.calc.main as main
    return main

@pytest.fixture()
def event_loop():
    """Provide event loop for async tests"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

# ---- Utility builders for tests ----
@pytest.fixture()
def bar():
    """Factory for creating market bar data"""
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
    """Current timestamp in milliseconds"""
    return int(time.time() * 1000)

@pytest.fixture()
def regime_patcher():
    """Helper to patch choose_regime with a sequence of regimes"""
    def _patch(regimes_sequence):
        idx = {"i": 0}
        
        def fake_choose_regime(_close, _ma20, _ma200):
            i = min(idx["i"], len(regimes_sequence) - 1)
            return regimes_sequence[i]
        
        return fake_choose_regime, idx
    
    return _patch

@pytest.fixture()
def fake_sma():
    """Create fake SMA class that returns values immediately (skips warmup)"""
    class FakeSMA:
        def __init__(self, period: int):
            self.period = period
            self._count = 0
            # Return fake but reasonable values based on period
            self._value = 100.0 if period == 20 else 95.0
        
        def update(self, x: float):
            self._count += 1
            # Always return a value (skip warmup)
            return self._value
        
        @property
        def count(self):
            return self._count
    
    return FakeSMA
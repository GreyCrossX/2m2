import os
import pytest
from decimal import Decimal

pytestmark = [pytest.mark.mainnet, pytest.mark.exchange]

pytest.importorskip(
    "binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures",
    reason="Binance USDS Futures SDK not installed",
)

def _have_creds():
    return bool(os.getenv("BINANCE_USDSF_API_KEY") and os.getenv("BINANCE_USDSF_API_SECRET"))

@pytest.mark.skipif(not _have_creds(), reason="mainnet creds not set")
def test_mainnet_balance_fetch(set_env_mainnet):
    from app.services.tasks.balance_source import get_balances_raw
    rows = get_balances_raw(user_id="u1")
    assert isinstance(rows, list)

@pytest.mark.skipif(not _have_creds(), reason="mainnet creds not set")
def test_mainnet_order_rejection_is_graceful(set_env_mainnet):
    """We don't assert rejection—only that the call is handled and returns a well-formed dict."""
    from app.services.tasks.exchange import new_order
    out = new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.001"),
    )
    assert isinstance(out, dict)
    assert "ok" in out
    if out["ok"] is False:
        # Facade should surface an error string or raw dict
        assert isinstance(out.get("error") or out.get("raw"), (str, dict))
    else:
        # If somehow accepted, at least the facade returns expected identifiers
        assert out.get("order_id") or out.get("client_order_id")

import pytest
from decimal import Decimal
from app.services.tasks.exchange_filters import (
    round_price, round_qty, enforce_mins, apply_symbol_filters
)

@pytest.mark.unit
class TestExchangeFiltersRounding:
    def test_round_price_floor_to_tick(self):
        filters = {"tick_size": Decimal("0.10")}
        assert round_price(Decimal("123.19"), filters) == Decimal("123.10")
        assert round_price(Decimal("123.10"), filters) == Decimal("123.10")
        assert round_price(Decimal("0.09"), filters) == Decimal("0.00")

    def test_round_qty_floor_to_step(self):
        filters = {"step_size": Decimal("0.001")}
        assert round_qty(Decimal("0.12345"), filters) == Decimal("0.123")
        assert round_qty(Decimal("1.0009"), filters) == Decimal("1.000")

    def test_enforce_mins_qty_and_notional(self):
        # Both mins present → result must satisfy the stricter one.
        filters = {
            "min_qty": Decimal("0.05"),
            "min_notional": Decimal("10"),
        }
        price = Decimal("100")
        qty = Decimal("0.01")
        p2, q2, warnings = enforce_mins(price, qty, filters)
        # Must be >= 0.10 due to notional (10/100)
        assert q2 == Decimal("0.10")
        assert any("min_qty" in w for w in warnings) or any("min_notional" in w for w in warnings)

        # Notional-only case
        p3, q3, warnings2 = enforce_mins(Decimal("100"), Decimal("0.01"), {"min_notional": Decimal("20")})
        assert q3 == Decimal("0.20")
        assert any("min_notional" in w for w in warnings2)

    def test_apply_symbol_filters_rounds_both_price_and_qty(self):
        filters = {
            "tick_size": Decimal("0.10"),
            "step_size": Decimal("0.001"),
            "min_qty": Decimal("0.01"),
            "min_notional": Decimal("10"),
        }
        payload = {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "order_type": "STOP_MARKET",
            "quantity": Decimal("0.0099"),
            "stop_price": Decimal("123.17"),
        }
        out = apply_symbol_filters(payload, filters)
        assert out["stop_price"] == Decimal("123.10")
        # Must satisfy min_notional => qty >= 10 / 123.10 ≈ 0.0812 → rounded to 0.081 (step=0.001)
        assert out["quantity"] >= Decimal("0.081")
        assert out["quantity"] == out["quantity"].quantize(Decimal("0.001"))

import pytest
from decimal import Decimal
from app.services.tasks.sizing import compute_position_size

class TestSizing:
    def test_invalid_distance_returns_not_ok(self):
        res = compute_position_size(
            trigger=Decimal("100"),
            stop=Decimal("100"),
            balance_free=Decimal("1000"),
            risk_per_trade=Decimal("0.01"),
            leverage=Decimal("5"),
            filters={"step_size": "0.001", "min_qty": "0", "min_notional": "0"},
        )
        assert res["ok"] is False
        assert res["distance"] == Decimal("0")

    def test_no_risk_budget_returns_not_ok(self):
        res = compute_position_size(
            trigger=Decimal("100"),
            stop=Decimal("90"),
            balance_free=Decimal("0"),
            risk_per_trade=Decimal("0.01"),
            leverage=Decimal("5"),
            filters={"step_size": "0.001", "min_qty": "0", "min_notional": "0"},
        )
        assert res["ok"] is False
        assert "no risk budget" in " ".join(res.get("notes", []))

    def test_basic_qty_with_constraints(self):
        res = compute_position_size(
            trigger=Decimal("100"),
            stop=Decimal("90"),
            balance_free=Decimal("1000"),         # risk_usdt = 10
            risk_per_trade=Decimal("0.01"),
            leverage=Decimal("5"),
            filters={
                "step_size": "0.001",
                "min_qty": "0.05",
                "min_notional": "10",             # qty * 100 >= 10 => qty >= 0.10
            },
        )
        assert res["ok"] is True
        # Enforced by min_notional -> qty >= 0.10 (then rounded to step)
        assert res["qty"] >= Decimal("0.10")
        assert (res["qty"] / Decimal("0.001")).to_integral() == (res["qty"] / Decimal("0.001"))

    def test_higher_leverage_increases_qty(self):
        low = compute_position_size(
            trigger=Decimal("100"),
            stop=Decimal("95"),
            balance_free=Decimal("1000"),
            risk_per_trade=Decimal("0.01"),
            leverage=Decimal("1"),
            filters={"step_size": "0.001", "min_qty": "0", "min_notional": "0"},
        )
        high = compute_position_size(
            trigger=Decimal("100"),
            stop=Decimal("95"),
            balance_free=Decimal("1000"),
            risk_per_trade=Decimal("0.01"),
            leverage=Decimal("10"),
            filters={"step_size": "0.001", "min_qty": "0", "min_notional": "0"},
        )
        assert high["ok"] and low["ok"]
        assert high["qty"] > low["qty"]

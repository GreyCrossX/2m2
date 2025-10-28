import pytest
from decimal import Decimal
from app.services.tasks.payloads import (
    build_entry_order, build_stop_loss_order, build_take_profit_order, build_brackets
)

BASE_FILTERS = {
    "tick_size": Decimal("0.10"),
    "step_size": Decimal("0.001"),
    "min_qty": Decimal("0.001"),
    "min_notional": Decimal("5"),
}

class TestPayloadBuilders:
    def test_build_entry_order_with_filters(self):
        out = build_entry_order(
            sym="BTCUSDT",
            side="long",
            qty=Decimal("0.00123"),
            trigger=Decimal("123.17"),
            working_type="MARK_PRICE",
            client_order_id="coid-entry",
            filters=BASE_FILTERS,
        )
        assert out["symbol"] == "BTCUSDT"
        assert out["side"] == "BUY"
        assert out["order_type"] == "STOP_MARKET"
        # rounded
        assert out["stop_price"] == Decimal("123.10")
        # must meet min_notional=5 → qty >= 5 / 123.10 ≈ 0.0406 → 0.040 after step floor
        assert out["quantity"] == Decimal("0.040")
        assert out["client_order_id"] == "coid-entry"

    def test_build_stop_loss_reduce_only(self):
        out = build_stop_loss_order(
            sym="BTCUSDT",
            side="long",  # closing long => SELL
            qty=Decimal("0.01"),
            stop=Decimal("120.07"),
            working_type="MARK_PRICE",
            client_order_id="coid-sl",
            filters=BASE_FILTERS,
        )
        assert out["side"] == "SELL"
        assert out["reduce_only"] is True
        assert out["close_position"] is True
        assert out["stop_price"] == Decimal("120.00")
        assert out["client_order_id"] == "coid-sl"

    def test_build_take_profit_from_r_multiple(self):
        payload, tp = build_take_profit_order(
            sym="BTCUSDT",
            side="long",
            qty=Decimal("0.05"),
            avg_entry_price=Decimal("100.00"),
            stop=Decimal("90.00"),
            tp_ratio=Decimal("1.5"),  # distance=10 → tp=115.00
            working_type="MARK_PRICE",
            client_order_id="coid-tp",
            filters=BASE_FILTERS,
        )
        assert payload["stop_price"] == Decimal("115.00")
        assert tp == Decimal("115.00")
        assert payload["reduce_only"] is True
        assert payload["side"] == "SELL"

    def test_build_brackets_both_orders(self):
        brackets, tp_price = build_brackets(
            sym="BTCUSDT",
            side="short",
            qty=Decimal("0.05"),
            avg_entry_price=Decimal("200.00"),
            stop=Decimal("210.00"),
            tp_ratio=Decimal("2"),  # distance=10 → tp=180
            working_type="MARK_PRICE",
            sl_client_order_id="sl-1",
            tp_client_order_id="tp-1",
            filters=BASE_FILTERS,
        )
        sl = brackets["stop_loss"]
        tp = brackets["take_profit"]
        assert sl["side"] == "BUY"
        assert sl["client_order_id"] == "sl-1"
        assert tp["side"] == "BUY"
        assert tp["client_order_id"] == "tp-1"
        assert tp["stop_price"] == Decimal("180.00")
        assert tp_price == Decimal("180.00")

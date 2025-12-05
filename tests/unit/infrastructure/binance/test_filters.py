from decimal import Decimal
import importlib.util
from pathlib import Path


def _load_filters_module():
    root = Path(__file__).resolve().parents[4]
    mod_path = (
        root
        / "app"
        / "services"
        / "infrastructure"
        / "binance"
        / "utils"
        / "filters.py"
    )
    spec = importlib.util.spec_from_file_location("filters_under_test", mod_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def test_build_symbol_filters_injects_fallback_tick() -> None:
    filters = _load_filters_module()

    exch = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.001",
                        "minQty": "0.001",
                    },
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.01",  # will be overridden to 0.1
                    },
                ],
                "pricePrecision": 3,
            }
        ]
    }

    out = filters.build_symbol_filters(exch)
    pf = out["BTCUSDT"]["PRICE_FILTER"]
    assert pf["tickSize"] == "0.1"  # fallback map applied

    # quantize_price should now floor to 0.1 ticks
    q_price = filters.quantize_price(out["BTCUSDT"], Decimal("90112.65"))
    assert q_price == Decimal("90112.6")

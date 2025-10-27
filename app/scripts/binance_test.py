# app/scripts/binance_test.py
import os, time, hmac, hashlib, requests
from urllib.parse import urlencode

API_KEY = os.getenv("BINANCE_USDSF_API_KEY", "")
API_SECRET = os.getenv("BINANCE_USDSF_API_SECRET", "")
BASE = "https://fapi.binance.com"  # prod USDⓈ-M Futures

def sign(params: dict) -> str:
    q = urlencode(params, doseq=True)
    return hmac.new(API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()

def server_time_ms() -> int:
    r = requests.get(f"{BASE}/fapi/v1/time", timeout=10)
    r.raise_for_status()
    return int(r.json()["serverTime"])

def order_test(
    symbol="BTCUSDT",
    side="BUY",
    order_type="LIMIT",
    quantity="0.001",      # keep as string to avoid float drift
    price="200000",        # far away so it wouldn't fill (just for shape)
    time_in_force="GTC",
    hedge_mode=False,      # set True if your account is in hedge mode
):
    ts = server_time_ms()
    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "price": price,
        "timeInForce": time_in_force,
        "timestamp": ts,
        "recvWindow": 5000,
    }
    # If your account is in hedge mode you MUST provide positionSide
    if hedge_mode:
        # For a BUY long: LONG. For a SELL short: SHORT.
        params["positionSide"] = "LONG"

    params["signature"] = sign(params)
    headers = {"X-MBX-APIKEY": API_KEY}

    url = f"{BASE}/fapi/v1/order/test"
    r = requests.post(url, params=params, headers=headers, timeout=20)

    # Don't raise; show their exact error message for diagnosis
    print("status:", r.status_code)
    try:
        print("body:", r.json())
    except Exception:
        print("body:", r.text)

if __name__ == "__main__":
    print("running order test...")
    order_test(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity="0.001",
        price="119512",
        time_in_force="GTC",
        hedge_mode=False,  # flip to True if you use Hedge Mode
    )

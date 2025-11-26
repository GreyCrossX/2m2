def normalize_closed_kline_1m(msg: dict) -> dict | None:
    if msg.get("e") != "kline":
        return None
    k = msg.get("k", {})
    if not k.get("x", False):
        return None
    if k.get("i") != "1m":
        return None
    sym = k["s"]
    ts = int(k["T"])
    o = k["o"]
    h = k["h"]
    low = k["l"]
    c = k["c"]
    return {
        "v": "1",
        "ts": str(ts),
        "sym": sym,
        "tf": "1m",
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "volume": str(k["v"]),
        "trades": str(k.get("n", 0)),
        "color": "green" if float(c) >= float(o) else "red",
        "src": "binance",
        "cid": f"binance:{sym}:1m:{ts}",
    }

def tag(sym: str, tf: str) -> str:
    return f"{sym}:{tf}"

def st_market(sym: str, tf: str) -> str:
    return f"stream:market|{{{tag(sym, tf)}}}"

def cid_1m(sym: str, ts_close_ms: int) -> str:
    return f"binance:{sym}:1m:{ts_close_ms}"

def cid_2m(sym: str, ts_close_ms_odd: int) -> str:
    return f"binance-agg:{sym}:2m:{ts_close_ms_odd}"

def dedupe_key(cid: str) -> str:
    return f"dedupe:{cid}"

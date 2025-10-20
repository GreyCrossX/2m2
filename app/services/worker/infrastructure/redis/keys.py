from __future__ import annotations

def stream_signal(sym: str, tf: str) -> str:
    return f"stream:signal|{{{sym}:{tf}}}"

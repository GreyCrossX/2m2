from __future__ import annotations


def st_market(sym: str, tf: str) -> str:
    return f"stream:market|{{{sym}:{tf}}}"


def st_ind(sym: str, tf: str) -> str:
    return f"stream:ind|{{{sym}:{tf}}}"


def st_signal(sym: str, tf: str) -> str:
    return f"stream:signal|{{{sym}:{tf}}}"


def snap_ind(sym: str, tf: str) -> str:
    return f"snap:ind|{{{sym}:{tf}}}"

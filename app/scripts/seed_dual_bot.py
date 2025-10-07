# app/scripts/seed_dual_bots_real.py
from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG (defaults baked in per your message; can override with env vars)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_USER_ID = os.getenv("SEED_USER_ID", "36458f2c-02ad-459e-ac26-7c8a5265cc54")
DEFAULT_CRED_ID = os.getenv("SEED_CRED_ID", "0ab59c84-cf45-4663-800d-550fc4cec39f")

# Real (not testnet)
TARGET_ENV = os.getenv("SEED_ENV", "prod")  # must match your prod/real enum in DB

# Risk/leverage/take-profit
RISK_PCT = Decimal(os.getenv("SEED_RISK", "0.005"))  # 0.5%
LEVERAGE = Decimal(os.getenv("SEED_LEV", "5"))
TP_RATIO = Decimal(os.getenv("SEED_TP", "1.5"))

# Whether to emit a *fake* ARM into the signal stream (OFF by default for real!)
EMIT_FAKE = os.getenv("EMIT_FAKE", "0") == "1"

# Symbols to seed
SYMBOLS = [s.strip().upper() for s in os.getenv("SEED_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# ──────────────────────────────────────────────────────────────────────────────
# Optional DB plumbing (seed/ensure bots if DB is configured)
# ──────────────────────────────────────────────────────────────────────────────
DB_URL = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").replace("+asyncpg", "")
USE_DB = bool(DB_URL)

if USE_DB:
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session

    engine = create_engine(DB_URL, future=True)
    # Adjust model import paths if your app differs
    from app.db.models.user import User
    from app.db.models.credentials import ApiCredential
    from app.db.models.bots import Bot

# ──────────────────────────────────────────────────────────────────────────────
# Redis + task-layer adapters
# ──────────────────────────────────────────────────────────────────────────────
from app.services.ingestor.redis_io import r  # shared Redis client

from app.services.tasks.state import (
    write_bot_config,
    read_bot_config,
    index_bot,
)

# filters seeding is optional; only used if available
try:
    from app.services.tasks.filters_source import set_symbol_filters  # type: ignore
except Exception:
    set_symbol_filters = None


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _to_str(v: Any) -> str:
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)

def stream_signal_by_tag(tag: str) -> str:
    """Canonical calc/poller stream name: stream:signal|{SYM:TF}"""
    return f"stream:signal|{{{tag}}}"

def idx_key_for_symbol(sym: str) -> str:
    # matches poller expectation idx:bots|{SYMBOL}
    return f"idx:bots|{{{sym.upper()}}}"


# ──────────────────────────────────────────────────────────────────────────────
# DB seed/update helpers (idempotent)
# ──────────────────────────────────────────────────────────────────────────────
def ensure_user_and_cred(session: "Session", user_id: str, cred_id: str, env: str):
    # Validate user exists
    u = session.execute(select(User).where(User.id == user_id)).scalar_one_or_none()
    if not u:
        raise SystemExit(f"[DB] User {user_id} not found")
    # Validate credential exists and belongs to user
    c = session.execute(select(ApiCredential).where(ApiCredential.id == cred_id)).scalar_one_or_none()
    if not c:
        raise SystemExit(f"[DB] Credential {cred_id} not found")
    if str(c.user_id) != str(user_id):
        raise SystemExit(f"[DB] Credential {cred_id} does not belong to user {user_id}")
    # Optional: verify env match (warn if mismatch)
    if getattr(c, "env", None) and str(c.env) != env:
        print(f"[WARN] Credential env is '{c.env}', but target env is '{env}'. Proceeding…")
    return u, c

def upsert_bot_real(session: "Session", *, user_id: str, cred_id: str, symbol: str) -> "Bot":
    b = session.execute(
        select(Bot).where((Bot.user_id == user_id) & (Bot.symbol == symbol))
    ).scalar_one_or_none()
    if b:
        # ensure fields are aligned for real env testing
        b.cred_id = cred_id
        b.timeframe = "2m"
        b.enabled = True
        b.env = TARGET_ENV
        b.side_whitelist = "both"
        b.leverage = int(LEVERAGE)
        b.use_balance_pct = True
        b.balance_pct = _to_str(RISK_PCT)  # e.g., 0.0050
        session.flush()
        return b

    b = Bot(
        user_id=user_id,
        cred_id=cred_id,
        symbol=symbol,
        timeframe="2m",
        enabled=True,
        env=TARGET_ENV,
        side_whitelist="both",
        leverage=int(LEVERAGE),
        use_balance_pct=True,
        balance_pct=_to_str(RISK_PCT),
    )
    session.add(b)
    session.flush()
    return b


# ──────────────────────────────────────────────────────────────────────────────
# Redis bot config + index
# ──────────────────────────────────────────────────────────────────────────────
def seed_bot_in_redis(
    *,
    bot_id: str,
    user_id: str,
    sym: str,
    status: str = "active",
    side_mode: str = "both",  # both | long_only | short_only
    risk_per_trade: Decimal = RISK_PCT,
    leverage: Decimal = LEVERAGE,
    tp_ratio: Decimal = TP_RATIO,
) -> Dict[str, Any]:
    cfg = {
        "bot_id": bot_id,
        "user_id": user_id,
        "sym": sym.upper(),
        "status": status,
        "side_mode": side_mode,
        "risk_per_trade": _to_str(risk_per_trade),
        "leverage": _to_str(leverage),
        "tp_ratio": _to_str(tp_ratio),
    }
    write_bot_config(bot_id, cfg)
    index_bot(sym.upper(), bot_id)
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Filters (optional seed)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_FILTERS = {
    # Adjust if you have real futures filters handy
    "pricePrecision": 2,
    "quantityPrecision": 3,
    "tickSize": "0.10",
    "stepSize": "0.001",
    "minQty": "0.001",
    "minNotional": "5",
}

def maybe_seed_filters(sym: str) -> None:
    if set_symbol_filters is None:
        print(f"[filters] Skipped for {sym} (filters_source not available).")
        return
    try:
        set_symbol_filters(sym.upper(), DEFAULT_FILTERS)
        print(f"[filters] Seeded filters for {sym}: {DEFAULT_FILTERS}")
    except Exception as e:
        print(f"[filters] Failed to seed filters for {sym} (non-fatal): {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Emit a fake ARM (goes to poller via calc’s stream) — OFF by default on real
# ──────────────────────────────────────────────────────────────────────────────
def emit_fake_arm(
    *,
    sym: str,
    tf: str = "2m",
    side: str = "long",           # or "short"
    trigger: str = "65000.0",
    stop: str = "64850.0",
    ind_ts_ms: Optional[int] = None,
) -> str:
    ts = int(ind_ts_ms or (int(time.time() * 1000)))
    tag = f"{sym.upper()}:{tf}"
    stream = stream_signal_by_tag(tag)

    fields = {
        "v": "1",
        "type": "arm",
        "side": side,      # "long" | "short"
        "sym": sym.upper(),
        "tf": tf,
        "ts": str(ts),
        "ind_ts": str(ts),
        "trigger": trigger,
        "stop": stop,
    }
    msg_id = r.xadd(stream, fields, id=f"{ts}-0", maxlen=2000, approximate=True)
    return msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    user_id = DEFAULT_USER_ID
    cred_id = DEFAULT_CRED_ID

    print(f"[setup] Target env: {TARGET_ENV}")
    print(f"[setup] Symbols: {SYMBOLS}")
    print(f"[setup] Using user_id={user_id} cred_id={cred_id}")
    print(f"[setup] Risk={_to_str(RISK_PCT)} leverage={_to_str(LEVERAGE)} tp_ratio={_to_str(TP_RATIO)}")

    # 1) DB ensure: create/update 2 bots for real env
    bot_ids_by_symbol: Dict[str, str] = {}

    if USE_DB:
        from sqlalchemy.exc import SQLAlchemyError

        try:
            with Session(engine) as s:
                # Validate user + credential
                u, c = ensure_user_and_cred(s, user_id, cred_id, TARGET_ENV)

                # Ensure/Upsert bots
                for sym in SYMBOLS:
                    b = upsert_bot_real(s, user_id=str(u.id), cred_id=str(c.id), symbol=sym)
                    bot_ids_by_symbol[sym] = str(b.id)

                s.commit()
                print("[DB] Upserted bots:", bot_ids_by_symbol)
        except SQLAlchemyError as e:
            raise SystemExit(f"[DB] Error: {e}")
    else:
        # If no DB, fallback to deterministic IDs via env or simple placeholders
        # NOTE: For real trading, DB should be configured; this branch is just a convenience.
        for i, sym in enumerate(SYMBOLS, start=1):
            bot_ids_by_symbol[sym] = os.getenv(f"SEED_BOT_ID_{sym}", f"bot-{sym.lower()}-dev")
        print("[DB] Not configured; using in-memory bot IDs:", bot_ids_by_symbol)

    # 2) Redis bot config + index (for BOTH symbols)
    for sym in SYMBOLS:
        cfg = seed_bot_in_redis(
            bot_id=bot_ids_by_symbol[sym],
            user_id=user_id,
            sym=sym,
            status="active",
            side_mode="both",
            risk_per_trade=RISK_PCT,
            leverage=LEVERAGE,
            tp_ratio=TP_RATIO,
        )
        print(f"[Redis] BotConfig set for {sym}:", cfg)
        print(f"[Redis] BotConfig readback for {sym}:", read_bot_config(bot_ids_by_symbol[sym]))

    # 3) Optional: seed filters (so sizing/rounding can pass)
    for sym in SYMBOLS:
        maybe_seed_filters(sym)

    # 4) (Optional/Safe) Emit FAKE ARM into calc’s stream (disabled by default on real)
    if EMIT_FAKE:
        for sym in SYMBOLS:
            msg_id = emit_fake_arm(sym=sym, side="long", trigger="125000.0", stop="124800.0")
            print(f"[FAKE] Emitted ARM to Redis stream for {sym}: msg_id={msg_id}")
    else:
        print("[FAKE] Skipped emitting ARM (EMIT_FAKE!=1 and env is real).")

    # 5) Sanity: show Redis index membership
    for sym in SYMBOLS:
        members = [m.decode() if isinstance(m, (bytes, bytearray)) else str(m)
                   for m in r.smembers(idx_key_for_symbol(sym))]
        print(f"[index] {idx_key_for_symbol(sym)} members={members}")

    print("Done. If pollers/workers are up, signals will now dispatch to BOTH bots when calc emits.")

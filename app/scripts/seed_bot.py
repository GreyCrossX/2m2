# app/scripts/seed_bot.py
from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

# ──────────────────────────────────────────────────────────────────────────────
# Optional DB plumbing (seed a user/cred/bot if DB is configured)
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


# ──────────────────────────────────────────────────────────────────────────────
# DB seed helpers (optional)
# ──────────────────────────────────────────────────────────────────────────────
def upsert_user(session, email="tester@example.com"):
    u = session.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if u:
        return u
    u = User(
        email=email,
        hashed_pw="x",
        first_name="Test",
        last_name="User",
        is_active=True,
    )
    session.add(u)
    session.flush()
    return u

def upsert_cred(session, user, label="default", env="testnet"):
    c = session.execute(
        select(ApiCredential).where(
            (ApiCredential.user_id == user.id) &
            (ApiCredential.env == env) &
            (ApiCredential.label == label)
        )
    ).scalar_one_or_none()
    if c:
        return c

    k = os.getenv("TESTNET_API_KEY") or ""
    s = os.getenv("TESTNET_API_SECRET") or os.getenv("TESTNET_SECRET") or ""
    if not k or not s:
        raise SystemExit("TESTNET_API_KEY / TESTNET_API_SECRET missing in env")

    c = ApiCredential(
        user_id=user.id, env=env, label=label,
        api_key_encrypted=k,           # plaintext OK in dev
        api_secret_encrypted=s,
    )
    session.add(c)
    session.flush()
    return c

def upsert_bot(session, user, cred, symbol="BTCUSDT"):
    b = session.execute(
        select(Bot).where((Bot.user_id == user.id) & (Bot.symbol == symbol))
    ).scalar_one_or_none()
    if b:
        return b
    b = Bot(
        user_id=user.id,
        cred_id=cred.id,
        symbol=symbol,
        timeframe="2m",
        enabled=True,
        env="testnet",
        side_whitelist="both",
        side_mode="both",
        leverage=5,
        status="active",
        risk_per_trade=Decimal("0.005"),
        tp_ratio=Decimal("1.5"),
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
    sym: str = "BTCUSDT",
    status: str = "active",
    side_mode: str = "both",  # both | long_only | short_only
    risk_per_trade: Decimal = Decimal("0.005"),
    leverage: Decimal = Decimal("5"),
    tp_ratio: Decimal = Decimal("1.5"),
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
    # Tweak to actual UM futures filters if you have them handy
    "pricePrecision": 2,
    "quantityPrecision": 3,
    "tickSize": "0.10",
    "stepSize": "0.001",
    "minQty": "0.001",
    "minNotional": "5",
}

def maybe_seed_filters(sym: str = "BTCUSDT") -> None:
    if set_symbol_filters is None:
        print("[filters] Skipped (filters_source.set_symbol_filters not available).")
        return
    try:
        set_symbol_filters(sym.upper(), DEFAULT_FILTERS)
        print(f"[filters] Seeded filters for {sym}: {DEFAULT_FILTERS}")
    except Exception as e:
        print(f"[filters] Failed to seed filters (non-fatal): {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Emit a fake ARM (goes to calc’s stream where the poller is listening)
# ──────────────────────────────────────────────────────────────────────────────
def emit_fake_arm(
    *,
    sym: str = "BTCUSDT",
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
    sym = os.getenv("SEED_SYMBOL", "BTCUSDT").upper()
    side = os.getenv("SEED_SIDE", "long")
    trigger = os.getenv("SEED_TRIGGER", "65000.0")
    stop = os.getenv("SEED_STOP", "64850.0")

    # Defaults/tunables
    risk = Decimal(os.getenv("SEED_RISK", "0.005"))   # 0.5%
    lev  = Decimal(os.getenv("SEED_LEV", "5"))
    tp   = Decimal(os.getenv("SEED_TP", "1.5"))

    # 1) Optional DB seed
    user_id = "user-dev"
    bot_id  = "bot-dev"

    if USE_DB:
        with Session(engine) as s:
            u = upsert_user(s)
            c = upsert_cred(s, u)
            b = upsert_bot(s, u, c, symbol=sym)
            s.commit()
            user_id = str(u.id)
            bot_id  = str(b.id)
            print("DB seeded:", {"user": user_id, "cred": str(c.id), "bot": bot_id})
    else:
        user_id = os.getenv("SEED_USER_ID", user_id)
        bot_id  = os.getenv("SEED_BOT_ID", bot_id)
        print("DB not configured; using in-memory IDs:", {"user": user_id, "bot": bot_id})

    # 2) Redis bot config + index
    cfg = seed_bot_in_redis(
        bot_id=bot_id,
        user_id=user_id,
        sym=sym,
        status="active",
        side_mode="both",
        risk_per_trade=risk,
        leverage=lev,
        tp_ratio=tp,
    )
    print("Redis BotConfig set:", cfg)

    # Sanity readback
    print("Redis BotConfig readback:", read_bot_config(bot_id))

    # 3) Optional: seed filters (so sizing/rounding can pass)
    maybe_seed_filters(sym)

    # 4) Emit ARM into calc’s signal stream (poller → Celery → actions/exchange)
    msg_id = emit_fake_arm(sym=sym, side=side, trigger=trigger, stop=stop)
    print(f"Emitted ARM to Redis stream for {sym}: msg_id={msg_id}")
    print("If poller and workers are up, watch the logs in `poller` and `worker-signals`.")

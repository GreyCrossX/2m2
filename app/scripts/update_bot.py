from __future__ import annotations
from decimal import Decimal
import os

from app.services.ingestor.redis_io import r
from app.services.tasks.state import read_bot_config, write_bot_config  # already in your tree

BOT_ID   = "568b9682-41f8-435a-ab4a-919c82974fbe"
NEW_RISK = 0.05
NEW_LEV  = 5              # optional, e.g. "5"
NEW_TP   = 1.5             # optional, e.g. "1.5"

def _fmt(x):  # preserve string formatting the same way seeding does
    return format(x, "f") if isinstance(x, Decimal) else str(x)

cfg = read_bot_config(BOT_ID)
if not cfg:
    raise SystemExit(f"Bot {BOT_ID} not found in Redis")

# 1) pause
cfg["status"] = "paused"
write_bot_config(BOT_ID, cfg)

# 2) change risk (+ optional fields)
cfg["risk_per_trade"] = _fmt(NEW_RISK)
if NEW_LEV is not None:
    cfg["leverage"] = str(NEW_LEV)
if NEW_TP is not None:
    cfg["tp_ratio"] = str(NEW_TP)

write_bot_config(BOT_ID, cfg)

# 3) resume
cfg["status"] = "active"
write_bot_config(BOT_ID, cfg)

print("Updated bot:", BOT_ID, "→", {
    "risk_per_trade": cfg["risk_per_trade"],
    "leverage": cfg.get("leverage"),
    "tp_ratio": cfg.get("tp_ratio"),
    "status": cfg["status"],
})

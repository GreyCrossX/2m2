"""
Celery Tasks: ARM / DISARM

- on_arm_signal(payload)
    1) idempotency check
    2) build plan (domain)
    3) place entry
    4) optionally pre-place brackets
    5) mark idempotent

- on_disarm_signal(payload)
    - DISARM semantics via actions.disarm()

Notes:
- Payloads come from the signal_poller (strings for prices; we cast in domain).
- Keep tasks tiny and idempotent; rely on actions/state/exchange for side effects.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from celery import shared_task

from .domain import build_plan
from .actions import place_entry_and_track, place_brackets_and_track, disarm
from .state import read_bot_config
from .idem import already_processed, mark_processed

LOG = logging.getLogger("handlers")


def _require(fields: list[str], data: Dict[str, Any]) -> tuple[bool, str | None]:
    for f in fields:
        if f not in data or data[f] in (None, ""):
            return False, f
    return True, None


@shared_task(
    name="app.services.tasks.handlers.on_arm_signal",
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
)
def on_arm_signal(self, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Payload from poller example:
    {
      "bot_id": "...",
      "signal_id": "BTCUSDT:169...:long",
      "sym": "BTCUSDT",
      "side": "long",
      "trigger": "67890.5",
      "stop": "67650.0"
    }
    """
    ok, missing = _require(["bot_id", "signal_id", "sym", "side", "trigger", "stop"], payload)
    if not ok:
        return {"ok": False, "error": f"missing field: {missing}"}

    bot_id = payload["bot_id"]
    signal_id = payload["signal_id"]

    # Idempotency: skip if we've already processed this signal for this bot
    if already_processed(bot_id, signal_id):
        return {"ok": True, "skipped": "duplicate", "signal_id": signal_id}

    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found", "bot_id": bot_id}

    # Build domain plan (casts Decimal internally; reads balance/filters)
    arm = {
        "bot_id": bot_id,
        "signal_id": signal_id,
        "sym": payload["sym"],
        "side": payload["side"],       # "long" | "short"
        "trigger": payload["trigger"], # keep as str here
        "stop": payload["stop"],
        "tp_ratio": payload.get("tp_ratio"),  # optional override
    }
    plan = build_plan(arm=arm, bot_cfg=cfg, preplace_brackets=True)

    if not plan.get("ok"):
        return {"ok": False, "error": "plan_not_ok", "diagnostics": plan.get("diagnostics")}

    # Place entry
    res_entry = place_entry_and_track(bot_id, plan)
    if not res_entry.get("ok"):
        return {"ok": False, "error": res_entry.get("error", "entry_failed"), "diagnostics": plan.get("diagnostics")}

    # Optionally pre-place reduce-only brackets (recommended without fills WS)
    res_br = {}
    if plan.get("preplace_brackets"):
        res_br = place_brackets_and_track(bot_id, plan)
        if not res_br.get("ok"):
            # Return partial success; entry is placed already
            return {
                "ok": False,
                "error": res_br.get("error", "brackets_failed"),
                "entry_id": res_entry.get("entry_id"),
                "placed": res_br.get("placed", []),
            }

    # Success → mark idempotent
    mark_processed(bot_id, signal_id)

    out = {
        "ok": True,
        "signal_id": signal_id,
        "entry_id": res_entry.get("entry_id"),
    }
    if res_br:
        out["sl_tp_ids"] = res_br.get("sl_tp_ids")
    return out


@shared_task(
    name="app.services.tasks.handlers.on_disarm_signal",
    bind=True,
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
)
def on_disarm_signal(self, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Payload from poller example:
    {
      "bot_id": "...",
      "signal_id": "BTCUSDT:169...:long",
      "sym": "BTCUSDT",
      "side": "long"
    }
    """
    ok, missing = _require(["bot_id", "signal_id", "sym", "side"], payload)
    if not ok:
        return {"ok": False, "error": f"missing field: {missing}"}

    bot_id = payload["bot_id"]

    # DISARM semantics handled in actions.disarm()
    res = disarm(bot_id)
    return {"ok": True, "result": res, "signal_id": payload["signal_id"]}

from __future__ import annotations

import time
import logging
from typing import Any, Dict, List, Tuple

# Celery app (our module at /app/celery_app.py)
import celery_app

# Redis + key helpers
from app.services.ingestor.redis_io import r
from services.ingestor.keys import tag as tag_tf
from app.services.tasks.keys import stream_signal_by_tag, key_symbol_index
from app.services.tasks.state import bots_for_symbol, read_bot_config

LOG = logging.getLogger("poller")

GROUP = "calc_signals_v1"
CONSUMER = "sig_poller_1"

# Celery send() resolver
_CELERY = getattr(celery_app, "celery", None) or getattr(celery_app, "app", None)
if _CELERY is None:
    raise RuntimeError("Could not locate Celery app in celery_app.py (expected variable named 'celery' or 'app').")
SEND = _CELERY.send_task


def _d(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="replace")
    return "" if x is None else str(x)


def _f(fields: Dict[Any, Any], name: str) -> str:
    """Fetch field by str or bytes key and return as str ('' if missing)."""
    if name in fields:
        return _d(fields[name])
    bname = name.encode()
    if bname in fields:
        return _d(fields[bname])
    return ""


def ensure_group(stream: str) -> None:
    try:
        # Start at 0-0 so we can drain history if signals were added before poller started
        r.xgroup_create(stream, GROUP, id="0-0", mkstream=True)
        LOG.info("created consumer group '%s' at id=0-0 on stream '%s'", GROUP, stream)
    except Exception:
        # BUSYGROUP etc. → fine
        pass


def _eligible(cfg: Dict[str, Any], side: str) -> bool:
    if not cfg:
        return False
    if (cfg.get("status") or "").lower() != "active":
        return False
    mode = (cfg.get("side_mode") or "both").lower()
    if mode == "long_only" and side != "long":
        return False
    if mode == "short_only" and side != "short":
        return False
    return True


def _process_batch(sym: str, tf: str, stream: str, entries: List[Tuple[bytes, Dict[bytes, bytes]]]) -> int:
    delivered = 0

    # Debug dump: which bots are currently indexed?
    idx_key = key_symbol_index(sym)
    idx_members = r.smembers(idx_key)
    bots_idx = sorted(_d(x) for x in idx_members)
    LOG.info("[poller %s] index key=%s members=%s", sym, idx_key, bots_idx)

    for msg_id, fields in entries:
        try:
            typ     = _f(fields, "type")
            sym_f   = _f(fields, "sym")
            tf_f    = _f(fields, "tf")
            ind_ts  = _f(fields, "ind_ts") or "-"
            side    = _f(fields, "side") or _f(fields, "prev_side")
            trigger = _f(fields, "trigger") or _f(fields, "tp")   # accept legacy field names
            stop    = _f(fields, "stop")    or _f(fields, "sl")
        except Exception as e:
            LOG.error("[poller %s] decode error for %s: %s → ack", sym, _d(msg_id), e)
            r.xack(stream, GROUP, msg_id)
            continue

        LOG.info("[poller %s] got %s id=%s sym=%s tf=%s side=%s ind_ts=%s trigger=%s stop=%s",
                 sym, typ.upper(), _d(msg_id), sym_f, tf_f, side, ind_ts, trigger, stop)

        # Only our symbol/tf
        if sym_f.upper() != sym.upper() or tf_f != tf:
            LOG.info("[poller %s] skip message id=%s (sym/tf mismatch)", sym, _d(msg_id))
            r.xack(stream, GROUP, msg_id)
            continue

        signal_id = f"{sym_f}:{ind_ts}:{side}"
        fan = 0
        enqueue_errors = 0

        # Fan-out
        targets = list(bots_for_symbol(sym_f))
        LOG.info("[poller %s] candidate bots=%s", sym, targets)

        for bot_id in targets:
            cfg = read_bot_config(bot_id) or {}
            if not _eligible(cfg, side):
                LOG.info("[poller %s] bot=%s ineligible cfg=%s", sym, bot_id, cfg)
                continue

            try:
                if typ == "arm":
                    SEND(
                        "app.services.tasks.handlers.on_arm_signal",
                        args=[{
                            "bot_id": bot_id,
                            "signal_id": signal_id,
                            "sym": sym_f,
                            "side": side,
                            "trigger": trigger,
                            "stop": stop,
                        }],
                        queue="signals",
                    )
                    fan += 1
                elif typ == "disarm":
                    SEND(
                        "app.services.tasks.handlers.on_disarm_signal",
                        args=[{
                            "bot_id": bot_id,
                            "signal_id": signal_id,
                            "sym": sym_f,
                            "side": side,
                        }],
                        queue="signals",
                    )
                    fan += 1
                else:
                    LOG.info("[poller %s] unknown type=%s (acked anyway)", sym, typ)
            except Exception as e:
                enqueue_errors += 1
                LOG.error("[poller %s] enqueue %s failed id=%s bot=%s err=%s",
                          sym, (typ or "?").upper(), _d(msg_id), bot_id, e)

        if enqueue_errors == 0:
            LOG.info("[poller %s] %s %s → dispatched to %d bot(s)", sym, (typ or "?").upper(), _d(msg_id), fan)
            r.xack(stream, GROUP, msg_id)
            delivered += 1
        else:
            LOG.warning("[poller %s] leaving id=%s unacked due to %d enqueue error(s)", sym, _d(msg_id), enqueue_errors)
            time.sleep(0.1)

    return delivered


def run_for_symbol(sym: str, tf: str = "2m") -> None:
    tag = tag_tf(sym, tf)
    stream = stream_signal_by_tag(tag)
    ensure_group(stream)

    LOG.info("poller bind: sym=%s tf=%s tag=%s stream=%s group=%s consumer=%s",
             sym, tf, tag, stream, GROUP, CONSUMER)

    # 1) Drain history (if any)
    while True:
        boot = r.xreadgroup(GROUP, CONSUMER, {stream: "0-0"}, count=128, block=200)
        if not boot:
            break
        _, entries = boot[0]
        handled = _process_batch(sym, tf, stream, entries)
        if handled == 0:
            break

    # 2) Live tail
    while True:
        resp = r.xreadgroup(GROUP, CONSUMER, {stream: ">"}, count=64, block=2000)
        if not resp:
            LOG.debug("[poller %s] idle", sym)
            continue
        _, entries = resp[0]
        _process_batch(sym, tf, stream, entries)

"""
Celery Periodic Tasks: Reconcile

Two entry points:
- reconcile_bot_task(bot_id)
- reconcile_symbol_bots(symbol)  # iterate bots indexed on a symbol

Wire these with beat or any scheduler you prefer.
"""

from __future__ import annotations

from typing import Any, Dict, List

from celery import shared_task

from .reconcile import reconcile_bot
from .state import bots_for_symbol


@shared_task(
    name="app.services.tasks.periodic.reconcile_bot_task",
    bind=True,
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    queue="reconcile",
)
def reconcile_bot_task(self, bot_id: str) -> Dict[str, Any]:
    return reconcile_bot(bot_id)


@shared_task(
    name="app.services.tasks.periodic.reconcile_symbol_bots",
    bind=True,
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    queue="reconcile",
)
def reconcile_symbol_bots(self, symbol: str) -> Dict[str, Any]:
    bot_ids: List[str] = bots_for_symbol(symbol)
    results: List[Dict[str, Any]] = []
    for b in bot_ids:
        try:
            results.append(reconcile_bot(b))
        except Exception as e:
            results.append({"ok": False, "bot_id": b, "error": str(e)})
    return {"ok": True, "symbol": symbol, "count": len(bot_ids), "results": results}

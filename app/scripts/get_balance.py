# app/scripts/get_balance_for_bot.py
from __future__ import annotations
import os
import time
import hmac
import hashlib
import json
import logging
import urllib.parse
import urllib.request
import argparse
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
from uuid import UUID

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("wallets")

# ──────────────────────────────────────────────────────────────────────────────
# Context (per-bot credentials + endpoints)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Ctx:
    api_key: str
    api_secret: str
    spot_api: str
    um_api: str
    cm_api: str
    recv_window: int = 5000


def _mask(s: str) -> str:
    return "" if not s else f"{s[:6]}…{s[-4:]}"


def _ts_ms() -> int:
    return int(time.time() * 1000)


def _sign(ctx: Ctx, params: Dict[str, Any]) -> str:
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(ctx.api_secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return f"{q}&signature={sig}"


def _request(
    ctx: Ctx,
    method: str,
    base: str,
    path: str,
    *,
    signed=False,
    params=None,
    timeout=20,
) -> Tuple[Any, Dict[str, str] | None]:
    params = params or {}
    headers = {"X-MBX-APIKEY": ctx.api_key} if ctx.api_key else {}
    url = f"{base}{path}"

    if signed:
        params.setdefault("recvWindow", ctx.recv_window)
        params.setdefault("timestamp", _ts_ms())
        body = _sign(ctx, params)
    else:
        body = urllib.parse.urlencode(params, doseq=True)

    data = None
    if method.upper() == "GET":
        if body:
            url = f"{url}?{body}"
    else:
        data = body.encode()

    req = urllib.request.Request(url, data=data, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            hdrs = dict(resp.headers.items())
            try:
                return json.loads(txt), hdrs
            except Exception:
                return txt, hdrs
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} {e.reason}: {err}") from None
    except Exception as e:
        raise RuntimeError(str(e)) from None


def _has_error(payload: Any) -> Tuple[bool, str]:
    if isinstance(payload, dict) and "code" in payload and "msg" in payload:
        return True, f"{payload.get('code')}: {payload.get('msg')}"
    return False, ""


def _nz_assets(
    rows: List[Dict[str, Any]], fields=("free", "locked")
) -> List[Dict[str, Any]]:
    out = []
    for r in rows or []:
        try:
            if any(float(r.get(f, 0) or 0) != 0 for f in fields):
                out.append(r)
        except Exception:
            pass
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints (by env)
# ──────────────────────────────────────────────────────────────────────────────


def _urls_for_env(env_name: str) -> tuple[str, str, str]:
    """
    env_name: "prod" | "testnet" (case-insensitive)
    Override with env vars if you like.
    """
    is_testnet = str(env_name).lower().strip() == "testnet"

    spot = os.getenv(
        "BINANCE_SPOT_BASE_URL",
        "https://api.binance.com"
        if not is_testnet
        else "https://testnet.binance.vision",
    )
    um = os.getenv(
        "BINANCE_USDSF_BASE_URL",
        "https://fapi.binance.com"
        if not is_testnet
        else "https://testnet.binancefuture.com",
    )
    cm = os.getenv(
        "BINANCE_COINMF_BASE_URL",
        "https://dapi.binance.com"
        if not is_testnet
        else "https://testnet.binancefuture.com/dapi",
    )
    return spot, um, cm


# ──────────────────────────────────────────────────────────────────────────────
# Spot / Funding
# ──────────────────────────────────────────────────────────────────────────────


def spot_balances(ctx: Ctx) -> List[Dict[str, Any]]:
    try:
        data, _ = _request(
            ctx,
            "POST",
            ctx.spot_api,
            "/sapi/v3/asset/getUserAsset",
            signed=True,
            params={"needBtcValuation": "false"},
        )
        err, msg = _has_error(data)
        if err:
            log.warning(f"Spot balances error: {msg}")
            return []
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"Spot balances error: {e}")
        return []


def funding_balances(ctx: Ctx) -> List[Dict[str, Any]]:
    try:
        data, _ = _request(
            ctx,
            "POST",
            ctx.spot_api,
            "/sapi/v1/asset/get-funding-asset",
            signed=True,
            params={},
        )
        err, msg = _has_error(data)
        if err:
            log.warning(f"Funding wallet error: {msg}")
            return []
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"Funding wallet error: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# USDS-M Futures
# ──────────────────────────────────────────────────────────────────────────────


def um_futures_overview(ctx: Ctx) -> Dict[str, Any]:
    out = {"account": {}, "balances": []}
    try:
        acct, _ = _request(ctx, "GET", ctx.um_api, "/fapi/v2/account", signed=True)
        err, msg = _has_error(acct)
        if err:
            log.error(f"UM /fapi/v2/account error: {msg}")
        else:
            out["account"] = acct or {}
    except Exception as e:
        log.error(f"UM /fapi/v2/account error: {e}")

    try:
        bals, _ = _request(ctx, "GET", ctx.um_api, "/fapi/v2/balance", signed=True)
        err, msg = _has_error(bals)
        if err:
            log.error(f"UM /fapi/v2/balance error: {msg}")
        elif isinstance(bals, list):
            out["balances"] = bals
    except Exception as e:
        log.error(f"UM /fapi/v2/balance error: {e}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
# COIN-M Futures
# ──────────────────────────────────────────────────────────────────────────────


def cm_futures_overview(ctx: Ctx) -> Dict[str, Any]:
    out = {"account": {}, "balances": []}
    try:
        acct, _ = _request(ctx, "GET", ctx.cm_api, "/dapi/v1/account", signed=True)
        err, msg = _has_error(acct)
        if err:
            log.error(f"CM /dapi/v1/account error: {msg}")
        else:
            out["account"] = acct or {}
    except Exception as e:
        log.error(f"CM /dapi/v1/account error: {e}")

    try:
        bals, _ = _request(ctx, "GET", ctx.cm_api, "/dapi/v1/balance", signed=True)
        err, msg = _has_error(bals)
        if err:
            log.error(f"CM /dapi/v1/balance error: {msg}")
        elif isinstance(bals, list):
            out["balances"] = bals
    except Exception as e:
        log.error(f"CM /dapi/v1/balance error: {e}")
    return out


def _fmt_usd(x: Any) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)


# ──────────────────────────────────────────────────────────────────────────────
# Bot credential loader (from DB) with safe fallbacks
# ──────────────────────────────────────────────────────────────────────────────


async def _load_bot_credentials(bot_id: UUID) -> tuple[str, str, str]:
    """
    Returns (env, api_key, api_secret) for the bot.
    Uses your repository if available; otherwise raises.
    """
    # Preferred path: reuse your repository adapter
    try:
        from app.services.worker.infrastructure.postgres.repositories import (
            BotRepository,
        )  # your file earlier
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from app.db.session import (
            async_session_factory,
        )  # <- adjust if your project exposes it differently

        if not isinstance(async_session_factory, async_sessionmaker):
            raise RuntimeError("async_session_factory missing or invalid")

        repo = BotRepository(async_session_factory)
        bot_cfg, api_key, api_secret = await repo.get_bot_credentials(bot_id)
        return bot_cfg.env, api_key, api_secret
    except Exception as e:
        # Fallback: environment variables
        raise RuntimeError(
            "Could not load bot credentials from DB. "
            "Ensure repositories and session factory are importable, or provide API key/secret via env."
        ) from e


def _ctx_from_env_fallback() -> Ctx:
    """
    Back-compat: if DB path fails, read env like the old script did.
    """
    is_testnet = os.getenv("BINANCE_TESTNET", "false").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    api_key = os.getenv("BINANCE_USDSF_API_KEY", "")
    api_secret = os.getenv("BINANCE_USDSF_API_SECRET", "")
    if not api_key or not api_secret:
        raise SystemExit(
            "Missing API key/secret. Set BINANCE_USDSF_API_KEY / BINANCE_USDSF_API_SECRET or use --bot-id."
        )
    spot, um, cm = _urls_for_env("testnet" if is_testnet else "prod")
    return Ctx(
        api_key=api_key,
        api_secret=api_secret,
        spot_api=spot,
        um_api=um,
        cm_api=cm,
        recv_window=int(os.getenv("BINANCE_RECV_WINDOW", "5000")),
    )


async def build_ctx(bot_id: Optional[str], override_env: Optional[str]) -> Ctx:
    if not bot_id:
        return _ctx_from_env_fallback()

    env_name, api_key, api_secret = await _load_bot_credentials(UUID(bot_id))
    # Allow manual override if provided (rare)
    env_eff = (override_env or env_name).lower().strip()
    spot, um, cm = _urls_for_env(env_eff)
    recv_window = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
    return Ctx(
        api_key=api_key,
        api_secret=api_secret,
        spot_api=spot,
        um_api=um,
        cm_api=cm,
        recv_window=recv_window,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def _print_overview(ctx: Ctx, env_label: str) -> None:
    log.info("=== Binance Wallets Probe ===")
    log.info(f"Env         : {env_label}")
    log.info(f"Spot URL    : {ctx.spot_api}")
    log.info(f"UM URL      : {ctx.um_api}")
    log.info(f"CM URL      : {ctx.cm_api}")
    log.info(f"API key     : {_mask(ctx.api_key)}")
    log.info(f"Secret set  : {'<set>' if ctx.api_secret else '<missing>'}")


def run_probe(ctx: Ctx) -> None:
    # Spot
    spot = spot_balances(ctx)
    nz = _nz_assets(spot, ("free", "locked"))
    if nz:
        log.info("— Spot non-zero assets —")
        for a in nz:
            log.info(
                f"  {a.get('asset')}: free={a.get('free')} locked={a.get('locked')}"
            )
    else:
        log.warning("Spot wallet: EMPTY or unavailable")

    # Funding
    funding = funding_balances(ctx)
    shown = 0
    for a in funding or []:
        asset = a.get("asset")
        free = a.get("free") or a.get("fundingFree") or a.get("balance")
        locked = a.get("locked") or a.get("freeze") or "0"
        try:
            if float(free or 0) != 0 or float(locked or 0) != 0:
                if shown == 0:
                    log.info("— Funding non-zero assets —")
                shown += 1
                log.info(f"  {asset}: free={free} locked={locked}")
        except Exception:
            pass
    if shown == 0:
        log.warning("Funding wallet: no non-zero assets")

    # UM Futures
    um = um_futures_overview(ctx)
    acct = um.get("account") or {}
    if acct:
        twb = acct.get("totalWalletBalance", "")
        avail = acct.get("availableBalance", "")
        upnl = acct.get("totalUnrealizedProfit", "")
        log.info(
            f"UM Futures: totalWalletBalance={_fmt_usd(twb)} availableBalance={_fmt_usd(avail)} uPNL={_fmt_usd(upnl)}"
        )
    else:
        log.warning("UM Futures: account unavailable")

    umb = um.get("balances") or []
    nonzero_um = []
    for b in umb:
        try:
            bal = float(b.get("balance", 0) or 0)
            ab = float(b.get("availableBalance", 0) or 0)
            if bal != 0 or ab != 0:
                nonzero_um.append(
                    (b.get("asset"), b.get("balance"), b.get("availableBalance"))
                )
        except Exception:
            pass
    if nonzero_um:
        log.info("— UM Futures non-zero assets —")
        for asset, bal, ab in nonzero_um:
            log.info(f"  {asset}: balance={bal} available={ab}")
    else:
        log.warning("UM Futures: no non-zero per-asset balances")

    # CM Futures
    cm = cm_futures_overview(ctx)
    cacct = cm.get("account") or {}
    if cacct:
        twb = cacct.get("totalWalletBalance", "")
        log.info(f"COIN-M Futures: totalWalletBalance={_fmt_usd(twb)}")
    else:
        log.warning("COIN-M Futures: account unavailable")

    cmb = cm.get("balances") or []
    nonzero_cm = []
    for b in cmb:
        try:
            bal = float(b.get("balance", 0) or 0)
            wa = float(b.get("withdrawAvailable", 0) or 0)
            if bal != 0 or wa != 0:
                nonzero_cm.append(
                    (b.get("asset"), b.get("balance"), b.get("withdrawAvailable"))
                )
        except Exception:
            pass
    if nonzero_cm:
        log.info("— COIN-M Futures non-zero assets —")
        for asset, bal, wa in nonzero_cm:
            log.info(f"  {asset}: balance={bal} withdrawAvailable={wa}")
    else:
        log.warning("COIN-M Futures: no non-zero per-asset balances")


async def _amain(bot_id: Optional[str], override_env: Optional[str]) -> None:
    ctx = await build_ctx(bot_id, override_env)
    env_label = (
        override_env or ("TESTNET" if "testnet" in ctx.um_api else "PROD")
    ).upper()
    _print_overview(ctx, env_label)
    run_probe(ctx)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe balances using a bot's credentials."
    )
    parser.add_argument(
        "--bot-id", help="Bot UUID to fetch credentials/env from DB (preferred)."
    )
    parser.add_argument(
        "--env", help="Override env for endpoints: prod|testnet (optional)."
    )
    args = parser.parse_args()
    asyncio.run(_amain(args.bot_id, args.env))


if __name__ == "__main__":
    main()

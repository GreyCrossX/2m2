# app/scripts/get_balance.py
from __future__ import annotations
import os, time, hmac, hashlib, json, logging, urllib.parse, urllib.request
from typing import Any, Dict, List, Tuple

BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "false").lower() in ("1","true","yes","y")
API_KEY    = os.getenv("BINANCE_USDSF_API_KEY", "")
API_SECRET = os.getenv("BINANCE_USDSF_API_SECRET", "")

SPOT_API = os.getenv(
    "BINANCE_SPOT_BASE_URL",
    "https://api.binance.com" if not BINANCE_TESTNET else "https://testnet.binance.vision",
)
UM_API   = os.getenv(
    "BINANCE_USDSF_BASE_URL",
    "https://fapi.binance.com" if not BINANCE_TESTNET else "https://testnet.binancefuture.com",
)
CM_API   = os.getenv(
    "BINANCE_COINMF_BASE_URL",
    "https://dapi.binance.com" if not BINANCE_TESTNET else "https://testnet.binancefuture.com/dapi",
)

RECV_WINDOW = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("wallets")

def _mask(s: str) -> str:
    return "" if not s else f"{s[:6]}…{s[-4:]}"

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(params: Dict[str, Any]) -> str:
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return f"{q}&signature={sig}"

def _request(method: str, base: str, path: str, *, signed=False, params=None, timeout=20) -> Tuple[Any, Dict[str,str] | None]:
    params = params or {}
    headers = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}
    url = f"{base}{path}"

    if signed:
        params.setdefault("recvWindow", RECV_WINDOW)
        params.setdefault("timestamp", _ts_ms())
        body = _sign(params)
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
                # endpoints sometimes return bare arrays/strings
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

def _nz_assets(rows: List[Dict[str, Any]], fields=("free","locked")) -> List[Dict[str, Any]]:
    out = []
    for r in rows or []:
        try:
            if any(float(r.get(f, 0) or 0) != 0 for f in fields):
                out.append(r)
        except Exception:
            pass
    return out

# ─────────── Spot / Funding ───────────
def spot_balances() -> List[Dict[str, Any]]:
    # MUST be POST (signed)
    try:
        data, _ = _request("POST", SPOT_API, "/sapi/v3/asset/getUserAsset", signed=True,
                           params={"needBtcValuation":"false"})
        err, msg = _has_error(data)
        if err:
            log.warning(f"Spot balances error: {msg}")
            return []
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"Spot balances error: {e}")
        return []

def funding_balances() -> List[Dict[str, Any]]:
    try:
        data, _ = _request("POST", SPOT_API, "/sapi/v1/asset/get-funding-asset", signed=True, params={})
        err, msg = _has_error(data)
        if err:
            log.warning(f"Funding wallet error: {msg}")
            return []
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"Funding wallet error: {e}")
        return []

# ─────────── USDS-M Futures ───────────
def um_futures_overview() -> Dict[str, Any]:
    out = {"account": {}, "balances": []}
    try:
        acct, _ = _request("GET", UM_API, "/fapi/v2/account", signed=True)
        err, msg = _has_error(acct)
        if err:
            log.error(f"UM /fapi/v2/account error: {msg}")
        else:
            out["account"] = acct or {}
    except Exception as e:
        log.error(f"UM /fapi/v2/account error: {e}")

    try:
        bals, _ = _request("GET", UM_API, "/fapi/v2/balance", signed=True)
        err, msg = _has_error(bals)
        if err:
            log.error(f"UM /fapi/v2/balance error: {msg}")
        elif isinstance(bals, list):
            out["balances"] = bals
    except Exception as e:
        log.error(f"UM /fapi/v2/balance error: {e}")
    return out

# ─────────── COIN-M Futures ───────────
def cm_futures_overview() -> Dict[str, Any]:
    out = {"account": {}, "balances": []}
    try:
        acct, _ = _request("GET", CM_API, "/dapi/v1/account", signed=True)
        err, msg = _has_error(acct)
        if err:
            log.error(f"CM /dapi/v1/account error: {msg}")
        else:
            out["account"] = acct or {}
    except Exception as e:
        log.error(f"CM /dapi/v1/account error: {e}")

    try:
        bals, _ = _request("GET", CM_API, "/dapi/v1/balance", signed=True)
        err, msg = _has_error(bals)
        if err:
            log.error(f"CM /dapi/v1/balance error: {msg}")
        elif isinstance(bals, list):
            out["balances"] = bals
    except Exception as e:
        log.error(f"CM /dapi/v1/balance error: {e}")
    return out

def _fmt_usd(x: Any) -> str:
    try: return f"{float(x):,.2f}"
    except: return str(x)

def main():
    env = "TESTNET" if BINANCE_TESTNET else "PROD"
    log.info("=== Binance Wallets Probe ===")
    log.info(f"Env         : {env}")
    log.info(f"Spot URL    : {SPOT_API}")
    log.info(f"UM URL      : {UM_API}")
    log.info(f"CM URL      : {CM_API}")
    log.info(f"API key     : {_mask(API_KEY)}")
    log.info(f"Secret set  : {'<set>' if API_SECRET else '<missing>'}")

    # Spot
    spot = spot_balances()
    nz = _nz_assets(spot, ("free","locked"))
    if nz:
        log.info("— Spot non-zero assets —")
        for a in nz:
            log.info(f"  {a.get('asset')}: free={a.get('free')} locked]={a.get('locked')}")
    else:
        log.warning("Spot wallet: EMPTY or unavailable")

    # Funding
    funding = funding_balances()
    shown = 0
    for a in (funding or []):
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
    um = um_futures_overview()
    acct = um.get("account") or {}
    if acct:
        twb = acct.get("totalWalletBalance", "")
        avail = acct.get("availableBalance", "")
        upnl = acct.get("totalUnrealizedProfit", "")
        log.info(f"UM Futures: totalWalletBalance={_fmt_usd(twb)} availableBalance={_fmt_usd(avail)} uPNL={_fmt_usd(upnl)}")
    else:
        log.warning("UM Futures: account unavailable")

    umb = um.get("balances") or []
    nonzero_um = []
    for b in umb:
        try:
            bal = float(b.get("balance", 0) or 0)
            ab  = float(b.get("availableBalance", 0) or 0)
            if bal != 0 or ab != 0:
                nonzero_um.append((b.get("asset"), b.get("balance"), b.get("availableBalance")))
        except Exception:
            pass
    if nonzero_um:
        log.info("— UM Futures non-zero assets —")
        for asset, bal, ab in nonzero_um:
            log.info(f"  {asset}: balance={bal} available={ab}")
    else:
        log.warning("UM Futures: no non-zero per-asset balances")

    # CM Futures
    cm = cm_futures_overview()
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
            wa  = float(b.get("withdrawAvailable", 0) or 0)
            if bal != 0 or wa != 0:
                nonzero_cm.append((b.get("asset"), b.get("balance"), b.get("withdrawAvailable")))
        except Exception:
            pass
    if nonzero_cm:
        log.info("— COIN-M Futures non-zero assets —")
        for asset, bal, wa in nonzero_cm:
            log.info(f"  {asset}: balance={bal} withdrawAvailable={wa}")
    else:
        log.warning("COIN-M Futures: no non-zero per-asset balances")

if __name__ == "__main__":
    if not API_KEY or not API_SECRET:
        log.error("Missing API key/secret envs. Set BINANCE_USDSF_API_KEY and BINANCE_USDSF_API_SECRET.")
        raise SystemExit(2)
    main()

#!/usr/bin/env python3
"""
Robust ETHUSDT diagnostic: handles missing fields, str/bytes keys,
and uses proper stream IDs for read/range ops.
"""

from services.ingestor.redis_io import r as redis_client
from typing import Any, Dict, Optional, Tuple

SYMBOL = "ETHUSDT"
MARKET = f"stream:market|{{{SYMBOL}:2m}}"
IND = f"stream:ind|{{{SYMBOL}:2m}}"
SIG = f"stream:signal|{{{SYMBOL}:2m}}"
SNAP = f"snap:ind|{{{SYMBOL}:2m}}"

# ---------- helpers ----------


def get_field(fields: Dict[Any, Any], name: str) -> Optional[Any]:
    """Return fields[name] whether keys are bytes or str; else None."""
    if name in fields:
        return fields[name]
    bname = name.encode()
    if bname in fields:
        return fields[bname]
    return None


def to_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def decode_str(x: Any) -> str:
    if isinstance(x, bytes):
        try:
            return x.decode()
        except Exception:
            return repr(x)
    return str(x)


def get_first_last(
    stream: str,
) -> Tuple[Optional[Tuple[str, Dict]], Optional[Tuple[str, Dict]]]:
    first = redis_client.xrange(stream, count=1)
    last = redis_client.xrevrange(stream, count=1)
    first_entry = first[0] if first else None
    last_entry = last[0] if last else None
    return first_entry, last_entry


def id_ms(stream_id: str) -> Optional[int]:
    # Stream IDs look like "1696375200000-0". Convert to ms (the first part).
    try:
        return int(stream_id.split("-")[0])
    except Exception:
        return None


def ts_from_entry(entry: Tuple[str, Dict]) -> Tuple[Optional[int], Optional[int], Dict]:
    """Return (ts_field_ms, id_ms_value, fields) from an entry."""
    sid, f = entry
    ts_val = get_field(f, "ts")
    ts_ms = to_int(ts_val)
    return ts_ms, id_ms(sid), f


def show_recent(stream: str, n: int = 3, label: str = ""):
    recent = redis_client.xrevrange(stream, count=n)
    if not recent:
        print("   (no recent entries)")
        return
    for sid, fields in recent:
        ts_ms = to_int(get_field(fields, "ts"))
        close = to_float(get_field(fields, "close"))
        regime_val = get_field(fields, "regime")
        regime = decode_str(regime_val) if regime_val is not None else "N/A"
        print(f"     {sid}: ts={ts_ms} close={close} regime={regime}")


def sample_missing_field(stream: str, field: str, sample: int = 5):
    """Print up to N entries that are missing a given field."""
    entries = redis_client.xrevrange(stream, count=50)  # small scan window
    bad = []
    for sid, f in entries:
        if get_field(f, field) is None:
            bad.append((sid, f))
        if len(bad) >= sample:
            break
    if bad:
        print(f"\n   ⚠ Found {len(bad)} recent entries missing '{field}':")
        for sid, f in bad:
            keys = [decode_str(k) for k in f.keys()]
            print(f"     {sid}: keys={keys}")
    else:
        print(f"\n   All recent entries include '{field}' (within last 50).")


def check_ethusdt():
    print("=" * 70)
    print("ETHUSDT Diagnostic")
    print("=" * 70)

    # 1) Market stream
    print("\n1. Market Stream (Input)")
    print("-" * 70)
    market_len = redis_client.xlen(MARKET)
    print(f"   Length: {market_len}")
    if market_len > 0:
        first_entry, last_entry = get_first_last(MARKET)
        if first_entry and last_entry:
            f_ts, f_id_ms, _ = ts_from_entry(first_entry)
            l_ts, l_id_ms, _ = ts_from_entry(last_entry)

            # Prefer field ts; fall back to ID-based ms if missing
            first_ms = f_ts if f_ts is not None else f_id_ms
            last_ms = l_ts if l_ts is not None else l_id_ms

            print(
                f"   First message ID: {first_entry[0]}  ts={f_ts}  (id_ms={f_id_ms})"
            )
            print(f"   Last  message ID: {last_entry[0]}  ts={l_ts}  (id_ms={l_id_ms})")

            if first_ms is not None and last_ms is not None:
                span_min = (last_ms - first_ms) / 60000.0
                print(f"   Time span: {span_min:.1f} minutes")
            else:
                print("   Time span: N/A (missing timestamps and/or ID parse)")

            # If ts missing, show sample to debug schema drift
            if f_ts is None or l_ts is None:
                sample_missing_field(MARKET, "ts")

    # 2) Indicator stream
    print("\n2. Indicator Stream (Output)")
    print("-" * 70)
    ind_len = redis_client.xlen(IND)
    print(f"   Length: {ind_len}")
    if ind_len > 0:
        first_entry, last_entry = get_first_last(IND)
        if first_entry and last_entry:
            f_ts, f_id_ms, _ = ts_from_entry(first_entry)
            l_ts, l_id_ms, _ = ts_from_entry(last_entry)

            print(
                f"   First message ID: {first_entry[0]}  ts={f_ts}  (id_ms={f_id_ms})"
            )
            print(f"   Last  message ID: {last_entry[0]}  ts={l_ts}  (id_ms={l_id_ms})")

            if (f_ts or f_id_ms) and (l_ts or l_id_ms):
                first_ms = f_ts if f_ts is not None else f_id_ms
                last_ms = l_ts if l_ts is not None else l_id_ms
                if first_ms is not None and last_ms is not None:
                    span_min = (last_ms - first_ms) / 60000.0
                    print(f"   Time span: {span_min:.1f} minutes")

            print("\n   Last 3 processed messages:")
            show_recent(IND, n=3)

            if l_ts is None:
                sample_missing_field(IND, "ts")

    # 3) Snapshot
    print("\n3. Snapshot Hash")
    print("-" * 70)
    if redis_client.exists(SNAP):
        snap = redis_client.hgetall(SNAP)

        def get_snap(k: str) -> str:
            v = snap.get(k) if k in snap else snap.get(k.encode(), None)
            return decode_str(v) if v is not None else "N/A"

        print("   Exists: Yes")
        print(f"   Current ts: {get_snap('ts')}")
        print(f"   Close:      {get_snap('close')}")
        print(f"   Regime:     {get_snap('regime')}")
        print(f"   MA20:       {get_snap('ma20')}")
        print(f"   MA200:      {get_snap('ma200')}")
    else:
        print("   Exists: No")

    # 4) Processing gap
    print("\n4. Processing Gap Analysis")
    print("-" * 70)
    if market_len > 0 and ind_len > 0:
        m_last = redis_client.xrevrange(MARKET, count=1)[0]
        i_last = redis_client.xrevrange(IND, count=1)[0]

        m_last_ts, m_last_id_ms, _ = ts_from_entry(m_last)
        i_last_ts, i_last_id_ms, _ = ts_from_entry(i_last)

        # Compare by ts field if available, else by stream ID ms
        m_ms = m_last_ts if m_last_ts is not None else m_last_id_ms
        i_ms = i_last_ts if i_last_ts is not None else i_last_id_ms

        if m_ms is not None and i_ms is not None:
            gap_ms = m_ms - i_ms
            missing = max(market_len - ind_len, 0)
            print(
                f"   Market last:    id={m_last[0]} ts={m_last_ts} (id_ms={m_last_id_ms})"
            )
            print(
                f"   Indicator last: id={i_last[0]} ts={i_last_ts} (id_ms={i_last_id_ms})"
            )
            print(f"   Gap: {gap_ms}ms ({gap_ms / 60000.0:.1f} minutes)")
            print(f"   Missing messages (len diff): {missing}")

            if gap_ms > 0:
                print(f"\n   ⚠️  ETHUSDT is behind by {gap_ms / 60000.0:.1f} minutes")
                print(f"   Need to process about {missing} messages")

                # Use the last **stream ID** from IND to read subsequent MARKET entries
                _start_id = m_last[0]  # kept for reference; not used directly
                # Better: find entries AFTER the last indicator's **stream ID**:
                after_id = i_last[0]
                print(
                    f"\n   Checking for unprocessed market entries after indicator ID {after_id}..."
                )
                # XRANGE (min=after_id, max='+') with exclusive min uses (after_id), but Python client takes strings; mimic exclusive with id +1 micro-step:
                unprocessed = redis_client.xrange(
                    MARKET, min=after_id, max="+", count=5
                )
                if unprocessed:
                    print("   Found unprocessed messages:")
                    for sid, f in unprocessed:
                        uts = to_int(get_field(f, "ts"))
                        uclose = to_float(get_field(f, "close"))
                        print(f"     {sid}: ts={uts} close={uclose}")
                else:
                    print(
                        "   ⚠️  No subsequent market messages found after indicator’s last ID."
                    )
        else:
            print("   Gap: N/A (could not parse timestamps from fields nor IDs)")

    # 5) Possible issues
    print("\n5. Possible Issues")
    print("-" * 70)
    if ind_len == 0:
        print("   ❌ No indicators written at all - processor never started")
        print("   → Check if ETHUSDT task is running")
        print("   → Check logs for exceptions during startup")
    elif market_len - ind_len == 1:
        print("   ⚠️  Only 1 message behind - might just be timing")
        print("   → Wait 2 minutes for next candle")
        print("   → Check if it catches up")
    elif market_len - ind_len > 1:
        print(
            f"   ❌ {market_len - ind_len} messages behind - processor likely stopped"
        )
        print("   → Check if task crashed")
        print("   → Look for exceptions in logs")
        print("   → Verify resume logic is working")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    check_ethusdt()

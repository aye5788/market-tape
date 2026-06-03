"""
tape/backfill.py — layered, provenance-tagged 1m gap backfill.

The collector records Kraken WS `ohlc` bars, and that feed only PUBLISHES a bar
on activity: a minute with zero trades produces no bar, so the raw tape has a
hole. That hole is benign (nothing happened) but it breaks the contiguity a
backtest assumes. This module fills such holes WITHOUT papering over the holes
that mean something actually went wrong.

Why blanket backfill would be wrong, and what makes this safe:

    A missing 1m bar is one of two very different things —
      (1) the exchange was SILENT  -> the faithful value is a flat carry-forward
          bar (O=H=L=C=prior close, vol=0). Filling it is RECONSTRUCTION.
      (2) WE were BLIND (disconnected / a capture bug) -> real trades may have
          happened that we missed. Filling that with a flat bar is FABRICATION.

We separate them with two arbiters and never silently conflate them:
  * local connectivity proof — were we connected, per our OWN events log, for
    the whole missing minute? (classifies, and sets how alarming a loss is)
  * Kraken public REST OHLC   — the exchange's own record of that minute.
    vol==0 confirms (1) benign silence; vol>0 proves (2) we lost real data.

Every filled bar is tagged in ohlc_1m.source (schema.SRC_BACKFILL /
SRC_RECOVERED) so it is never indistinguishable from a live-observed bar, and
(2)-class fills ESCALATE rather than clear the flag. Bounded: a gap longer than
max_gap_bars — or any gap while auto-actions are frozen (a sustained-degradation
window) — is left flagged for a human, not filled.

Pure: stdlib + requests + sqlite3, no MAGI imports, writes only ohlc_1m.

CLI: python -m tape.backfill        (one-shot: fill resolvable gaps in the
                                      recent settled window, print a summary)
"""
import logging
import sqlite3
import time

import requests

from tape import config
from tape import schema

log = logging.getLogger("tape.backfill")

_MIN_MS = 60_000


def fetch_rest_1m(since_sec, *, pair=None, url=None, timeout=None):
    """Kraken public REST OHLC at the 1m interval. Returns
    {ts_begin_ms: (open, high, low, close, vwap, volume, count)} for every
    SETTLED bar at-or-after since_sec, or None on any failure (so the caller
    leaves the gap flagged rather than guessing). The still-forming bar Kraken
    marks via the 'last' cursor is excluded."""
    pair = pair or config.BACKFILL_REST_PAIR
    url = url or config.BACKFILL_REST_URL
    timeout = timeout or config.BACKFILL_REST_TIMEOUT
    try:
        r = requests.get(
            url, params={"pair": pair, "interval": 1, "since": int(since_sec)},
            timeout=timeout)
        d = r.json()
    except Exception as e:
        log.warning("REST OHLC fetch failed: %r", e)
        return None
    if not isinstance(d, dict) or d.get("error"):
        log.warning("REST OHLC error: %s", (d or {}).get("error"))
        return None
    result = d.get("result") or {}
    try:
        last = int(result.get("last")) if result.get("last") is not None else None
    except (TypeError, ValueError):
        last = None
    out = {}
    for key, bars in result.items():
        if key == "last" or not isinstance(bars, list):
            continue
        for b in bars:
            try:
                t = int(b[0])
                if last is not None and t >= last:
                    continue  # the in-progress bar — not settled
                out[t * 1000] = (
                    float(b[1]), float(b[2]), float(b[3]), float(b[4]),
                    float(b[5]), float(b[6]), int(b[7]),
                )
            except (TypeError, ValueError, IndexError):
                continue
    return out


def was_connected_through(conn, start_ms, end_ms):
    """Per our OWN events log, was the WS connected for the whole window
    [start_ms, end_ms]? True iff the latest ws_state event at-or-before start_ms
    says 'connected' AND no disconnect was logged within the window. Used to set
    how alarming a recovered (vol>0) bar is: missing data while DISCONNECTED is
    expected; missing it while CONNECTED implies a capture bug worth a loud flag."""
    pre = conn.execute(
        "SELECT message FROM events WHERE category='ws_state' AND ts<=? "
        "ORDER BY ts DESC LIMIT 1", (start_ms,)).fetchone()
    if pre is None or not str(pre[0]).startswith("ws connected"):
        return False
    dropped = conn.execute(
        "SELECT COUNT(*) FROM events WHERE category='ws_state' AND ts>? AND ts<=? "
        "AND message LIKE 'ws disconnected%'", (start_ms, end_ms)).fetchone()[0]
    return dropped == 0


def backfill_gaps(conn, gaps, *, max_gap_bars=None):
    """Resolve each detected gap via the layered gate. `gaps` is
    [(gap_start_ms, missing)] as returned by the collector's _detect_gaps.

    Returns a list of per-gap result dicts:
      {gap_start, missing, filled_silent, recovered, unresolved, connected,
       escalate, detail}
    Inserts (OR IGNORE) provenance-tagged bars and commits once. NEVER raises
    through to the caller — a gap that errors is reported with unresolved>0."""
    max_gap_bars = config.BACKFILL_MAX_GAP_BARS if max_gap_bars is None else max_gap_bars
    ins = schema.INSERTS["ohlc_1m_provenanced"]
    results = []
    for gap_start, missing in gaps:
        res = {"gap_start": gap_start, "missing": missing, "filled_silent": 0,
               "recovered": 0, "unresolved": 0, "connected": None,
               "escalate": False, "detail": ""}
        try:
            # bound: a long gap is more likely a real outage than silence —
            # leave it for a human rather than auto-filling a big hole.
            if missing > max_gap_bars:
                res["unresolved"] = missing
                res["escalate"] = True
                res["detail"] = (f"{missing} bars exceeds max {max_gap_bars} — "
                                 f"left flagged for review, not auto-filled")
                results.append(res)
                continue

            gap_end = gap_start + missing * _MIN_MS
            res["connected"] = was_connected_through(conn, gap_start, gap_end)
            rest = fetch_rest_1m(gap_start // 1000 - 60)
            if rest is None:
                res["unresolved"] = missing
                res["detail"] = "REST unavailable — left flagged (no confident fill)"
                results.append(res)
                continue

            for k in range(missing):
                m = gap_start + k * _MIN_MS
                bar = rest.get(m)
                if bar is None:
                    res["unresolved"] += 1  # REST has no bar either — stays flagged
                    continue
                o, h, l, c, vwap, vol, cnt = bar
                if vol == 0 and cnt == 0:
                    # (1) confirmed silent minute. vwap is undefined with no
                    # trades, so use the standing close (the only real price).
                    conn.execute(ins, (m, c, c, c, c, 0.0, c, 0, schema.SRC_BACKFILL))
                    res["filled_silent"] += 1
                else:
                    # (2) real volume in a minute we have no bar for: we were
                    # blind. Recover the bar but tag it and escalate — do NOT
                    # let it pass as benign. (The underlying trade tape for this
                    # minute is NOT recoverable from OHLC; only the bar is.)
                    conn.execute(ins, (m, o, h, l, c, vol, vwap, cnt, schema.SRC_RECOVERED))
                    res["recovered"] += 1

            parts = []
            if res["filled_silent"]:
                parts.append(f"{res['filled_silent']} silent min backfilled (flat)")
            if res["recovered"]:
                res["escalate"] = True
                where = ("while CONNECTED (capture bug?)" if res["connected"]
                         else "while disconnected")
                parts.append(f"{res['recovered']} min with REAL volume recovered {where}")
            if res["unresolved"]:
                parts.append(f"{res['unresolved']} min unresolved (REST had no bar)")
            res["detail"] = "; ".join(parts) or "nothing to fill"
        except Exception as e:
            res["unresolved"] = missing
            res["detail"] = f"backfill error: {e!r}"
            log.warning("backfill error for gap %s: %r", gap_start, e)
        results.append(res)

    try:
        conn.commit()
    except Exception as e:
        log.warning("backfill commit failed: %r", e)
    return results


def _detect_gaps(conn, now_ms):
    """Standalone gap finder for the CLI, mirroring the collector's settled
    window so a one-shot run fills exactly what the live loop would."""
    settle = now_ms - config.GAP_SETTLE_SECS * 1000
    win = now_ms - config.GAP_LOOKBACK_HOURS * 3_600_000
    rows = [r[0] for r in conn.execute(
        "SELECT ts_begin FROM ohlc_1m WHERE ts_begin>=? AND ts_begin<=? ORDER BY ts_begin",
        (win, settle))]
    gaps = []
    for i in range(1, len(rows)):
        missing = (rows[i] - rows[i - 1]) // _MIN_MS - 1
        if missing > 0:
            gaps.append((rows[i - 1] + _MIN_MS, int(missing)))
    return gaps


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    schema.init_db(config.DB_PATH)  # ensure the source column exists
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        now = int(time.time() * 1000)
        gaps = _detect_gaps(conn, now)
        if not gaps:
            print("no resolvable gaps in the settled window")
            return
        print(f"detected {len(gaps)} gap(s); attempting layered backfill...")
        for r in backfill_gaps(conn, gaps):
            t0 = time.strftime("%Y-%m-%d %H:%M", time.gmtime(r["gap_start"] / 1000))
            print(f"  {t0} UTC  missing={r['missing']:>3}  "
                  f"silent={r['filled_silent']} recovered={r['recovered']} "
                  f"unresolved={r['unresolved']}  escalate={r['escalate']}")
            print(f"      {r['detail']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

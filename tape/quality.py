"""
tape/quality.py — data-quality report for the market tape.

Read-only. Answers one question: is what we're ingesting trustworthy, or is
it garbage that makes the tape useless as a reality anchor? Computes a small
set of checks over a trailing window from the tables the collector already
writes — no new deps, no writes, no MAGI imports.

Thresholds are anchored to SANE EXOGENOUS BOUNDS — a 1m bar closes every 60s;
XRP does not move >5% in one minute absent a bad print; a COMPLETED rollup
bucket must reconcile to its 1m bars exactly — NOT fitted to the data. They
flag real corruption, not normal market behaviour.

Key subtlety: rollup reconciliation only looks at COMPLETED buckets
(ts_begin + interval <= now). The in-progress bucket legitimately lags the
live 1m bars, so reconciling it would false-positive every poll.

CLI: python -m tape.quality   (one-shot report to stdout)
"""
import sqlite3
import time

from tape import config

# --- thresholds (anchored to exogenous bounds, tunable; NOT fitted to data) ---
WINDOW_HOURS     = 24
BAR_WARN_SEC     = 135     # a 1m bar closes every 60s; some delivery slack is fine
BAR_STALE_SEC    = 195     # >~3 missed closes = the bar feed has stalled
PRICE_JUMP_PCT   = 0.05    # |Δ| between consecutive 1m closes >5% ≈ bad print / flash
COVERAGE_RED     = 0.95    # <95% of expected 1m bars present in the captured span
COVERAGE_YELLOW  = 0.995   # 95–99.5% = degraded
ROLLUP_DRIFT_REL = 1e-4    # settled 1h bucket vol vs sum of its 1m vol; ~0 expected
# A just-closed bucket isn't reconcilable until the periodic rollup loop
# (config.ROLLUP_EVERY_SECS) has re-run with the bucket's FINAL 1m bars — until
# then the rollup row legitimately lags the live minute. Only reconcile buckets
# settled at least two rollup cycles ago, so the check never false-positives on
# that lag (which is exactly what tripped it the first time).
ROLLUP_SETTLE_SEC = getattr(config, "ROLLUP_EVERY_SECS", 300) * 2
BEACON_STALE_SEC = 30      # beacon cadence is 5s; >30s = collector not reporting

_RANK = {"green": 0, "gray": 0, "yellow": 1, "red": 2}


def _worst(checks):
    w = "green"
    for ch in checks:
        if _RANK.get(ch["status"], 0) > _RANK[w]:
            w = ch["status"]
    return w


def report(conn, now_ms=None, window_hours=WINDOW_HOURS):
    """Run the quality checks against an OPEN sqlite connection (so the
    dashboard reuses its own per-poll connection). Returns
    {verdict, window_hours, checks:[{key,label,detail,status,value}]}."""
    now = now_ms or int(time.time() * 1000)
    win = now - window_hours * 3_600_000
    checks = []

    def scalar(sql, args=()):
        r = conn.execute(sql, args).fetchone()
        return r[0] if r and r[0] is not None else None

    # ---- 1m coverage over the captured span (measures internal gaps) ----
    first = scalar("SELECT MIN(ts_begin) FROM ohlc_1m WHERE ts_begin>=?", (win,))
    last = scalar("SELECT MAX(ts_begin) FROM ohlc_1m WHERE ts_begin>=?", (win,))
    present = scalar("SELECT COUNT(*) FROM ohlc_1m WHERE ts_begin>=?", (win,)) or 0
    if first and last and last >= first:
        expected = (last - first) // 60_000 + 1
        missing = max(0, expected - present)
        cov = present / expected if expected else 1.0
        st = ("red" if cov < COVERAGE_RED
              else "yellow" if cov < COVERAGE_YELLOW else "green")
        checks.append({"key": "coverage", "label": "1m coverage",
                       "detail": f"{present}/{expected} bars · {cov*100:.2f}% · {missing} missing",
                       "status": st, "value": round(cov * 100, 2)})
    else:
        checks.append({"key": "coverage", "label": "1m coverage",
                       "detail": "no 1m bars in window yet", "status": "gray", "value": None})

    # ---- backfill provenance (transparency: silent fills are benign; a
    #      REST-recovered bar means we were BLIND for that minute -> yellow) ----
    has_source = any(r[1] == "source" for r in conn.execute("PRAGMA table_info(ohlc_1m)"))
    if has_source:
        silent = scalar("SELECT COUNT(*) FROM ohlc_1m WHERE ts_begin>=? AND source=1", (win,)) or 0
        recov = scalar("SELECT COUNT(*) FROM ohlc_1m WHERE ts_begin>=? AND source=2", (win,)) or 0
        if silent == 0 and recov == 0:
            checks.append({"key": "backfill", "label": "backfilled bars",
                           "detail": "0 · all bars live-observed", "status": "green", "value": 0})
        else:
            checks.append({"key": "backfill", "label": "backfilled bars",
                           "detail": f"{silent} silent-fill · {recov} REST-recovered",
                           "status": "yellow" if recov else "green", "value": silent + recov})

    # ---- largest contiguous gap (consecutive missing minutes) ----
    gap = scalar("""SELECT MAX(g) FROM (
        SELECT (ts_begin - LAG(ts_begin) OVER (ORDER BY ts_begin))/60000 - 1 AS g
        FROM ohlc_1m WHERE ts_begin>=?)""", (win,)) or 0
    gap = int(gap)
    st = "green" if gap == 0 else "yellow" if gap <= 2 else "red"
    checks.append({"key": "gaps", "label": "largest gap",
                   "detail": f"{gap} consecutive min missing", "status": st, "value": gap})

    # ---- last 1m bar freshness ----
    age = None if last is None else round((now - last) / 1000.0, 1)
    st = ("gray" if age is None else "green" if age <= BAR_WARN_SEC
          else "yellow" if age <= BAR_STALE_SEC else "red")
    checks.append({"key": "bar_fresh", "label": "last 1m bar",
                   "detail": ("—" if age is None else f"{age:.0f}s ago"),
                   "status": st, "value": age})

    # ---- 1m bar internal validity (low<=o,c<=high; positive; vol>=0) ----
    bad = scalar("""SELECT COUNT(*) FROM ohlc_1m WHERE ts_begin>=? AND (
        high<low OR high<open OR high<close OR low>open OR low>close
        OR open<=0 OR high<=0 OR low<=0 OR close<=0 OR volume<0)""", (win,)) or 0
    checks.append({"key": "bar_valid", "label": "bar validity",
                   "detail": f"{bad} malformed", "status": "green" if bad == 0 else "red",
                   "value": bad})

    # ---- spread integrity (no crossed/zero/null bbo) ----
    sp_tot = scalar("SELECT COUNT(*) FROM spread WHERE ts>=?", (win,)) or 0
    crossed = scalar("""SELECT COUNT(*) FROM spread WHERE ts>=? AND
        (bid IS NULL OR ask IS NULL OR bid<=0 OR ask<=0 OR bid>=ask)""", (win,)) or 0
    checks.append({"key": "spread_valid", "label": "spread integrity",
                   "detail": f"{crossed} crossed/invalid of {sp_tot:,}",
                   "status": "green" if crossed == 0 else "red", "value": crossed})

    # ---- trade validity + ordering ----
    tr_bad = scalar("""SELECT COUNT(*) FROM trades WHERE ts>=? AND
        (price<=0 OR qty<=0 OR side NOT IN (0,1))""", (win,)) or 0
    checks.append({"key": "trades_valid", "label": "trade validity",
                   "detail": f"{tr_bad} bad price/qty/side",
                   "status": "green" if tr_bad == 0 else "red", "value": tr_bad})
    ooo = scalar("""SELECT COUNT(*) FROM (
        SELECT ts - LAG(ts) OVER (ORDER BY trade_id) d
        FROM trades WHERE ts>=?) WHERE d<0""", (win,)) or 0
    checks.append({"key": "trades_order", "label": "trade ordering",
                   "detail": f"{ooo} ts-decreasing by id",
                   "status": "green" if ooo == 0 else "red", "value": ooo})

    # ---- price anomalies (likely bad prints; yellow = review, not alarm) ----
    jumps = scalar("""SELECT COUNT(*) FROM (
        SELECT close, LAG(close) OVER (ORDER BY ts_begin) p
        FROM ohlc_1m WHERE ts_begin>=?)
        WHERE p IS NOT NULL AND p>0 AND ABS(close-p)/p > ?""", (win, PRICE_JUMP_PCT)) or 0
    checks.append({"key": "price_anomaly", "label": "price anomalies",
                   "detail": f"{jumps} 1m jumps >{PRICE_JUMP_PCT*100:.0f}%",
                   "status": "green" if jumps == 0 else "yellow", "value": jumps})

    # ---- rollup consistency: SETTLED 1h buckets must reconcile to 1m vol ----
    settle_cutoff = now - ROLLUP_SETTLE_SEC * 1000   # bucket must have closed before this
    rows = conn.execute("""SELECT ts_begin, volume FROM rollup_bars
        WHERE interval_min=60 AND ts_begin>=? AND ts_begin+3600000<=?
        ORDER BY ts_begin""", (win, settle_cutoff)).fetchall()
    max_rel, n, worst = 0.0, 0, None
    for ts_begin, vol in rows:
        base = scalar("SELECT COALESCE(SUM(volume),0) FROM ohlc_1m "
                      "WHERE ts_begin>=? AND ts_begin<?", (ts_begin, ts_begin + 3_600_000)) or 0.0
        vol = vol or 0.0
        denom = base if base else (vol if vol else 1.0)
        rel = abs(vol - base) / denom
        n += 1
        if rel > max_rel:
            max_rel, worst = rel, ts_begin
    if n == 0:
        checks.append({"key": "rollup", "label": "rollup consistency",
                       "detail": "no settled 1h buckets yet", "status": "gray", "value": None})
    else:
        st = "green" if max_rel <= ROLLUP_DRIFT_REL else "red"
        checks.append({"key": "rollup", "label": "rollup consistency",
                       "detail": f"max drift {max_rel*100:.3f}% over {n} settled 1h",
                       "status": st, "value": round(max_rel * 100, 4)})

    # ---- collector beacon (is the writer even alive + connected?) ----
    hb = conn.execute("SELECT ts, ws_state, reconnects_1h "
                      "FROM collector_health WHERE id=1").fetchone()
    if hb:
        bage = round((now - hb[0]) / 1000.0, 1)
        ws, rc = hb[1], (hb[2] or 0)
        st = "red" if bage > BEACON_STALE_SEC else "yellow" if ws != "connected" else "green"
        checks.append({"key": "beacon", "label": "collector beacon",
                       "detail": f"{bage:.0f}s ago · ws {ws} · {rc} reconn/1h",
                       "status": st, "value": bage})
    else:
        checks.append({"key": "beacon", "label": "collector beacon",
                       "detail": "no beacon row", "status": "red", "value": None})

    return {"verdict": _worst(checks), "window_hours": window_hours, "checks": checks}


def main():
    conn = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    try:
        rep = report(conn)
    finally:
        conn.close()
    print(f"DATA QUALITY: {rep['verdict'].upper()}  (window {rep['window_hours']}h)")
    for ch in rep["checks"]:
        print(f"  [{ch['status']:>6}] {ch['label']:<20} {ch['detail']}")


if __name__ == "__main__":
    main()

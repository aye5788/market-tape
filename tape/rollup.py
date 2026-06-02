"""
tape/rollup.py — derive coarser bars + microstructure aggregates from the
raw tape, and prune raw rows past the retention horizon.

For each configured interval it produces one rollup_bars row per bucket:
  - OHLCV + vwap + trades  : aggregated from ohlc_1m (the 1m base)
  - buy_vol / sell_vol / trade_count : from the trade tape
  - mean_spread_bps        : from the spread tape

Idempotent: re-running recomputes the trailing window and upserts
(INSERT OR REPLACE on the (interval_min, ts_begin) key), so late data and
overlapping runs are harmless.

Run standalone (e.g. from cron/systemd-timer):
  python -m tape.rollup            # trailing-window incremental
  python -m tape.rollup --full     # recompute the entire history once
"""
import logging
import sqlite3
import time

from tape import config

log = logging.getLogger("tape.rollup")


def _rollup_interval(conn, interval_min, since_ms):
    iv_ms = interval_min * 60_000

    # OHLCV + vwap from the 1m base. Bucketed in Python (a 48h window is a
    # few thousand 1m rows — trivial). vwap is volume-weighted across bars.
    rows = conn.execute(
        "SELECT ts_begin, open, high, low, close, volume, vwap, trades "
        "FROM ohlc_1m WHERE ts_begin >= ? ORDER BY ts_begin",
        (since_ms,),
    ).fetchall()

    buckets = {}
    for ts, o, h, l, c, v, vw, tr in rows:
        b = (ts // iv_ms) * iv_ms
        v = v or 0.0
        d = buckets.get(b)
        if d is None:
            buckets[b] = {
                "open": o, "high": h, "low": l, "close": c,
                "vol": v, "vwap_num": (vw or 0.0) * v, "trades": tr or 0,
                "first": ts, "last": ts,
            }
        else:
            if ts < d["first"]:
                d["first"], d["open"] = ts, o
            if ts > d["last"]:
                d["last"], d["close"] = ts, c
            if h is not None and (d["high"] is None or h > d["high"]):
                d["high"] = h
            if l is not None and (d["low"] is None or l < d["low"]):
                d["low"] = l
            d["vol"] += v
            d["vwap_num"] += (vw or 0.0) * v
            d["trades"] += tr or 0

    # Order flow from the trade tape (pure SQL aggregate — keep big tables
    # out of Python). side: 0=buy 1=sell.
    flow = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT (ts / ?) * ? AS b, "
            "SUM(CASE WHEN side=0 THEN qty ELSE 0 END), "
            "SUM(CASE WHEN side=1 THEN qty ELSE 0 END), "
            "COUNT(*) "
            "FROM trades WHERE ts >= ? GROUP BY b",
            (iv_ms, iv_ms, since_ms),
        ).fetchall()
    }

    # Mean relative spread in basis points from the spread tape.
    spr = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT (ts / ?) * ? AS b, "
            "AVG((ask - bid) / ((ask + bid) / 2.0) * 10000.0) "
            "FROM spread WHERE ts >= ? AND bid > 0 AND ask > 0 GROUP BY b",
            (iv_ms, iv_ms, since_ms),
        ).fetchall()
    }

    for b, d in buckets.items():
        vwap = (d["vwap_num"] / d["vol"]) if d["vol"] else None
        bv, sv, tc = flow.get(b, (None, None, None))
        conn.execute(
            "INSERT OR REPLACE INTO rollup_bars "
            "(interval_min, ts_begin, open, high, low, close, volume, vwap, "
            " trades, buy_vol, sell_vol, trade_count, mean_spread_bps) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (interval_min, b, d["open"], d["high"], d["low"], d["close"],
             d["vol"], vwap, d["trades"], bv, sv, tc, spr.get(b)),
        )
    return len(buckets)


def _prune(conn, retention_days, now_ms):
    cutoff = now_ms - retention_days * 86_400_000
    total = 0
    for tbl in ("trades", "spread", "book_l2"):
        cur = conn.execute(f"DELETE FROM {tbl} WHERE ts < ?", (cutoff,))
        total += cur.rowcount or 0
    return total


def run_once(db_path, intervals, lookback_hours, retention_days, full=False):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        now_ms = int(time.time() * 1000)
        since_ms = 0 if full else now_ms - lookback_hours * 3_600_000
        for iv in intervals:
            n = _rollup_interval(conn, iv, since_ms)
            log.info("rollup %dmin: %d buckets (since_ms=%d full=%s)",
                     iv, n, since_ms, full)
        pruned = _prune(conn, retention_days, now_ms)
        conn.commit()
        if pruned:
            log.info("pruned %d raw rows older than %d days", pruned, retention_days)
            conn.execute("PRAGMA incremental_vacuum")
            conn.commit()
    finally:
        conn.close()


def main():
    import sys
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    full = "--full" in sys.argv
    run_once(config.DB_PATH, config.ROLLUP_INTERVALS_MIN,
             config.ROLLUP_LOOKBACK_HOURS, config.RAW_RETENTION_DAYS, full=full)


if __name__ == "__main__":
    main()

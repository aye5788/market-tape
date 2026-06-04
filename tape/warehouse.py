"""
tape/warehouse.py — the contiguous XRP history store, SEPARATE from the collector.

Two datasets, by design:
  * market_tape.db  — the live collector's capture (Kraken WS). Pristine. The
                      collector owns it and never reads this warehouse.
  * history.db      — THIS store: one contiguous 1-minute XRP/USD history built
                      from Bitstamp (2017 -> the tape's first bar) + the live
                      Kraken bars (tape start -> now), each tagged by source, plus
                      the SAME rollups the tape builds (5/60/360/1440 min).

Flow of data is ONE-WAY: history.db is fed FROM market_tape.db, never the other
direction, so the collector is fully decoupled. The append step is local-only
(both DBs on the box) — no GCS, no cost — so it can run hourly; a separate daily
job snapshots history.db to GCS as a single rolling object.

Provenance: ohlc_1m.source distinguishes Kraken (0/1/2, copied from the tape) from
Bitstamp (3). Kraken WINS on any overlap — Bitstamp is imported only for the span
BEFORE the tape's first bar. The deep (pre-tape) history is OHLC only; there was
no trade/spread tape then, so rollup flow columns are NULL for that span.

CLI:
  python -m tape.warehouse build           # one-time: import Bitstamp + bridge Kraken + rollups
  python -m tape.warehouse append          # hourly: pull new tape bars + incremental rollup
  python -m tape.warehouse backup          # daily: consistent snapshot -> gzip -> GCS (rolling)
  python -m tape.warehouse status          # ranges / source breakdown / rollup counts
"""
import argparse
import csv
import glob
import gzip
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

from tape import config
from tape import rollup
from tape import schema

log = logging.getLogger("tape.warehouse")

_MIN_MS = 60_000

# Trailing reconciliation window for the hourly append (a). We re-scan this far
# back and rely on INSERT OR IGNORE (ts_begin / trade_id are PKs) for dedup, so
# out-of-order silent-minute backfills the collector writes BELOW its high-water
# mark are still picked up instead of being permanently skipped by a strict MAX()
# watermark (e.g. the 2026-06-04 07:02-07:05 hole).
APPEND_RECONCILE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000   # 7d trailing window

# Sentinel retention for the WAREHOUSE rollup call only: so large the prune cutoff
# is effectively -inf, so rollup.run_once never deletes a raw row here. history.db
# is a keep-everything downtime instrument; the live collector keeps its own 60d
# rolling prune (config.RAW_RETENTION_DAYS) untouched.
NO_PRUNE_RETENTION_DAYS = 100_000


def _connect(path, wal=True):
    c = sqlite3.connect(path, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    if wal:
        c.execute("PRAGMA journal_mode=WAL")
    return c


def _tape_min_max(tape_path):
    t = sqlite3.connect(f"file:{tape_path}?mode=ro", uri=True)
    try:
        return t.execute("SELECT MIN(ts_begin), MAX(ts_begin) FROM ohlc_1m").fetchone()
    finally:
        t.close()


# ----------------------------------------------------------------- build steps

def import_bitstamp(conn, csv_dir, before_ms=None):
    """Stream every Bitstamp_XRPUSD_*.csv into ohlc_1m (source=bitstamp).
    CryptoDataDownload format: junk line 1, header line 2, then
    unix(sec),date,symbol,open,high,low,close,Volume XRP,Volume USD — newest
    first. We INSERT OR IGNORE on the ts_begin PK, so descending order and
    year-boundary overlaps dedup automatically. vwap is unavailable in the feed,
    so we store close as a stand-in (fine for a volume-weighted rollup); trade
    count is unknown -> NULL. before_ms (the tape's first bar) caps the import so
    Kraken owns the overlap."""
    files = sorted(glob.glob(os.path.join(csv_dir, "Bitstamp_XRPUSD_*.csv")))
    if not files:
        raise SystemExit(f"no Bitstamp CSVs found in {csv_dir}")
    ins = schema.INSERTS["ohlc_1m_provenanced"]  # (ts,o,h,l,c,vol,vwap,trades,source)
    total = 0
    for path in files:
        n = 0
        batch = []
        with open(path, newline="") as fh:
            r = csv.reader(fh)
            for row in r:
                if not row or not row[0].lstrip("-").isdigit():
                    continue  # junk first line + header
                try:
                    ts = int(row[0]) * 1000
                    if before_ms is not None and ts >= before_ms:
                        continue
                    o, h, l, c = float(row[3]), float(row[4]), float(row[5]), float(row[6])
                    vol = float(row[7]) if len(row) > 7 and row[7] else 0.0
                except (ValueError, IndexError):
                    continue
                batch.append((ts, o, h, l, c, vol, c, None, schema.SRC_BITSTAMP))
                if len(batch) >= 50_000:
                    conn.executemany(ins, batch)
                    conn.commit()
                    n += len(batch)
                    batch.clear()
        if batch:
            conn.executemany(ins, batch)
            conn.commit()
            n += len(batch)
        total += n
        log.info("imported %s: %d rows", os.path.basename(path), n)
    return total


def bridge_kraken(conn, tape_path):
    """Copy the live Kraken ohlc_1m bars from the collector's DB into the
    warehouse, preserving their source (0/1/2). One-way; the collector is
    untouched. Bitstamp was imported only before the tape's first bar, so there
    is no overlap to reconcile."""
    conn.execute("ATTACH DATABASE ? AS live", (tape_path,))
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO ohlc_1m "
            "(ts_begin, open, high, low, close, volume, vwap, trades, source) "
            "SELECT ts_begin, open, high, low, close, volume, vwap, trades, source "
            "FROM live.ohlc_1m")
        conn.commit()
        n = cur.rowcount
    finally:
        conn.execute("DETACH DATABASE live")
    log.info("bridged %d Kraken bars from the tape", n)
    return n


def build_rollups_full(db_path):
    """Memory-safe full rollup: a SINGLE streaming pass over the ts-ordered 1m
    base, maintaining one open bucket per interval and flushing on rollover.
    O(1) heap (a handful of buckets + a write batch), so it never loads the
    multi-million-row history into Python. Read and write use separate WAL
    connections. Flow columns are NULL (the warehouse has no trade/spread tape)."""
    intervals = config.ROLLUP_INTERVALS_MIN
    rconn = _connect(db_path)
    wconn = _connect(db_path)
    ins = ("INSERT OR REPLACE INTO rollup_bars "
           "(interval_min, ts_begin, open, high, low, close, volume, vwap, trades, "
           " buy_vol, sell_vol, trade_count, mean_spread_bps) "
           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
    state = {iv: None for iv in intervals}
    batch = []
    written = 0

    def flush(iv, d):
        vwap = (d["vn"] / d["v"]) if d["v"] else None
        batch.append((iv, d["b"], d["o"], d["h"], d["l"], d["c"], d["v"], vwap, d["t"],
                      None, None, None, None))

    try:
        cur = rconn.execute(
            "SELECT ts_begin, open, high, low, close, volume, vwap, trades "
            "FROM ohlc_1m ORDER BY ts_begin")
        for ts, o, h, l, c, v, vw, tr in cur:
            v = v or 0.0
            for iv in intervals:
                ivms = iv * _MIN_MS
                b = (ts // ivms) * ivms
                d = state[iv]
                if d is None or d["b"] != b:
                    if d is not None:
                        flush(iv, d)
                    state[iv] = {"b": b, "o": o, "h": h, "l": l, "c": c,
                                 "v": v, "vn": (vw or 0.0) * v, "t": (tr or 0)}
                else:
                    d["c"] = c
                    if h is not None and (d["h"] is None or h > d["h"]):
                        d["h"] = h
                    if l is not None and (d["l"] is None or l < d["l"]):
                        d["l"] = l
                    d["v"] += v
                    d["vn"] += (vw or 0.0) * v
                    d["t"] += (tr or 0)
            if len(batch) >= 5000:
                wconn.executemany(ins, batch)
                wconn.commit()
                written += len(batch)
                batch.clear()
        for iv in intervals:
            if state[iv] is not None:
                flush(iv, state[iv])
        if batch:
            wconn.executemany(ins, batch)
            wconn.commit()
            written += len(batch)
    finally:
        rconn.close()
        wconn.close()
    log.info("built %d rollup bars across intervals %s", written, intervals)
    return written


def cmd_build(args):
    csv_dir = args.csv_dir or config.HISTORY_IMPORT_DIR
    tape_path = config.DB_PATH
    schema.init_db(config.HISTORY_DB_PATH)
    conn = _connect(config.HISTORY_DB_PATH)
    try:
        tmin, tmax = _tape_min_max(tape_path)
        if tmin is None:
            raise SystemExit("tape has no bars yet — start the collector first")
        log.info("tape covers %s -> %s; importing Bitstamp before tape start",
                 _utc(tmin), _utc(tmax))
        nb = import_bitstamp(conn, csv_dir, before_ms=tmin)
        nk = bridge_kraken(conn, tape_path)
    finally:
        conn.close()
    nr = build_rollups_full(config.HISTORY_DB_PATH)
    log.info("build done: %d Bitstamp + %d Kraken 1m bars, %d rollup bars", nb, nk, nr)
    cmd_status(args)


# --------------------------------------------------------------- ongoing append

def cmd_append(args):
    """Hourly, local-only: pull new 1m bars + the rich tape (trades / spread) from
    the live collector over a trailing reconciliation window, then incremental-roll
    the trailing window. No GCS, no cost.

    Idempotent and out-of-order-safe:
      (a) ohlc_1m — re-scanned over APPEND_RECONCILE_WINDOW_MS and deduped by the
          ts_begin PK via INSERT OR IGNORE, so silent-minute backfills the collector
          writes behind its high-water mark are healed, not skipped.
      (b) trades  — same trailing-window + trade_id PK dedup.
      (c) spread  — no natural key, so copied by a strict ts watermark (> MAX(ts));
          its own autoincrement id is NOT carried (warehouse assigns its own).
    book_l2 is out of scope (the "book" channel is disabled upstream; 0 rows). The
    warehouse never prunes (NO_PRUNE_RETENTION_DAYS) — keep-everything instrument."""
    if not os.path.exists(config.HISTORY_DB_PATH):
        raise SystemExit("history.db does not exist — run `build` first")
    conn = _connect(config.HISTORY_DB_PATH)
    try:
        have = conn.execute("SELECT COALESCE(MAX(ts_begin), 0) FROM ohlc_1m").fetchone()[0]
        conn.execute("ATTACH DATABASE ? AS live", (config.DB_PATH,))
        # (a) 1m bars — trailing-window merge (gap-aware; ts_begin PK dedups).
        cur = conn.execute(
            "INSERT OR IGNORE INTO ohlc_1m "
            "(ts_begin, open, high, low, close, volume, vwap, trades, source) "
            "SELECT ts_begin, open, high, low, close, volume, vwap, trades, source "
            "FROM live.ohlc_1m WHERE ts_begin >= ?", (have - APPEND_RECONCILE_WINDOW_MS,))
        n = cur.rowcount
        # (b) trade tape — trailing-window merge (trade_id PK dedups).
        ct = conn.execute(
            "INSERT OR IGNORE INTO trades "
            "(trade_id, ts, price, qty, side, ord_type) "
            "SELECT trade_id, ts, price, qty, side, ord_type FROM live.trades "
            "WHERE ts >= (SELECT COALESCE(MAX(ts), 0) FROM trades) - ?",
            (APPEND_RECONCILE_WINDOW_MS,))
        nt = ct.rowcount
        # (c) spread tape — strict ts watermark; id NOT carried (no cross-db key).
        cs = conn.execute(
            "INSERT INTO spread (ts, bid, bid_qty, ask, ask_qty, last) "
            "SELECT ts, bid, bid_qty, ask, ask_qty, last FROM live.spread "
            "WHERE ts > (SELECT COALESCE(MAX(ts), 0) FROM spread)")
        ns = cs.rowcount
        conn.commit()
        conn.execute("DETACH DATABASE live")
    finally:
        conn.close()
    # incremental rollup over the trailing window (memory-safe; small window).
    # downtime instrument: keep rich data, do not prune the warehouse (live db prunes itself)
    rollup.run_once(config.HISTORY_DB_PATH, config.ROLLUP_INTERVALS_MIN,
                    config.ROLLUP_LOOKBACK_HOURS, NO_PRUNE_RETENTION_DAYS, full=False)
    log.info("appended %d bars, %d trades, %d spread rows (ohlc since %s)",
             n, nt, ns, _utc(have) if have else "start")
    return n


# --------------------------------------------------------------- gap refill (bitstamp)

_BITSTAMP_OHLC = "https://www.bitstamp.net/api/v2/ohlc/xrpusd/"
_REFILL_THROTTLE = 0.12   # seconds between API pages (well under Bitstamp's limit)


def _detect_gaps(conn):
    """Missing 1m ranges as [start_ms, end_ms). SQL LEAD so we never load the
    multi-million-row series into Python."""
    gaps = []
    for prev, nxt in conn.execute(
            "SELECT ts_begin, next_ts FROM (SELECT ts_begin, "
            "LEAD(ts_begin) OVER (ORDER BY ts_begin) next_ts FROM ohlc_1m) "
            "WHERE next_ts IS NOT NULL AND next_ts - ts_begin > 60000"):
        gaps.append((prev + _MIN_MS, nxt))
    return gaps


def _bitstamp_page(start_sec, limit=1000):
    """One page of 1m OHLC from Bitstamp's public API, ascending from start_sec.
    Returns [(ts_sec,o,h,l,c,vol)] or None on error."""
    url = f"{_BITSTAMP_OHLC}?step=60&limit={limit}&start={int(start_sec)}"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            d = json.load(r)
    except Exception as e:
        log.warning("bitstamp page @%s failed: %r", start_sec, e)
        return None
    out = []
    for b in (d.get("data") or {}).get("ohlc") or []:
        try:
            out.append((int(b["timestamp"]), float(b["open"]), float(b["high"]),
                        float(b["low"]), float(b["close"]), float(b["volume"])))
        except (KeyError, ValueError, TypeError):
            continue
    out.sort()
    return out


def cmd_refill(args):
    """Fill every gap from Bitstamp's OWN OHLC API — real, same-source data
    (real-volume bars where CryptoDataDownload dropped them; flat bars where the
    market was genuinely quiet). Nothing is fabricated. Reports the residual —
    minutes Bitstamp itself never recorded — rather than silently flat-filling."""
    if not os.path.exists(config.HISTORY_DB_PATH):
        raise SystemExit("history.db does not exist — run `build` first")
    conn = _connect(config.HISTORY_DB_PATH)
    ins = schema.INSERTS["ohlc_1m_provenanced"]
    gaps = _detect_gaps(conn)
    total_missing = sum((e - s) // _MIN_MS for s, e in gaps)
    log.info("refill: %d gaps, %d missing minutes — fetching from Bitstamp",
             len(gaps), total_missing)

    calls, added = 0, 0
    try:
        for gi, (s_ms, e_ms) in enumerate(gaps):
            s_sec, e_sec = s_ms // 1000, e_ms // 1000
            cur = s_sec
            while cur < e_sec:
                page = _bitstamp_page(cur)
                calls += 1
                if not page:                       # error or genuinely empty
                    break
                batch = [(ts * 1000, o, h, l, c, v, c, None, schema.SRC_BITSTAMP)
                         for ts, o, h, l, c, v in page if s_sec <= ts < e_sec]
                if batch:
                    conn.executemany(ins, batch)
                    conn.commit()
                    added += len(batch)
                last = page[-1][0]
                if last < cur + 60:                # no forward progress
                    break
                cur = last + 60
                time.sleep(_REFILL_THROTTLE)
            if (gi + 1) % 25 == 0:
                log.info("refill: %d/%d gaps · %d calls · %d bars added",
                         gi + 1, len(gaps), calls, added)
    finally:
        conn.close()
    log.info("refill: fetch done — %d Bitstamp calls, %d real bars added", calls, added)

    nr = build_rollups_full(config.HISTORY_DB_PATH)
    log.info("refill: rebuilt %d rollup bars", nr)

    c2 = sqlite3.connect(f"file:{config.HISTORY_DB_PATH}?mode=ro", uri=True)
    try:
        resid = _detect_gaps(c2)
    finally:
        c2.close()
    resid_min = sum((e - s) // _MIN_MS for s, e in resid)
    print("\n=== REFILL RESULT ===")
    print(f"  added {added:,} real Bitstamp bars across {len(gaps)} gaps ({calls} API calls)")
    print(f"  residual (Bitstamp itself has no data): {len(resid)} gaps, {resid_min:,} min")
    if resid:
        print("  largest residual gaps:")
        for s, e in sorted(resid, key=lambda x: x[1] - x[0], reverse=True)[:8]:
            print(f"    {(e - s) // _MIN_MS:>6} min  {_utc(s)} -> {_utc(e)}")
    cmd_status(args)


# ------------------------------------------------------------------ gcs backup

def cmd_backup(args):
    """Daily: consistent snapshot of history.db -> gzip -> GCS as a SINGLE rolling
    object (overwrite, no accumulation). Upload-only, so it's free."""
    if not os.path.exists(config.HISTORY_DB_PATH):
        raise SystemExit("history.db does not exist — run `build` first")
    tmp_db = config.HISTORY_DB_PATH + ".snap.tmp"
    gz = config.HISTORY_DB_PATH + ".snap.gz"
    src = sqlite3.connect(config.HISTORY_DB_PATH)
    try:
        dst = sqlite3.connect(tmp_db)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    try:
        with open(tmp_db, "rb") as fi, gzip.open(gz, "wb", compresslevel=6) as fo:
            shutil.copyfileobj(fi, fo)
    finally:
        if os.path.exists(tmp_db):
            os.remove(tmp_db)
    gsutil = getattr(config, "GSUTIL_BIN", None) or shutil.which("gsutil")
    rc = subprocess.run([gsutil, "-q", "cp", gz, config.HISTORY_BACKUP_REMOTE],
                        capture_output=True, text=True)
    size = os.path.getsize(gz)
    os.remove(gz)
    if rc.returncode != 0:
        log.error("history backup upload failed: %s", rc.stderr.strip())
        sys.exit(1)
    log.info("history backup -> %s (%.1f MB)", config.HISTORY_BACKUP_REMOTE, size / 1e6)


# ----------------------------------------------------------------------- status

def _utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def cmd_status(args):
    if not os.path.exists(config.HISTORY_DB_PATH):
        print("history.db does not exist yet")
        return
    c = sqlite3.connect(f"file:{config.HISTORY_DB_PATH}?mode=ro", uri=True)
    try:
        n, mn, mx = c.execute("SELECT COUNT(*), MIN(ts_begin), MAX(ts_begin) FROM ohlc_1m").fetchone()
        print(f"history.db: {n:,} 1m bars · {_utc(mn)} -> {_utc(mx)} UTC"
              if n else "history.db: empty")
        if n:
            span = (mx - mn) / 86_400_000
            print(f"  span: {span:.0f} days ({span/365.25:.2f} years)")
            print("  source breakdown:")
            names = {0: "kraken_ws", 1: "kraken_backfill", 2: "kraken_recovered",
                     3: "bitstamp_hist"}
            for s, cnt in c.execute("SELECT source, COUNT(*) FROM ohlc_1m GROUP BY source ORDER BY source"):
                print(f"    {names.get(s, s):16} {cnt:,}")
            print("  rollups:")
            for iv, cnt, rmn, rmx in c.execute(
                    "SELECT interval_min, COUNT(*), MIN(ts_begin), MAX(ts_begin) "
                    "FROM rollup_bars GROUP BY interval_min ORDER BY interval_min"):
                print(f"    {iv:>5}m: {cnt:,} bars · {_utc(rmn)} -> {_utc(rmx)}")
            db_mb = os.path.getsize(config.HISTORY_DB_PATH) / 1e6
            print(f"  db size: {db_mb:.0f} MB")
    finally:
        c.close()


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    p = argparse.ArgumentParser(prog="tape.warehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build"); b.add_argument("--csv-dir", default=None)
    sub.add_parser("refill")
    sub.add_parser("append")
    sub.add_parser("backup")
    sub.add_parser("status")
    args = p.parse_args()
    {"build": cmd_build, "refill": cmd_refill, "append": cmd_append,
     "backup": cmd_backup, "status": cmd_status}[args.cmd](args)


if __name__ == "__main__":
    main()

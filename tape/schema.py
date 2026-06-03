"""
tape/schema.py — SQLite DDL + parameterized inserts for the market tape.

Design notes:
- Timestamps are INTEGER epoch-milliseconds, not ISO text: smaller, and
  faster for the range queries the rollup/backtest path does.
- Raw tables are append-only. Where the feed can re-send rows (the trade
  snapshot on reconnect, OHLC closed-bar snapshots), the natural key is a
  PRIMARY KEY and inserts use OR IGNORE so reconnects don't duplicate.
- spread/book have no natural id, so they get an autoincrement rowid and
  are append-only; bounded by retention pruning in rollup.py.
- Indexes are kept minimal (ts only) — every index is a tax on insert.
"""

# ohlc_1m.source — provenance of a 1m bar, so a backfilled bar is NEVER
# indistinguishable from one observed live on the wire. The hot path
# (collector WS callback) writes SRC_WS implicitly via the column DEFAULT.
SRC_WS = 0          # observed live on the Kraken WS ohlc feed (the default)
SRC_BACKFILL = 1    # backfilled flat carry-forward bar for a CONFIRMED zero-trade minute
SRC_RECOVERED = 2   # recovered from Kraken REST for a minute we were blind to (real volume)

DDL = """
PRAGMA auto_vacuum=INCREMENTAL;   -- lets rollup reclaim space after pruning

-- 1m closed OHLC bars (the base granularity). ts_begin PK dedups the
-- closed-bar snapshots Kraken re-sends on reconnect. `source` records how
-- the bar got here (0=ws live, 1=backfill of a silent minute, 2=REST-recovered)
-- — Kraken's ohlc feed only emits on activity, so a zero-trade minute leaves a
-- hole the backfill layer fills as a flat bar; tagging keeps it auditable.
CREATE TABLE IF NOT EXISTS ohlc_1m (
    ts_begin INTEGER PRIMARY KEY,
    open  REAL, high REAL, low REAL, close REAL,
    volume REAL, vwap REAL, trades INTEGER,
    source INTEGER NOT NULL DEFAULT 0
);

-- The trade tape. trade_id PK dedups the 50-trade snapshot re-sent on
-- every reconnect. side: 0=buy 1=sell. ord_type: 0=market 1=limit.
CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY,
    ts INTEGER, price REAL, qty REAL, side INTEGER, ord_type INTEGER
);
CREATE INDEX IF NOT EXISTS ix_trades_ts ON trades(ts);

-- Best bid/ask (from the ticker channel). High rate; pruned by retention.
CREATE TABLE IF NOT EXISTS spread (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, bid REAL, bid_qty REAL, ask REAL, ask_qty REAL, last REAL
);
CREATE INDEX IF NOT EXISTS ix_spread_ts ON spread(ts);

-- L2 depth levels (only written if "book" is in CHANNELS). side: 0=bid 1=ask.
-- is_snapshot: 1 for the full-book snapshot rows, 0 for incremental updates.
CREATE TABLE IF NOT EXISTS book_l2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, side INTEGER, price REAL, qty REAL, is_snapshot INTEGER
);
CREATE INDEX IF NOT EXISTS ix_book_ts ON book_l2(ts);

-- Single-row liveness beacon written by the collector every few seconds.
-- Lets a SEPARATE process (the dashboard) see real process health — ws
-- state, reconnects, rows written/dropped — not just data freshness.
CREATE TABLE IF NOT EXISTS collector_health (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    ts INTEGER, ws_state TEXT, last_msg_age_sec REAL,
    reconnects_1h INTEGER, rows_written INTEGER, rows_dropped INTEGER,
    started_at INTEGER
);

-- Append-only event log: alerts (feed down/recovered/dropping) and ws state
-- changes (connect/disconnect/reconnect). Powers the dashboard Events panel —
-- centralizes the history that was otherwise only an in-memory/ntfy ephemeral.
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, severity TEXT, category TEXT, message TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);

-- Derived, PERMANENT rollups: coarser OHLCV (+vwap/trades) from ohlc_1m,
-- plus order-flow (buy/sell vol, trade count) and mean spread from the
-- raw tape. (interval_min, ts_begin) PK makes the rollup idempotent.
CREATE TABLE IF NOT EXISTS rollup_bars (
    interval_min INTEGER, ts_begin INTEGER,
    open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL, trades INTEGER,
    buy_vol REAL, sell_vol REAL, trade_count INTEGER, mean_spread_bps REAL,
    PRIMARY KEY (interval_min, ts_begin)
);
"""

# Parameterized inserts used by TapeWriter. Tuple order MUST match the
# column order here; the collector builds rows to match.
INSERTS = {
    "ohlc_1m": (
        "INSERT OR IGNORE INTO ohlc_1m "
        "(ts_begin, open, high, low, close, volume, vwap, trades) "
        "VALUES (?,?,?,?,?,?,?,?)"
    ),
    # Provenanced variant used ONLY by the backfill layer (tape/backfill.py),
    # which sets `source` explicitly. The hot WS path stays on "ohlc_1m" above
    # and inherits source=0 via the column DEFAULT.
    "ohlc_1m_provenanced": (
        "INSERT OR IGNORE INTO ohlc_1m "
        "(ts_begin, open, high, low, close, volume, vwap, trades, source) "
        "VALUES (?,?,?,?,?,?,?,?,?)"
    ),
    "trades": (
        "INSERT OR IGNORE INTO trades "
        "(trade_id, ts, price, qty, side, ord_type) "
        "VALUES (?,?,?,?,?,?)"
    ),
    "spread": (
        "INSERT INTO spread "
        "(ts, bid, bid_qty, ask, ask_qty, last) "
        "VALUES (?,?,?,?,?,?)"
    ),
    "book_l2": (
        "INSERT INTO book_l2 "
        "(ts, side, price, qty, is_snapshot) "
        "VALUES (?,?,?,?,?)"
    ),
}


def _migrate(conn):
    """Idempotent in-place migrations for DBs created before a column existed.
    CREATE TABLE IF NOT EXISTS will NOT add a column to a pre-existing table, so
    any additive column has to be ALTERed in on startup. Safe to run every boot."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(ohlc_1m)")}
    if "source" not in cols:
        conn.execute("ALTER TABLE ohlc_1m ADD COLUMN source INTEGER NOT NULL DEFAULT 0")


def init_db(db_path):
    """Create the schema if absent + run additive migrations. Safe to call on
    every startup."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        conn.executescript(DDL)
        _migrate(conn)
        conn.commit()
    finally:
        conn.close()

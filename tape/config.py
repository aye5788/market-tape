"""
tape/config.py — collector configuration.

This is the collector's OWN config. It intentionally does NOT import the
MAGI root config.py; keeping it standalone is the whole point. Edit the
values here, not anything under magi/.
"""
import os

# Absolute path to this package dir, so the DB lives beside the code and
# the collector works regardless of the process CWD.
_HERE = os.path.dirname(os.path.abspath(__file__))

# --- exchange / feed ---
# Kraken WS v2 pair format uses a slash.
SYMBOL = "XRP/USD"
SYMBOLS = [SYMBOL]

# Channels to record. The core set is the recommendation from design:
#   ticker -> spread (best bid/ask), trade -> the trade tape,
#   ohlc   -> 1m closed bars (base granularity; coarser is derived in rollup)
# Add "book" to also capture L2 depth snapshots (heavier; default off).
CHANNELS = ("ticker", "trade", "ohlc")

# Base OHLC granularity. 1 = 1-minute. Do NOT raise this — coarser bars
# (5m/1h/6h/1d) are DERIVED in rollup.py from the 1m base. 1m is the
# finest native Kraken bar and the resolution that makes 1.5%-spacing
# grid level-crossings unambiguous.
OHLC_INTERVAL_MIN = 1

# L2 book capture (only used if "book" is in CHANNELS). Depth options:
# 10/25/100/500/1000. Keep shallow if you enable it.
BOOK_DEPTH = 10

# --- storage ---
DB_PATH = os.path.join(_HERE, "market_tape.db")

# Buffered-writer batching. One fsync per flush, not per row.
FLUSH_ROWS = 500      # flush when this many rows are buffered, OR
FLUSH_SECS = 1.0      # ...this many seconds have elapsed, whichever first
QUEUE_MAX = 100_000   # bounded ingest queue; drop (don't OOM) if we fall behind

# --- rollup / retention ---
# Coarser bars + microstructure aggregates derived from the raw tape.
# 5m / 1h / 6h / 1d. 1h+1d match the inputs the MAGI indicator pipeline
# consumes; 6h keeps roc_6h parity; 5m is a cheap intermediate.
ROLLUP_INTERVALS_MIN = [5, 60, 360, 1440]

ROLLUP_IN_PROCESS = True     # run the rollup loop inside the collector process
ROLLUP_EVERY_SECS = 300      # every 5 min
ROLLUP_LOOKBACK_HOURS = 48   # recompute the trailing window each run (idempotent upsert)

# Raw high-rate tables (trades / spread / book) are pruned past this age.
# ohlc_1m is kept forever — it is tiny.
RAW_RETENTION_DAYS = 60

# --- backup (consistent snapshot -> gzip -> GCS, via tape-backup.timer) ---
BACKUP_BUCKET = "gs://xrp-grid-tape-backups-ayn88"   # off-box durability target
BACKUP_GCS_PREFIX = "tape"
BACKUP_LOCAL_DIR = os.path.join(_HERE, "backups")    # rolling local copies
BACKUP_LOCAL_KEEP = 6                                # fast-restore window kept on disk
GSUTIL_BIN = "/root/google-cloud-sdk/bin/gsutil"

# --- phone alerts (reuses MAGI's ntfy topic via NTFY_TOPIC_URL in .env) ---
ALERT_ENABLED = True
ALERT_STALE_SECS = 30            # ws not connected / no data this long = unhealthy
ALERT_DOWN_GRACE_SECS = 90       # ...sustained this long before firing a critical
ALERT_DROP_THRESHOLD = 1000      # writer-dropped rows jump this much = critical

# --- self-assessment / containment (flag-only; NEVER mutates collected data) ---
# The collector periodically self-grades data quality. Philosophy: auto-act only
# on small benign gaps; for anything SUSTAINED, stop + flag + escalate, don't
# overcorrect. Gaps are FLAGGED ONLY (logged as events) — no backfill yet.
SELFCHECK_EVERY_SECS = 60        # how often the collector self-grades quality
DQ_SUSTAINED_SECS = 180          # quality RED this long = sustained (not a blip) -> escalate
GAP_SETTLE_SECS = 180            # only flag 1m gaps older than this (lets reconnects re-send)
GAP_LOOKBACK_HOURS = 6           # recent window scanned for new gaps each self-check

# --- dead-man's-switch (external watchdog; reuses .env like the ntfy alerts) ---
# If HEALTHCHECK_PING_URL is set in .env, the collector pings it every
# HEALTHCHECK_EVERY_SECS while the PROCESS is alive. An EXTERNAL monitor (e.g.
# healthchecks.io free tier) pages you when the pings STOP — which covers total
# collector/box death, the one failure the in-process ntfy alert can't catch.
# Unset URL = silent no-op, so this stays inert until you create the check.
# Set the monitor's grace WELL above a few-second blip so the daily Kraken
# reconnect never trips it.
HEALTHCHECK_EVERY_SECS = 60

# --- health beacon / dashboard ---
HEALTH_EVERY_SECS = 5            # collector writes a liveness row this often
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5000            # same slot the MAGI dashboard used

# --- logging ---
LOG_LEVEL = "INFO"

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

# --- self-assessment / containment ---
# The collector periodically self-grades data quality. Philosophy: auto-act only
# on small, CONFIRMED-benign conditions; for anything SUSTAINED, stop + flag +
# escalate, don't overcorrect. A detected 1m gap is first run through the layered
# backfill below; only what it can't confidently resolve stays FLAGGED (logged as
# events) for a human.
SELFCHECK_EVERY_SECS = 60        # how often the collector self-grades quality
DQ_SUSTAINED_SECS = 180          # quality RED this long = sustained (not a blip) -> escalate
GAP_SETTLE_SECS = 180            # only act on 1m gaps older than this (lets reconnects re-send)
GAP_LOOKBACK_HOURS = 6           # recent window scanned for new gaps each self-check

# --- gap backfill (layered: local connectivity proof + Kraken REST arbiter) ---
# A Kraken WS ohlc bar is only published on activity, so a zero-trade minute
# leaves a hole. This fills holes that REST confirms were SILENT (vol==0) with a
# flat carry-forward bar (tagged source=1), and RECOVERS holes REST shows had
# real volume (source=2) while ESCALATING them (we were blind). It NEVER fills a
# gap it can't confirm, never fills while auto-actions are frozen, and never
# fills a gap larger than BACKFILL_MAX_GAP_BARS (that smells like an outage, not
# silence — leave it for review). See tape/backfill.py for the decision tree.
BACKFILL_ENABLED = True
BACKFILL_MAX_GAP_BARS = 15       # gaps longer than this stay flagged + escalate, not filled
BACKFILL_REST_URL = "https://api.kraken.com/0/public/OHLC"
BACKFILL_REST_PAIR = "XRPUSD"    # REST pair code (REST echoes it back as XXRPZUSD)
BACKFILL_REST_TIMEOUT = 15       # seconds; failure => gap left flagged, never guessed

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

# --- analysis page: interpretation layer (static rules + optional Gemini) ---
# The Analysis tab explains what the charts mean in plain English. Static rules
# are always on ($0, private, instant). The LLM narrative is a free-tier Gemini
# Flash synthesis over the SAME pre-computed numbers — ON by default, toggleable
# off on the page. Pinned to the free-tier model; every call logs token usage.
INTERPRET_LLM_ENABLED = True            # master switch for the optional LLM narrative
INTERPRET_LLM_DEFAULT_ON = True         # page default state (operator can toggle to static)
INTERPRET_MODEL = "gemini-2.5-flash"    # FREE-tier model (same one Casper uses); never paid
INTERPRET_KEY_VAR = "GOOGLE_API_KEY"    # reuse the project's existing Gemini key
INTERPRET_MAX_OUTPUT_TOKENS = 700
INTERPRET_CACHE_SECS = 300              # cache the narrative server-side (page also won't auto-poll)

# --- history warehouse (SEPARATE store; merged deep history + live append) ---
# Built and maintained by tape/warehouse.py — a DB DISTINCT from the collector's
# market_tape.db. Holds the contiguous XRP 1m history (Bitstamp 2017->Jun2 +
# Kraken live Jun2->now, tagged by ohlc_1m.source) plus the SAME rollups
# (5/60/360/1440). The live collector NEVER touches it: an hourly local job
# appends new bars from market_tape.db (free, no GCS), and a daily job snapshots
# it to GCS as a single rolling object. Same schema as the tape, so the Analysis
# tab can read it identically (flow columns stay NULL — the deep history is OHLC
# only, no order-flow tape existed then).
HISTORY_DB_PATH = os.path.join(_HERE, "history.db")
HISTORY_IMPORT_DIR = "/root/hist_import"             # where the Bitstamp yearly CSVs are staged
HISTORY_BACKUP_REMOTE = f"{BACKUP_BUCKET}/history/history.db.gz"   # single rolling GCS snapshot

# --- logging ---
LOG_LEVEL = "INFO"

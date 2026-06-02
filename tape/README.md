# tape/ — Kraken Market Tape Collector

Standalone, always-on recorder for Kraken **public** WS v2 market data for
`XRP/USD`. Streams the live feed and persists it to its own SQLite file for
later eval / backtest work. It is **decoupled from MAGI on purpose**.

## What it records

| Source channel | Table | What |
|---|---|---|
| `ohlc` (interval=1) | `ohlc_1m` | 1-minute **closed** bars (OHLCV + vwap + trade count) — the base granularity |
| `trade` | `trades` | every execution (price, qty, side, ord_type, trade_id, ts) |
| `ticker` | `spread` | best bid/ask + qty + last |
| `book` *(optional, off)* | `book_l2` | L2 depth-10 levels (enable via `CHANNELS` in `config.py`) |

Coarser granularities are **derived, not captured** — `rollup.py` builds
permanent `rollup_bars` rows for 5m / 1h / 6h / 1d from the 1m base plus
order-flow (buy/sell volume) and mean spread. Add an interval by editing
`ROLLUP_INTERVALS_MIN` in `config.py`; no schema change, no new subscription.

Why 1m as the base: it's the finest native Kraken bar and makes ~1.5%
grid level-crossings unambiguous within a bar. Anything finer than 1m is
the trade tape's job, not a smaller bar.

## Separation from MAGI (by design)

- Imports nothing from `magi/`, `grid/`, `observer.py`, `database.py`,
  root `config.py`, `scheduler.py`. Verify: `grep -rE "^(from|import) (magi|grid|observer|database|scheduler|config)\b" tape/` returns nothing.
- Writes only to `tape/market_tape.db`. Never opens `observer.db`.
- Own systemd service, own config, own logger. Runs whether MAGI is up or down.
- The WS client is a **vendored copy** (`ws_client.py`), extended with the
  `trade`/`book` channels the MAGI gate client lacks — so it evolves
  independently and shares no live state.
- Needs no secrets for the public feed. (Only L3 would need the Kraken key.)

## Run

```bash
# foreground smoke test (Ctrl-C to stop; flushes tail cleanly)
/root/xrp_grid/venv/bin/python3 -m tape.collector

# as a service
cp tape/tape-collector.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now tape-collector
journalctl -u tape-collector -f
```

Rollup runs in-process every 5 min by default (`ROLLUP_IN_PROCESS`). To run
it standalone instead (e.g. a systemd timer), set that to `False` and:

```bash
/root/xrp_grid/venv/bin/python3 -m tape.rollup          # incremental
/root/xrp_grid/venv/bin/python3 -m tape.rollup --full   # rebuild all history
```

## Storage model

- Raw tables are append-only; `trades`/`spread`/`book_l2` are pruned past
  `RAW_RETENTION_DAYS` (default 60). `ohlc_1m` is kept forever (tiny).
- `rollup_bars` is permanent.
- Writer uses WAL + `synchronous=NORMAL` + batched `executemany` (one fsync
  per flush) so plain SQLite keeps up with the tick rate.

## Query examples

```sql
-- recent 1h derived bars with order-flow + spread
SELECT datetime(ts_begin/1000,'unixepoch') t, open, high, low, close,
       volume, buy_vol, sell_vol, trade_count, mean_spread_bps
FROM rollup_bars WHERE interval_min=60 ORDER BY ts_begin DESC LIMIT 24;

-- raw trade tape for a window
SELECT datetime(ts/1000,'unixepoch') t, price, qty,
       CASE side WHEN 0 THEN 'buy' ELSE 'sell' END side
FROM trades WHERE ts BETWEEN ? AND ? ORDER BY ts;
```

## Monitoring (reuses the existing dashboard plumbing)

The real-time monitor is `tape/dashboard.py`, served through the **same
plumbing the MAGI dashboard used** — no new tunnel/service/port:

- Root `dashboard.py` is now a shim → `tape.dashboard`, so `python -m dashboard`
  and **`magi-dashboard.service` on :5000** serve the monitor unchanged.
- The **`ethobs.uk` cloudflared tunnel** (already active, → localhost:5000)
  reaches it with no config change.
- Same **login**: it reuses `SECRET_KEY` / `DASHBOARD_PASSWORD` from `.env`,
  so the URL stays protected and existing session cookies keep working.
- The original MAGI dashboard is archived at
  `archive/magi_dashboard_2026-06-02/dashboard.py`; restore with
  `cp archive/magi_dashboard_2026-06-02/dashboard.py dashboard.py`.

Deploy: `systemctl restart magi-dashboard` (re-enable if needed). It reads
`market_tape.db`, so it works whether or not the collector is up — it just
shows "COLLECTOR NOT REPORTING" until the collector writes health beacons.

Panels: **Data Quality** (the full-width control-panel headline — see below),
process health (ws state / reconnects / rows written-dropped, from the
`collector_health` beacon), feed freshness, throughput + trades/min sparkline,
market snapshot, storage usage, rollup status, **Backup & durability**,
**Events** (alert + ws-state history from the `events` table), and **Grid
Conditions** (advisory market-analytics: realized vol → suggested spacing,
regime, flow imbalance, harvest rate — `tape/conditions.py`, enforces nothing).
Page polls
`/api/status` every 2s. Styled in the MAGI terminal palette (amber/orange on
black, Michroma title + VT323 verdict + Courier-New body — same fonts the
archived MAGI dashboard loads) so it reads as the same control surface.

## Data quality (garbage-in guard)

`tape/quality.py` is a read-only report that answers one question: is the tape
trustworthy, or is it ingesting garbage that makes it useless as a reality
anchor? It runs ~10 checks over a trailing window (default 24h) from tables the
collector already writes — no new deps, no writes — and rolls them into one
GREEN / YELLOW / RED verdict shown as the dashboard headline (and in
`/api/status` under `quality`).

Checks: 1m **coverage** (% of expected bars present in the captured span),
largest contiguous **gap**, last-bar **freshness**, bar **validity**
(low≤o,c≤high, positive, vol≥0), **spread integrity** (no crossed/zero/null
bbo), **trade validity** + **ordering**, **price anomalies** (1m close jumps
>5% ≈ bad print), **rollup consistency** (completed 1h buckets must reconcile
to their 1m volume), and the **collector beacon** (alive + connected).

Thresholds are anchored to sane exogenous bounds (a 1m bar closes every 60s;
XRP doesn't move >5% in a minute absent a bad print; a *completed* rollup bucket
must reconcile exactly), NOT fitted to the data — they flag corruption, not
normal market behaviour. The rollup check deliberately ignores the in-progress
bucket, which always lags the live minute.

One-shot report from the CLI:

```bash
/root/xrp_grid/venv/bin/python3 -m tape.quality
```

## Backups (durability, not capacity)

A single SQLite file is one point of failure (disk/droplet loss, corruption,
`rm`), and the tick/trade/spread stream is **not re-fetchable** once lost — so
the tape is backed up off-box.

- `tape/backup.py` takes a **consistent online-backup snapshot** (never a `cp`
  of the live WAL DB), gzips it, and `gsutil cp`s it to
  `gs://xrp-grid-tape-backups-ayn88/tape/`. Keeps the last `BACKUP_LOCAL_KEEP`
  copies in `tape/backups/` for instant local restore.
- `tape-backup.timer` runs it **hourly** (`Persistent=true`, so a missed tick
  fires on boot). The GCS bucket has a lifecycle rule deleting snapshots >30 d.
- Uses the already-configured `gsutil` (project `xrp-grid-brain-monitor`) — no
  new account or service.
- Each run writes `tape/backups/.last_backup.json` (GCS-confirmed timestamp,
  size, upload-ok, local copy count). The dashboard's **Backup & durability**
  panel reads that file — so it shows real upload status with no per-poll
  network call, and goes red if the last backup is overdue (>150 min vs the
  hourly timer) or the GCS upload failed.

Run a backup by hand: `python -m tape.backup`

**Restore:**
```bash
# from a local rolling copy
gunzip -c tape/backups/market_tape_<UTC>.db.gz > tape/market_tape.db
# or pull the latest from GCS
gsutil ls gs://xrp-grid-tape-backups-ayn88/tape/ | tail -1
gsutil cp gs://xrp-grid-tape-backups-ayn88/tape/market_tape_<UTC>.db.gz /tmp/
gunzip -c /tmp/market_tape_<UTC>.db.gz > tape/market_tape.db
# (stop tape-collector first; remove stale -wal/-shm sidecars before swapping)
```

Note: backups protect against *losing* the file. Disk *capacity* is not a
concern — SQLite's limit is ~281 TB; the real ceiling is disk free space, and
`RAW_RETENTION_DAYS` keeps the DB at a steady-state ~hundreds of MB.

## Phone alerts

The collector posts critical alerts to your phone by **reusing the same ntfy
topic MAGI used** — `NTFY_TOPIC_URL` in `.env`, same phone subscription. No new
topic or app setup. `tape/notify.py` is a ~dozen-line POST (not a MAGI import,
so the package stays standalone); pushes are tagged `tape` to tell them apart
from MAGI's. In-memory dedup, no writes to MAGI's DB.

Fires (from the health loop, `config.ALERT_*`):
- **FEED DOWN** (critical, priority 5 — bypasses DND): ws not connected / no data
  for > `ALERT_DOWN_GRACE_SECS` (90 s).
- **RECOVERED** (warning): when the feed comes back.
- **DROPPING ROWS** (critical): writer queue overflow.

Test by hand: `python -m tape.collector` is live; to fire a test push:
`python -c "from tape import notify; notify.send('Tape: test','[TEST] ignore','warning')"`

## Dead-man's-switch (catches total death)

The ntfy alerts above are emitted *by the collector* — so they can't fire if the
collector process or the whole box dies. That blind spot is closed by an
**external** heartbeat: the collector pings `HEALTHCHECK_PING_URL` every
`HEALTHCHECK_EVERY_SECS` (60 s) while it's alive (`notify.heartbeat()`, same
env-driven fail-silent pattern as the ntfy push). An external monitor pages you
when the pings **stop**. The ping is *unconditional* (it proves the process
runs; feed problems are alerted separately), so the daily Kraken reconnect never
trips it. Unset URL = silent no-op, so it's inert until you set one up.

Setup (~2 min, free, no card — [healthchecks.io](https://healthchecks.io)):
1. Create a free account → **Add Check**. Set Period ≈ 2 min, **Grace ≈ 5 min**
   (well above any brief reconnect). Add a notification (email, or point it at
   the same ntfy topic).
2. Copy the check's ping URL (`https://hc-ping.com/<uuid>`).
3. Put `HEALTHCHECK_PING_URL=https://hc-ping.com/<uuid>` in `.env` and
   `systemctl restart tape-collector`. Within a minute the check goes green.

## Extending later

- **L2 book:** add `"book"` to `CHANNELS`. Already wired; just heavier.
- **L3 (per-order book):** the only non-public feed. Needs a Kraken WS token
  from `/0/private/GetWebSocketsToken` (the signing machinery exists in the
  trading code's `kraken.py`). Would require adding token-refresh-on-reconnect
  to `ws_client.py`. Not built — public feed needs no auth.

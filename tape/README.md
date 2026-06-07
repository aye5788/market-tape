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
- `ohlc_1m.source` records a bar's **provenance**: `0` = observed live on the WS
  feed (the default, set by the column DEFAULT on the hot path), `1` = a
  backfilled flat bar for a minute Kraken REST confirmed was **silent** (zero
  trades), `2` = a bar **REST-recovered** for a minute we were blind to (real
  volume). A backfilled bar is therefore never indistinguishable from a live one
  — filter `WHERE source = 0` for wire-only data. See the backfill section below.
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
regime, drawdown-from-high, flow imbalance, harvest rate — `tape/conditions.py`,
enforces nothing — see below). Each Grid-Conditions chart carries reference lines
for its grid-relevant thresholds (fee floor / optimal spacing, choppy-vs-trending,
the downtrend-bleed level, harvestable step). Page polls
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

## Grid conditions (favorability)

`tape/conditions.py` is the advisory companion to Data Quality: it assumes the
data is good and asks **would current conditions favour the adaptive grid, or
bleed it?** Read-only, pure stdlib, 24h window. Every threshold is anchored to
the grid's **real exogenous parameters** — 1.5% spacing, 0.50% maker round-trip
fee floor, the 0.3–2.5% spacing clamps — NOT fitted to the data. It enforces
nothing; the verdict is decision-support, shown in `/api/status` under
`conditions`.

The headline GREEN / YELLOW / RED is the **worst of three drivers** — hourly
volatility, regime, harvest rate. **Flow imbalance and drawdown-from-high are
context only and never move the verdict** — short-window flow is too noisy to gate
on, and drawdown is left advisory deliberately (a sustained one-way fall already
trips the regime driver, and MAGI itself treats drawdown as a risk-judgment input,
not a gate).

- **hourly volatility** — realized σ (1m log-returns ×√60) plus a vol-tracking
  *adaptive spacing* (σ clamped to 0.3–2.5%). 🔴 σ<0.50% (below the fee floor —
  can't clear a round-trip), 🟡 0.50–1.5%, 🟢 ≥1.5%. The yellow band has a
  **load-bearing internal gradient**, surfaced as `firm` vs `thin`: `firm`
  (σ≥0.75%, i.e. ≥0.25% margin over the floor) = the default 1.5% spacing is
  wider than it needs to be — tighten toward σ and harvest more; `thin` (σ<0.75%)
  = the vol-tracked spacing barely breaks even, degrading toward too-quiet. Same
  colour, **opposite** recommendation — the detail spells out the exact fee margin
  (`+0.42% over fee floor`), so read the number, not just the chip.
- **regime** — efficiency ratio (|net move| ÷ summed minute moves) + net %/24h.
  🟢 ER<0.30 choppy / mean-reverting (grid-favourable), 🟡 0.30–0.50 mixed, 🔴
  ≥0.50 trending (the grid-downtrend-bleed early warning). Note ER is **direction
  blind** — it is built on |net move|, so a clean rally and a clean crash both read
  "trending." Which way it is going (and whether that is the dangerous way) is the
  drawdown signal's job.
- **drawdown from high** — signed % of price below its running high over the window
  — the **directional** counterpart to the direction-blind regime ER. Only the
  *downward* half of a trend bleeds the grid (it keeps buying into the fall and
  books losing inventory); an uptrend that breaks the grid costs opportunity, not
  capital. 🟢 within one grid step of the high (>−1.5%), 🟡 one-to-two steps below,
  🔴 ≥ two steps below (−3% = 2×spacing — active capital erosion). Threshold is grid
  geometry, not fitted. Context only; uses MAGI's exact `drawdown_from_high`
  definition so the tape monitor and the trading brain agree on the measure.
- **flow imbalance (6h)** — aggressor buy vs sell volume; context only, excluded
  from the verdict.
- **harvest rate** — fraction of completed 1h buckets whose high–low range ≥1.5%
  spacing; the most direct "is there anything to harvest" measure. 🟢 ≥25%, 🟡
  ≥10%, 🔴 <10%. Measured against the fixed 1.5%, so it *understates* the
  opportunity at a tighter adaptive spacing.

The instructive combination is **vol-yellow-`firm` + harvest-green**: per-minute
σ sits under 1.5% but hourly ranges still clear the spacing and the path is
choppy — exactly where an *adaptive* grid earns its keep over a static one by
tightening toward σ. Watch the vol number's drift inside the band (`firm → thin`),
not just the colour: that is the early signal conditions are thinning toward
stand-down **before** the colour flips red.

One-shot report from the CLI:

```bash
/root/xrp_grid/venv/bin/python3 -m tape.conditions
```

### Conditions history (`signals_1h` in the warehouse)

The same `conditions.report()` metrics are persisted as an **hourly time series**
in the history warehouse (`tape/history.db`, table `signals_1h`), so the
grid-favourability read is queryable over time, not just "right now." One row per
hour, *as of* that hour (each metric over its trailing window **ending** there):
the overall verdict plus every metric's value and status.

Because each signal is a **deterministic function of the stored OHLC/trade bars**,
the table is pure replay — it can be rebuilt at any time and can never disagree
with what the dashboard renders for a given instant. (`conditions.report()` takes a
`now_ms` as-of argument and bounds both its windows at `≤ now_ms`, so a replay never
peeks past its as-of time.)

- **Backfill the full history:** `python -m tape.warehouse build-signals` — replays
  hourly across the whole ~9.5y (~83k rows), idempotent (`INSERT OR REPLACE`).
- **Going forward:** the hourly `warehouse-append` writes new rows automatically
  (see `_signals_incremental` in `warehouse.py`) — no extra service or timer.

Provenance: a `source` column flags each row `1` = backfilled / `0` = live.
**Flow imbalance is NULL before 2026-06-02** — it needs the trade tape, which only
exists from the live-collector era; the other four signals (vol, regime, drawdown,
harvest) reconstruct across the entire history. `status` is `gray` where a window
lacked the data to compute a metric (e.g. flow over the pre-tape span).

## Dashboard performance (two clocks — keep heavy queries off the 2s path)

The page polls `/api/status` every **2 s**, but the two heavy panels (Data
Quality + Grid Conditions, ~24h window-function scans) are **cached on a 15 s
TTL** (`_ANALYTICS_TTL_SEC` in `dashboard.py`) — they recompute ~4×/min, **not**
30×/min, and are off the real-time path entirely. Measured on 2026-06-02 (8h of
data): the hot per-poll work (feed freshness, throughput, market snapshot, five
`COUNT(*)` totals) is **~0.06 ms**; the cached recompute is **quality ~86 ms +
conditions ~9 ms**. So the live numbers you watch tick are sub-millisecond, and
adding a metric to a cached panel cannot slow them.

Two properties make this safe to extend:

- **Cost is bounded by the 24h window + indexes, not total DB size.** Every
  analytics query filters to the trailing window and those filters are
  index-backed (`ix_trades_ts`, `ix_spread_ts`, the `ohlc_1m` / `rollup_bars`
  PKs). As history accumulates the scans keep covering ~24h, so the cost rises
  only until the window fills (~3× today's, then **plateaus**) — it does **not**
  grow with months of data.
- **WAL + a separate read-only dashboard connection** mean a slow analytics scan
  can't block the collector's writes; worst case it slows only the dashboard's
  own render, and the 15 s cache caps even that.

**The rule when adding panels / metrics:**
- ✅ Add to the **cached** path (`conditions.py` / `quality.py`) — windowed,
  indexed, cheap. conditions at ~9 ms has large headroom.
- ⚠️ Never add a query to the **hot** path (`build_status` outside the
  `_analytics()` block) unless it is indexed / `LIMIT 1` / windowed. That is the
  only way to actually slow the 2 s updates.
- ⚠️ If `quality.report` ever gets heavy (it already dominates at ~86 ms and does
  an N+1 loop over settled hourly buckets), raise `_ANALYTICS_TTL_SEC` (15→30 s) —
  a 24h panel does not need 15 s freshness. Don't drop metrics; slow the cadence.

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

## Self-assessment, gap backfill & containment

The collector self-grades data quality every `SELFCHECK_EVERY_SECS` (60 s) in its
health loop. Design philosophy (operator's): **err on caution — contain a
*sustained* or systemic problem, and only auto-act on the small,
exchange-CONFIRMED-benign case.** A detected 1m gap is first run through the
layered backfill (`tape/backfill.py`); only what it can't confidently resolve is
flagged.

### Why a gap needs a distinguisher (not blanket backfill)

Kraken's WS `ohlc` channel only publishes a bar **on activity** — a minute with
zero trades produces no bar, so the tape has a hole. That hole is one of two very
different things, and they must not be conflated:

1. **The exchange was silent** (zero trades). The faithful value is a flat
   carry-forward bar (`O=H=L=C` = prior close, `vol=0`). Filling it is
   *reconstruction*, not fabrication.
2. **We were blind** (disconnected, or a capture bug). Real trades may have
   happened that we missed. Filling *that* with a flat bar would be fabrication.

The backfill tells them apart with two arbiters and tags every fill:

- **Local connectivity proof** — were we connected, per our own `events` log,
  for the whole missing minute? (classifies, and sets how loud a loss is).
- **Kraken public REST OHLC** — the exchange's own record. `vol==0` confirms (1)
  benign silence → write a flat bar tagged `source=1`. `vol>0` proves (2) we lost
  real data → recover the bar tagged `source=2` **and escalate** (a `warning`
  event + ntfy push; missing it *while connected* is flagged as a capture bug).
- **Bounds:** a gap larger than `BACKFILL_MAX_GAP_BARS` (15) smells like an
  outage, not silence — left flagged for review, never auto-filled. Backfill
  **always runs** (it is the conservative, bounded, self-escalating remediation),
  so it can never worsen corruption and is never gated behind the degraded window
  — gating it there deadlocked: the unfilled gap kept the verdict red, which kept
  the freeze on, which kept backfill from clearing the gap.

So benign silent minutes self-heal into a contiguous series; genuine data loss is
recovered *but surfaced loudly*; and anything unconfirmed stays a flagged `gap`
event with the reason attached. Confirmed-silent fills make the bar present, so
they no longer count against the quality verdict; the `backfilled bars` quality
check reports the running `silent-fill` / `REST-recovered` counts for an audit
trail. Run a one-shot fill of the settled window with
`python -m tape.backfill`.

### Sustained-problem escalation

- **Only a SUSTAINED our-side problem escalates** — an `ESCALATE_KEYS` check
  (collector beacon dead, malformed bars, crossed spreads, broken rollups) red
  for ≥ `DQ_SUSTAINED_SECS` (180 s), i.e. not a single blip. On escalation: a
  distinct **critical** ntfy push and a `degraded_start` event marking the
  window; recovery emits `degraded_end`. Exchange-side, self-healing conditions
  (gaps, coverage, a briefly-stale feed during a Kraken maintenance window) are
  deliberately **excluded** — they auto-heal via backfill, so paging on them is
  noise and freezing on them is the deadlock above. The dashboard verdict still
  reflects ALL checks, so a healing gap stays visible while it clears.
- **Backfill only ever ADDS confirmed-or-recovered bars; it never deletes or
  overwrites** (`INSERT OR IGNORE`), so an observed bar always wins.

All of this surfaces in the dashboard **Events** panel.

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

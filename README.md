# market-tape

Reference snapshot of the standalone **Kraken XRP/USD market-tape collector**
(+ its real-time monitor dashboard). This repo is documentation-of-record for
what the system is and how it's wired — **it is not the live deployment**.

> The running collector, dashboard, and hourly backup all execute from
> `/root/xrp_grid` on the box (own systemd units, own SQLite file). This repo
> is a clean copy of that code for reference; nothing here is meant to be run
> from this checkout.

## What's here

```
dashboard.py          # the repurposed root shim → tape.dashboard (served on :5000)
tape/                 # the collector package
  collector.py        # WS callbacks → buffered writer; health beacon; phone alerts
  ws_client.py        # vendored Kraken WS v2 client, extended with trade/book channels
  writer.py           # buffered single-writer thread (WAL, batched executemany)
  schema.py           # SQLite DDL (ohlc_1m / trades / spread / book_l2 / rollup_bars / health)
  rollup.py           # derives 5m/1h/6h/1d bars + order-flow + spread from the 1m base
  config.py           # all knobs (symbol, channels, retention, backup, alerts)
  backup.py           # consistent online-backup snapshot → gzip → GCS
  notify.py           # ntfy phone-alert POST (standalone, no MAGI import)
  dashboard.py        # Flask real-time monitor
  tape-collector.service / tape-backup.service / tape-backup.timer
  README.md           # full operational docs (run, storage, backups, alerts)
requirements.txt      # pinned runtime deps (for reference)
```

Start with **`tape/README.md`** — it's the full operational writeup (what it
records, the storage model, query examples, backups + restore, phone alerts).

## Separation from MAGI (by design)

The collector imports nothing from `magi/`, `grid/`, `observer.py`,
`database.py`, the MAGI root `config.py`, or `scheduler.py`. It writes only to
its own `tape/market_tape.db`. It reuses MAGI's *plumbing* (the `ethobs.uk`
tunnel/login for the dashboard, the `NTFY_TOPIC_URL` ntfy topic for alerts) but
shares no live state and runs whether MAGI is up or down.

## Not in this repo

The live runtime artifacts are intentionally excluded: `market_tape.db*`,
`tape/backups/`, `__pycache__/`, and `.env` (secrets). The collector reads
`NTFY_TOPIC_URL`, `SECRET_KEY`, and `DASHBOARD_PASSWORD` from the box's `.env`.

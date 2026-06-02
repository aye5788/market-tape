"""
tape/ — standalone Kraken market-data recorder.

A self-contained, always-on collector that streams Kraken public WS v2
data (1m OHLC + trade tape + spread) and persists it to its OWN SQLite
file (market_tape.db). It is deliberately decoupled from the MAGI
trading system:

  - imports NOTHING from magi/, grid/, observer.py, database.py,
    config.py (root), scheduler.py — zero code coupling
  - writes only to tape/market_tape.db — never touches observer.db
  - runs as its own systemd service, independent of magi.service

Purpose: build a reality-anchored raw tape for eval/backtest work,
not to drive any live trading decision. Related to MAGI only by sharing
the asset (XRP/USD) and the exchange (Kraken).
"""

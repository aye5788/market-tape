"""
tape/collector.py — the recorder. Wires the Kraken WS callbacks to the
buffered TapeWriter and (optionally) runs the rollup loop in-process.

Run:  python -m tape.collector
Stop: SIGINT/SIGTERM (systemd) — flushes the writer tail before exit.

This process imports ONLY this package + stdlib + websocket. No MAGI
imports, writes only to tape/market_tape.db.
"""
import logging
import re
import signal
import sqlite3
import threading
import time
from datetime import datetime, timezone

from tape import config
from tape import notify
from tape import quality
from tape import rollup
from tape import schema
from tape.writer import TapeWriter
from tape.ws_client import KrakenWebSocketClient

log = logging.getLogger("tape.collector")

# ---- timestamp parsing ----

_FRAC = re.compile(r"\.(\d+)")


def _iso_to_ms(s):
    """RFC3339 (Kraken WS v2) -> epoch milliseconds. Tolerates 'Z' and
    fractional seconds longer than microseconds (book/L3 use nanos);
    Python's fromisoformat only accepts up to 6 fractional digits, so we
    truncate. Returns None on anything unparseable."""
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    m = _FRAC.search(s)
    if m and len(m.group(1)) > 6:
        s = s[: m.start() + 7] + s[m.start() + 1 + len(m.group(1)):]
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _now_ms():
    return int(time.time() * 1000)


_SIDE = {"buy": 0, "sell": 1}
_ORD_TYPE = {"market": 0, "limit": 1}


class Collector:
    def __init__(self):
        self.writer = TapeWriter(
            config.DB_PATH, schema.INSERTS,
            flush_rows=config.FLUSH_ROWS,
            flush_secs=config.FLUSH_SECS,
            queue_max=config.QUEUE_MAX,
        )
        self.client = KrakenWebSocketClient(
            symbols=config.SYMBOLS,
            channels=config.CHANNELS,
            ohlc_interval_min=config.OHLC_INTERVAL_MIN,
            book_depth=config.BOOK_DEPTH,
        )
        self.client.on_ticker = self._on_ticker
        self.client.on_trade = self._on_trade
        self.client.on_ohlc_closed = self._on_ohlc_closed
        self.client.on_book = self._on_book
        self.client.on_state_change = self._on_state_change

        self._stop = threading.Event()
        self._rollup_thread = None
        self._health_thread = None
        self._started_at = None
        # alert state — in-memory only, no MAGI db writes
        self._unhealthy_since = None
        self._down_alerted = False
        self._last_drop_alert_count = 0
        self._last_heartbeat = 0.0   # dead-man's-switch ping cadence
        # event log queue — appended from any thread (ws + health), drained and
        # written by the single health-loop connection (events stay single-writer)
        self._event_lock = threading.Lock()
        self._events = []
        # self-assessment / containment state (flag-only, never mutates data)
        self._last_selfcheck = 0.0
        self._selfcheck_seeded = False     # first pass seeds existing gaps silently
        self._logged_gaps = set()
        self._dq_red_since = None
        self._dq_escalated = False
        self.auto_actions_frozen = False   # set True on sustained problem (gates future backfill)

    # ----- WS callbacks -> writer.put -----

    def _on_ticker(self, e):
        ts = _iso_to_ms(e.get("timestamp")) or _now_ms()
        self.writer.put("spread", (
            ts, e.get("bid"), e.get("bid_qty"),
            e.get("ask"), e.get("ask_qty"), e.get("last"),
        ))

    def _on_trade(self, e):
        tid = e.get("trade_id")
        if tid is None:
            return
        ts = _iso_to_ms(e.get("timestamp")) or _now_ms()
        self.writer.put("trades", (
            int(tid), ts, e.get("price"), e.get("qty"),
            _SIDE.get(e.get("side"), -1), _ORD_TYPE.get(e.get("ord_type"), -1),
        ))

    def _on_ohlc_closed(self, bar):
        # Only the 1m base granularity is recorded raw; coarser is derived.
        if int(bar.get("interval") or 0) != config.OHLC_INTERVAL_MIN:
            return
        ts = _iso_to_ms(bar.get("interval_begin"))
        if ts is None:
            return
        self.writer.put("ohlc_1m", (
            ts, bar.get("open"), bar.get("high"), bar.get("low"),
            bar.get("close"), bar.get("volume"), bar.get("vwap"), bar.get("trades"),
        ))

    def _on_book(self, e, msg_type):
        is_snap = 1 if msg_type == "snapshot" else 0
        ts = _iso_to_ms(e.get("timestamp")) or _now_ms()
        for lvl in (e.get("bids") or []):
            self.writer.put("book_l2", (ts, 0, lvl.get("price"), lvl.get("qty"), is_snap))
        for lvl in (e.get("asks") or []):
            self.writer.put("book_l2", (ts, 1, lvl.get("price"), lvl.get("qty"), is_snap))

    def _on_state_change(self, state, notes):
        log.info("ws state -> %s (%s) reconnects_1h=%d",
                 state, notes, self.client.reconnect_count_1h)
        sev = "info" if state == "connected" else "warning"
        self._emit_event(sev, "ws_state", f"ws {state}" + (f": {notes}" if notes else ""))

    # ----- event log (alerts + ws state changes) -----

    def _emit_event(self, severity, category, message):
        """Queue an event for the health loop to persist. Thread-safe; bounded
        so a stalled drain can't grow it without limit."""
        with self._event_lock:
            self._events.append((_now_ms(), severity, category, (message or "")[:300]))
            if len(self._events) > 500:
                self._events = self._events[-500:]

    def _drain_events(self, conn):
        with self._event_lock:
            batch, self._events = self._events, []
        if batch:
            conn.executemany(
                "INSERT INTO events (ts, severity, category, message) VALUES (?,?,?,?)",
                batch)
            conn.commit()

    # ----- self-assessment / containment (flag-only, NEVER mutates data) -----

    def _detect_gaps(self, conn, now):
        """Find missing 1m bars in the recent SETTLED window (older than
        GAP_SETTLE_SECS, so a reconnect's re-sent bars aren't false-flagged).
        Returns [(gap_start_ms, missing_count)]. Read-only."""
        settle = now - config.GAP_SETTLE_SECS * 1000
        win = now - config.GAP_LOOKBACK_HOURS * 3_600_000
        rows = [r[0] for r in conn.execute(
            "SELECT ts_begin FROM ohlc_1m WHERE ts_begin>=? AND ts_begin<=? ORDER BY ts_begin",
            (win, settle))]
        gaps = []
        for i in range(1, len(rows)):
            missing = (rows[i] - rows[i - 1]) // 60_000 - 1
            if missing > 0:
                gaps.append((rows[i - 1] + 60_000, int(missing)))
        return gaps

    def _self_assess(self, conn):
        """Periodic data-quality self-grade. FLAG-ONLY containment — never
        mutates collected data. Logs newly-detected 1m gaps as events, and
        escalates ONLY a SUSTAINED red verdict (not a single blip): a distinct
        critical alert + a degraded-window marker + freezing auto-actions.
        Recovery clears it. The philosophy: small benign gaps are tolerated and
        merely flagged; a sustained/systemic problem makes us LESS aggressive
        (freeze + escalate), not more."""
        now = _now_ms()

        # --- flag-only gap detection (no backfill) ---
        gaps = self._detect_gaps(conn, now)
        if not self._selfcheck_seeded:
            # first pass: adopt existing gaps silently so we only flag NEW ones
            self._logged_gaps.update(g[0] for g in gaps)
            self._selfcheck_seeded = True
        else:
            for gap_start, missing in gaps:
                if gap_start in self._logged_gaps:
                    continue
                self._logged_gaps.add(gap_start)
                t0 = time.strftime("%H:%M", time.gmtime(gap_start / 1000))
                t1 = time.strftime("%H:%M", time.gmtime((gap_start + missing * 60_000) / 1000))
                self._emit_event("warning", "gap",
                                 f"1m gap: {missing} bar(s) missing {t0}-{t1} UTC "
                                 f"(flagged, not backfilled)")
        if len(self._logged_gaps) > 1000:
            self._logged_gaps = set(sorted(self._logged_gaps)[-1000:])

        # --- sustained-problem detection on the quality verdict ---
        try:
            rep = quality.report(conn, now)
        except Exception as e:
            log.warning("self-assess quality report failed: %r", e)
            return
        if rep.get("verdict") == "red":
            if self._dq_red_since is None:
                self._dq_red_since = time.time()
            red_for = time.time() - self._dq_red_since
            if red_for >= config.DQ_SUSTAINED_SECS and not self._dq_escalated:
                failing = ", ".join(c["label"] for c in rep.get("checks", [])
                                    if c.get("status") == "red") or "unknown"
                self.auto_actions_frozen = True
                self._dq_escalated = True
                self._emit_event("critical", "degraded_start",
                                 f"SUSTAINED data-quality problem ({int(red_for)}s): {failing} "
                                 f"— auto-actions FROZEN, window marked degraded")
                notify.send("Tape: SUSTAINED data-quality problem",
                            f"[CRITICAL] red {int(red_for)}s: {failing} -> open dashboard",
                            "critical")
                log.error("SUSTAINED data-quality problem: %s (frozen)", failing)
        else:
            if self._dq_escalated:
                self._emit_event("warning", "degraded_end",
                                 "data quality recovered — auto-actions unfrozen")
                notify.send("Tape: data quality RECOVERED",
                            "[OK] quality back to non-red", "warning")
                log.info("data quality recovered — unfrozen")
            self._dq_red_since = None
            self._dq_escalated = False
            self.auto_actions_frozen = False

    # ----- health beacon (read by the dashboard, a separate process) -----

    def _write_health(self, conn):
        conn.execute(
            "INSERT OR REPLACE INTO collector_health "
            "(id, ts, ws_state, last_msg_age_sec, reconnects_1h, "
            " rows_written, rows_dropped, started_at) "
            "VALUES (1,?,?,?,?,?,?,?)",
            (_now_ms(), self.client.state, self.client.last_message_age_sec,
             self.client.reconnect_count_1h, self.writer.written,
             self.writer.dropped, self._started_at),
        )
        conn.commit()

    def _health_loop(self):
        conn = sqlite3.connect(config.DB_PATH)
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            self._write_health(conn)  # write once immediately
            self._maybe_heartbeat()
            while not self._stop.wait(config.HEALTH_EVERY_SECS):
                try:
                    self._write_health(conn)
                except Exception as e:
                    log.warning("health write failed: %r", e)
                try:
                    self._check_alerts()
                except Exception as e:
                    log.warning("alert check failed: %r", e)
                try:
                    self._maybe_heartbeat()
                except Exception as e:
                    log.debug("heartbeat failed: %r", e)
                try:
                    if time.time() - self._last_selfcheck >= config.SELFCHECK_EVERY_SECS:
                        self._self_assess(conn)
                        self._last_selfcheck = time.time()
                except Exception as e:
                    log.warning("self-assess failed: %r", e)
                try:
                    self._drain_events(conn)
                except Exception as e:
                    log.warning("event drain failed: %r", e)
        finally:
            try:
                self._drain_events(conn)   # flush tail on shutdown
            except Exception:
                pass
            conn.close()

    def _maybe_heartbeat(self):
        """Ping the external dead-man's-switch on a fixed cadence while the
        process is alive. UNCONDITIONAL: it proves the PROCESS is running (feed
        problems are alerted separately via _check_alerts), so the daily Kraken
        reconnect doesn't suppress it. No-op until HEALTHCHECK_PING_URL is set."""
        now = time.time()
        if now - self._last_heartbeat >= config.HEALTHCHECK_EVERY_SECS:
            notify.heartbeat()
            self._last_heartbeat = now

    def _check_alerts(self):
        """Edge-triggered phone alerts via the shared ntfy topic. Fires once
        per outage (not every beat), and a recovery note when it clears."""
        if not config.ALERT_ENABLED:
            return
        now = time.time()
        st = self.client.state
        age = self.client.last_message_age_sec
        unhealthy = (st != "connected") or (age is None) or (age > config.ALERT_STALE_SECS)

        if unhealthy:
            if self._unhealthy_since is None:
                self._unhealthy_since = now
            down_for = now - self._unhealthy_since
            if down_for >= config.ALERT_DOWN_GRACE_SECS and not self._down_alerted:
                age_str = f"{int(age)}s" if age is not None else "n/a"
                notify.send(
                    "Tape collector: FEED DOWN",
                    f"[CRITICAL] ws={st} last_data={age_str} "
                    f"down~{int(down_for)}s -> open dashboard",
                    "critical",
                )
                self._down_alerted = True
                self._emit_event("critical", "feed_down",
                                 f"feed down: ws={st} last_data={age_str} for ~{int(down_for)}s")
                log.error("ALERT: feed down (ws=%s age=%s) — pushed", st, age)
        else:
            if self._down_alerted:
                notify.send(
                    "Tape collector: RECOVERED",
                    "[OK] ws connected, data flowing again",
                    "warning",
                )
                log.info("ALERT: feed recovered — pushed")
                self._emit_event("warning", "recovered", "feed recovered — data flowing again")
            self._unhealthy_since = None
            self._down_alerted = False

        dropped = self.writer.dropped
        if dropped - self._last_drop_alert_count >= config.ALERT_DROP_THRESHOLD:
            notify.send(
                "Tape collector: DROPPING ROWS",
                f"[CRITICAL] writer dropped {dropped} rows (queue full)",
                "critical",
            )
            self._last_drop_alert_count = dropped
            self._emit_event("critical", "dropping", f"writer dropped {dropped} rows (queue full)")
            log.error("ALERT: writer dropping (%d) — pushed", dropped)

    # ----- rollup loop (optional, in-process) -----

    def _rollup_loop(self):
        while not self._stop.wait(config.ROLLUP_EVERY_SECS):
            try:
                rollup.run_once(
                    config.DB_PATH, config.ROLLUP_INTERVALS_MIN,
                    config.ROLLUP_LOOKBACK_HOURS, config.RAW_RETENTION_DAYS,
                )
            except Exception as e:
                log.exception("rollup run failed: %s", e)

    # ----- lifecycle -----

    def start(self):
        schema.init_db(config.DB_PATH)
        self._started_at = _now_ms()
        self.writer.start()
        self.client.start()
        self._health_thread = threading.Thread(
            target=self._health_loop, name="tape_health", daemon=True)
        self._health_thread.start()
        if config.ROLLUP_IN_PROCESS:
            self._rollup_thread = threading.Thread(
                target=self._rollup_loop, name="tape_rollup", daemon=True)
            self._rollup_thread.start()
            log.info("in-process rollup loop started (every %ds)", config.ROLLUP_EVERY_SECS)
        log.info("collector running — channels=%s symbol=%s db=%s",
                 config.CHANNELS, config.SYMBOL, config.DB_PATH)

    def run_forever(self):
        self.start()

        def _handle_signal(signum, frame):
            log.info("signal %s received — shutting down", signum)
            self._stop.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        while not self._stop.is_set():
            self._stop.wait(1.0)

        self.client.shutdown()
        self.writer.stop()
        log.info("collector stopped cleanly")


def main():
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    Collector().run_forever()


if __name__ == "__main__":
    main()

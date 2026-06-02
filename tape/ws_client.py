"""
tape/ws_client.py — Kraken WebSocket v2 public-channel client.

VENDORED from grid/exchanges/kraken_ws.py and EXTENDED with the `trade`
(and optional `book`) channels the MAGI gate client doesn't have. It is a
copy on purpose: the collector is decoupled from the trading code, so this
client evolves independently and shares no state with it. Public channels
only (ticker, trade, ohlc, book, status, heartbeat) — no auth required.

Design (unchanged from the original):
- Library: websocket-client (sync, threaded) — start() spawns a worker
  thread running run_forever(); writes go through the thread-safe sqlite3
  module downstream.
- Reconnect backoff floor 5s (Kraken docs + Cloudflare ban threshold).
- Heartbeat: ANY inbound message resets the liveness timer; >10s silent
  forces a reconnect. Optional app-level ping during quiet markets.
- Subscribe-on-connect, gated by status.system == online/cancel_only/
  post_only ('maintenance' defers).

Public surface (consumed by tape/collector.py):
  client = KrakenWebSocketClient(symbols=["XRP/USD"],
                                 channels=("ticker","trade","ohlc"))
  client.on_ticker = lambda entry: ...
  client.on_trade  = lambda entry: ...
  client.on_ohlc_closed = lambda closed_bar: ...
  client.on_book   = lambda entry, msg_type: ...   # only if "book" subscribed
  client.on_state_change = lambda state, notes: ...
  client.start(); client.shutdown()
  client.state  # 'starting'|'connected'|'reconnecting'|'disconnected'
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Callable, Optional

import websocket  # websocket-client

log = logging.getLogger("tape.ws")

KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"

RECONNECT_BACKOFF_SEC = [5, 5, 10, 20, 30]
HEARTBEAT_DEAD_THRESHOLD_SEC = 10.0
PING_INTERVAL_SEC = 5.0
PING_QUIET_SEC = 3.0
RECONNECT_HISTORY_CAP = 200
DEFAULT_OHLC_INTERVAL_MIN = 1


class KrakenWebSocketClient:
    def __init__(
        self,
        symbols: list,
        channels=("ticker", "trade", "ohlc"),
        ohlc_interval_min: int = DEFAULT_OHLC_INTERVAL_MIN,
        book_depth: int = 10,
        url: str = KRAKEN_WS_V2_URL,
    ):
        self.symbols = list(symbols)
        self.channels = tuple(channels)
        self.ohlc_interval_min = int(ohlc_interval_min)
        self.book_depth = int(book_depth)
        self.url = url

        # Callback slots — set by the collector before start()
        self.on_ticker: Optional[Callable[[dict], None]] = None
        self.on_trade: Optional[Callable[[dict], None]] = None
        self.on_ohlc_update: Optional[Callable[[dict], None]] = None
        self.on_ohlc_closed: Optional[Callable[[dict], None]] = None
        self.on_book: Optional[Callable[[dict, str], None]] = None
        self.on_status: Optional[Callable[[dict], None]] = None
        self.on_state_change: Optional[Callable[[str, str], None]] = None

        # State exposed to callers
        self.state: str = "starting"
        self.last_message_time: float = 0.0
        self.last_tick: Optional[dict] = None
        self._closed_bars: dict = {}

        # Internal
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._reconnect_attempt = 0
        self._reconnect_history: deque = deque(maxlen=RECONNECT_HISTORY_CAP)
        self._inflight_ohlc: dict = {}

        self._subscribed = False
        self._system_status: Optional[str] = None

    # ----- lifecycle -----

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            log.warning("start() called twice; ignoring")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever_loop,
                                        name="tape_ws_main", daemon=True)
        self._thread.start()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_watcher,
                                                  name="tape_ws_heartbeat", daemon=True)
        self._heartbeat_thread.start()
        log.info("KrakenWebSocketClient started (symbols=%s channels=%s)",
                 self.symbols, self.channels)

    def shutdown(self, timeout: float = 5.0):
        log.info("shutdown() called")
        self._stop_event.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception as e:
            log.warning("ws.close() during shutdown raised: %r", e)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1.0)
        self._set_state("disconnected", "shutdown")

    # ----- public properties -----

    @property
    def last_message_age_sec(self) -> Optional[float]:
        if self.last_message_time <= 0:
            return None
        return time.time() - self.last_message_time

    @property
    def reconnect_count_1h(self) -> int:
        cutoff = time.time() - 3600.0
        return sum(1 for t in self._reconnect_history if t >= cutoff)

    def last_closed_bar(self, interval_min: int) -> Optional[dict]:
        return self._closed_bars.get(int(interval_min))

    # ----- main loop -----

    def _run_forever_loop(self):
        while not self._stop_event.is_set():
            try:
                backoff = self._current_backoff()
                if self._reconnect_attempt > 0:
                    log.info("ws reconnect attempt %d after %ds",
                             self._reconnect_attempt, backoff)
                    self._set_state("reconnecting",
                                    f"attempt={self._reconnect_attempt} backoff={backoff}s")
                    end_t = time.time() + backoff
                    while time.time() < end_t and not self._stop_event.is_set():
                        time.sleep(0.5)
                    if self._stop_event.is_set():
                        break

                self._subscribed = False
                self._system_status = None
                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=0, skip_utf8_validation=True)
            except Exception as e:
                log.exception("ws run_forever raised: %s", e)
            if not self._stop_event.is_set():
                self._reconnect_history.append(time.time())
                self._reconnect_attempt += 1
        self._set_state("disconnected", "main loop exited")

    def _current_backoff(self) -> int:
        if self._reconnect_attempt <= 0:
            return 0
        idx = min(self._reconnect_attempt - 1, len(RECONNECT_BACKOFF_SEC) - 1)
        return RECONNECT_BACKOFF_SEC[idx]

    def _heartbeat_watcher(self):
        while not self._stop_event.is_set():
            try:
                time.sleep(1.0)
                if self.state != "connected":
                    continue
                age = self.last_message_age_sec
                if age is None:
                    continue
                if age > HEARTBEAT_DEAD_THRESHOLD_SEC:
                    log.warning("no inbound for %.1fs > %.1fs — forcing reconnect",
                                age, HEARTBEAT_DEAD_THRESHOLD_SEC)
                    try:
                        if self._ws is not None:
                            self._ws.close()
                    except Exception:
                        pass
                    continue
                if age > PING_QUIET_SEC:
                    self._send_ping()
            except Exception as e:
                log.warning("heartbeat watcher iteration raised: %r", e)

    def _send_ping(self):
        if self._ws is None:
            return
        try:
            self._ws.send(json.dumps({"method": "ping", "req_id": int(time.time())}))
        except Exception as e:
            log.warning("ping send failed: %r", e)

    # ----- handlers -----

    def _on_open(self, ws):
        log.info("ws connected to %s", self.url)
        self.last_message_time = time.time()
        self._set_state("connected", "ws_open")

    def _on_message(self, ws, raw):
        self.last_message_time = time.time()
        try:
            msg = json.loads(raw)
        except Exception as e:
            log.warning("could not parse json: %r raw=%s", e, str(raw)[:200])
            return

        channel = msg.get("channel")
        if channel == "status":
            self._handle_status(msg)
            return
        if channel == "heartbeat":
            return
        if channel == "ticker":
            self._handle_ticker(msg)
            return
        if channel == "trade":
            self._handle_trade(msg)
            return
        if channel == "ohlc":
            self._handle_ohlc(msg)
            return
        if channel == "book":
            self._handle_book(msg)
            return
        if msg.get("method") == "pong":
            return
        if msg.get("method") == "subscribe":
            self._handle_subscribe_ack(msg)
            return
        log.debug("unrecognised message: %s", str(msg)[:200])

    def _on_error(self, ws, err):
        log.warning("ws on_error: %r", err)

    def _on_close(self, ws, code, reason):
        log.info("ws closed code=%s reason=%s", code, reason)
        self._subscribed = False

    # ----- channel handlers -----

    def _handle_status(self, msg):
        data = msg.get("data") or []
        if not data:
            return
        entry = data[0]
        self._system_status = entry.get("system")
        if self.on_status:
            try:
                self.on_status(entry)
            except Exception as e:
                log.exception("on_status callback raised: %s", e)
        log.info("ws status: system=%s api_version=%s connection_id=%s",
                 entry.get("system"), entry.get("api_version"),
                 entry.get("connection_id"))
        if self._system_status in ("online", "cancel_only", "post_only") and not self._subscribed:
            self._subscribe_all()
        elif self._system_status == "maintenance":
            log.warning("status=maintenance — deferring subscribe until status changes")

    def _subscribe_all(self):
        log.info("subscribing channels=%s for %s", self.channels, self.symbols)
        try:
            for ch in self.channels:
                params = {"channel": ch, "symbol": self.symbols}
                if ch == "ohlc":
                    params["interval"] = self.ohlc_interval_min
                elif ch == "trade":
                    params["snapshot"] = True
                elif ch == "book":
                    params["depth"] = self.book_depth
                self._ws.send(json.dumps({"method": "subscribe", "params": params}))
            self._subscribed = True
            # Working connection + subscribes sent — reset backoff.
            self._reconnect_attempt = 0
        except Exception as e:
            log.exception("subscribe send raised: %s", e)

    def _handle_subscribe_ack(self, msg):
        ok = msg.get("success")
        log.info("subscribe ack: channel=%s success=%s",
                 (msg.get("result") or {}).get("channel"), ok)

    def _handle_ticker(self, msg):
        for entry in (msg.get("data") or []):
            self.last_tick = entry
            if self.on_ticker:
                try:
                    self.on_ticker(entry)
                except Exception as e:
                    log.exception("on_ticker callback raised: %s", e)

    def _handle_trade(self, msg):
        if not self.on_trade:
            return
        for entry in (msg.get("data") or []):
            try:
                self.on_trade(entry)
            except Exception as e:
                log.exception("on_trade callback raised: %s", e)

    def _handle_book(self, msg):
        if not self.on_book:
            return
        mtype = msg.get("type")  # 'snapshot' | 'update'
        for entry in (msg.get("data") or []):
            try:
                self.on_book(entry, mtype)
            except Exception as e:
                log.exception("on_book callback raised: %s", e)

    def _handle_ohlc(self, msg):
        data = msg.get("data") or []
        msg_type = msg.get("type")  # 'snapshot' or 'update'
        for entry in data:
            interval = int(entry.get("interval") or 0)
            iv_begin = entry.get("interval_begin")
            if not interval or not iv_begin:
                continue
            prior = self._inflight_ohlc.get(interval)
            if msg_type == "snapshot":
                if prior is None or iv_begin > prior.get("interval_begin", ""):
                    if prior is not None and iv_begin > prior.get("interval_begin", ""):
                        self._surface_closed(interval, prior)
                    self._inflight_ohlc[interval] = entry
                continue
            if prior is None:
                self._inflight_ohlc[interval] = entry
                continue
            if iv_begin == prior.get("interval_begin"):
                self._inflight_ohlc[interval] = entry
                if self.on_ohlc_update:
                    try:
                        self.on_ohlc_update(entry)
                    except Exception as e:
                        log.exception("on_ohlc_update callback raised: %s", e)
            elif iv_begin > prior.get("interval_begin", ""):
                self._surface_closed(interval, prior)
                self._inflight_ohlc[interval] = entry
            else:
                log.debug("ohlc out-of-order: msg iv_begin=%s < inflight=%s",
                          iv_begin, prior.get("interval_begin"))

    def _surface_closed(self, interval_min: int, closed_bar: dict):
        self._closed_bars[interval_min] = closed_bar
        log.info("ohlc bar closed: interval=%dmin begin=%s close=%s",
                 interval_min, closed_bar.get("interval_begin"), closed_bar.get("close"))
        if self.on_ohlc_closed:
            try:
                self.on_ohlc_closed(closed_bar)
            except Exception as e:
                log.exception("on_ohlc_closed callback raised: %s", e)

    # ----- state transition -----

    def _set_state(self, new_state: str, notes: str = ""):
        if new_state == self.state:
            return
        old = self.state
        self.state = new_state
        log.info("ws state: %s -> %s (%s)", old, new_state, notes)
        if self.on_state_change:
            try:
                self.on_state_change(new_state, notes)
            except Exception as e:
                log.exception("on_state_change callback raised: %s", e)

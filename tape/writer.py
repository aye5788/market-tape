"""
tape/writer.py — buffered single-writer for the market tape.

The streaming callbacks must never block on disk, and SQLite allows only
one writer at a time. So every ingest path calls put() (non-blocking,
enqueue-and-return) and a single daemon thread drains the queue, batching
rows into one transaction per flush — one fsync per flush instead of one
per row. That single change is what lets plain SQLite absorb a tick feed.

WAL + synchronous=NORMAL: readers (rollup, backtests) never block the
writer; durability is preserved across an app crash, only the last
unflushed transaction is at risk on an OS/power crash — the right trade
for a market tape.
"""
import logging
import queue
import sqlite3
import threading
import time

log = logging.getLogger("tape.writer")


class TapeWriter:
    def __init__(self, db_path, inserts, flush_rows=500, flush_secs=1.0,
                 queue_max=100_000):
        self.db_path = db_path
        self.inserts = inserts
        self.flush_rows = flush_rows
        self.flush_secs = flush_secs
        self.q = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._dropped = 0
        self._written = 0
        self._t = threading.Thread(target=self._run, name="tape_writer", daemon=True)

    @property
    def written(self):
        return self._written

    @property
    def dropped(self):
        return self._dropped

    def start(self):
        self._t.start()
        log.info("TapeWriter started -> %s", self.db_path)

    def put(self, table, row):
        """Non-blocking enqueue, called from WS callbacks. If the queue is
        full we drop rather than block the socket thread — a tape gap is
        preferable to backpressuring the feed."""
        try:
            self.q.put_nowait((table, row))
        except queue.Full:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                log.warning("writer queue full — dropped %d rows so far", self._dropped)

    def stop(self, timeout=10.0):
        self._stop.set()
        self._t.join(timeout=timeout)
        log.info("TapeWriter stopped (written=%d dropped=%d)", self._written, self._dropped)

    # ----- writer thread -----

    def _run(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        buf = {}
        n = 0
        last = time.monotonic()

        def flush():
            nonlocal buf, n, last
            if not n:
                return
            with conn:  # single transaction -> single fsync
                for table, rows in buf.items():
                    if rows:
                        conn.executemany(self.inserts[table], rows)
            self._written += n
            buf = {}
            n = 0
            last = time.monotonic()

        while not self._stop.is_set():
            try:
                table, row = self.q.get(timeout=self.flush_secs)
                buf.setdefault(table, []).append(row)
                n += 1
            except queue.Empty:
                pass
            if n and (n >= self.flush_rows or time.monotonic() - last >= self.flush_secs):
                try:
                    flush()
                except Exception as e:
                    log.exception("flush failed: %s", e)

        # Shutdown: drain whatever is left, then a final flush so we don't
        # lose the tail on a graceful stop.
        try:
            while True:
                table, row = self.q.get_nowait()
                buf.setdefault(table, []).append(row)
                n += 1
        except queue.Empty:
            pass
        try:
            flush()
        except Exception as e:
            log.exception("final flush failed: %s", e)
        conn.close()

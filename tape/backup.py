"""
tape/backup.py — consistent snapshot of market_tape.db -> gzip -> GCS.

Run by tape-backup.timer (hourly). Uses SQLite's ONLINE BACKUP API
(Connection.backup) so the snapshot is consistent even while the collector
is actively writing — never a plain file copy (a cp of a live WAL database
can be torn/unopenable).

Flow each run:
  1. online-backup market_tape.db -> a temp .db
  2. gzip it into tape/backups/market_tape_<UTC>.db.gz
  3. gsutil cp it to gs://<bucket>/<prefix>/
  4. keep the last BACKUP_LOCAL_KEEP local copies (fast restore), prune older

Off-box durability (droplet/disk loss) is the GCS copy; the local copies
cover corruption / accidental deletion with an instant restore.

Standalone:  python -m tape.backup
"""
import gzip
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone

from tape import config

log = logging.getLogger("tape.backup")

# The dashboard reads this (in BACKUP_LOCAL_DIR) for the Backup & Durability
# panel — so it shows GCS-CONFIRMED status without a per-poll network call.
STATUS_FILE = ".last_backup.json"


def _write_status(name, gz_path, gcs_ok, remote, error=None):
    """Record the outcome of this run for the dashboard. Best-effort."""
    try:
        d = config.BACKUP_LOCAL_DIR
        local = [f for f in os.listdir(d)
                 if f.startswith("market_tape_") and f.endswith(".db.gz")]
        status = {
            "ts_ms": int(time.time() * 1000),
            "name": name,
            "bytes": os.path.getsize(gz_path) if os.path.exists(gz_path) else None,
            "gcs_ok": gcs_ok,
            "remote": remote,
            "local_count": len(local),
        }
        if error:
            status["error"] = error[:200]
        tmp = os.path.join(d, STATUS_FILE + ".tmp")
        with open(tmp, "w") as fh:
            json.dump(status, fh)
        os.replace(tmp, os.path.join(d, STATUS_FILE))
    except Exception as e:
        log.warning("status write failed: %r", e)


def _gsutil():
    return (getattr(config, "GSUTIL_BIN", None) or shutil.which("gsutil")
            or "/root/google-cloud-sdk/bin/gsutil")


def _snapshot(src_path, dst_path):
    """Consistent online backup of a live SQLite DB."""
    src = sqlite3.connect(src_path)
    try:
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)          # online backup API — atomic, copy-on-read
        finally:
            dst.close()
    finally:
        src.close()


def _prune_local():
    d = config.BACKUP_LOCAL_DIR
    files = sorted(
        (os.path.join(d, f) for f in os.listdir(d)
         if f.startswith("market_tape_") and f.endswith(".db.gz")),
        reverse=True,
    )
    for old in files[config.BACKUP_LOCAL_KEEP:]:
        try:
            os.remove(old)
            log.info("pruned local %s", os.path.basename(old))
        except Exception as e:
            log.warning("prune failed %s: %r", old, e)


def run_once():
    if not os.path.exists(config.DB_PATH):
        log.error("source DB does not exist: %s", config.DB_PATH)
        sys.exit(2)

    os.makedirs(config.BACKUP_LOCAL_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = f"market_tape_{ts}.db.gz"
    gz_path = os.path.join(config.BACKUP_LOCAL_DIR, name)
    tmp_db = gz_path[:-3] + ".tmp"

    # 1) consistent snapshot
    _snapshot(config.DB_PATH, tmp_db)

    # 2) gzip
    try:
        with open(tmp_db, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
    finally:
        if os.path.exists(tmp_db):
            os.remove(tmp_db)
    size = os.path.getsize(gz_path)
    log.info("snapshot -> %s (%.2f MB)", gz_path, size / 1e6)

    # 3) upload to GCS
    remote = f"{config.BACKUP_BUCKET}/{config.BACKUP_GCS_PREFIX}/{name}"
    rc = subprocess.run([_gsutil(), "-q", "cp", gz_path, remote],
                        capture_output=True, text=True)
    if rc.returncode != 0:
        log.error("gsutil upload failed (rc=%d): %s", rc.returncode, rc.stderr.strip())
        _prune_local()          # local snapshot still kept; surface failure to systemd
        _write_status(name, gz_path, False, remote, error=rc.stderr.strip())
        sys.exit(1)
    log.info("uploaded -> %s", remote)

    # 4) prune local rolling copies
    _prune_local()
    _write_status(name, gz_path, True, remote)


def main():
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    run_once()


if __name__ == "__main__":
    main()

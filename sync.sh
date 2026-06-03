#!/bin/bash
# Refresh the market-tape REFERENCE repo from the live code in /root/xrp_grid
# and push to GitHub. The live deployment is the source of truth; this repo is
# a clean code-only mirror. Safe to run anytime — no-op if nothing changed.
# Run: bash /root/market-tape/sync.sh   (or the `tape-sync` alias)
set -e

SRC=/root/xrp_grid
DST=/root/market-tape

cd "$DST"

# 1. mirror the tape package — code only, drop runtime artifacts. --delete so a
#    file removed from the live tree is removed here too.
rsync -a --delete \
  --exclude='market_tape.db' --exclude='market_tape.db-wal' \
  --exclude='market_tape.db-shm' --exclude='backups/' \
  --exclude='history.db' --exclude='history.db-wal' \
  --exclude='history.db-shm' --exclude='history.db.snap.*' \
  --exclude='__pycache__/' --exclude='*.pyc' \
  "$SRC/tape/" "$DST/tape/"

# 2. the repurposed root dashboard shim
cp "$SRC/dashboard.py" "$DST/dashboard.py"

git add -A

if git diff --cached --quiet; then
  echo "market-tape: already up to date — nothing to push."
  exit 0
fi

git commit -m "sync from live xrp_grid tape $(date -u +'%Y-%m-%d %H:%M UTC')"
git push origin main
echo "Pushed to github.com/aye5788/market-tape."
git --no-pager log --oneline -1

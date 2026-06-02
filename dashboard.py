"""
dashboard.py — REPURPOSED to serve the market-tape collector monitor.

The original MAGI dashboard (3704 lines) is archived intact at
    archive/magi_dashboard_2026-06-02/dashboard.py
To return to MAGI, copy that file back over this one:
    cp archive/magi_dashboard_2026-06-02/dashboard.py dashboard.py

This is a thin shim so the existing plumbing is reused unchanged: the same
magi-dashboard.service on :5000, behind the same ethobs.uk cloudflared
tunnel, behind the same login (it reuses SECRET_KEY / DASHBOARD_PASSWORD
from .env, so existing session cookies stay valid). `python -m dashboard`
keeps working with no service-file edit — it just serves the tape monitor
now. All rendering logic lives in tape/dashboard.py; it reads only
market_tape.db (plus those two env vars for auth) and imports no MAGI code.
"""
from tape.dashboard import app, main  # noqa: F401  (app re-exported for WSGI use)

if __name__ == "__main__":
    main()

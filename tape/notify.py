"""
tape/notify.py — phone alerts for the collector.

Reuses the SAME ntfy connection MAGI used: the topic URL in .env
(NTFY_TOPIC_URL) and the subscription already on the operator's phone.
With MAGI off, that topic is idle, so the collector posts to it directly.

Kept as a ~dozen-line POST (not `from magi.notify import ...`) so the tape
package stays standalone and liftable — but it is the same topic, so the
push lands on the same phone. Same severity→priority mapping and the same
public-topic discipline (short body, no secrets) as magi/notify.py.

Fail-silent: a notification never raises and never blocks the caller.
"""
import logging
import os

import requests

try:
    from dotenv import load_dotenv
except Exception:  # dotenv optional
    load_dotenv = None

log = logging.getLogger("tape.notify")

_TIMEOUT_SEC = 3
_BODY_MAX = 200
_PRIORITY = {"critical": 5, "warning": 3, "warn": 3}  # info → not sent


def send(title, body, severity="critical"):
    """Fire a push to the shared ntfy topic. Returns True on HTTP 2xx,
    False otherwise (unset topic, non-2xx, or any error). Never raises."""
    if severity not in _PRIORITY:
        return False
    if load_dotenv:
        load_dotenv("/root/xrp_grid/.env", override=False)
    url = (os.environ.get("NTFY_TOPIC_URL") or "").strip()
    if not url:
        return False
    try:
        r = requests.post(
            url,
            data=(body or "")[:_BODY_MAX].encode("utf-8"),
            headers={
                "Title": (title or "Tape collector")[:120],
                "Priority": str(_PRIORITY[severity]),
                "Tags": "tape",  # distinguishes collector pushes from MAGI's
            },
            timeout=_TIMEOUT_SEC,
        )
        if 200 <= r.status_code < 300:
            return True
        log.warning("ntfy non-2xx: %s %s", r.status_code, r.text[:200])
        return False
    except Exception as e:
        log.warning("ntfy send failed: %r", e)
        return False


def heartbeat():
    """Ping the external dead-man's-switch at HEALTHCHECK_PING_URL (.env).

    Same env-driven, fail-silent pattern as send(): a GET the collector fires
    while it's alive. An external monitor (healthchecks.io free tier) pages the
    operator when the pings STOP — catching total collector/box death, which the
    in-process ntfy alert cannot. No-op (returns False) if the URL is unset, so
    this stays inert until a check is created. Never raises, never blocks long."""
    if load_dotenv:
        load_dotenv("/root/xrp_grid/.env", override=False)
    url = (os.environ.get("HEALTHCHECK_PING_URL") or "").strip()
    if not url:
        return False
    try:
        r = requests.get(url, timeout=_TIMEOUT_SEC)
        return 200 <= r.status_code < 300
    except Exception as e:
        log.debug("heartbeat ping failed: %r", e)
        return False

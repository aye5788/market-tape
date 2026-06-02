"""
tape/conditions.py — GRID CONDITIONS: advisory market-analytics for the grid.

Read-only, pure stdlib (no pandas/numpy, no MAGI imports). Computes a few
grid-relevant signals from data the collector already stores, each anchored to
the grid's REAL exogenous parameters (1.5% spacing, 0.50% maker fee floor,
0.3–2.5% clamps) — NOT fitted to the data. This is DECISION SUPPORT: it
enforces nothing, it just surfaces whether conditions favour an adaptive grid.

Signals:
  - hourly volatility   : realized σ (from 1m log-returns) + a vol-tracking
                          suggested spacing, clamped; flags "too quiet to clear
                          fees" vs "enough movement".
  - regime              : efficiency ratio + net move → choppy/mean-reverting
                          (grid-favourable) vs trending (grid bleeds). This is
                          the grid-downtrend-bleed early warning.
  - flow imbalance      : buy vs sell aggressor volume — one-sided pressure.
  - harvest rate        : fraction of completed 1h buckets that actually swung
                          ≥ the spacing — the direct "is there anything to
                          harvest" measure.

CLI: python -m tape.conditions
"""
import math
import sqlite3
import time

from tape import config

# --- the grid's REAL parameters (mirror CLAUDE.md / MAGI config; exogenous) ---
SPACING_PCT     = 0.015    # XRP optimal grid spacing
FEE_FLOOR_PCT   = 0.005    # 2 x maker 0.25% = per-level round-trip break-even
MIN_SPACING_PCT = 0.003    # config.py clamp
MAX_SPACING_PCT = 0.025    # config.py clamp

WINDOW_HOURS = 24
FLOW_HOURS   = 6
MIN_BARS     = 20          # below this the metrics are too noisy to report

_RANK = {"green": 0, "gray": 0, "yellow": 1, "red": 2}
_VERDICT = {0: "green", 1: "yellow", 2: "red"}
_DRIVERS = ("hourly volatility", "regime", "harvest rate")  # flow is context only


def report(conn, now_ms=None, window_hours=WINDOW_HOURS):
    now = now_ms or int(time.time() * 1000)
    win = now - window_hours * 3_600_000

    bars = conn.execute(
        "SELECT open, high, low, close FROM ohlc_1m WHERE ts_begin>=? ORDER BY ts_begin",
        (win,)).fetchall()
    closes = [b[3] for b in bars if b[3]]
    if len(closes) < MIN_BARS:
        return {"verdict": "gray", "advisory": True, "window_hours": window_hours,
                "metrics": [{"label": "status", "detail": "warming up — need more 1m bars",
                             "status": "gray"}]}

    metrics = []

    # ---- realized hourly volatility + vol-tracking suggested spacing ----
    rets = [math.log(closes[i] / closes[i - 1])
            for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    mean = sum(rets) / len(rets)
    sigma_1m = math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets))
    sigma_hr = sigma_1m * math.sqrt(60)
    sugg = min(max(sigma_hr, MIN_SPACING_PCT), MAX_SPACING_PCT)
    vol_status = ("red" if sigma_hr < FEE_FLOOR_PCT
                  else "yellow" if sigma_hr < SPACING_PCT else "green")
    metrics.append({"label": "hourly volatility", "status": vol_status,
                    "detail": f"{sigma_hr*100:.2f}% · suggested spacing {sugg*100:.2f}% "
                              f"(clamp 0.3–2.5%, fee floor 0.50%)",
                    "value": round(sigma_hr * 100, 3)})

    # ---- regime: efficiency ratio + net move over the window ----
    net = closes[-1] - closes[0]
    path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
    er = abs(net) / path if path else 0.0
    net_pct = (net / closes[0] * 100) if closes[0] else 0.0
    if er < 0.30:
        reg_status, reg_word = "green", "choppy / mean-reverting"
    elif er < 0.50:
        reg_status, reg_word = "yellow", "mixed"
    else:
        reg_status, reg_word = "red", "trending"
    metrics.append({"label": "regime", "status": reg_status,
                    "detail": f"{reg_word} · ER {er:.2f} · "
                              f"{'↑' if net >= 0 else '↓'}{abs(net_pct):.2f}%/{window_hours}h",
                    "value": round(er, 3)})

    # ---- flow imbalance (aggressor buy vs sell, recent) ----
    buy, sell = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN side=0 THEN qty END),0), "
        "       COALESCE(SUM(CASE WHEN side=1 THEN qty END),0) "
        "FROM trades WHERE ts>=?", (now - FLOW_HOURS * 3_600_000,)).fetchone()
    tot = (buy or 0) + (sell or 0)
    if tot > 0:
        imb = (buy - sell) / tot
        fl_status = ("green" if abs(imb) < 0.15 else "yellow" if abs(imb) < 0.35 else "red")
        metrics.append({"label": f"flow imbalance ({FLOW_HOURS}h)", "status": fl_status,
                        "detail": f"buy {buy/tot*100:.0f}% / sell {sell/tot*100:.0f}% "
                                  f"({imb*100:+.0f}%)", "value": round(imb, 3)})
    else:
        metrics.append({"label": f"flow imbalance ({FLOW_HOURS}h)",
                        "detail": "no trades", "status": "gray", "value": None})

    # ---- harvest rate: completed 1h buckets that swung >= spacing ----
    hb = conn.execute(
        "SELECT high, low, open FROM rollup_bars WHERE interval_min=60 "
        "AND ts_begin>=? AND ts_begin+3600000<=?", (win, now)).fetchall()
    if hb:
        swung = sum(1 for h, l, o in hb if o and (h - l) / o >= SPACING_PCT)
        frac = swung / len(hb)
        hv_status = ("green" if frac >= 0.25 else "yellow" if frac >= 0.10 else "red")
        metrics.append({"label": "harvest rate", "status": hv_status,
                        "detail": f"{swung}/{len(hb)} h swung ≥{SPACING_PCT*100:.1f}%",
                        "value": swung})
    else:
        metrics.append({"label": "harvest rate", "detail": "no completed 1h buckets yet",
                        "status": "gray", "value": None})

    worst = max((_RANK.get(m["status"], 0) for m in metrics if m["label"] in _DRIVERS),
                default=0)
    return {"verdict": _VERDICT[worst], "advisory": True,
            "window_hours": window_hours, "metrics": metrics}


def main():
    conn = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    try:
        rep = report(conn)
    finally:
        conn.close()
    print(f"GRID CONDITIONS: {rep['verdict'].upper()}  (advisory, window {rep['window_hours']}h)")
    for m in rep["metrics"]:
        print(f"  [{m['status']:>6}] {m['label']:<22} {m['detail']}")


if __name__ == "__main__":
    main()

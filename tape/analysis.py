"""
tape/analysis.py — TIME-SERIES analytics for the Analysis page.

Where conditions.py answers "what do the grid signals say RIGHT NOW" (one value
per metric, for the fast main-page chip), this answers "how have those same
signals moved OVER TIME" — the series the chart-first Analysis tab draws.

Read-only, pandas/numpy. No MAGI imports. Reuses conditions.py's exogenous grid
parameters (1.5% spacing, 0.50% maker fee floor) so the tab and the chip can
never disagree. Output is JSON-serializable and shaped for lightweight-charts:
seconds-epoch `time`, candical {open,high,low,close}, line {time,value},
histogram {time,value,color}.

This is the HEAVY path — it is computed on demand for /api/analysis (cached,
off the main page's 2s poll), never on the fast path.

CLI: python -m tape.analysis [range_hours] [resolution_min]
"""
import math
import sqlite3
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from tape import conditions
from tape import config

# Selectable candle resolutions. 1 = the raw ohlc_1m base; the rest are the
# permanent rollups the collector already builds (no new computation).
ALLOWED_RES = [1, 5, 60, 360, 1440]
_MAX_POINTS = 1800          # cap line/candle density so payloads stay light
_VOL_WIN = 60               # rolling window (minutes) for σ and efficiency ratio
_MIN_PERIODS = 20           # below this the rolling stat is too noisy to plot

# colours (match the dashboard palette: teal up / red down / muted)
_UP = "rgba(38,166,154,0.5)"
_DN = "rgba(239,83,80,0.5)"
_MUTED = "rgba(120,130,150,0.45)"


def _default_res(range_hours):
    """Pick a candle resolution that keeps the chart legible for the range."""
    if range_hours <= 36:
        return 1
    if range_hours <= 24 * 10:
        return 5
    if range_hours <= 24 * 45:
        return 60
    return 360


def _candles_df(conn, res_min, since_ms):
    if res_min == 1:
        q = ("SELECT ts_begin, open, high, low, close, volume FROM ohlc_1m "
             "WHERE ts_begin>=? ORDER BY ts_begin")
        return pd.read_sql_query(q, conn, params=(since_ms,))
    q = ("SELECT ts_begin, open, high, low, close, volume FROM rollup_bars "
         "WHERE interval_min=? AND ts_begin>=? ORDER BY ts_begin")
    return pd.read_sql_query(q, conn, params=(res_min, since_ms))


def _decimate(df, n_max=_MAX_POINTS):
    """Thin a frame to <= n_max rows by even striding (keeps the last row)."""
    if len(df) <= n_max:
        return df
    step = math.ceil(len(df) / n_max)
    out = df.iloc[::step]
    if out.index[-1] != df.index[-1]:
        out = pd.concat([out, df.iloc[[-1]]])
    return out


def price_series(df):
    """Candlesticks + a volume histogram coloured by bar direction."""
    d = _decimate(df.reset_index(drop=True))
    candles, vol = [], []
    for r in d.itertuples(index=False):
        t = int(r.ts_begin // 1000)
        candles.append({"time": t, "open": r.open, "high": r.high,
                        "low": r.low, "close": r.close})
        vol.append({"time": t, "value": round(r.volume or 0.0, 4),
                    "color": _UP if (r.close or 0) >= (r.open or 0) else _DN})
    return candles, vol


def _vol_regime_lines(conn, since_ms):
    """Rolling realized volatility (hourly-equiv σ, %) and regime efficiency
    ratio, BOTH computed on the 1m base (the honest resolution for them) then
    decimated. Returns (vol_line, regime_line)."""
    df = pd.read_sql_query(
        "SELECT ts_begin, close FROM ohlc_1m WHERE ts_begin>=? ORDER BY ts_begin",
        conn, params=(since_ms,))
    if len(df) < _MIN_PERIODS + 1:
        return [], []
    close = df["close"].astype(float)
    # realized vol: rolling std of 1m log-returns, scaled to an hourly σ, in %.
    logret = np.log(close / close.shift(1))
    sigma_hr = logret.rolling(_VOL_WIN, min_periods=_MIN_PERIODS).std() * math.sqrt(60) * 100.0
    # regime: efficiency ratio = |net move| / total path over the rolling window.
    net = (close - close.shift(_VOL_WIN)).abs()
    path = close.diff().abs().rolling(_VOL_WIN, min_periods=_MIN_PERIODS).sum()
    er = (net / path).clip(0, 1)
    out = pd.DataFrame({"ts_begin": df["ts_begin"], "sigma": sigma_hr, "er": er})
    out = _decimate(out.reset_index(drop=True))
    vol_line = [{"time": int(t // 1000), "value": round(float(s), 4)}
                for t, s in zip(out["ts_begin"], out["sigma"]) if pd.notna(s)]
    reg_line = [{"time": int(t // 1000), "value": round(float(e), 4)}
                for t, e in zip(out["ts_begin"], out["er"]) if pd.notna(e)]
    return vol_line, reg_line


def _drawdown_line(conn, since_ms):
    """Signed drawdown (%) of close from the running peak high over the visible
    range — the DIRECTIONAL 'downtrend bleed' counterpart to the (direction-blind)
    regime efficiency ratio. Same 'drawdown from high' measure MAGI tracks as
    drawdown_from_high_7d (magi/orchestrator.py), but referenced to the chart's
    range high rather than a fixed 7d window, so the line scales with whatever
    range the page is showing. <= 0.0; 0.0 = at/above the range high. Computed on
    the 1m base then decimated, like the vol/regime lines."""
    df = pd.read_sql_query(
        "SELECT ts_begin, high, close FROM ohlc_1m WHERE ts_begin>=? ORDER BY ts_begin",
        conn, params=(since_ms,))
    if len(df) < _MIN_PERIODS + 1:
        return []
    peak = df["high"].astype(float).cummax()
    dd = ((df["close"].astype(float) - peak) / peak * 100.0).where(peak > 0)
    out = _decimate(pd.DataFrame({"ts_begin": df["ts_begin"], "dd": dd})
                    .reset_index(drop=True))
    return [{"time": int(t // 1000), "value": round(float(v), 4)}
            for t, v in zip(out["ts_begin"], out["dd"]) if pd.notna(v)]


def flow_series(conn, since_ms, bucket_min):
    """Net aggressor flow (buy-vol minus sell-vol) per time bucket, as a signed
    histogram. Aggregated in SQL so we never load the whole trade tape."""
    b = max(1, int(bucket_min)) * 60_000
    q = (f"SELECT (ts/{b})*{b} AS bucket, "
         "COALESCE(SUM(CASE WHEN side=0 THEN qty END),0) AS buy, "
         "COALESCE(SUM(CASE WHEN side=1 THEN qty END),0) AS sell "
         "FROM trades WHERE ts>=? GROUP BY bucket ORDER BY bucket")
    bars = []
    for bucket, buy, sell in conn.execute(q, (since_ms,)):
        net = (buy or 0.0) - (sell or 0.0)
        bars.append({"time": int(bucket // 1000), "value": round(net, 4),
                     "color": _UP if net >= 0 else _DN})
    return bars


def harvest_series(df, spacing_pct):
    """Per-bar swing range as % of open, coloured by whether it cleared the grid
    spacing — the direct 'was there anything to harvest' measure."""
    d = _decimate(df.reset_index(drop=True))
    bars = []
    for r in d.itertuples(index=False):
        o = r.open or 0.0
        rng = ((r.high - r.low) / o * 100.0) if o else 0.0
        bars.append({"time": int(r.ts_begin // 1000), "value": round(rng, 4),
                     "color": _UP if rng >= spacing_pct * 100 else _MUTED})
    return bars


def build(conn, res_min=None, range_hours=24, now_ms=None):
    """Assemble the full Analysis payload (everything except interpretation)."""
    now = now_ms or int(time.time() * 1000)
    range_hours = max(1, int(range_hours))
    since = now - range_hours * 3_600_000
    res_min = res_min if res_min in ALLOWED_RES else _default_res(range_hours)

    df = _candles_df(conn, res_min, since)
    candles, volume = price_series(df) if len(df) else ([], [])
    vol_line, reg_line = _vol_regime_lines(conn, since)
    dd_line = _drawdown_line(conn, since)
    flow = flow_series(conn, since, res_min if res_min >= 5 else 5)
    harvest = harvest_series(df, conditions.SPACING_PCT) if len(df) else []
    summary = conditions.report(conn, now)

    return {
        "range_hours": range_hours,
        "resolution_min": res_min,
        "allowed_res": ALLOWED_RES,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bars": len(df),
        "price": {"candles": candles, "volume": volume},
        "volatility": {"line": vol_line,
                       "fee_floor_pct": conditions.FEE_FLOOR_PCT * 100,
                       "firm_pct": conditions.FIRM_SPACING_PCT * 100,
                       "optimal_pct": conditions.SPACING_PCT * 100},
        "regime": {"line": reg_line, "choppy_max": 0.30, "trending_min": 0.50},
        # Signed % drawdown from the running range high (<= 0); bleed_pct is the
        # negative threshold the chart draws as the 'downtrend bleed' line.
        "drawdown": {"line": dd_line, "bleed_pct": -conditions.BLEED_DD_PCT * 100},
        "flow": {"bars": flow},
        "harvest": {"bars": harvest, "spacing_pct": conditions.SPACING_PCT * 100},
        "summary": summary,
    }


def main():
    import sys
    rng = int(sys.argv[1]) if len(sys.argv) > 1 else 24
    res = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    conn = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    try:
        p = build(conn, res_min=res or None, range_hours=rng)
    finally:
        conn.close()
    print(f"range={p['range_hours']}h res={p['resolution_min']}m bars={p['bars']} "
          f"@ {p['generated_at_utc']}")
    print(f"  price candles : {len(p['price']['candles'])}")
    print(f"  volatility pts: {len(p['volatility']['line'])} "
          f"(floor {p['volatility']['fee_floor_pct']:.2f}% optimal {p['volatility']['optimal_pct']:.2f}%)")
    print(f"  regime pts    : {len(p['regime']['line'])}")
    print(f"  flow buckets  : {len(p['flow']['bars'])}")
    print(f"  harvest bars  : {len(p['harvest']['bars'])}")
    print(f"  summary       : {p['summary']['verdict'].upper()}")
    for m in p["summary"]["metrics"]:
        print(f"    [{m['status']:>6}] {m['label']:<22} {m['detail']}")


if __name__ == "__main__":
    main()

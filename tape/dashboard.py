"""
tape/dashboard.py — real-time monitor for the market tape collector.

A self-contained Flask app that reads ONLY tape/market_tape.db (WAL, so
its reads never block the collector's writes) and renders live process
health + ingest throughput + the current market snapshot + storage usage.
No MAGI imports. The page polls /api/status every 2s.

Run:  python -m tape.dashboard   (or via the repurposed root dashboard.py)
"""
import json
import logging
import os
import sqlite3
import time
from datetime import timedelta

from flask import (Flask, jsonify, redirect, render_template_string,
                   request, session, url_for)

from tape import analysis
from tape import conditions
from tape import config
from tape import interpret
from tape import quality

# Reuse the EXISTING dashboard plumbing: this app is served by the same
# magi-dashboard.service on :5000 behind the same ethobs.uk cloudflared
# tunnel, so it must carry the same app-side login the old dashboard had —
# otherwise the previously-protected URL would become public. We read the
# same SECRET_KEY / DASHBOARD_PASSWORD from the shared .env. (This is the
# only place the tape package touches .env, and only for auth — the
# collector core stays env-free.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "")
app.permanent_session_lifetime = timedelta(days=365)
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
# Optional token bypass for automation, identical to the old dashboard.
MAGI_TRIGGER_TOKEN = os.environ.get("MAGI_TRIGGER_TOKEN", "")

LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tape Monitor — Login</title>
<style>
  body{background:#0a0a0a;color:#00ffcc;font-family:monospace;display:flex;
       align-items:center;justify-content:center;height:100vh;margin:0;}
  .box{border:2px solid #00ff88;border-radius:6px;padding:32px 40px;
       background:#0f0f0f;text-align:center;box-shadow:0 0 24px #00ff8833;}
  h1{font-size:1.2em;letter-spacing:2px;margin:0 0 20px;}
  input[type=password]{background:#000;border:1px solid #00ff88;color:#00ffcc;
       font-family:monospace;padding:10px;font-size:1em;width:240px;border-radius:4px;}
  button{margin-top:16px;background:#00ff8822;color:#00ffcc;border:2px solid #00ff88;
       padding:10px 28px;font-family:monospace;font-size:1em;font-weight:bold;
       cursor:pointer;border-radius:4px;width:100%;}
  .err{color:#ff6666;margin-top:14px;font-size:0.85em;}
</style></head>
<body>
  <form class="box" method="POST" action="/login">
    <h1>⬡ TAPE MONITOR</h1>
    <input type="password" name="password" placeholder="password" autofocus
           autocomplete="current-password"/>
    <button type="submit">ENTER</button>
    {% if error %}<div class="err">{{ error }}</div>{% endif %}
  </form>
</body></html>
"""


@app.after_request
def _no_cache(resp):
    # The page ships its CSS inline, so a cached HTML copy also pins stale
    # styling. Force a fresh fetch every load — palette/layout edits then show
    # up on a normal refresh instead of being masked by the browser cache.
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.before_request
def _require_login():
    if request.endpoint in ("login", "logout", "static"):
        return None
    token = request.headers.get("X-Magi-Token", "") or request.args.get("token", "")
    if MAGI_TRIGGER_TOKEN and token == MAGI_TRIGGER_TOKEN:
        return None
    if session.get("authed"):
        return None
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "authentication required"}), 401
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if DASHBOARD_PASSWORD and request.form.get("password", "") == DASHBOARD_PASSWORD:
            session.permanent = True
            session["authed"] = True
            return redirect(url_for("index"))
        return render_template_string(LOGIN_TEMPLATE, error="Incorrect password."), 401
    return render_template_string(LOGIN_TEMPLATE, error=None)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# Freshness thresholds (seconds) per feed: (warn, crit). Ticker/spread is
# lenient — it only fires on bbo change, sparse in quiet markets.
_FEED_THRESH = {
    "trades": (45, 180),
    "spread": (90, 600),
    "ohlc_1m": (150, 360),
}

# Backup freshness verdict — anchored to the hourly tape-backup.timer.
# <90m = fresh; 90–150m = a tick was likely missed; >150m = overdue/stalled.
_BACKUP_WARN_SEC = 90 * 60
_BACKUP_RED_SEC = 150 * 60


def _conn():
    # Read-only-ish connection; WAL lets this coexist with the writer.
    c = sqlite3.connect(config.DB_PATH, timeout=5)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=5000")
    return c


def _scalar(c, sql, default=None):
    try:
        row = c.execute(sql).fetchone()
        return row[0] if row and row[0] is not None else default
    except Exception:
        return default


def _now_ms():
    return int(time.time() * 1000)


def _age(now_ms, ts):
    return None if ts is None else round((now_ms - ts) / 1000.0, 1)


# The analytics panels (Data Quality + Grid Conditions) are 24h-window metrics
# that don't need 2s freshness, and they're ~90% of a poll's cost (~80ms of
# window-function scans vs ~8ms for the live operational panels). Cache them and
# recompute on a slower cadence so the real-time numbers stay snappy; the heavy
# recompute is paid once per TTL, not 30x/min.
_ANALYTICS_TTL_SEC = 15
_analytics_cache = {"ts": 0.0, "quality": None, "conditions": None}


def _analytics(c, now_ms):
    if (_analytics_cache["quality"] is None
            or time.time() - _analytics_cache["ts"] >= _ANALYTICS_TTL_SEC):
        try:
            q = quality.report(c, now_ms)
        except Exception as e:
            q = {"verdict": "gray", "checks": [], "error": str(e),
                 "window_hours": quality.WINDOW_HOURS}
        try:
            cond = conditions.report(c, now_ms)
        except Exception as e:
            cond = {"verdict": "gray", "metrics": [], "error": str(e),
                    "window_hours": conditions.WINDOW_HOURS}
        _analytics_cache.update(quality=q, conditions=cond, ts=time.time())
    return _analytics_cache["quality"], _analytics_cache["conditions"]


def build_status():
    now = _now_ms()
    out = {"now_ms": now, "db_path": config.DB_PATH}

    if not os.path.exists(config.DB_PATH):
        out["error"] = "market_tape.db does not exist yet — collector has not started"
        return out

    c = _conn()
    try:
        # ---- process health beacon ----
        h = None
        try:
            r = c.execute("SELECT * FROM collector_health WHERE id=1").fetchone()
            if r:
                h = dict(r)
                h["health_age_sec"] = _age(now, h.get("ts"))
                h["uptime_sec"] = (round((now - h["started_at"]) / 1000.0)
                                   if h.get("started_at") else None)
        except Exception:
            pass
        out["health"] = h

        # ---- feed freshness + totals ----
        feeds = {}
        last_ts = {
            "trades": _scalar(c, "SELECT MAX(ts) FROM trades"),
            "spread": _scalar(c, "SELECT MAX(ts) FROM spread"),
            "ohlc_1m": _scalar(c, "SELECT MAX(ts_begin) FROM ohlc_1m"),
        }
        totals = {
            "trades": _scalar(c, "SELECT COUNT(*) FROM trades", 0),
            "spread": _scalar(c, "SELECT COUNT(*) FROM spread", 0),
            "ohlc_1m": _scalar(c, "SELECT COUNT(*) FROM ohlc_1m", 0),
            "book_l2": _scalar(c, "SELECT COUNT(*) FROM book_l2", 0),
            "rollup_bars": _scalar(c, "SELECT COUNT(*) FROM rollup_bars", 0),
        }
        for name in ("trades", "spread", "ohlc_1m"):
            age = _age(now, last_ts[name])
            warn, crit = _FEED_THRESH[name]
            status = "green"
            if age is None or age >= crit:
                status = "red"
            elif age >= warn:
                status = "yellow"
            feeds[name] = {"last_ts": last_ts[name], "age_sec": age,
                           "total": totals[name], "status": status}
        out["feeds"] = feeds
        out["totals"] = totals

        # ---- throughput: rows in last hour + per-minute trade series (30m) ----
        hour_ago = now - 3_600_000
        out["throughput"] = {
            "trades_1h": _scalar(c, f"SELECT COUNT(*) FROM trades WHERE ts>={hour_ago}", 0),
            "spread_1h": _scalar(c, f"SELECT COUNT(*) FROM spread WHERE ts>={hour_ago}", 0),
            "ohlc_1m_1h": _scalar(c, f"SELECT COUNT(*) FROM ohlc_1m WHERE ts_begin>={hour_ago}", 0),
        }
        try:
            start = now - 30 * 60_000
            rows = c.execute(
                "SELECT (ts/60000) AS m, COUNT(*) FROM trades WHERE ts>=? GROUP BY m",
                (start,)).fetchall()
            by_min = {int(r[0]): r[1] for r in rows}
            base_min = start // 60_000
            out["throughput"]["trades_per_min"] = [
                by_min.get(base_min + i, 0) for i in range(30)
            ]
        except Exception:
            out["throughput"]["trades_per_min"] = []

        # ---- current market snapshot ----
        mkt = {}
        sp = c.execute("SELECT ts,bid,ask,last FROM spread ORDER BY ts DESC LIMIT 1").fetchone()
        if sp:
            bid, ask = sp["bid"], sp["ask"]
            mkt["bid"], mkt["ask"], mkt["last"] = bid, ask, sp["last"]
            if bid and ask and (bid + ask) > 0:
                mkt["spread_bps"] = round((ask - bid) / ((ask + bid) / 2.0) * 10000.0, 2)
        mkt["last_price"] = _scalar(c, "SELECT price FROM trades ORDER BY ts DESC LIMIT 1")
        bar = c.execute("SELECT ts_begin,open,high,low,close,volume FROM ohlc_1m "
                        "ORDER BY ts_begin DESC LIMIT 1").fetchone()
        if bar:
            mkt["last_bar"] = dict(bar)
        out["market"] = mkt

        # ---- storage ----
        db_bytes = os.path.getsize(config.DB_PATH)
        for ext in ("-wal", "-shm"):
            p = config.DB_PATH + ext
            if os.path.exists(p):
                db_bytes += os.path.getsize(p)
        out["storage"] = {
            "db_bytes": db_bytes,
            "db_mb": round(db_bytes / 1e6, 2),
            "oldest_trade_ts": _scalar(c, "SELECT MIN(ts) FROM trades"),
            "retention_days": config.RAW_RETENTION_DAYS,
            "tables": totals,
        }

        # ---- rollup status per interval ----
        rollup = {}
        try:
            for r in c.execute("SELECT interval_min, MAX(ts_begin), COUNT(*) "
                               "FROM rollup_bars GROUP BY interval_min").fetchall():
                rollup[int(r[0])] = {"last_ts": r[1], "age_sec": _age(now, r[1]), "count": r[2]}
        except Exception:
            pass
        out["rollup"] = rollup

        # ---- backup & durability (reads backup.py's status file; no per-poll
        #      network call — the file records GCS-confirmed success) ----
        bk = {"bucket": f"{config.BACKUP_BUCKET}/{config.BACKUP_GCS_PREFIX}",
              "keep": config.BACKUP_LOCAL_KEEP}
        try:
            sf = os.path.join(config.BACKUP_LOCAL_DIR, ".last_backup.json")
            if os.path.exists(sf):
                with open(sf) as fh:
                    st = json.load(fh)
                bk.update({"last_ts": st.get("ts_ms"), "age_sec": _age(now, st.get("ts_ms")),
                           "bytes": st.get("bytes"), "gcs_ok": st.get("gcs_ok"),
                           "local_count": st.get("local_count"), "name": st.get("name"),
                           "error": st.get("error")})
            else:
                # fallback before the first run after this upgrade: scan the dir
                d = config.BACKUP_LOCAL_DIR
                files = sorted(os.path.join(d, f) for f in os.listdir(d)
                               if f.startswith("market_tape_") and f.endswith(".db.gz")) \
                    if os.path.isdir(d) else []
                if files:
                    newest = files[-1]
                    lt = int(os.path.getmtime(newest) * 1000)
                    bk.update({"last_ts": lt, "age_sec": _age(now, lt),
                               "bytes": os.path.getsize(newest), "gcs_ok": None,
                               "local_count": len(files), "name": os.path.basename(newest)})
                else:
                    bk["last_ts"] = None
            age = bk.get("age_sec")
            if bk.get("last_ts") is None or bk.get("gcs_ok") is False or age is None or age > _BACKUP_RED_SEC:
                bk["status"] = "red"
            elif age > _BACKUP_WARN_SEC:
                bk["status"] = "yellow"
            else:
                bk["status"] = "green"
        except Exception as e:
            bk["status"], bk["error"] = "gray", str(e)
        out["backup"] = bk

        # ---- recent events (alerts + ws state changes) ----
        try:
            ev = c.execute("SELECT ts, severity, category, message FROM events "
                           "ORDER BY ts DESC LIMIT 12").fetchall()
            out["events"] = [{"ts": r[0], "severity": r[1], "category": r[2],
                              "message": r[3]} for r in ev]
        except Exception:
            out["events"] = []

        # ---- analytics (Data Quality + Grid Conditions): cached on a slower
        #      cadence so the heavy window-function scans don't run every poll ----
        out["quality"], out["conditions"] = _analytics(c, now)
        return out
    finally:
        c.close()


@app.route("/api/status")
def api_status():
    return jsonify(build_status())


@app.route("/")
def index():
    return _PAGE


_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Market Tape Monitor</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Michroma&family=VT323&display=swap" rel="stylesheet">
<style>
  /* MAGI terminal palette — copied from the archived MAGI dashboard */
  :root{
    --bg:#000000; --panel-bg:#0a0a0a;
    --magi-cyan:#00e5e5; --magi-orange:#ff6600; --magi-orange-bright:#ffd27a;
    --magi-text:#ffc36b; --magi-text-dim:#f3b877; --magi-grid:#221100;
    --signal-green:#00ff66; --signal-amber:#ffaa00; --signal-red:#ff3333;
  }
  body{background:var(--bg);
    background-image:
      repeating-linear-gradient(0deg,transparent 0,transparent 39px,var(--magi-grid) 39px,var(--magi-grid) 40px),
      repeating-linear-gradient(90deg,transparent 0,transparent 39px,var(--magi-grid) 39px,var(--magi-grid) 40px);
    color:var(--magi-text);font:13px/1.5 "Courier New","Consolas","Liberation Mono",monospace;
    margin:0;padding:20px;letter-spacing:.5px}
  h1{font-family:"Michroma","Eurostile","Helvetica Neue","Arial",sans-serif;
    font-size:20px;margin:0 0 10px;color:var(--magi-orange-bright);text-transform:uppercase;
    letter-spacing:5px;border-bottom:2px solid var(--magi-orange);padding-bottom:8px}
  .sub{color:var(--magi-text-dim);font-size:11px;margin-bottom:14px;letter-spacing:1px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px}
  .card{background:var(--panel-bg);border:2px solid var(--magi-orange);padding:14px 18px;position:relative}
  .card::before{content:"";position:absolute;top:-2px;left:-2px;width:11px;height:11px;
    border-top:2px solid var(--magi-orange-bright);border-left:2px solid var(--magi-orange-bright)}
  .card::after{content:"";position:absolute;bottom:-2px;right:-2px;width:11px;height:11px;
    border-bottom:2px solid var(--magi-orange-bright);border-right:2px solid var(--magi-orange-bright)}
  .card.head{grid-column:1/-1}
  .card.head::before,.card.head::after{display:none}
  /* verdict glow — copied verbatim from the MAGI .verdict-* / agent-health panels:
     the border + background take the signal colour, box-shadow on top */
  .card.glow-green{border-color:var(--signal-green);background:rgba(0,255,102,.14);box-shadow:0 0 16px rgba(0,255,102,.55)}
  .card.glow-yellow{border-color:var(--signal-amber);background:rgba(255,170,0,.14);box-shadow:0 0 16px rgba(255,170,0,.55)}
  .card.glow-red{border-color:var(--signal-red);background:rgba(255,51,51,.16);box-shadow:0 0 18px rgba(255,51,51,.60);
    animation:glow-red-pulse 1.4s ease-in-out infinite alternate}
  .card.glow-gray{}
  @keyframes glow-red-pulse{from{box-shadow:0 0 6px rgba(255,51,51,.4)}to{box-shadow:0 0 14px rgba(255,51,51,.85)}}
  .card h2{font-family:"Helvetica Neue","Helvetica","Arial",sans-serif;font-weight:700;
    font-size:11px;text-transform:uppercase;letter-spacing:3px;color:var(--magi-orange-bright);
    margin:0 0 9px;border-left:4px solid var(--magi-orange);padding-left:8px}
  .row{display:flex;justify-content:space-between;padding:2px 0;gap:10px}
  .k{color:var(--magi-text-dim);white-space:nowrap}
  .v{color:var(--magi-orange-bright);text-align:right;min-width:0;overflow-wrap:anywhere}
  .chip{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:7px;vertical-align:middle}
  .green{background:var(--signal-green);box-shadow:0 0 6px rgba(0,255,102,.8)}
  .yellow{background:var(--signal-amber);box-shadow:0 0 6px rgba(255,170,0,.8)}
  .red{background:var(--signal-red);box-shadow:0 0 7px rgba(255,51,51,.9)}
  .gray{background:#665522}
  /* data-quality check items: stacked label+detail with breathing room */
  .qgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(225px,1fr));gap:10px 16px;margin-top:8px}
  .qitem{padding:7px 11px;border-left:2px solid var(--magi-orange);background:rgba(255,153,0,.04)}
  .qlabel{color:var(--magi-orange-bright);white-space:nowrap}
  .qdetail{color:var(--magi-text-dim);font-size:11px;margin-top:3px;overflow-wrap:anywhere}
  .verdict{font-size:34px;letter-spacing:6px;text-transform:uppercase;line-height:1;
    font-family:"VT323","Courier New",monospace}
  .v-green{color:var(--signal-green);text-shadow:0 0 12px rgba(0,255,102,.4)}
  .v-yellow{color:var(--signal-amber);text-shadow:0 0 12px rgba(255,170,0,.4)}
  .v-red{color:var(--signal-red);text-shadow:0 0 12px rgba(255,51,51,.4)}
  .v-gray{color:#665522}
  .banner{padding:10px 12px;margin-bottom:14px;font-weight:bold;letter-spacing:1px;border:2px solid}
  .banner.ok{background:#001a0d;border-color:var(--signal-green);color:var(--signal-green)}
  .banner.bad{background:#1a0000;border-color:var(--signal-red);color:var(--signal-red)}
  .spark{display:flex;align-items:flex-end;height:36px;gap:2px;margin-top:6px}
  .spark span{flex:1;background:var(--magi-cyan);min-height:1px}
  .big{font-size:20px;color:var(--magi-orange-bright)}
</style></head>
<body>
  <h1>Market Tape Monitor <span id="sym" class="sub"></span>
    <a href="/analysis" style="float:right;font-size:12px;color:var(--magi-cyan);text-decoration:none;letter-spacing:2px">ANALYSIS →</a></h1>
  <div class="sub" id="updated">connecting…</div>
  <div id="banner" class="banner bad">waiting for first poll…</div>
  <div class="grid" id="grid"></div>
<script>
const $=id=>document.getElementById(id);
function fmtAge(s){if(s==null)return '—';if(s<90)return s.toFixed(0)+'s';if(s<5400)return (s/60).toFixed(1)+'m';return (s/3600).toFixed(1)+'h';}
function chip(c){return '<span class="chip '+c+'"></span>';}
function card(title,rowsHtml,extra){return '<div class="card"><h2>'+title+'</h2>'+rowsHtml+(extra||'')+'</div>';}
function row(k,v){return '<div class="row"><span class="k">'+k+'</span><span class="v">'+v+'</span></div>';}

async function tick(){
  let d;
  try{ d=await (await fetch('/api/status')).json(); }
  catch(e){ $('banner').className='banner bad'; $('banner').textContent='dashboard cannot reach /api/status'; return; }
  $('updated').textContent='updated '+new Date().toLocaleTimeString()+'  ·  '+(d.db_path||'');

  // banner = process health
  const h=d.health;
  let bcls='banner bad', btxt='COLLECTOR NOT REPORTING — no health beacon';
  if(d.error){ btxt='COLLECTOR DOWN — '+d.error; }
  else if(h && h.health_age_sec!=null && h.health_age_sec<30){
    const st=h.ws_state;
    if(st==='connected'){ bcls='banner ok'; btxt='● COLLECTOR LIVE — ws '+st; }
    else { bcls='banner bad'; btxt='COLLECTOR ws '+(st||'?'); }
    btxt+='  ·  up '+fmtAge(h.uptime_sec)+'  ·  reconnects/1h '+(h.reconnects_1h??'—')
        +'  ·  written '+(h.rows_written??'—')+'  ·  dropped '+(h.rows_dropped??0);
  } else if(h){ btxt='COLLECTOR STALE — last beacon '+fmtAge(h.health_age_sec)+' ago (ws '+(h.ws_state||'?')+')'; }
  $('banner').className=bcls; $('banner').textContent=btxt;

  const g=[];

  // data quality — the control-panel headline (full-width card, first)
  const q=d.quality||{}, qc=q.checks||[], qv=(q.verdict||'gray');
  let ql='';
  for(const ch of qc){ ql+='<div class="qitem"><div class="qlabel">'+chip(ch.status||'gray')+ch.label
       +'</div><div class="qdetail">'+(ch.detail||'—')+'</div></div>'; }
  if(!qc.length) ql='<div class="qitem"><div class="qdetail">'+(q.error?('error: '+q.error):'no quality data yet')+'</div></div>';
  const okN=qc.filter(ch=>ch.status==='green'||ch.status==='gray').length;
  const qsum=qc.length?(okN+'/'+qc.length+' ok · '+(q.window_hours||24)+'h window'):'';
  const qhead='<div class="row" style="align-items:center;margin-bottom:4px">'
            +'<span class="verdict v-'+qv+'">'+qv.toUpperCase()+'</span>'
            +'<span class="k">'+qsum+'</span></div>';
  g.push('<div class="card head glow-'+qv+'"><h2>Data Quality</h2>'+qhead+'<div class="qgrid">'+ql+'</div></div>');

  // grid conditions — advisory market-analytics (full-width, no glow)
  const gc=d.conditions||{}, gm=gc.metrics||[], gv=(gc.verdict||'gray');
  let gi='';
  for(const m of gm){ gi+='<div class="qitem"><div class="qlabel">'+chip(m.status||'gray')+m.label+'</div><div class="qdetail">'+(m.detail||'—')+'</div></div>'; }
  if(!gm.length) gi='<div class="qitem"><div class="qdetail">'+(gc.error||'warming up')+'</div></div>';
  const gch='<div class="row" style="align-items:center;margin-bottom:4px">'
          +'<span class="verdict v-'+gv+'" style="font-size:22px">'+(gv==='gray'?'—':gv.toUpperCase())+'</span>'
          +'<span class="k">grid favorability · advisory, enforces nothing · '+(gc.window_hours||24)+'h</span></div>';
  g.push('<div class="card head"><h2>Grid Conditions</h2>'+gch+'<div class="qgrid">'+gi+'</div></div>');

  // feeds
  let f=d.feeds||{}, fr='';
  for(const name of ['trades','spread','ohlc_1m']){const x=f[name]||{};fr+=row(chip(x.status||'gray')+name, fmtAge(x.age_sec)+' ago · '+(x.total??0).toLocaleString());}
  g.push(card('Feed freshness', fr));

  // throughput + sparkline
  const t=d.throughput||{};
  let tr=row('trades / 1h',(t.trades_1h??0).toLocaleString())+row('spread / 1h',(t.spread_1h??0).toLocaleString())+row('1m bars / 1h',(t.ohlc_1m_1h??0));
  const sp=(t.trades_per_min||[]);const mx=Math.max(1,...sp);
  let spark='<div class="sub" style="margin-top:8px">trades/min (30m)</div><div class="spark">'+sp.map(v=>'<span style="height:'+(v/mx*100)+'%"></span>').join('')+'</div>';
  g.push(card('Throughput', tr, spark));

  // market
  const m=d.market||{};
  let mr=row('last price', m.last_price!=null?m.last_price:'—')
       +row('bid / ask',(m.bid!=null?m.bid:'—')+' / '+(m.ask!=null?m.ask:'—'))
       +row('spread', m.spread_bps!=null?m.spread_bps+' bps':'—');
  const b=m.last_bar; if(b){mr+=row('last 1m bar', new Date(b.ts_begin).toLocaleTimeString())+row('o/h/l/c', b.open+' / '+b.high+' / '+b.low+' / '+b.close);}
  g.push(card('Market snapshot', mr));

  // storage
  const s=d.storage||{}, tb=s.tables||{};
  let sr=row('db size', (s.db_mb??0)+' MB')
       +row('retention', (s.retention_days??'—')+' d (raw)');
  for(const k of ['trades','spread','ohlc_1m','book_l2','rollup_bars']) sr+=row('  '+k,(tb[k]??0).toLocaleString());
  if(s.oldest_trade_ts) sr+=row('oldest trade', new Date(s.oldest_trade_ts).toLocaleString());
  g.push(card('Storage', sr));

  // backup & durability
  const bk=d.backup||{};
  const gcs = bk.gcs_ok===true?'✓ uploaded':(bk.gcs_ok===false?'✗ FAILED':'— local only');
  const stamp = bk.name?(' · '+bk.name.replace('market_tape_','').replace('.db.gz','')):'';
  let kr=row(chip(bk.status||'gray')+'last backup', bk.age_sec!=null?(fmtAge(bk.age_sec)+' ago'):'none yet')
       +row('size', bk.bytes!=null?((bk.bytes/1e6).toFixed(2)+' MB'+stamp):'—')
       +row('off-box (GCS)', gcs)
       +row('local copies', (bk.local_count!=null?bk.local_count:'—')+' / '+(bk.keep??'—')+' kept')
       +row('bucket', bk.bucket||'—');
  if(bk.error) kr+=row('error', bk.error);
  g.push(card('Backup & durability', kr));

  // rollup
  const rb=d.rollup||{}; let rr=''; const names={5:'5m',60:'1h',360:'6h',1440:'1d'};
  const keys=Object.keys(rb); if(!keys.length) rr=row('status','no rollups yet');
  for(const k of keys.sort((a,b)=>a-b)){const x=rb[k];rr+=row(names[k]||(k+'m'),(x.count??0)+' bars · '+fmtAge(x.age_sec)+' old');}
  g.push(card('Rollups', rr));

  // events — alert + ws-state history
  const ev=d.events||[];
  const sevChip=s=>s==='critical'?'red':(s==='warning'||s==='warn')?'yellow':'gray';
  let er='';
  for(const e of ev){
    const t=new Date(e.ts).toLocaleTimeString();
    er+='<div class="row"><span class="k">'+chip(sevChip(e.severity))+t+'</span><span class="v">'+(e.message||e.category||'')+'</span></div>';
  }
  if(!ev.length) er=row('status','no events yet');
  g.push(card('Events', er));

  $('grid').innerHTML=g.join('');
}
tick(); setInterval(tick, 2000);
</script>
</body></html>"""


# ---- Analysis page (separate route; HEAVY, kept entirely off the 2s path) ----
# Cached by (range, res, llm) so opening/refreshing the page doesn't recompute
# the time-series — or re-call Gemini — on every hit. The page itself does NOT
# auto-poll (load-on-open + manual refresh), so LLM spend stays minimal.
_ANALYSIS_TTL_SEC = 60
_analysis_cache = {}                       # (range,res) -> {ts, payload}  (chart data)
# Interpretation depends ONLY on the 24h conditions, not the chart range/res, so
# it is cached separately keyed by the llm flag — clicking through range/res with
# AI on therefore reuses one narrative instead of re-calling Gemini each time.
_interp_cache = {True: {"ts": 0.0, "val": None}, False: {"ts": 0.0, "val": None}}


def _parse_range(v):
    if str(v).lower() == "all":
        return 87_600          # ~10y of hours -> effectively the whole tape
    try:
        return max(1, int(v))
    except Exception:
        return 24


def _cached_interpretation(summary, use_llm):
    slot = _interp_cache[bool(use_llm)]
    now = time.time()
    if slot["val"] is None or now - slot["ts"] >= config.INTERPRET_CACHE_SECS:
        slot["val"] = interpret.interpret(summary, use_llm=use_llm)
        slot["ts"] = now
    return slot["val"]


@app.route("/api/analysis")
def api_analysis():
    rng = _parse_range(request.args.get("range", 24))
    try:
        res = int(request.args.get("res", 0))
    except Exception:
        res = 0
    use_llm = request.args.get("llm", "1") not in ("0", "false", "off")

    # chart data: cached by (range,res); cheap to recompute, refreshed each minute
    dkey = (rng, res)
    ent = _analysis_cache.get(dkey)
    now = time.time()
    if ent and now - ent["ts"] < _ANALYSIS_TTL_SEC:
        payload = ent["payload"]
    else:
        c = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
        try:
            payload = analysis.build(c, res_min=(res or None), range_hours=rng)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            c.close()
        _analysis_cache[dkey] = {"ts": now, "payload": payload}
        if len(_analysis_cache) > 24:                  # bound the cache
            for k in list(_analysis_cache)[:-24]:
                _analysis_cache.pop(k, None)

    out = dict(payload)                                # don't mutate the cached copy
    out["interpretation"] = _cached_interpretation(payload["summary"], use_llm)
    out["llm_default_on"] = config.INTERPRET_LLM_DEFAULT_ON
    return jsonify(out)


@app.route("/analysis")
def analysis_page():
    return _ANALYSIS_PAGE


_ANALYSIS_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Tape Analysis</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="/static/lightweight-charts.standalone.production.js"></script>
<style>
  :root{--bg:#000;--panel-bg:#0a0a0a;--magi-cyan:#00e5e5;--magi-orange:#ff6600;
    --magi-orange-bright:#ffd27a;--magi-text:#ffc36b;--magi-text-dim:#f3b877;--magi-grid:#221100;
    --signal-green:#00ff66;--signal-amber:#ffaa00;--signal-red:#ff3333;}
  body{background:var(--bg);color:var(--magi-text);
    font:13px/1.5 "Courier New","Consolas",monospace;margin:0;padding:18px;letter-spacing:.4px}
  h1{font-family:"Michroma","Arial",sans-serif;font-size:18px;margin:0 0 8px;
    color:var(--magi-orange-bright);text-transform:uppercase;letter-spacing:4px;
    border-bottom:2px solid var(--magi-orange);padding-bottom:8px}
  h1 a{float:right;font-size:12px;color:var(--magi-cyan);text-decoration:none;letter-spacing:2px}
  .sub{color:var(--magi-text-dim);font-size:11px;margin:6px 0 12px;letter-spacing:1px}
  .controls{display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin-bottom:14px;
    background:var(--panel-bg);border:1px solid var(--magi-grid);padding:10px 12px}
  .controls .grp{display:flex;gap:5px;align-items:center}
  .controls .lbl{color:var(--magi-text-dim);font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-right:2px}
  .controls button{background:#160c00;color:var(--magi-text);border:1px solid var(--magi-orange);
    padding:4px 10px;font-family:inherit;font-size:11px;cursor:pointer;letter-spacing:1px}
  .controls button.on{background:var(--magi-orange);color:#000;font-weight:bold}
  .controls label{color:var(--magi-text-dim);font-size:11px;cursor:pointer;user-select:none}
  .controls input[type=checkbox]{accent-color:var(--magi-orange);vertical-align:middle}
  .card{background:var(--panel-bg);border:2px solid var(--magi-orange);padding:12px 16px;margin-bottom:12px;position:relative}
  .card h2{font-family:"Arial",sans-serif;font-weight:700;font-size:11px;text-transform:uppercase;
    letter-spacing:3px;color:var(--magi-orange-bright);margin:0 0 8px;border-left:4px solid var(--magi-orange);padding-left:8px}
  .glow-green{border-color:var(--signal-green);background:rgba(0,255,102,.10);box-shadow:0 0 14px rgba(0,255,102,.4)}
  .glow-yellow{border-color:var(--signal-amber);background:rgba(255,170,0,.10);box-shadow:0 0 14px rgba(255,170,0,.4)}
  .glow-red{border-color:var(--signal-red);background:rgba(255,51,51,.12);box-shadow:0 0 16px rgba(255,51,51,.5)}
  .badge{font-size:9px;font-weight:normal;letter-spacing:1px;color:var(--magi-text-dim);
    border:1px solid var(--magi-text-dim);border-radius:3px;padding:1px 6px;margin-left:8px;vertical-align:middle}
  .narr{font-size:15px;line-height:1.55;color:var(--magi-orange-bright);font-family:"Courier New",monospace}
  .verdict{font-size:16px;letter-spacing:3px;margin-left:10px}
  .v-green{color:var(--signal-green)} .v-yellow{color:var(--signal-amber)}
  .v-red{color:var(--signal-red)} .v-gray{color:#665522}
  .chart{width:100%}
  .chart.big{height:360px} .chart.mid{height:200px} .chart.sm{height:180px}
  .caption{color:var(--magi-text-dim);font-size:12px;margin-top:7px;line-height:1.5;
    border-left:2px solid var(--magi-grid);padding-left:9px}
  .two{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  @media(max-width:860px){.two{grid-template-columns:1fr}}
  .err{color:var(--signal-red)}
</style></head>
<body>
  <h1>Tape Analysis <a href="/">← MONITOR</a></h1>
  <div class="sub" id="meta">loading…</div>

  <div class="controls">
    <div class="grp"><span class="lbl">range</span>
      <button data-range="24" class="on">24h</button>
      <button data-range="168">7d</button>
      <button data-range="720">30d</button>
      <button data-range="all">all</button></div>
    <div class="grp"><span class="lbl">res</span>
      <button data-res="0" class="on">auto</button>
      <button data-res="1">1m</button>
      <button data-res="5">5m</button>
      <button data-res="60">1h</button>
      <button data-res="360">6h</button>
      <button data-res="1440">1d</button></div>
    <div class="grp">
      <label><input type="checkbox" id="ovVol" checked> volume</label>
      <label><input type="checkbox" id="ovMA"> MA(20)</label>
      <label><input type="checkbox" id="ovLLM" checked> AI interpretation</label></div>
    <div class="grp"><button id="refresh">&#8635; refresh</button></div>
  </div>

  <div class="card" id="interpCard">
    <h2>What this is telling you <span class="badge" id="interpSrc"></span>
      <span class="verdict" id="interpVerdict"></span></h2>
    <div class="narr" id="interpOverall">…</div>
  </div>

  <div class="card"><h2>Price &mdash; candles + volume</h2>
    <div id="chPrice" class="chart big"></div></div>

  <div class="two">
    <div class="card"><h2>Movement vs fees</h2>
      <div id="chVol" class="chart mid"></div>
      <div class="caption" id="capVol"></div></div>
    <div class="card"><h2>Trend vs chop</h2>
      <div id="chReg" class="chart mid"></div>
      <div class="caption" id="capReg"></div></div>
    <div class="card"><h2>Buy / sell pressure</h2>
      <div id="chFlow" class="chart sm"></div>
      <div class="caption" id="capFlow"></div></div>
    <div class="card"><h2>Harvest &mdash; swing vs grid step</h2>
      <div id="chHarv" class="chart sm"></div>
      <div class="caption" id="capHarv"></div></div>
  </div>

<script>
const $=id=>document.getElementById(id);
const LC=window.LightweightCharts;
const THEME={
  layout:{background:{color:'#0a0a0a'},textColor:'#ffc36b',fontFamily:'Courier New, monospace',fontSize:11},
  grid:{vertLines:{color:'rgba(255,102,0,.06)'},horzLines:{color:'rgba(255,102,0,.06)'}},
  rightPriceScale:{borderColor:'#3a2400'},
  timeScale:{borderColor:'#3a2400',timeVisible:true,secondsVisible:false},
  crosshair:{mode:0,vertLine:{color:'#ff6600',labelBackgroundColor:'#ff6600'},
             horzLine:{color:'#ff6600',labelBackgroundColor:'#ff6600'}},
};
const state={range:'24',res:'0',llm:true,volume:true,ma:false};
let charts={},S={},built=false;

function mk(id,h){
  const c=LC.createChart($(id),Object.assign({height:h,width:$(id).clientWidth},THEME));
  new ResizeObserver(()=>{try{c.applyOptions({width:$(id).clientWidth})}catch(e){}}).observe($(id));
  return c;
}
function ma(candles,n){
  const out=[],q=[];let sum=0;
  for(const c of candles){q.push(c.close);sum+=c.close;if(q.length>n)sum-=q.shift();
    if(q.length===n)out.push({time:c.time,value:+(sum/n).toFixed(5)});}
  return out;
}
function buildCharts(d){
  charts.price=mk('chPrice',360);
  S.candle=charts.price.addCandlestickSeries({upColor:'#00ff66',downColor:'#ff3333',
    wickUpColor:'#00ff66',wickDownColor:'#ff3333',borderVisible:false});
  S.vol=charts.price.addHistogramSeries({priceFormat:{type:'volume'},priceScaleId:'vol'});
  charts.price.priceScale('vol').applyOptions({scaleMargins:{top:0.82,bottom:0}});
  S.ma=charts.price.addLineSeries({color:'#00e5e5',lineWidth:1,priceLineVisible:false,lastValueVisible:false});

  charts.vol=mk('chVol',200);
  S.volLine=charts.vol.addLineSeries({color:'#ffaa00',lineWidth:2,priceLineVisible:false,lastValueVisible:false});
  S.volLine.createPriceLine({price:d.volatility.fee_floor_pct,color:'#ff3333',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'fee floor'});
  S.volLine.createPriceLine({price:d.volatility.optimal_pct,color:'#00ff66',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'optimal 1.5%'});

  charts.reg=mk('chReg',200);
  S.reg=charts.reg.addLineSeries({color:'#00e5e5',lineWidth:2,priceLineVisible:false,lastValueVisible:false});
  S.reg.createPriceLine({price:d.regime.choppy_max,color:'#00ff66',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'choppy'});
  S.reg.createPriceLine({price:d.regime.trending_min,color:'#ff3333',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'trending'});

  charts.flow=mk('chFlow',180);
  S.flow=charts.flow.addHistogramSeries({priceFormat:{type:'volume'},base:0});

  charts.harv=mk('chHarv',180);
  S.harv=charts.harv.addHistogramSeries({base:0});
  S.harv.createPriceLine({price:d.harvest.spacing_pct,color:'#00ff66',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'1 step'});
  built=true;
}
function render(d){
  if(d.error){$('meta').innerHTML='<span class="err">error: '+d.error+'</span>';return;}
  if(!built) buildCharts(d);
  S.candle.setData(d.price.candles);
  S.vol.setData(state.volume?d.price.volume:[]);
  S.ma.setData(state.ma?ma(d.price.candles,20):[]);
  S.volLine.setData(d.volatility.line);
  S.reg.setData(d.regime.line);
  S.flow.setData(d.flow.bars);
  S.harv.setData(d.harvest.bars);
  for(const k in charts){try{charts[k].timeScale().fitContent();}catch(e){}}

  const it=d.interpretation||{};
  $('interpOverall').textContent=it.overall||'—';
  let src=it.source==='llm'?'AI · Gemini Flash':'rules';
  if(it.usage&&it.usage.total_tokens) src+=' · '+it.usage.total_tokens+' tok';
  if(it.llm_attempted) src+=' · AI unavailable → rules';
  $('interpSrc').textContent=src;
  const v=(d.summary&&d.summary.verdict)||'gray';
  $('interpVerdict').textContent=v==='gray'?'':v.toUpperCase();
  $('interpVerdict').className='verdict v-'+v;
  $('interpCard').className='card glow-'+v;
  const pm=it.per_metric||{};
  $('capVol').textContent=pm.volatility||'';
  $('capReg').textContent=pm.regime||'';
  $('capFlow').textContent=pm.flow||'';
  $('capHarv').textContent=pm.harvest||'';
  $('meta').textContent='range '+d.range_hours+'h · res '+d.resolution_min+'m · '+d.bars+
    ' bars · generated '+d.generated_at_utc;
}
async function load(){
  $('meta').textContent='loading…';
  const url='/api/analysis?range='+state.range+'&res='+state.res+'&llm='+(state.llm?1:0);
  try{const r=await fetch(url);const d=await r.json();render(d);}
  catch(e){$('meta').innerHTML='<span class="err">fetch failed: '+e+'</span>';}
}
function pick(group,attr,val){
  document.querySelectorAll('button['+attr+']').forEach(b=>{
    if(b.getAttribute(attr)!==null && (group==='range'?b.hasAttribute('data-range'):b.hasAttribute('data-res')))
      b.classList.toggle('on',b.getAttribute(attr)===val);});
}
document.querySelectorAll('button[data-range]').forEach(b=>b.onclick=()=>{
  state.range=b.getAttribute('data-range');
  document.querySelectorAll('button[data-range]').forEach(x=>x.classList.toggle('on',x===b));load();});
document.querySelectorAll('button[data-res]').forEach(b=>b.onclick=()=>{
  state.res=b.getAttribute('data-res');
  document.querySelectorAll('button[data-res]').forEach(x=>x.classList.toggle('on',x===b));load();});
$('ovVol').onchange=e=>{state.volume=e.target.checked;S.vol&&S.vol.setData(state.volume?lastData.price.volume:[]);};
$('ovMA').onchange=e=>{state.ma=e.target.checked;S.ma&&S.ma.setData(state.ma?ma(lastData.price.candles,20):[]);};
$('ovLLM').onchange=e=>{state.llm=e.target.checked;load();};
$('refresh').onclick=()=>load();
let lastData={price:{volume:[],candles:[]}};
const _render=render; render=d=>{lastData=d.error?lastData:d;_render(d);};
load();
</script>
</body></html>"""


def main():
    # INFO-level so the interpreter's per-call Gemini token-usage breadcrumb
    # (tape.interpret) lands in the journal — that's the bill-watch trail.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)


if __name__ == "__main__":
    main()

"""
tape/dashboard.py — real-time monitor for the market tape collector.

A self-contained Flask app that reads ONLY tape/market_tape.db (WAL, so
its reads never block the collector's writes) and renders live process
health + ingest throughput + the current market snapshot + storage usage.
No MAGI imports. The page polls /api/status every 2s.

Run:  python -m tape.dashboard   (or via the repurposed root dashboard.py)
"""
import os
import sqlite3
import time
from datetime import timedelta

from flask import (Flask, jsonify, redirect, render_template_string,
                   request, session, url_for)

from tape import config
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

        # ---- data quality (the control-panel headline) ----
        try:
            out["quality"] = quality.report(c, now)
        except Exception as e:
            out["quality"] = {"verdict": "gray", "checks": [],
                              "error": str(e), "window_hours": quality.WINDOW_HOURS}
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
<style>
  /* MAGI terminal palette — copied from the archived MAGI dashboard */
  :root{
    --bg:#000000; --panel-bg:#0a0a0a;
    --magi-cyan:#00d4d4; --magi-orange:#ff6600; --magi-orange-bright:#ff9933;
    --magi-text:#ff9933; --magi-text-dim:#cc7722; --magi-grid:#221100;
    --signal-green:#00ff66; --signal-amber:#ffaa00; --signal-red:#ff3333;
  }
  body{background:var(--bg);
    background-image:
      repeating-linear-gradient(0deg,transparent 0,transparent 39px,var(--magi-grid) 39px,var(--magi-grid) 40px),
      repeating-linear-gradient(90deg,transparent 0,transparent 39px,var(--magi-grid) 39px,var(--magi-grid) 40px);
    color:var(--magi-text);font:13px/1.5 "Courier New","Consolas","Liberation Mono",monospace;
    margin:0;padding:20px;letter-spacing:.5px}
  h1{font-size:18px;margin:0 0 10px;color:var(--magi-orange-bright);text-transform:uppercase;
    letter-spacing:5px;border-bottom:2px solid var(--magi-orange);padding-bottom:8px}
  .sub{color:var(--magi-text-dim);font-size:11px;margin-bottom:14px;letter-spacing:1px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px}
  .card{background:var(--panel-bg);border:2px solid var(--magi-orange);padding:12px 14px;position:relative}
  .card::before{content:"";position:absolute;top:-2px;left:-2px;width:11px;height:11px;
    border-top:2px solid var(--magi-orange-bright);border-left:2px solid var(--magi-orange-bright)}
  .card::after{content:"";position:absolute;bottom:-2px;right:-2px;width:11px;height:11px;
    border-bottom:2px solid var(--magi-orange-bright);border-right:2px solid var(--magi-orange-bright)}
  .card.head{grid-column:1/-1;border-color:var(--magi-orange-bright)}
  .card h2{font-size:11px;text-transform:uppercase;letter-spacing:3px;color:var(--magi-orange);
    margin:0 0 9px;border-left:4px solid var(--magi-orange);padding-left:8px}
  .row{display:flex;justify-content:space-between;padding:2px 0}
  .k{color:var(--magi-text-dim)}.v{color:var(--magi-orange-bright)}
  .chip{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:7px;vertical-align:middle}
  .green{background:var(--signal-green)}.yellow{background:var(--signal-amber)}
  .red{background:var(--signal-red)}.gray{background:#665522}
  .verdict{font-size:26px;letter-spacing:6px;font-weight:bold;text-transform:uppercase;
    font-family:"Arial Black","Helvetica",sans-serif}
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
  <h1>Market Tape Monitor <span id="sym" class="sub"></span></h1>
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
  for(const ch of qc){ ql+=row(chip(ch.status||'gray')+ch.label, ch.detail||'—'); }
  if(!qc.length) ql=row('status', q.error?('error: '+q.error):'no quality data yet');
  const okN=qc.filter(ch=>ch.status==='green'||ch.status==='gray').length;
  const qsum=qc.length?(okN+'/'+qc.length+' ok · '+(q.window_hours||24)+'h window'):'';
  const qhead='<div class="row" style="align-items:center;margin-bottom:8px">'
            +'<span class="verdict v-'+qv+'">'+qv.toUpperCase()+'</span>'
            +'<span class="k">'+qsum+'</span></div>';
  const qgrid='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:0 18px">'+ql+'</div>';
  g.push('<div class="card head"><h2>Data Quality</h2>'+qhead+qgrid+'</div>');

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
  const b=m.last_bar; if(b){mr+=row('last 1m bar', new Date(b.ts_begin).toLocaleTimeString())+row('  o/h/l/c', b.open+'/'+b.high+'/'+b.low+'/'+b.close);}
  g.push(card('Market snapshot', mr));

  // storage
  const s=d.storage||{}, tb=s.tables||{};
  let sr=row('db size', (s.db_mb??0)+' MB')
       +row('retention', (s.retention_days??'—')+' d (raw)');
  for(const k of ['trades','spread','ohlc_1m','book_l2','rollup_bars']) sr+=row('  '+k,(tb[k]??0).toLocaleString());
  if(s.oldest_trade_ts) sr+=row('oldest trade', new Date(s.oldest_trade_ts).toLocaleString());
  g.push(card('Storage', sr));

  // rollup
  const rb=d.rollup||{}; let rr=''; const names={5:'5m',60:'1h',360:'6h',1440:'1d'};
  const keys=Object.keys(rb); if(!keys.length) rr=row('status','no rollups yet');
  for(const k of keys.sort((a,b)=>a-b)){const x=rb[k];rr+=row(names[k]||(k+'m'),(x.count??0)+' bars · '+fmtAge(x.age_sec)+' old');}
  g.push(card('Rollups', rr));

  $('grid').innerHTML=g.join('');
}
tick(); setInterval(tick, 2000);
</script>
</body></html>"""


def main():
    app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)


if __name__ == "__main__":
    main()

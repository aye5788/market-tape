"""
tape/interpret.py — plain-English interpretation of the grid signals.

The operator is not a market-analytics expert, so the Analysis page needs to say
what the charts MEAN for the grid, not just draw them. Two layers:

  - STATIC rules: deterministic plain-English per metric, derived straight from
    conditions.py's green/yellow/red classification. Always available, $0, fully
    private, instant. This is the guaranteed baseline AND the grounding facts fed
    to the LLM (so the LLM can't invent thresholds), AND the fallback if the LLM
    call fails for any reason.
  - LLM narrative (Gemini 2.5 Flash, free tier): a dynamic, readable synthesis
    over the SAME pre-computed numbers. ON by default, with a page toggle to turn
    it off. It interprets the facts we hand it — it never reads raw data freely.

Bill-safety: the model is pinned to the free-tier gemini-2.5-flash, the prompt is
tiny, every call logs its token usage, and the caller caches the result (the page
also doesn't auto-poll). Any failure / missing key -> silent fallback to static.

No MAGI imports. Reuses the project's GOOGLE_API_KEY (same key Casper uses).
"""
import logging
import os
import time

from tape import config

try:
    from dotenv import load_dotenv
except Exception:  # dotenv optional
    load_dotenv = None

log = logging.getLogger("tape.interpret")

# Map a conditions.py metric label to a short chart key the page places it under.
_KEY_BY_PREFIX = {
    "hourly volatility": "volatility",
    "regime": "regime",
    "drawdown from high": "drawdown",
    "flow imbalance": "flow",
    "harvest rate": "harvest",
}


def _key_for(label):
    for pref, key in _KEY_BY_PREFIX.items():
        if label.startswith(pref):
            return key
    return label


def _metric(summary, prefix):
    for m in summary.get("metrics", []):
        if m.get("label", "").startswith(prefix):
            return m
    return {}


# ---------------------------------------------------------------- static layer

def static_interpret(summary):
    """Deterministic plain-English read per metric + an overall. Each read leads
    with what the observation MEANS for running the grid and what to do/watch —
    not just a restatement of the number. Always works (the LLM-off + fallback
    layer)."""
    per = {}

    vol = _metric(summary, "hourly volatility")
    vst = vol.get("status")
    band = ("thin" if "thin" in vol.get("detail", "") else
            "ample" if "ample" in vol.get("detail", "") else
            "firm" if "firm" in vol.get("detail", "") else "")
    if vst == "red":
        per["volatility"] = ("Too quiet to run a grid profitably: swings are smaller than the ~0.50% "
                             "a round trip pays in fees, so the grid would churn fees without covering "
                             "them. Tighter spacing won't help — there's nothing to catch. The move is "
                             "to wait for movement to return; standing down protects capital from fee "
                             "bleed.")
    elif vst == "yellow" and band == "thin":
        per["volatility"] = ("Marginal: there's barely enough movement to clear fees, so each round "
                             "trip nets only a sliver above cost — and spacing set even slightly too "
                             "tight tips those trades net-negative. Keep spacing at or above the "
                             "fee-safe floor and don't over-trade. Run it carefully, don't press it.")
    elif vst == "yellow":
        per["volatility"] = ("Healthy operating conditions: movement clears fees with real headroom, "
                             "so completed round trips bank a meaningful margin and the adaptive "
                             "spacing is well-matched to current swings. This is normal, profitable "
                             "grid weather — let it work.")
    elif vst == "green":
        per["volatility"] = ("Lots of movement — strong harvest potential, but read it WITH the trend "
                             "signal. Big swings inside a range are ideal for the grid; the same swings "
                             "as part of a one-way move are how it bleeds. Profitable if choppy, "
                             "dangerous if trending.")

    reg = _metric(summary, "regime")
    rst = reg.get("status")
    if rst == "green":
        per["regime"] = ("The regime the grid is built for: price keeps round-tripping inside a range, "
                         "so both sides fill and you bank the spacing between them, repeatedly. Key "
                         "tell — if the grid ISN'T earning while it's this choppy, the problem is the "
                         "engine or where it's centered, not the market.")
    elif rst == "yellow":
        per["regime"] = ("Drifting: part-ranging, part-trending, so one side of the grid fills more "
                         "than the other and your inventory slowly skews (e.g. piling up XRP as it "
                         "sags). It still works, but watch the inventory balance and be ready to "
                         "recenter if the drift hardens into a trend.")
    elif rst == "red":
        per["regime"] = ("The grid's failure mode and the most important warning here: price is moving "
                         "decisively one way, so the grid keeps adding to the losing side — buying into "
                         "a fall or selling into a rise — and books unrealized losses (the 'bleed'). "
                         "This is when to stand down or recenter; a static grid run into a trend is how "
                         "it loses money.")

    dd = _metric(summary, "drawdown from high")
    dst = dd.get("status")
    if dst == "green":
        per["drawdown"] = ("At or near the recent high — no downtrend bleed. The grid's buy fills are "
                           "close to water, so accumulated inventory isn't underwater. This is the safe "
                           "side of the trend signal; the regime read is the one to watch here.")
    elif dst == "yellow":
        per["drawdown"] = ("Price has slipped below its recent high — early downtrend bleed. The lower "
                           "buys are now underwater and inventory is worth less than it was bought for: "
                           "not severe yet, but if price keeps falling the grid keeps catching the knife. "
                           "Watch whether the regime is also turning trending — that pair is the bleed.")
    elif dst == "red":
        per["drawdown"] = ("Deep drawdown from the high — active capital erosion. Price has fallen more "
                           "than two grid steps off its peak, so the grid has been buying the whole way "
                           "down and is sitting on losing inventory. Recenter or stand down rather than "
                           "keep adding to the falling side.")

    flow = _metric(summary, "flow imbalance")
    fst, val = flow.get("status"), flow.get("value")
    side = "buyers" if (val or 0) >= 0 else "sellers"
    if fst == "green":
        per["flow"] = ("Balanced pressure — buyers and sellers are trading roughly evenly, consistent "
                       "with the range-bound chop a grid wants. No directional warning from order flow.")
    elif fst == "yellow":
        per["flow"] = (f"Leaning toward {side}: one side is pushing a bit harder lately. Not alarming "
                       f"alone, but it's the kind of early tilt that precedes a directional move — if "
                       f"it persists AND the regime turns trending, that's the setup that bleeds a grid. "
                       f"Watch, don't act yet.")
    elif fst == "red":
        per["flow"] = (f"Heavily one-sided — {side} are dominating. Strong, persistent imbalance often "
                       f"front-runs a sustained move, the trend a grid struggles with. Caution flag: "
                       f"not the moment to widen exposure, and a reason to watch the trend signal "
                       f"closely.")
    elif flow:
        per["flow"] = "Not enough trades in the window to read flow yet."

    hv = _metric(summary, "harvest rate")
    hst = hv.get("status")
    if hst == "green":
        per["harvest"] = ("Real opportunity: in a good share of recent hours price swung at least a "
                         "full grid step (1.5%), so the grid has had genuine round trips to complete. "
                         "Spacing is well-matched to actual movement — if profit still isn't showing, "
                         "look at execution/centering, not opportunity.")
    elif hst == "yellow":
        per["harvest"] = ("Thin opportunity: only some hours offered a full-step swing, so the grid "
                         "fills slowly at current spacing. If volatility is still above the fee floor, "
                         "tightening spacing toward the fee-safe minimum would catch more of the "
                         "smaller swings that ARE happening.")
    elif hst == "red":
        per["harvest"] = ("Little to harvest: few recent hours swung a full grid step, so at 1.5% "
                         "spacing the grid mostly sits. Either wait for movement, or — only if "
                         "volatility still clears the fee floor — tighten spacing to capture the "
                         "smaller moves that are occurring.")

    # ---- overall: cross-metric, not just verdict-keyed. Order matters: a trend
    #      or a too-quiet tape overrides an otherwise-rosy verdict. ----
    verdict = summary.get("verdict", "gray")
    if verdict == "gray":
        overall = "Still warming up — not enough data yet to read conditions."
    elif rst == "red":
        overall = ("Caution — the trend signal dominates everything else. Price is moving one way, the "
                   "condition a grid bleeds in, so running it now risks piling up losing inventory no "
                   "matter how much movement there is. The move is to stand down or recenter and wait "
                   "for the chop to return.")
    elif vst == "red":
        overall = ("Stand-down conditions — there isn't enough movement to cover trading fees, so a "
                   "running grid would churn costs without catching the swings to pay for them. Wait "
                   "for volatility back above the fee floor before deploying; tighter spacing can't "
                   "fix an empty tape.")
    elif verdict == "red":
        # red, but not from a trend or a dead tape -> driven by thin harvest.
        overall = ("Poor harvest — even though the tape isn't trending or dead, price rarely swings a "
                   "full grid step right now, so at 1.5% spacing the grid mostly sits. If volatility "
                   "still clears the fee floor, the lever is tighter spacing to catch the smaller "
                   "swings; otherwise wait for bigger moves.")
    elif verdict == "green":
        overall = ("Green light — this is the weather the grid is designed to earn in: enough movement "
                   "to clear fees, and price ranging rather than trending. Deploy and let it work, and "
                   "if it ISN'T earning under these conditions, suspect the engine or centering, not "
                   "the market.")
    else:
        overall = ("Workable but not a clear green light — the grid can earn, but it's marginal on at "
                   "least one of movement, trend, or harvestable swing, so watch it rather than "
                   "set-and-forget. The per-chart reads below point to the weak lever: thin margin → "
                   "don't over-trade; directional drift → watch inventory and be ready to recenter; "
                   "thin harvest → consider spacing.")
    return {"overall": overall, "per_metric": per, "source": "static"}


# ------------------------------------------------------------------- llm layer

def _facts_block(summary):
    """Compact, deterministic facts string the LLM must ground itself in."""
    lines = [f"Overall grid-favorability verdict: {summary.get('verdict', '?').upper()}",
             f"(window: {summary.get('window_hours', '?')}h)"]
    for m in summary.get("metrics", []):
        lines.append(f"- {m.get('label')}: {m.get('detail')} [{m.get('status')}]")
    return "\n".join(lines)


_PROMPT = """You are explaining live market conditions to the operator of an XRP/USD \
spot GRID bot who is NOT a trading expert. A grid bot places ladders of buy/sell \
orders and profits when price OSCILLATES inside a range; it bleeds when price \
TRENDS hard in one direction, and it stalls when price is too quiet to cover the \
~0.50% round-trip maker fee. Per grid trade it needs price to move at least the \
grid spacing (1.5%).

Below are PRE-COMPUTED facts. Explain ONLY these — do not invent numbers or \
thresholds. For EACH, lead with what the observation MEANS for running the grid \
and what to do or watch — the implication and the action, NOT a restatement of \
the number. Plain, concrete, calm; no hype, no disclaimers.

FACTS:
{facts}

Return STRICT JSON, no markdown, with this shape:
{{"overall": "<2-3 sentences: what this means for whether to run the grid right now, and the single most important thing to do or watch>",
  "per_metric": {{
     "volatility": "<1-2 sentences: what it means + what to do>",
     "regime": "<1-2 sentences: what it means + what to do>",
     "drawdown": "<1 sentence: how far below the recent high + whether it is downtrend bleed to act on>",
     "flow": "<1 sentence: what it implies + whether to act>",
     "harvest": "<1 sentence: what it implies for spacing/opportunity>"}}}}"""


def llm_interpret(summary):
    """Gemini 2.5 Flash narrative over the pre-computed facts. Returns a dict
    {overall, per_metric, source:'llm', usage:{...}} or None on ANY failure
    (missing key, network, rate limit, bad JSON) so the caller falls back."""
    if load_dotenv:
        load_dotenv("/root/xrp_grid/.env", override=False)
    key = (os.environ.get(config.INTERPRET_KEY_VAR) or "").strip()
    if not key:
        log.info("interpret: no %s set — static only", config.INTERPRET_KEY_VAR)
        return None
    try:
        import json
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=key)
        prompt = _PROMPT.format(facts=_facts_block(summary))
        t0 = time.time()
        resp = client.models.generate_content(
            model=config.INTERPRET_MODEL,
            contents=prompt,
            # thinking_budget=0 disables Flash's default "thinking" pass — it's
            # wasted tokens (and truncates the answer) for a task this simple,
            # and keeps spend minimal.
            config=types.GenerateContentConfig(
                temperature=0.3,
                max_output_tokens=config.INTERPRET_MAX_OUTPUT_TOKENS,
                response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        dt = time.time() - t0
        um = getattr(resp, "usage_metadata", None)
        usage = {
            "model": config.INTERPRET_MODEL,
            "input_tokens": getattr(um, "prompt_token_count", None),
            "output_tokens": getattr(um, "candidates_token_count", None),
            "total_tokens": getattr(um, "total_token_count", None),
            "latency_s": round(dt, 2),
        }
        # bill-watch breadcrumb in the journal
        log.info("interpret: %s ok — in=%s out=%s total=%s tokens, %.2fs (free-tier)",
                 config.INTERPRET_MODEL, usage["input_tokens"], usage["output_tokens"],
                 usage["total_tokens"], dt)
        data = json.loads(resp.text)
        overall = (data.get("overall") or "").strip()
        per = {k: (v or "").strip() for k, v in (data.get("per_metric") or {}).items() if v}
        if not overall and not per:
            return None
        return {"overall": overall, "per_metric": per, "source": "llm", "usage": usage}
    except Exception as e:
        log.warning("interpret: LLM failed (%r) — falling back to static", e)
        return None


# ---------------------------------------------------------------- public entry

def interpret(summary, use_llm=None):
    """Return a plain-English interpretation. LLM-on by default (toggleable);
    always merges with static so every field is populated and a failed/partial
    LLM degrades gracefully rather than leaving blanks."""
    if use_llm is None:
        use_llm = config.INTERPRET_LLM_DEFAULT_ON
    static = static_interpret(summary)
    if not use_llm or not config.INTERPRET_LLM_ENABLED:
        return static
    llm = llm_interpret(summary)
    if not llm:
        out = dict(static)
        out["llm_attempted"] = True   # tells the page it tried and fell back
        return out
    # LLM primary, static backfills any field the LLM left empty
    per = dict(static["per_metric"])
    per.update(llm.get("per_metric") or {})
    return {"overall": llm["overall"] or static["overall"],
            "per_metric": per, "source": "llm", "usage": llm.get("usage")}


def main():
    import sqlite3
    from tape import conditions
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    conn = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    try:
        summary = conditions.report(conn)
    finally:
        conn.close()
    print("=== STATIC ===")
    s = static_interpret(summary)
    print("overall:", s["overall"])
    for k, v in s["per_metric"].items():
        print(f"  [{k}] {v}")
    print("\n=== LLM (gemini-2.5-flash) ===")
    out = interpret(summary, use_llm=True)
    print("source:", out.get("source"), "| usage:", out.get("usage"))
    print("overall:", out["overall"])
    for k, v in out["per_metric"].items():
        print(f"  [{k}] {v}")


if __name__ == "__main__":
    main()

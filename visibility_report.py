"""
visibility_report.py
─────────────────────────────────────────────────────────────────────────────
Turns ai_visibility_cache.json + seo_cache.json into window.VISIBILITY_DATA
for the Search & AI Visibility sections of each client report:

  AI Tracking   — visibility score, per-engine cards, trend, prompt table,
                  answers, prompt ideas
  Classic SEO   — search visibility score, positions, top 3 / top 10, trend
  Competitors   — you vs tracked competitors in Google rankings and AI answers

Scores use the same bands everywhere:
  Poor < 20%  ·  Moderate 20–50%  ·  Good 50–80%  ·  Great 80%+

Used by generate_reports_v2.py (per client) and its index builder (all clients).
"""

import datetime
from collections import Counter
from html import escape

from fetch_ai_visibility import ENGINES, ENGINE_LABELS, brand_names, domain_of

ENGINE_COLORS = {
    "ai_overviews": "#2a78d6", "ai_mode": "#e0603c", "chatgpt": "#1fa67a",
    "claude": "#d9a01c", "gemini": "#e05c94", "perplexity": "#7b55d6",
}
ANSWERED = ("ok", "no_answer")


def band(pct):
    if pct is None:
        return None
    return "Great" if pct >= 80 else "Good" if pct >= 50 else "Moderate" if pct >= 20 else "Poor"


def pct(n, d):
    return round(n / d * 100) if d else None


def fmt_date(iso):
    try:
        return datetime.date.fromisoformat(iso[:10]).strftime("%b %-d, %Y")
    except Exception:
        return iso or ""


def short_date(iso):
    try:
        return datetime.date.fromisoformat(iso[:10]).strftime("%b %-d")
    except Exception:
        return iso or ""


def change_phrase(delta, unit="points"):
    if delta is None:
        return "first update"
    if delta == 0:
        return "no change since the last update"
    word = "up" if delta > 0 else "down"
    return f'<b class="{"up" if delta > 0 else "down"}">{word} {abs(delta)} {unit}</b> since the last update'


def join_quoted(items, n=2):
    return " and ".join(f"“{escape(i)}”" for i in items[:n])


# ── AI TRACKING ───────────────────────────────────────────────────────────────

def visible(cell):
    return cell.get("status") in ANSWERED and (cell.get("named") or cell.get("cited"))


def run_stats(run):
    """Score + per-engine counts for one stored run."""
    engines = run.get("live_engines") or run.get("engines") or []
    per = {}
    for eng in engines:
        cells = [p.get(eng, {}) for p in run["results"].values()]
        answered = [c for c in cells if c.get("status") in ANSWERED]
        per[eng] = {"visible": sum(1 for c in answered if visible(c)), "total": len(answered)}
    v = sum(e["visible"] for e in per.values())
    t = sum(e["total"] for e in per.values())
    return {"score": pct(v, t), "visible": v, "total": t, "per": per}


def cell_state(cell):
    s = cell.get("status", "skipped")
    if s == "skipped":
        return "untracked"
    if s == "error":
        return "error"
    if cell.get("named") and cell.get("cited"):
        return "cited_named"
    if cell.get("cited"):
        return "cited"
    if cell.get("named"):
        return "named"
    return "no_answer" if s == "no_answer" else "not_visible"


def build_ai(entry, seo_entry=None):
    runs = (entry or {}).get("runs") or []
    if not runs:
        return None
    cur, prev = runs[-1], (runs[-2] if len(runs) > 1 else None)
    brand = (entry.get("brand_names") or ["You"])[0]
    domain = entry.get("domain", "")
    engines = [e for e in ENGINES if e in (cur.get("engines") or ENGINES)]
    live = set(cur.get("live_engines") or engines)
    st, pst = run_stats(cur), (run_stats(prev) if prev else None)
    score = st["score"]
    score_delta = (score - pst["score"]) if pst and pst["score"] is not None and score is not None else None

    engine_stats = []
    for e in engines:
        es = st["per"].get(e)
        p = pct(es["visible"], es["total"]) if es else None
        prev_es = (pst or {}).get("per", {}).get(e) if pst else None
        engine_stats.append({
            "key": e, "label": ENGINE_LABELS[e], "color": ENGINE_COLORS[e],
            "tracked": e in live and bool(es and es["total"]),
            "visible": es["visible"] if es else 0, "total": es["total"] if es else 0,
            "pct": p, "band": band(p),
            "delta": (es["visible"] - prev_es["visible"]) if es and prev_es and prev_es["total"] else None,
        })

    # Prompt table
    rows, uncovered = [], []
    other_sites = Counter()
    for prompt in cur.get("prompts") or list(cur["results"]):
        per = cur["results"].get(prompt, {})
        cells, vis, of = {}, 0, 0
        for e in engines:
            c = per.get(e, {"status": "skipped"})
            state = cell_state(c)
            cells[e] = {
                "state": state, "named": bool(c.get("named")), "cited": bool(c.get("cited")),
                "sites": c.get("sites", []), "excerpt": c.get("excerpt", ""),
                "error": c.get("error", ""),
                "comps": {d: v for d, v in (c.get("competitors") or {}).items() if v.get("named") or v.get("cited")},
            }
            if c.get("status") in ANSWERED:
                of += 1
                if visible(c):
                    vis += 1
                else:
                    other_sites.update(s for s in c.get("sites", []) if s != domain)
        rows.append({"prompt": prompt, "cells": cells, "visible_in": vis, "of": of})
        if of and not vis:
            uncovered.append(prompt)

    # Summary
    tracked = [e for e in engine_stats if e["tracked"]]
    summary = []
    if score is not None:
        summary.append(
            f"<b>{escape(brand)}</b> shows up in <b>{score}% of AI answers</b> ({st['visible']} of {st['total']}), "
            f'which is <span class="band band-{band(score).lower()}">{band(score)}</span> visibility, '
            f"{change_phrase(score_delta)}.")
    if len(tracked) > 1:
        best = max(tracked, key=lambda e: (e["pct"] or 0))
        worst = min(tracked, key=lambda e: (e["pct"] or 0))
        summary.append(f"Strongest in <b>{best['label']}</b> ({best['pct']}% of prompts) and weakest in "
                       f"<b>{worst['label']}</b> ({worst['pct']}%).")
    if uncovered:
        summary.append(f"<b>{len(uncovered)} of {len(rows)} prompts</b> get no mention from any AI, for example "
                       f"{join_quoted(uncovered)}.")
    top_sites = [d for d, _ in other_sites.most_common(3)]
    if top_sites:
        summary.append("When AI doesn't mention you, it most often cites " +
                       ", ".join(f"<b>{escape(d)}</b> ({other_sites[d]})" for d in top_sites) + ".")
    opportunity = ""
    if tracked:
        worst = min(tracked, key=lambda e: (e["pct"] or 0))
        worst_sites = Counter()
        for r in rows:
            c = r["cells"][worst["key"]]
            if c["state"] in ("not_visible", "no_answer"):
                worst_sites.update(s for s in c["sites"] if s != domain)
        lean = [d for d, _ in worst_sites.most_common(2)]
        opportunity = (f"<b>{worst['label']}</b>" +
                       (f", which leans on {' and '.join('<b>'+escape(d)+'</b>' for d in lean)}" if lean else "") +
                       f". Getting {escape(brand)} mentioned on the sources it trusts"
                       + (f", and publishing content that answers the {len(uncovered)} uncovered prompt"
                          f"{'s' if len(uncovered) != 1 else ''}," if uncovered else ",")
                       + " is the fastest way to raise the score.")

    # Trend
    trend = {"labels": [short_date(r["date"]) for r in runs], "overall": [], "series": {e: [] for e in engines}}
    for r in runs:
        rs = run_stats(r)
        trend["overall"].append(rs["score"])
        for e in engines:
            es = rs["per"].get(e)
            trend["series"][e].append(pct(es["visible"], es["total"]) if es and es["total"] else None)

    # Competitors in AI answers
    comps = []
    for c in entry.get("competitors") or []:
        d = c.get("domain")
        if not d:
            continue
        per, v, t = {}, 0, 0
        for e in engines:
            ev = et = 0
            for prompt in cur["results"]:
                cell = cur["results"][prompt].get(e, {})
                if cell.get("status") in ANSWERED:
                    et += 1
                    cc = (cell.get("competitors") or {}).get(d, {})
                    ev += 1 if (cc.get("named") or cc.get("cited")) else 0
            per[e] = {"visible": ev, "total": et}
            v, t = v + ev, t + et
        comps.append({"name": c.get("name") or d, "domain": d, "score": pct(v, t), "band": band(pct(v, t)), "per": per})
    you = {"name": brand, "domain": domain, "you": True, "score": score, "band": band(score),
           "per": {e: st["per"].get(e, {"visible": 0, "total": 0}) for e in engines}}

    # Prompt ideas
    ideas = entry.get("prompt_ideas") or {}
    tracked_lower = {p.lower() for p in cur.get("prompts", [])}
    paa = list(ideas.get("paa") or [])
    for k in (seo_entry or {}).get("keyword_rankings") or []:
        for q in k.get("people_also_ask") or []:
            paa.append({"q": q, "from": k["keyword"]})
    seen, paa_clean = set(), []
    for item in paa:
        if item["q"].lower() in seen or item["q"].lower() in tracked_lower:
            continue
        seen.add(item["q"].lower())
        paa_clean.append(item)

    return {
        "checked": fmt_date(cur.get("checked_at") or cur["date"]),
        "brand": brand, "domain": domain,
        "engines": [{"key": e, "label": ENGINE_LABELS[e], "color": ENGINE_COLORS[e], "tracked": e in live}
                    for e in engines],
        "prompts_n": len(rows), "score": score, "score_delta": score_delta, "band": band(score),
        "visible_answers": st["visible"], "total_answers": st["total"],
        "prompts_any": sum(1 for r in rows if r["visible_in"]),
        "summary": summary, "opportunity": opportunity,
        "engine_stats": engine_stats, "trend": trend, "rows": rows,
        "competitors": [you] + comps,
        "ideas": {"paa": paa_clean[:25], "ai": [p for p in ideas.get("ai") or [] if p.lower() not in tracked_lower]},
    }


# ── CLASSIC SEO ───────────────────────────────────────────────────────────────

def build_seo(client, seo_entry):
    rankings = [k for k in (seo_entry or {}).get("keyword_rankings") or [] if "error" not in k]
    history = (seo_entry or {}).get("rank_history") or []
    if not rankings:
        return None
    brand = brand_names(client)[0]
    n = len(rankings)
    ranked = [k for k in rankings if k.get("position")]
    top3 = len([k for k in ranked if k["position"] <= 3])
    top10 = len([k for k in ranked if k["position"] <= 10])
    avg = round(sum(k["position"] for k in ranked) / len(ranked), 1) if ranked else None
    ai_cited = len([k for k in rankings if k.get("ai_overview_cited")])
    ai_shown = len([k for k in rankings if k.get("ai_overview")])
    score = pct(top10, n)

    prev = history[-2] if len(history) > 1 else None
    prev_pos = (prev or {}).get("positions", {})

    def d(cur, key):
        return (cur - prev[key]) if prev and prev.get(key) is not None and cur is not None else None

    keywords, up, down = [], 0, 0
    for k in sorted(rankings, key=lambda k: k.get("position") or 999):
        p0 = prev_pos.get(k["keyword"])
        p1 = k.get("position")
        change = (p0 - p1) if p0 and p1 else None  # positive = moved up
        if change:
            up += change > 0
            down += change < 0
        keywords.append({
            "kw": k["keyword"], "pos": p1, "change": change, "new": bool(p1 and prev and not p0),
            "url": k.get("url"), "ai_overview": bool(k.get("ai_overview")),
            "ai_cited": bool(k.get("ai_overview_cited")), "local_pack": bool(k.get("in_local_pack")),
        })

    summary = [
        f"<b>{escape(brand)}</b> has a search visibility score of <b>{score}%</b>, which is "
        f'<span class="band band-{band(score).lower()}">{band(score)}</span>: <b>{top10} of {n} keywords</b> '
        f"are on page 1 (top 10), {top3} of them in the top 3."
        + (f" Average position {avg}." if avg else "")]
    if prev:
        summary.append("Since the last update, " +
                       (f'<b class="up">{up} keyword{"s" if up != 1 else ""} moved up</b>' if up else "none moved up")
                       + " and " +
                       (f'<b class="down">{down} moved down</b>' if down else "none moved down") + ".")
    firsts = [k["kw"] for k in keywords if k["pos"] == 1]
    if firsts:
        summary.append(f"<b>#1 for {len(firsts)} keyword{'s' if len(firsts) != 1 else ''}</b>, including {join_quoted(firsts, 1)}.")
    quick = [k for k in keywords if k["pos"] and 4 <= k["pos"] <= 10]
    if quick:
        summary.append(f"<b>{len(quick)} quick win{'s' if len(quick) != 1 else ''}</b> on page one but outside the top 3, "
                       f"such as {join_quoted([quick[0]['kw']], 1)} (#{quick[0]['pos']}).")
    p2 = [k for k in keywords if k["pos"] and k["pos"] > 10]
    missing = [k for k in keywords if not k["pos"]]
    if p2 or missing:
        summary.append(f"{len(p2)} keyword{'s' if len(p2) != 1 else ''} on page two and {len(missing)} not in the top 20"
                       + (f" (e.g. {join_quoted([missing[0]['kw']], 1)})" if missing else "") + ".")
    if ai_shown:
        summary.append(f"Google shows an AI Overview for {ai_shown} of {n} keywords; it cites you on {ai_cited}.")

    if quick:
        opportunity = (f"push the {len(quick)} keyword{'s' if len(quick) != 1 else ''} sitting at #4–10 into the top 3. "
                       "Those pages already rank; better on-page content and a few strong links usually move them fastest.")
    elif missing or p2:
        opportunity = ("build a dedicated page for the keywords that don't rank yet. One focused page per topic "
                       "usually reaches page one faster than adding them to an existing page.")
    else:
        opportunity = "defend the top spots — keep these pages fresh and keep earning local links and reviews."

    # Competitors (Google rankings, latest check)
    comp_rows = [{"name": brand, "domain": (seo_entry or {}).get("website", ""), "you": True,
                  **_rank_summary([k.get("position") for k in rankings], n)}]
    comp_rows[0]["domain"] = domain_of(comp_rows[0]["domain"])
    for c in client.get("competitors", []):
        cd = domain_of(c.get("domain", ""))
        if cd:
            comp_rows.append({"name": c.get("name") or cd, "domain": cd,
                              **_rank_summary([(k.get("competitors") or {}).get(cd) for k in rankings], n)})

    return {
        "checked": fmt_date(rankings[0].get("checked_at") or seo_entry.get("fetched_at", "")),
        "location": seo_entry.get("location") or "",
        "tracked": n, "score": score, "band": band(score),
        "score_delta": (score - pct(prev["top10"], prev["tracked"])) if prev and prev.get("tracked") else None,
        "avg_position": avg, "avg_delta": d(avg, "avg_position"),
        "top3": top3, "top3_delta": d(top3, "top3"),
        "top10": top10, "top10_delta": d(top10, "top10"),
        "ai_cited": ai_cited, "ai_cited_delta": d(ai_cited, "ai_cited"), "ai_shown": ai_shown,
        "summary": summary, "opportunity": opportunity, "keywords": keywords,
        "trend": {
            "labels": [short_date(h["date"]) for h in history],
            "avg": [h.get("avg_position") for h in history],
            "top3": [h.get("top3") for h in history],
            "top10": [h.get("top10") for h in history],
        },
        "competitors": comp_rows,
    }


def _rank_summary(positions, n):
    ranked = [p for p in positions if p]
    return {
        "avg": round(sum(ranked) / len(ranked), 1) if ranked else None,
        "n1": len([p for p in ranked if p == 1]),
        "top3": len([p for p in ranked if p <= 3]),
        "top10": len([p for p in ranked if p <= 10]),
        "ranking": len(ranked), "tracked": n,
    }


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

def build_aeo(client, ai_entry):
    """AEO readiness audit + action plan (both produced by fetch_ai_visibility.py)."""
    audit = (ai_entry or {}).get("audit")
    plan = (ai_entry or {}).get("action_plan")
    out = {"audit": None, "plan": None}
    if audit:
        checks = audit.get("checks") or []
        out["audit"] = {
            "status": audit.get("status"), "score": audit.get("score"), "band": band(audit.get("score")),
            "url": audit.get("url") or client.get("website", ""), "error": audit.get("error", ""),
            "checked": fmt_date(audit.get("checked_at", "")),
            "checks": sorted(checks, key=lambda c: {"fail": 0, "warn": 1, "pass": 2}[c["status"]]),
            "counts": {s: sum(1 for c in checks if c["status"] == s) for s in ("pass", "warn", "fail")},
        }
    if plan and plan.get("actions"):
        order = {"high": 0, "medium": 1, "low": 2}
        out["plan"] = {**plan, "generated": fmt_date(plan.get("generated_at", "")),
                       "actions": sorted(plan["actions"], key=lambda a: order.get(a.get("priority"), 3))}
    return out


def build_visibility_data(client, ai_entry, seo_entry):
    seo_enrolled = bool(client.get("local_seo_enrolled"))
    return {
        "aeo": build_aeo(client, ai_entry),
        "ai":  build_ai(ai_entry, seo_entry if seo_enrolled else None),
        "seo": build_seo(client, seo_entry) if seo_enrolled else None,
        "seo_enrolled": seo_enrolled,
        "has_competitors": bool(client.get("competitors")),
        "brand": brand_names(client)[0],
    }

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

# Sites AI cites that are directories / platforms rather than peer organizations.
# They aren't competitors — they're places worth being listed on.
DIRECTORIES = {
    "yelp.com", "reddit.com", "facebook.com", "instagram.com", "youtube.com", "wikipedia.org",
    "en.wikipedia.org", "google.com", "maps.google.com", "tripadvisor.com", "nextdoor.com",
    "linkedin.com", "x.com", "twitter.com", "tiktok.com", "pinterest.com", "quora.com", "medium.com",
    "petfinder.com", "adoptapet.com", "rescueme.org", "charitynavigator.org", "guidestar.org",
    "candid.org", "greatnonprofits.org", "idealist.org", "volunteermatch.org", "eventbrite.com",
    "gofundme.com", "givebutter.com", "bbb.org", "yellowpages.com", "mapquest.com", "foursquare.com",
    "patch.com", "theknot.com", "weddingwire.com", "angi.com", "thumbtack.com", "indeed.com",
    "glassdoor.com", "apple.com", "bing.com", "chatgpt.com", "openai.com", "perplexity.ai",
}


def is_directory(domain):
    return domain in DIRECTORIES or any(domain.endswith("." + d) for d in DIRECTORIES) \
        or domain.endswith(".gov") or domain.endswith(".edu")


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

    # Who AI recommends instead: peer organizations (suggested competitors) vs
    # directories / platforms (places to get listed)
    tracked_comp = {c.get("domain") for c in entry.get("competitors") or []}
    cited = Counter()
    for r in rows:
        for c in r["cells"].values():
            cited.update(set(c["sites"]))
    suggested = [{"domain": d, "count": n} for d, n in cited.most_common()
                 if d != domain and d not in tracked_comp and not is_directory(d)][:6]
    listings = [{"domain": d, "count": n} for d, n in cited.most_common() if is_directory(d)][:8]

    # Accuracy of what AI says about the org
    acc = entry.get("accuracy")

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
        "suggested_competitors": suggested,
        "listings": listings,
        "accuracy": ({**acc, "generated": fmt_date(acc.get("generated_at", ""))} if acc else None),
        "ideas": {"paa": paa_clean[:25], "ai": [p for p in ideas.get("ai") or [] if p.lower() not in tracked_lower]},
    }


# ── CLASSIC SEO ───────────────────────────────────────────────────────────────

def build_seo(client, seo_entry):
    rankings = [k for k in (seo_entry or {}).get("keyword_rankings") or [] if "error" not in k]
    history = (seo_entry or {}).get("rank_history") or []
    if not rankings:
        return None
    brand = brand_names(client)[0]
    own_domain = domain_of((seo_entry or {}).get("website") or client.get("website", ""))
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
        trend = [h.get("positions", {}).get(k["keyword"]) for h in history]
        seen = [x for x in trend + [p1] if x]
        top = (k.get("top_domains") or [None])[0]
        keywords.append({
            "kw": k["keyword"], "pos": p1, "change": change, "new": bool(p1 and prev and not p0),
            "url": k.get("url"), "ai_overview": bool(k.get("ai_overview")),
            "ai_cited": bool(k.get("ai_overview_cited")), "local_pack": bool(k.get("in_local_pack")),
            "volume": k.get("volume"), "best": min(seen) if seen else None, "trend": trend[-12:],
            "top": top if top and top != own_domain else None, "ai_sources": k.get("ai_sources") or [],
            "comps": {cd: p for cd, p in (k.get("competitors") or {}).items()},
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
        summary.append(f"{len(p2)} keyword{'s' if len(p2) != 1 else ''} on pages 2–5 and {len(missing)} not in the top 50"
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

    # Who's on page one for your keywords (suggested competitors)
    tracked = {domain_of(c.get("domain", "")) for c in client.get("competitors", [])}
    page1 = Counter(d for k in rankings for d in (k.get("top_domains") or []))
    serp_suggest = [{"domain": d, "count": c} for d, c in page1.most_common()
                    if d and d != own_domain and d not in tracked and not is_directory(d)][:6]

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
        "suggested": serp_suggest,
        "comp_names": {domain_of(c.get("domain", "")): c.get("name") for c in client.get("competitors", [])},
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

def build_kit(client, audit, ga4_entry):
    """
    Ready-to-paste llms.txt and Organization JSON-LD built from what we know about
    the client. Anything we don't have is left as a clearly marked [placeholder].
    """
    website = (client.get("website") or "").rstrip("/")
    if not website:
        return None
    name = brand_names(client)[0]
    themes = (client.get("keywords") or {}).get("include_themes", [])
    locs = [l for l in (client.get("geo") or {}).get("locations", []) if l]
    desc = (audit or {}).get("description") or ""
    if not desc:
        desc = f"{name} is a nonprofit focused on {', '.join(themes[:3]) or 'its community'}" + \
               (f", serving {', '.join(locs)}." if locs else ".")
    pages = []
    for p in (ga4_entry or {}).get("landing_pages", [])[:8]:
        path = p.get("landingPage") or ""
        if path.startswith("/") and "(not set)" not in path and "?" not in path:
            label = "Home" if path == "/" else path.strip("/").replace("-", " ").replace("/", " › ").title()
            pages.append((label, website + path))
    if not pages:
        pages = [("Home", website + "/")]
    llms = "\n".join(
        [f"# {name}", "", f"> {desc}", "", "## What we do"] +
        [f"- {t[:1].upper() + t[1:]}" for t in themes] +
        (["", "## Where we serve"] + [f"- {l}" for l in locs] if locs else []) +
        ["", "## Key pages"] + [f"- [{l}]({u})" for l, u in dict(pages).items()] +
        ["", "## Contact", "- Phone: [add phone]", "- Email: [add email]", "- Address: [add street address or service area]"]
    ) + "\n"
    schema = {
        "@context": "https://schema.org",
        "@type": "NGO",
        "name": name,
        "url": website + "/",
        "description": desc,
        "logo": "[add logo URL]",
        "telephone": "[add phone]",
        "email": "[add email]",
        "address": {"@type": "PostalAddress", "streetAddress": "[add street]",
                    "addressLocality": locs[0].split(",")[0].strip() if locs and "," in locs[0] else "[add city]",
                    "addressRegion": locs[0].split(",")[1].strip() if locs and "," in locs[0] else "[add state]",
                    "addressCountry": "US"},
        "areaServed": locs or ["[add service area]"],
        "knowsAbout": themes,
        "sameAs": ["[add Facebook URL]", "[add Instagram URL]", "[add Candid/GuideStar profile URL]"],
    }
    import json as _json
    return {"llms_txt": llms,
            "schema": '<script type="application/ld+json">\n' + _json.dumps(schema, indent=2) + "\n</script>"}


def build_aeo(client, ai_entry, ga4_entry=None):
    """AEO readiness audit + action plan (both produced by fetch_ai_visibility.py)."""
    audit = (ai_entry or {}).get("audit")
    plan = (ai_entry or {}).get("action_plan")
    out = {"audit": None, "plan": None, "kit": build_kit(client, audit, ga4_entry)}
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


REFERRAL_COLORS = {"ChatGPT": "#1fa67a", "Perplexity": "#7b55d6", "Gemini": "#e05c94",
                   "Copilot": "#2a78d6", "Claude": "#d9a01c", "Other AI": "#94a3b8"}


def build_referrals(ga4_entry):
    """Visits sent by AI assistants (GA4) — the outcome AEO work is meant to move."""
    r = (ga4_entry or {}).get("ai_referrals")
    if r is None:
        return None
    total, prior = r.get("total_30d", 0), r.get("prior_total_30d", 0)
    labels = sorted({k for w in r.get("weekly", []) for k in w if k != "week"},
                    key=lambda k: list(REFERRAL_COLORS).index(k) if k in REFERRAL_COLORS else 99)

    def week_label(iso):
        try:
            return datetime.date.fromisocalendar(int(iso[:4]), int(iso[4:]), 1).strftime("%b %-d")
        except Exception:
            return iso
    return {
        "total": total, "prior": prior,
        "delta_pct": round((total - prior) / prior * 100) if prior else None,
        "sources": [{**x, "color": REFERRAL_COLORS.get(x["source"], "#94a3b8"),
                     "prior": (r.get("prior_by_source_30d") or {}).get(x["source"], 0)}
                    for x in r.get("by_source_30d", [])],
        "conversions": sum(x.get("conversions", 0) for x in r.get("by_source_30d", [])),
        "weekly": {"labels": [week_label(w["week"]) for w in r.get("weekly", [])],
                   "series": [{"label": l, "color": REFERRAL_COLORS.get(l, "#94a3b8"),
                               "data": [w.get(l, 0) for w in r.get("weekly", [])]} for l in labels]},
        "top_pages": r.get("top_pages", []),
    }


def build_local(client, local_entry):
    """Business Profile + heat map grids + map pack / Google Maps by city (fetch_local.py)."""
    if not client.get("local_tracking"):
        return None
    e = local_entry or {}
    runs = e.get("runs") or []
    cur, prev = (runs[-1] if runs else None), (runs[-2] if len(runs) > 1 else None)
    kws = []
    for kw, k in ((cur or {}).get("keywords") or {}).items():
        p = (prev or {}).get("keywords", {}).get(kw) or {}
        # Businesses that show up most across the grid, for the business picker
        seen = Counter(i for row in k.get("pts", []) for ids in row for i in ids[:20])
        top = [i for i, _ in seen.most_common(8)]
        you = next((i for i, b in enumerate(k.get("businesses", [])) if b.get("you")), None)
        if you is not None and you not in top:
            top.insert(0, you)
        kws.append({**{x: k[x] for x in ("grid", "solv", "arp", "atrp", "found", "points") if x in k},
                    "keyword": kw, "pts": k.get("pts", []), "businesses": k.get("businesses", []),
                    "picker": sorted(top, key=lambda i: (i != you, -seen[i])),
                    "center_top3": k.get("center_top3", []),
                    "solv_delta": (k["solv"] - p["solv"]) if p else None,
                    "atrp_delta": (k["atrp"] - p["atrp"]) if p and p.get("atrp") is not None else None})
    cities = []
    for city, per in ((cur or {}).get("cities") or {}).items():
        for kw, row in per.items():
            pv = ((prev or {}).get("cities") or {}).get(city, {}).get(kw, {})
            mp, gm = row.get("map_pack") or {}, row.get("google_maps") or {}
            cities.append({"city": city, "keyword": kw,
                           "pack_shown": mp.get("shown"), "pack_rank": mp.get("rank"), "pack_top": mp.get("top", []),
                           "pack_prev": (pv.get("map_pack") or {}).get("rank"),
                           "maps_rank": gm.get("rank"), "maps_top": gm.get("top", []),
                           "maps_prev": (pv.get("google_maps") or {}).get("rank"),
                           "has_pack": "map_pack" in row, "has_maps": "google_maps" in row})
    prof = e.get("profile")
    cfg = client.get("local_tracking") or {}
    summary, opportunity = [], ""
    for k in kws:
        summary.append(f"“{escape(k['keyword'])}”: top 3 at <b>{k.get('solv', 0)}%</b> of {k.get('points', 0)} map points"
                       + (f", average rank <b>{k['arp']}</b> where you show up" if k.get("arp") else ", not in the top 20 anywhere")
                       + (f" ({change_phrase(k['solv_delta'])})" if k.get("solv_delta") is not None else "") + ".")
    if prof and prof.get("rating") is not None:
        summary.append(f"Google rating <b>{prof['rating']}</b> from {prof.get('reviews') or 0:,} reviews.")
    if prof and prof.get("is_claimed") is False:
        opportunity = "Claim the Google Business Profile. Unclaimed listings rank lower and can't be updated."
    elif kws:
        weak = min(kws, key=lambda k: k.get("solv", 0))
        leader = next((b.get("title") for b in weak.get("center_top3") or [] if not b.get("you")), None)
        opportunity = (f"“{escape(weak['keyword'])}” is the weakest search on the map"
                       + (f", where <b>{escape(leader)}</b> leads" if leader else "") + ". Add it to the profile's "
                       "categories, services and description, and ask happy visitors for reviews that mention it.")
    return {
        "summary": summary, "opportunity": opportunity,
        "configured": True,
        "keywords_configured": cfg.get("keywords") or [],
        "profile": {**prof, "checked": fmt_date(prof.get("checked_at", ""))} if prof else None,
        "checked": fmt_date(cur["date"]) if cur else None,
        "center": (cur or {}).get("center"), "spacing_miles": (cur or {}).get("spacing_miles"),
        "points": (cur or {}).get("points"), "keywords": kws, "cities": cities,
    }


def build_organic(entry):
    """SEMrush-style organic data (fetch_organic.py)."""
    if not entry or not entry.get("overview"):
        return None
    hist = entry.get("history") or []
    prev = hist[-2] if len(hist) > 1 else None
    o = entry["overview"]

    def d(k):
        return (o[k] - prev[k]) if prev and prev.get(k) is not None else None
    kws = entry.get("keywords") or []
    page1 = o["pos_1"] + o["pos_2_3"] + o["pos_4_10"]
    summary = [f"The site ranks for <b>{o['keywords']:,} keywords</b> on Google, bringing an estimated "
               f"<b>{o['traffic']:,} visits a month</b>" + (f" (worth ${o['traffic_value']:,} a month in ads)" if o.get("traffic_value") else "")
               + (f", {change_phrase(d('traffic'), 'visits')}" if prev else "") + "."]
    if o["keywords"]:
        summary.append(f"<b>{page1:,}</b> of them are on page 1 and <b>{o['pos_11_20']:,}</b> on page 2.")
    pages = entry.get("pages") or []
    if pages and o["traffic"]:
        top = pages[0]
        summary.append(f"Top page: <b>{escape(top['url'].split('//')[-1])}</b>, about {round(top['traffic'] / max(o['traffic'], 1) * 100)}% "
                       f"of visits, mostly from “{escape(top['top_keyword'] or '')}”.")
    near = sorted((k for k in kws if k.get("position") and 11 <= k["position"] <= 20 and k.get("volume")),
                  key=lambda k: k["volume"], reverse=True)
    ideas = entry.get("opportunities") or []
    if near:
        k = near[0]
        opportunity = (f"“{escape(k['keyword'])}” ({k['volume']:,} searches a month) is at #{k['position']}, just off page 1. "
                       f"Strengthening {escape((k.get('url') or 'that page').split('//')[-1])} for it is the quickest win.")
    elif ideas:
        k = ideas[0]
        opportunity = (f"Publish a page for “{escape(k['keyword'])}” ({k['volume']:,} searches a month). "
                       "The site doesn't rank for it yet.")
    else:
        opportunity = ""
    return {
        "summary": summary, "opportunity": opportunity,
        "domain": entry["domain"], "checked": fmt_date(entry.get("fetched_at", "")),
        "overview": o, "keywords_delta": d("keywords"), "traffic_delta": d("traffic"),
        "keywords": entry.get("keywords") or [], "pages": entry.get("pages") or [],
        "competitors": entry.get("competitors") or [], "opportunities": entry.get("opportunities") or [],
        "authority": entry.get("authority"),
        "history": [{"month": datetime.date.fromisoformat(h["month"] + "-01").strftime("%b %Y"),
                     "keywords": h["keywords"], "traffic": h["traffic"]} for h in hist],
    }


def auth_band(v):
    if v is None:
        return None
    return "Great" if v >= 50 else "Good" if v >= 30 else "Moderate" if v >= 10 else "Poor"


def build_authority(client, entry):
    """Backlink authority (fetch_authority.py) with a plain-English summary."""
    if not entry or not entry.get("summary"):
        return None
    brand = brand_names(client)[0]
    sm, hist, prev = entry["summary"], entry.get("history") or [], entry.get("previous") or {}
    a, rd = sm["authority"], sm["referring_domains"]
    best, back = entry.get("best_links") or [], entry.get("links_to_get_back") or []
    nl = entry.get("new_lost_30d_clean") or {"new": 0, "lost": 0}
    raw = entry.get("new_lost_30d") or {}
    a_delta = (a - prev["authority"]) if prev.get("authority") is not None else None
    rd_delta = (rd - prev["referring_domains"]) if prev.get("referring_domains") else None
    best_delta = (len(best) - prev["best_links"]) if prev.get("best_links") is not None else None

    bullets = [f'<b>Authority is <span class="band band-{auth_band(a).lower()}">{auth_band(a)}</span>: {escape(brand)} '
               f'has an authority score of {a} out of 100.</b> Under 10 is Poor, 10–30 Moderate, 30–50 Good, 50+ Great.']
    if len(hist) > 1:
        first = hist[0]
        word = "risen" if a > first["authority"] else "fallen" if a < first["authority"] else "held steady"
        cls = "up" if a > first["authority"] else "down" if a < first["authority"] else ""
        bullets.append(f'Authority has <b class="{cls}">{word}' +
                       (f' from {first["authority"]} to {a}' if word != "held steady" else f' at {a}') +
                       '</b> over the last 12 months.')
        bullets.append(f'Linking sites went from {first["referring_domains"]:,} to {rd:,} this year, but '
                       f'<b>only {len(best)} are best links</b> ({round(len(best) / rd * 100, 1) if rd else 0}%): '
                       f'authority 20+, real traffic, followed and not spam.')
    if back:
        top = max(back, key=lambda r: r["authority"])
        bullets.append(f'<b>{len(back)} best link{"s were" if len(back) != 1 else " was"} lost in the last 12 months</b>, '
                       f'including {escape(top["domain"])} (authority {top["authority"]}). See <b>Links to get back</b>.')
    if entry.get("spam_count"):
        bullets.append(f'<b>{entry["spam_count"]} linking sites are flagged as spam.</b> Spam links are left out of these counts; '
                       f'a lot of them usually means bought link packages, which can hold rankings back.')
    extra = ""
    if raw.get("new") or raw.get("lost"):
        sn, sl = max(0, raw.get("new", 0) - nl["new"]), max(0, raw.get("lost", 0) - nl["lost"])
        if sn or sl:
            extra = f", plus {sn} spam sites gained and {sl} lost that don't count"
    bullets.append(f'In the last 30 days {escape(brand)} gained {nl["new"]} real linking site{"s" if nl["new"] != 1 else ""} '
                   f'and lost {nl["lost"]}{extra}.')
    target = 10 if a < 10 else 30 if a < 30 else 50 if a < 50 else a + 5
    comps = [c for c in entry.get("competitors") or [] if not c.get("you")]
    nxt = (f"keep adding best links every month, focused on sites competitors already have, to push authority past {target}."
           if comps else f"earn a few best links every month (local news, partner organizations, directories AI trusts) "
                         f"to push authority past {target}. Add competitors to see which sites link to them but not you.")
    if back:
        nxt = f"win back the {len(back)} lost best link{'s' if len(back) != 1 else ''} first (fastest gain), then " + nxt
    return {
        "checked": fmt_date(entry.get("fetched_at", "")), "domain": entry.get("domain"),
        "score": a, "band": auth_band(a), "delta": a_delta,
        "referring_domains": rd, "rd_delta": rd_delta, "best": len(best), "best_delta": best_delta,
        "new_lost": nl, "spam_count": entry.get("spam_count", 0),
        "summary": bullets, "next_step": nxt[:1].upper() + nxt[1:],
        "history": [{"label": datetime.date.fromisoformat(h["date"]).strftime("%b %-d") if h.get("date") else "",
                     "authority": h["authority"], "referring_domains": h["referring_domains"]} for h in hist],
        "tables": {"best": best, "get_back": back, "highest": entry.get("highest") or [],
                   "new": entry.get("new_links") or [], "lost": entry.get("lost_links") or [],
                   "anchors": entry.get("anchors") or [], "competitors": entry.get("competitors") or []},
    }


def build_overview(vis):
    """The three headline scores (AI · keyword · authority), each with one opportunity."""
    ai, seo, org, auth = vis.get("ai"), vis.get("seo"), vis.get("organic"), vis.get("authority")
    cards = []
    if ai and ai.get("score") is not None:
        cards.append({"key": "ai", "title": "AI visibility score", "value": ai["score"], "unit": "%", "band": ai["band"],
                      "delta": ai["score_delta"], "href": "#sec-ai",
                      "note": f"You show up in {ai['visible_answers']} of {ai['total_answers']} AI answers and in at "
                              f"least one AI for {ai['prompts_any']} of {ai['prompts_n']} prompts.",
                      "opportunity": ai.get("opportunity")})
    if seo:
        cards.append({"key": "seo", "title": "Keyword visibility score", "value": seo["score"], "unit": "%",
                      "band": seo["band"], "delta": seo.get("score_delta"), "href": "#sec-classic-seo",
                      "note": f"{seo['top10']} of {seo['tracked']} tracked keywords are on page 1, {seo['top3']} in the top 3.",
                      "opportunity": seo.get("opportunity")})
    elif org:
        o = org["overview"]
        p1 = o["pos_1"] + o["pos_2_3"] + o["pos_4_10"]
        sc = round(p1 / o["keywords"] * 100) if o["keywords"] else 0
        cards.append({"key": "seo", "title": "Keyword visibility score", "value": sc, "unit": "%", "band": band(sc),
                      "delta": None, "href": "#sec-organic",
                      "note": f"{p1:,} of the {o['keywords']:,} keywords your site ranks for are on page 1 of Google.",
                      "opportunity": f"{o['pos_11_20']:,} keywords sit on page 2. Improving those pages is the quickest "
                                     f"way to grow visits." if o.get("pos_11_20") else None})
    if auth:
        cards.append({"key": "authority", "title": "Authority score", "value": auth["score"], "unit": "",
                      "band": auth["band"], "delta": auth["delta"], "href": "#sec-authority", "scale": "authority",
                      "note": f"0–100: the strength of the sites linking here. {auth['best']} of "
                              f"{auth['referring_domains']:,} linking sites are best links.",
                      "opportunity": auth["next_step"], "opportunity_label": "Next step"})
    return cards


def build_watch(client, entry):
    """Competitor website changes: page edits, new pages, removed pages."""
    if not entry or not entry.get("sites"):
        return None
    changes = entry.get("changes") or []
    sites = [{"domain": d, "name": v.get("name") or d, "pages": v.get("url_count", 0),
              "watched": len(v.get("pages") or {}), "source": v.get("source"),
              "baseline": bool(v.get("baseline")), "error": v.get("error")}
             for d, v in entry["sites"].items()]
    for c in changes:
        c["when"] = short_date(c["date"])
    last = max((c["date"] for c in changes), default=None)
    latest = [c for c in changes if c["date"] == last]
    summary = []
    if all(s["baseline"] for s in sites):
        summary.append(f"Now watching {len(sites)} competitor site{'s' if len(sites) != 1 else ''}. "
                       "Changes show up from the next weekly check.")
    elif latest:
        by = Counter(c["name"] for c in latest)
        new_n = sum(1 for c in latest if c["kind"] == "new_page")
        edit_n = sum(1 for c in latest if c["kind"] == "changed")
        parts = ([f"<b>{new_n} new page{'s' if new_n != 1 else ''}</b>"] if new_n else []) + \
                ([f"<b>{edit_n} page edit{'s' if edit_n != 1 else ''}</b>"] if edit_n else [])
        summary.append(f"Latest check ({fmt_date(last)}): " + (" and ".join(parts) or "pages removed") + " across "
                       + ", ".join(f"<b>{escape(n)}</b>" for n, _ in by.most_common(3)) + ".")
    else:
        summary.append("No competitor changes found in the last 4 months.")
    return {"checked": fmt_date(entry.get("checked_at", "")), "sites": sites, "changes": changes,
            "latest": latest[:8], "summary": summary,
            "new_30d": sum(1 for c in changes if c["kind"] == "new_page" and c["date"] >= (datetime.date.today() - datetime.timedelta(days=30)).isoformat()),
            "edits_30d": sum(1 for c in changes if c["kind"] == "changed" and c["date"] >= (datetime.date.today() - datetime.timedelta(days=30)).isoformat())}


def _age(iso):
    try:
        return (datetime.date.today() - datetime.date.fromisoformat((iso or "")[:10])).days
    except ValueError:
        return None


def build_health(client, ai_entry, seo_entry, local_entry, organic_entry, authority_entry, watch_entry):
    """Problems with the latest data, shown as a 'last update had problems' banner."""
    out = []
    runs = (ai_entry or {}).get("runs") or []
    if runs:
        cur = runs[-1]
        if (_age(cur.get("date")) or 0) > 14:
            out.append(("ai", f"AI tracking last updated {fmt_date(cur['date'])}."))
        fails = Counter(e for per in cur["results"].values() for e, c in per.items() if c.get("status") == "error")
        for e, n in fails.most_common():
            out.append(("ai", f"{ENGINE_LABELS.get(e, e)} checks failed for {n} of {len(cur['results'])} prompts. They're retried next week."))
    if client.get("local_seo_enrolled") and seo_entry and (_age(seo_entry.get("fetched_at")) or 0) > 14:
        out.append(("keywords", f"Keyword rankings last updated {fmt_date(seo_entry['fetched_at'])}."))
    lr = (local_entry or {}).get("runs") or []
    if lr and (_age(lr[-1].get("date")) or 0) > 14:
        out.append(("local", f"Local map last updated {fmt_date(lr[-1]['date'])}."))
    for key, label, e in (("organic", "Organic search", organic_entry), ("authority", "Authority", authority_entry)):
        if e and (_age(e.get("fetched_at")) or 0) > 45:
            out.append((key, f"{label} last updated {fmt_date(e['fetched_at'])}."))
    for d, site in ((watch_entry or {}).get("sites") or {}).items():
        if site.get("error"):
            out.append(("competitors", f"Couldn't load {escape(d)} to check for changes."))
    return [{"area": k, "text": t} for k, t in out]


# Sections a client's plan can lock: key in clients.json "locked_sections" →
# (report sections, VISIBILITY_DATA keys removed so the data never reaches the page)
LOCKABLE = {
    "ai": (["sec-ai"], ["ai", "referrals"]),
    "aeo": (["sec-aeo-plan", "sec-aeo-audit"], ["aeo"]),
    "keywords": (["sec-classic-seo"], ["seo"]),
    "organic": (["sec-organic"], ["organic"]),
    "authority": (["sec-authority"], ["authority"]),
    "competitors": (["sec-competitors"], ["watch"]),
    "local": (["sec-gbp"], ["local"]),
}


def apply_locks(vis, locked):
    """Strip locked sections' data and record which report sections to lock."""
    sections = []
    for key in locked or []:
        secs, data_keys = LOCKABLE.get(key, ([], []))
        sections += secs
        for k in data_keys:
            vis[k] = None
    vis["locked"] = sections
    vis["health"] = [h["text"] for h in vis.get("health") or [] if h["area"] not in (locked or [])][:6]
    return vis


def build_visibility_data(client, ai_entry, seo_entry, ga4_entry=None, local_entry=None, organic_entry=None,
                          authority_entry=None, watch_entry=None):
    seo_enrolled = bool(client.get("local_seo_enrolled"))
    return {
        "authority": build_authority(client, authority_entry),
        "watch": build_watch(client, watch_entry),
        "health": build_health(client, ai_entry, seo_entry, local_entry, organic_entry, authority_entry, watch_entry),
        "organic": build_organic(organic_entry),
        "local": build_local(client, local_entry),
        "referrals": build_referrals(ga4_entry),
        "aeo": build_aeo(client, ai_entry, ga4_entry),
        "ai":  build_ai(ai_entry, seo_entry if seo_enrolled else None),
        "seo": build_seo(client, seo_entry) if seo_enrolled else None,
        "seo_enrolled": seo_enrolled,
        "has_competitors": bool(client.get("competitors")),
        "brand": brand_names(client)[0],
    }

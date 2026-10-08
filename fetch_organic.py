"""
fetch_organic.py
─────────────────────────────────────────────────────────────────────────────
SEMrush-style organic search data for every client with a website, from
DataForSEO Labs (Google, United States):

  Overview        organic keywords, estimated monthly traffic and its ad value,
                  position distribution (1 · 2–3 · 4–10 · 11–20 · 21–100),
                  keywords new / up / down / lost
  Keywords        top 100 ranking keywords by traffic: position, change,
                  search volume, CPC, difficulty, ranking URL, traffic
  Top pages       ranking pages by estimated traffic
  Competitors     domains that rank for the same keywords (directories filtered)
  Opportunities   keyword ideas from the client's themes they don't rank for yet
  Authority       (optional) DataForSEO Backlinks summary — needs the paid
                  Backlinks API subscription; set DATAFORSEO_BACKLINKS=1

Runs monthly (skips a client fetched in the last 25 days unless --force).
COST ≈ $0.07 per client per month (≈ $1/month for 13 clients) + backlinks if on.

OUTPUT  organic_cache.json (12 monthly snapshots of the overview kept for trends)

USAGE
  python fetch_organic.py                 # all clients with a website
  python fetch_organic.py --slug pup-profile --force
  python fetch_organic.py --dry-run
"""

import argparse
import datetime
import json
import os

from fetch_ai_visibility import DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, _dfs_headers, domain_of, post_json
from visibility_report import is_directory

CACHE_PATH = "organic_cache.json"
BASE = "https://api.dataforseo.com/v3"
LOC = {"location_name": "United States", "language_code": "en"}
REFRESH_DAYS = 25
BACKLINKS_ON = os.environ.get("DATAFORSEO_BACKLINKS", "") in ("1", "true", "yes")


def dfs(endpoint, task):
    data = post_json(f"{BASE}/{endpoint}", [task], _dfs_headers(), timeout=120)
    t = (data.get("tasks") or [{}])[0]
    if t.get("status_code") != 20000:
        raise RuntimeError(f"DataForSEO {t.get('status_code')}: {t.get('status_message')}")
    return (t.get("result") or [{}])[0] or {}


def overview(domain):
    items = dfs("dataforseo_labs/google/domain_rank_overview/live", {"target": domain, **LOC}).get("items") or []
    o = ((items[0] if items else {}).get("metrics") or {}).get("organic") or {}
    return {
        "keywords": o.get("count", 0), "traffic": round(o.get("etv") or 0),
        "traffic_value": round(o.get("estimated_paid_traffic_cost") or 0),
        "pos_1": o.get("pos_1", 0), "pos_2_3": o.get("pos_2_3", 0), "pos_4_10": o.get("pos_4_10", 0),
        "pos_11_20": o.get("pos_11_20", 0),
        "pos_21_100": sum(o.get(k, 0) for k in ("pos_21_30", "pos_31_40", "pos_41_50", "pos_51_60",
                                                 "pos_61_70", "pos_71_80", "pos_81_90", "pos_91_100")),
        "new": o.get("is_new", 0), "up": o.get("is_up", 0), "down": o.get("is_down", 0), "lost": o.get("is_lost", 0),
    }


def ranked_keywords(domain, limit=100):
    items = dfs("dataforseo_labs/google/ranked_keywords/live", {
        "target": domain, **LOC, "limit": limit,
        "order_by": ["ranked_serp_element.serp_item.etv,desc"],
    }).get("items") or []
    out = []
    for it in items:
        kd = it.get("keyword_data") or {}
        info = kd.get("keyword_info") or {}
        serp = (it.get("ranked_serp_element") or {}).get("serp_item") or {}
        ch = serp.get("rank_changes") or {}
        prev = ch.get("previous_rank_absolute")
        cur = serp.get("rank_absolute") or serp.get("rank_group")
        out.append({
            "keyword": kd.get("keyword"), "position": serp.get("rank_group") or cur,
            "change": (prev - cur) if (prev and cur) else None, "new": bool(ch.get("is_new")),
            "volume": info.get("search_volume"), "cpc": info.get("cpc"),
            "difficulty": (kd.get("keyword_properties") or {}).get("keyword_difficulty"),
            "intent": ((kd.get("search_intent_info") or {}).get("main_intent")),
            "url": serp.get("url"), "traffic": round(serp.get("etv") or 0, 1),
        })
    return out


def competitors(domain, own_keywords):
    items = dfs("dataforseo_labs/google/competitors_domain/live", {"target": domain, **LOC, "limit": 20}).get("items") or []
    out = []
    for it in items:
        d = domain_of(it.get("domain") or "")
        if not d or d == domain or is_directory(d):
            continue
        org = ((it.get("full_domain_metrics") or {}).get("organic")) or {}
        out.append({"domain": d, "common_keywords": it.get("intersections", 0),
                    "avg_position": round(it.get("avg_position") or 0, 1),
                    "keywords": org.get("count", 0), "traffic": round(org.get("etv") or 0)})
    return out[:8]


def opportunities(themes, ranking):
    if not themes:
        return []
    items = dfs("dataforseo_labs/google/keyword_ideas/live", {
        "keywords": themes[:5], **LOC, "limit": 60,
        "order_by": ["keyword_info.search_volume,desc"],
    }).get("items") or []
    have = {k["keyword"].lower() for k in ranking if k.get("keyword")}
    out = []
    for it in items:
        kw = it.get("keyword") or ""
        info = it.get("keyword_info") or {}
        if not kw or kw.lower() in have or not info.get("search_volume"):
            continue
        out.append({"keyword": kw, "volume": info.get("search_volume"), "cpc": info.get("cpc"),
                    "difficulty": (it.get("keyword_properties") or {}).get("keyword_difficulty"),
                    "intent": (it.get("search_intent_info") or {}).get("main_intent")})
    return out[:25]


def backlinks(domain):
    r = dfs("backlinks/summary/live", {"target": domain, "include_subdomains": True})
    return {"rank": r.get("rank"), "backlinks": r.get("backlinks"), "referring_domains": r.get("referring_domains"),
            "referring_main_domains": r.get("referring_main_domains"), "broken_backlinks": r.get("broken_backlinks"),
            "nofollow_domains": r.get("referring_domains_nofollow")}


def top_pages(ranking):
    pages = {}
    for k in ranking:
        if not k.get("url"):
            continue
        p = pages.setdefault(k["url"], {"url": k["url"], "keywords": 0, "traffic": 0.0, "top_keyword": k["keyword"]})
        p["keywords"] += 1
        p["traffic"] += k.get("traffic") or 0
    return sorted(({**p, "traffic": round(p["traffic"])} for p in pages.values()),
                  key=lambda p: p["traffic"], reverse=True)[:10]


def fetch_client(client, cache, force=False, dry_run=False):
    domain = domain_of(client.get("website", ""))
    if not domain or (client.get("organic_tracking") or {}).get("enabled") is False:
        return False
    slug = client["slug"]
    entry = cache.get(slug) or {}
    last = entry.get("fetched_at", "")
    if last and not force:
        age = (datetime.date.today() - datetime.date.fromisoformat(last[:10])).days
        if age < REFRESH_DAYS:
            print(f"  ⏭  {client['name']} — fetched {age} days ago")
            return False
    print(f"\n  🔎 {client['name']} ({domain})")
    if dry_run:
        return False

    out = {"domain": domain, "fetched_at": datetime.datetime.now().isoformat(timespec="seconds")}
    try:
        out["overview"] = overview(domain)
        o = out["overview"]
        print(f"     {o['keywords']} keywords · ~{o['traffic']} visits/mo · ${o['traffic_value']} value")
    except Exception as e:
        print(f"     overview failed: {str(e)[:120]}")
        out["overview"] = None
    try:
        out["keywords"] = ranked_keywords(domain)
    except Exception as e:
        print(f"     ranked keywords failed: {str(e)[:120]}")
        out["keywords"] = []
    out["pages"] = top_pages(out["keywords"])
    try:
        out["competitors"] = competitors(domain, out["keywords"])
    except Exception as e:
        print(f"     competitors failed: {str(e)[:120]}")
        out["competitors"] = []
    try:
        out["opportunities"] = opportunities((client.get("keywords") or {}).get("include_themes", []), out["keywords"])
    except Exception as e:
        print(f"     keyword ideas failed: {str(e)[:120]}")
        out["opportunities"] = []
    if BACKLINKS_ON:
        try:
            out["authority"] = backlinks(domain)
            print(f"     authority rank {out['authority']['rank']} · {out['authority']['referring_domains']} referring domains")
        except Exception as e:
            print(f"     backlinks failed (needs the Backlinks API subscription): {str(e)[:100]}")
    else:
        out["authority"] = entry.get("authority")

    history = entry.get("history", [])
    if out["overview"]:
        month = datetime.date.today().strftime("%Y-%m")
        history = [h for h in history if h["month"] != month] + [{"month": month, **{
            k: out["overview"][k] for k in ("keywords", "traffic", "pos_1", "pos_2_3", "pos_4_10")}}]
    out["history"] = history[-12:]
    cache[slug] = out
    return True


def run(slug_filter=None, force=False, dry_run=False):
    with open("clients.json") as f:
        clients = json.load(f)
    print(f"\nFetch organic search (DataForSEO Labs) · backlinks {'ON' if BACKLINKS_ON else 'off'}")
    if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD) and not dry_run:
        print("No DataForSEO credentials — nothing to do")
        return
    cache = {}
    if os.path.exists(CACHE_PATH):
        try:
            cache = json.load(open(CACHE_PATH))
        except Exception:
            cache = {}
    n = sum(fetch_client(c, cache, force or bool(slug_filter), dry_run)
            for c in clients if not slug_filter or c["slug"] == slug_filter)
    if dry_run:
        return
    cache["_meta"] = {"fetched_at": datetime.datetime.now().isoformat(timespec="seconds"), "clients_fetched": n}
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=1)
    print(f"\n✓ {CACHE_PATH} saved — {n} clients fetched")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="SEMrush-style organic search data")
    ap.add_argument("--slug")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(a.slug, a.force, a.dry_run)

"""
fetch_authority.py
─────────────────────────────────────────────────────────────────────────────
Authority (backlinks) for every client with a website, from the DataForSEO
Backlinks API. Needs the Backlinks API subscription on the DataForSEO account,
so it only runs when DATAFORSEO_BACKLINKS=1.

  Authority score   DataForSEO domain rank (0–1000) shown on a 0–100 scale,
                    with bands Poor < 10 · Moderate 10–30 · Good 30–50 · Great 50+
  Referring domains unique sites linking in, with 12-month history
  Best links        linking sites that pass every quality check:
                      authority 20+ · 100+ organic visits a month ·
                      at least one followed link · not spam
  New vs lost       linking sites gained / lost in the last 30 days, spam excluded
  Tables            best links · links to get back (best links lost in the last
                    12 months) · vs competitors · highest-authority sites ·
                    new links · lost links · anchor text

Monthly (skips clients fetched in the last 25 days unless --force).
COST  ≈ $0.10–0.30 per client per month on top of the subscription.

OUTPUT  authority_cache.json

USAGE
  DATAFORSEO_BACKLINKS=1 python fetch_authority.py
  DATAFORSEO_BACKLINKS=1 python fetch_authority.py --slug pup-profile --force
"""

import argparse
import datetime
import json
import os

from fetch_ai_visibility import DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, _dfs_headers, domain_of, post_json

CACHE_PATH = "authority_cache.json"
BASE = "https://api.dataforseo.com/v3"
ENABLED = os.environ.get("DATAFORSEO_BACKLINKS", "") in ("1", "true", "yes")
REFRESH_DAYS = 25

# Best-link quality checks
MIN_AUTHORITY = 20      # on the 0–100 scale
MIN_TRAFFIC = 100       # organic visits / month
MAX_SPAM = 30           # DataForSEO backlinks_spam_score (0–100)


def dfs(endpoint, task):
    data = post_json(f"{BASE}/{endpoint}", [task], _dfs_headers(), timeout=180)
    t = (data.get("tasks") or [{}])[0]
    if t.get("status_code") != 20000:
        raise RuntimeError(f"DataForSEO {t.get('status_code')}: {t.get('status_message')}")
    return (t.get("result") or [{}])[0] or {}


def score(rank):
    """DataForSEO rank is 0–1000; show it on the familiar 0–100 scale."""
    return round(rank / 10) if rank is not None else None  # None = no data, not 0


def summary(domain):
    r = dfs("backlinks/summary/live", {"target": domain, "include_subdomains": True})
    return {"authority": score(r.get("rank")), "backlinks": r.get("backlinks", 0),
            "referring_domains": r.get("referring_domains", 0),
            "spam_score": r.get("backlinks_spam_score"),
            "nofollow_domains": r.get("referring_domains_nofollow", 0)}


def history(domain):
    start = (datetime.date.today().replace(day=1) - datetime.timedelta(days=365)).isoformat()
    items = dfs("backlinks/history/live", {"target": domain, "date_from": start}).get("items") or []
    return [{"date": (it.get("date") or "")[:10], "authority": score(it.get("rank")) or 0,
             "referring_domains": it.get("referring_domains", 0), "backlinks": it.get("backlinks", 0)}
            for it in items]


def referring_domains(domain):
    items = dfs("backlinks/referring_domains/live", {
        "target": domain, "include_subdomains": True, "backlinks_status_type": "all",
        "limit": 1000, "order_by": ["rank,desc"],
    }).get("items") or []
    out = []
    for it in items:
        d = domain_of(it.get("domain") or "")
        if not d:
            continue
        nofollow = ((it.get("referring_links_attributes") or {}).get("nofollow")) or 0
        links = it.get("backlinks", 0)
        dofollow = it.get("dofollow") if it.get("dofollow") is not None else max(0, links - nofollow)
        out.append({
            "domain": d, "authority": score(it.get("rank")) or 0, "links": links, "followed": dofollow,
            "spam": (it.get("backlinks_spam_score") or 0) >= MAX_SPAM,
            "spam_score": it.get("backlinks_spam_score"),
            "first_seen": (it.get("first_seen") or "")[:10], "lost": (it.get("lost_date") or "")[:10] or None,
        })
    return out


def traffic(domains):
    """Organic visits / month for a batch of domains (DataForSEO Labs)."""
    out = {}
    for i in range(0, len(domains), 1000):
        items = dfs("dataforseo_labs/google/bulk_traffic_estimation/live", {
            "targets": domains[i:i + 1000], "location_name": "United States", "language_code": "en",
            "item_types": ["organic"],
        }).get("items") or []
        for it in items:
            out[domain_of(it.get("target") or "")] = round(((it.get("metrics") or {}).get("organic") or {}).get("etv") or 0)
    return out


def new_lost_30d(domain):
    start = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    items = dfs("backlinks/timeseries_new_lost_summary/live", {
        "target": domain, "date_from": start, "group_range": "month", "include_subdomains": True,
    }).get("items") or []
    return {"new": sum(it.get("new_referring_domains", 0) for it in items),
            "lost": sum(it.get("lost_referring_domains", 0) for it in items)}


def anchors(domain):
    items = dfs("backlinks/anchors/live", {"target": domain, "limit": 50,
                                           "order_by": ["referring_domains,desc"]}).get("items") or []
    total = sum(it.get("referring_domains", 0) for it in items) or 1
    return [{"anchor": it.get("anchor") or "(empty)", "sites": it.get("referring_domains", 0),
             "share": round(it.get("referring_domains", 0) / total * 100, 1), "links": it.get("backlinks", 0)}
            for it in items]


def competitors_table(client, own):
    targets = [own["domain"]] + [domain_of(c.get("domain", "")) for c in client.get("competitors", [])]
    targets = [t for t in dict.fromkeys(targets) if t][:10]
    if len(targets) < 2:
        return []
    ranks = {domain_of(it.get("target", "")): it.get("rank")
             for it in (dfs("backlinks/bulk_ranks/live", {"targets": targets}).get("items") or [])}
    refs = {domain_of(it.get("target", "")): it for it in
            (dfs("backlinks/bulk_referring_domains/live", {"targets": targets}).get("items") or [])}
    links = {domain_of(it.get("target", "")): it.get("backlinks") for it in
             (dfs("backlinks/bulk_backlinks/live", {"targets": targets}).get("items") or [])}
    names = {domain_of(c.get("domain", "")): c.get("name") for c in client.get("competitors", [])}
    return [{"domain": t, "name": "You" if t == own["domain"] else names.get(t, t), "you": t == own["domain"],
             "authority": score(ranks.get(t)), "referring_domains": (refs.get(t) or {}).get("referring_domains"),
             "backlinks": links.get(t)} for t in targets]


def fetch_client(client, cache, force=False):
    from plans import has
    if not has(client, "authority"):
        print(f"  ⏭  {client['name']} — authority isn't in their plan")
        return False
    domain = domain_of(client.get("website", ""))
    if not domain:
        return False
    slug = client["slug"]
    entry = cache.get(slug) or {}
    if entry.get("fetched_at") and not force:
        age = (datetime.date.today() - datetime.date.fromisoformat(entry["fetched_at"][:10])).days
        if age < REFRESH_DAYS:
            print(f"  ⏭  {client['name']} — fetched {age} days ago")
            return False
    print(f"\n  🔗 {client['name']} ({domain})")
    out = {"domain": domain, "fetched_at": datetime.datetime.now().isoformat(timespec="seconds")}
    out["summary"] = summary(domain)
    out["history"] = history(domain)
    refs = referring_domains(domain)
    live = [r for r in refs if not r["lost"]]
    try:
        t = traffic([r["domain"] for r in refs if r["authority"] >= MIN_AUTHORITY][:1000])
    except Exception as e:
        print(f"     traffic lookup failed (best links will skip the traffic check): {str(e)[:100]}")
        t = None
    for r in refs:
        r["traffic"] = (t or {}).get(r["domain"])
        r["best"] = (r["authority"] >= MIN_AUTHORITY and not r["spam"] and r["followed"] > 0
                     and (t is None or (r["traffic"] or 0) >= MIN_TRAFFIC))
    year_ago = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    month_ago = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    out["best_links"] = [r for r in live if r["best"]]
    out["links_to_get_back"] = [r for r in refs if r["lost"] and r["lost"] >= year_ago and r["best"]]
    out["highest"] = live[:100]
    out["new_links"] = sorted([r for r in live if r["first_seen"] >= month_ago and not r["spam"]],
                              key=lambda r: r["authority"], reverse=True)[:100]
    out["lost_links"] = sorted([r for r in refs if r["lost"] and r["lost"] >= month_ago and not r["spam"]],
                               key=lambda r: r["authority"], reverse=True)[:100]
    out["spam_count"] = sum(1 for r in live if r["spam"])
    try:
        out["new_lost_30d"] = new_lost_30d(domain)
    except Exception:
        out["new_lost_30d"] = None
    out["new_lost_30d_clean"] = {"new": len(out["new_links"]), "lost": len(out["lost_links"])}
    out["anchors"] = anchors(domain)
    try:
        out["competitors"] = competitors_table(client, out)
    except Exception as e:
        print(f"     competitor comparison failed: {str(e)[:100]}")
        out["competitors"] = []
    out["previous"] = {"authority": (entry.get("summary") or {}).get("authority"),
                       "referring_domains": (entry.get("summary") or {}).get("referring_domains"),
                       "best_links": len(entry.get("best_links") or []) if entry else None}
    s = out["summary"]
    print(f"     authority {s['authority']} · {s['referring_domains']} referring domains · "
          f"{len(out['best_links'])} best links · {len(out['links_to_get_back'])} to get back")
    cache[slug] = out
    return True


def run(slug_filter=None, force=False):
    if not ENABLED:
        print("Authority is off — set DATAFORSEO_BACKLINKS=1 (needs the DataForSEO Backlinks API subscription)")
        return
    if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD):
        print("No DataForSEO credentials — nothing to do")
        return
    clients = json.load(open("clients.json"))
    cache = json.load(open(CACHE_PATH)) if os.path.exists(CACHE_PATH) else {}
    n = 0
    for c in clients:
        if slug_filter and c["slug"] != slug_filter:
            continue
        try:
            n += fetch_client(c, cache, force or bool(slug_filter))
        except Exception as e:
            print(f"  ✗ {c['name']}: {str(e)[:160]}")
    cache["_meta"] = {"fetched_at": datetime.datetime.now().isoformat(timespec="seconds"), "clients_fetched": n}
    json.dump(cache, open(CACHE_PATH, "w"), indent=1)
    print(f"\n✓ {CACHE_PATH} saved — {n} clients")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backlink authority")
    ap.add_argument("--slug")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    run(a.slug, a.force)

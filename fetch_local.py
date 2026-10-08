"""
fetch_local.py
─────────────────────────────────────────────────────────────────────────────
Local map tracking (Google Maps "heat map") + Google Business Profile details.

For each client with "local_tracking" in clients.json:

  1. Business profile — looks the business up on Google (DataForSEO Business
     Data API): rating, review count, category, address, phone, hours,
     claimed status, coordinates.
  2. Map grid — searches each keyword on Google Maps from a grid of points
     around the business (default 5×5 across 5 miles) and records where the
     business ranks at each point. That's the heat map.

Per keyword it reports:
  SoLV  share of local voice — % of grid points where you're in the top 3
        (the map pack people actually see)
  ARP   average rank where you appear
  ATRP  average rank counting "not in top 20" as 21

CONFIG (clients.json)
  "local_tracking": {
    "business_name": "Humane Society of Northwest Montana",
    "keywords": ["animal shelter", "adopt a dog", "humane society"],
    "grid": 5,                 # 5 → 5×5 = 25 points (3, 5 or 7)
    "radius_miles": 5,         # center to edge
    "lat": 48.19, "lng": -114.3,   # optional; found automatically
    "place_id": "ChIJ..."          # optional; makes matching exact
  }

COST  ~$0.002 per grid point per keyword: 25 points × 3 keywords ≈ $0.15
      per client per run, plus ~$0.004 for the business lookup.

OUTPUT  local_cache.json — read by generate_reports_v2.py (history kept, 26 runs)

USAGE
  python fetch_local.py                       # every configured client
  python fetch_local.py --slug humane-society-nw-montana
  python fetch_local.py --dry-run             # show grid + keywords, call nothing
"""

import argparse
import datetime
import json
import math
import os
import re

from fetch_ai_visibility import DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, _dfs_headers, domain_of, post_json, seo_location

CACHE_PATH = "local_cache.json"
BASE = "https://api.dataforseo.com/v3"
NOT_FOUND = 21  # rank used for "not in top 20" in ATRP


def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def dfs(endpoint, task):
    data = post_json(f"{BASE}/{endpoint}", [task], _dfs_headers(), timeout=120)
    t = (data.get("tasks") or [{}])[0]
    if t.get("status_code") != 20000:
        raise RuntimeError(f"DataForSEO {t.get('status_code')}: {t.get('status_message')}")
    return ((t.get("result") or [{}])[0] or {}).get("items") or []


# ── Business profile ──────────────────────────────────────────────────────────

def business_info(cfg, client):
    items = dfs("business_data/google/my_business_info/live", {
        "keyword": cfg["business_name"], "location_name": seo_location(client), "language_code": "en",
    })
    if not items:
        return None
    want_domain = domain_of(client.get("website", ""))
    best = None
    for it in items:
        if cfg.get("place_id") and it.get("place_id") == cfg["place_id"]:
            best = it
            break
        if want_domain and domain_of(it.get("url") or it.get("domain") or "") == want_domain:
            best = best or it
    it = best or items[0]
    rating = it.get("rating") or {}
    hours = []
    timetable = (((it.get("work_time") or {}).get("work_hours") or {}).get("timetable")) or {}
    for day, spans in timetable.items():
        hours.append({"day": day.title(), "spans": [
            f"{sp['open']['hour']:02d}:{sp['open']['minute']:02d}–{sp['close']['hour']:02d}:{sp['close']['minute']:02d}"
            for sp in (spans or []) if sp.get("open") and sp.get("close")]})
    return {
        "title": it.get("title"), "category": it.get("category"),
        "additional_categories": it.get("additional_categories") or [],
        "address": it.get("address"), "phone": it.get("phone"), "url": it.get("url"),
        "rating": rating.get("value"), "reviews": rating.get("votes_count"),
        "is_claimed": it.get("is_claimed"), "place_id": it.get("place_id"), "cid": it.get("cid"),
        "lat": it.get("latitude"), "lng": it.get("longitude"),
        "photos": it.get("total_photos"), "hours": hours,
        "matched_by": "place_id" if cfg.get("place_id") and it.get("place_id") == cfg["place_id"]
                      else ("website" if best else "name"),
    }


# ── Grid ──────────────────────────────────────────────────────────────────────

def grid_points(lat, lng, n, radius_miles):
    """n×n points spread evenly from -radius to +radius around the center."""
    n = max(3, min(7, int(n) | 1))  # odd so the business sits on the center point
    dlat = radius_miles / 69.0
    dlng = radius_miles / (69.0 * math.cos(math.radians(lat)))
    steps = [(-1 + 2 * i / (n - 1)) for i in range(n)]
    return [[(round(lat + dy * dlat, 6), round(lng + dx * dlng, 6)) for dx in steps] for dy in reversed(steps)]


def zoom_for(radius_miles):
    return 15 if radius_miles <= 2 else 14 if radius_miles <= 5 else 13 if radius_miles <= 10 else 12


def is_us(item, biz, cfg, want_domain):
    if biz and biz.get("place_id") and item.get("place_id") == biz["place_id"]:
        return True
    if biz and biz.get("cid") and str(item.get("cid")) == str(biz["cid"]):
        return True
    if want_domain and domain_of(item.get("url") or item.get("domain") or "") == want_domain:
        return True
    return norm(item.get("title")) == norm(cfg["business_name"])


def rank_at(keyword, lat, lng, zoom, biz, cfg, want_domain):
    items = dfs("serp/google/maps/live/advanced", {
        "keyword": keyword, "location_coordinate": f"{lat},{lng},{zoom}z",
        "language_code": "en", "depth": 20,
    })
    rank, top = None, []
    for it in items:
        if it.get("type") not in ("maps_search", "local_pack"):
            continue
        r = it.get("rank_group") or it.get("rank_absolute")
        if len(top) < 3:
            top.append({"title": it.get("title"), "rating": (it.get("rating") or {}).get("value"),
                        "reviews": (it.get("rating") or {}).get("votes_count")})
        if rank is None and is_us(it, biz, cfg, want_domain):
            rank = r
    return rank, top


def keyword_stats(grid):
    flat = [r for row in grid for r in row]
    found = [r for r in flat if r]
    return {
        "solv": round(sum(1 for r in found if r <= 3) / len(flat) * 100) if flat else 0,
        "arp": round(sum(found) / len(found), 1) if found else None,
        "atrp": round(sum(r or NOT_FOUND for r in flat) / len(flat), 1) if flat else None,
        "found": len(found), "points": len(flat),
    }


def fetch_client(client, cache, dry_run=False):
    cfg = client.get("local_tracking") or {}
    slug = client["slug"]
    cfg.setdefault("business_name", client["name"])
    keywords = (cfg.get("keywords") or [])[:5]
    n, radius = cfg.get("grid", 5), cfg.get("radius_miles", 5)
    print(f"\n  📍 {client['name']} — {len(keywords)} keywords · {n}×{n} grid · {radius} mi")
    if dry_run:
        for k in keywords:
            print(f"     • {k}")
        return False

    entry = cache.get(slug) or {}
    biz = None
    try:
        biz = business_info(cfg, client)
        if biz:
            print(f"     ✓ Profile: {biz['title']} · {biz.get('rating')}★ ({biz.get('reviews')} reviews) · matched by {biz['matched_by']}")
    except Exception as e:
        print(f"     Business lookup failed: {str(e)[:120]}")
    if biz:
        entry["profile"] = {**biz, "checked_at": datetime.datetime.now().isoformat(timespec="seconds")}

    lat = cfg.get("lat") or (biz or {}).get("lat")
    lng = cfg.get("lng") or (biz or {}).get("lng")
    if not (lat and lng):
        print("     No coordinates (add lat/lng to local_tracking) — skipping map grid")
        cache[slug] = entry
        return True

    points = grid_points(float(lat), float(lng), n, float(radius))
    zoom = zoom_for(float(radius))
    want_domain = domain_of(client.get("website", ""))
    run = {"date": datetime.date.today().isoformat(), "center": [lat, lng], "radius_miles": radius,
           "points": points, "keywords": {}}
    for kw in keywords:
        grid, center_top = [], []
        for i, row in enumerate(points):
            out_row = []
            for j, (plat, plng) in enumerate(row):
                try:
                    rank, top = rank_at(kw, plat, plng, zoom, biz, cfg, want_domain)
                except Exception as e:
                    print(f"     {kw} @ {plat},{plng}: {str(e)[:80]}")
                    rank, top = None, []
                out_row.append(rank)
                if i == len(points) // 2 and j == len(row) // 2:
                    center_top = top
            grid.append(out_row)
        stats = keyword_stats(grid)
        run["keywords"][kw] = {"grid": grid, "center_top3": center_top, **stats}
        print(f"     {kw:<30} SoLV {stats['solv']}% · ARP {stats['arp']} · found at {stats['found']}/{stats['points']}")

    entry["runs"] = ([r for r in entry.get("runs", []) if r["date"] != run["date"]] + [run])[-26:]
    cache[slug] = entry
    return True


def run(slug_filter=None, dry_run=False):
    with open("clients.json") as f:
        clients = [c for c in json.load(f) if c.get("local_tracking")]
    print(f"\nFetch local map rankings — {len(clients)} client(s) with local_tracking")
    if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD) and not dry_run:
        print("No DataForSEO credentials (DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD) — nothing to do")
        return
    cache = {}
    if os.path.exists(CACHE_PATH):
        try:
            cache = json.load(open(CACHE_PATH))
        except Exception:
            cache = {}
    n = 0
    for c in clients:
        if slug_filter and c["slug"] != slug_filter:
            continue
        n += fetch_client(c, cache, dry_run)
    if dry_run:
        return
    cache["_meta"] = {"fetched_at": datetime.datetime.now().isoformat(timespec="seconds"), "clients_fetched": n}
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=1)
    print(f"\n✓ {CACHE_PATH} saved — {n} clients")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Local map grid + Business Profile")
    ap.add_argument("--slug")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(a.slug, a.dry_run)

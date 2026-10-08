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
    "grid": 7,                 # 7 → 7×7 = 49 points (odd, 3–9)
    "spacing_miles": 1,        # distance between grid points
    "cities": ["Kalispell, MT"],   # map pack / Google Maps checks; default geo.locations
    "modes": ["heat_map", "map_pack", "google_maps"],
    "lat": 48.19, "lng": -114.3,   # optional; found automatically
    "place_id": "ChIJ..."          # optional; makes matching exact
  }

COST  ~$0.002 per grid point per keyword: 49 points × 3 keywords ≈ $0.30 per
      client per run, plus ~$0.004 per city per keyword for map pack / Maps.

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


def maps_results(keyword, task_location, biz, cfg, want_domain):
    """Top 20 Google Maps results: [{title, place_id, rating, reviews, you}] in rank order."""
    items = dfs("serp/google/maps/live/advanced", {"keyword": keyword, "language_code": "en", "depth": 20, **task_location})
    out = []
    for it in items:
        if it.get("type") not in ("maps_search", "local_pack"):
            continue
        out.append({"title": it.get("title") or "", "place_id": it.get("place_id") or it.get("cid") or "",
                    "rating": (it.get("rating") or {}).get("value"),
                    "reviews": (it.get("rating") or {}).get("votes_count"),
                    "you": is_us(it, biz, cfg, want_domain)})
    return out[:20]


def map_pack(keyword, location_name, biz, cfg, want_domain):
    """Position in the Google search map pack (the 3-business box) for a city: 1–3 or None."""
    items = dfs("serp/google/organic/live/advanced", {"keyword": keyword, "location_name": location_name,
                                                      "language_code": "en", "device": "desktop"})
    pack = [it for it in items if it.get("type") == "local_pack"]
    if not pack:
        return {"shown": False, "rank": None, "top": []}
    rank = next((i + 1 for i, it in enumerate(pack) if is_us(it, biz, cfg, want_domain)), None)
    return {"shown": True, "rank": rank, "top": [it.get("title") for it in pack[:3]]}


def keyword_stats(grid):
    """-1 marks a point whose check failed; it's left out of every stat."""
    failed = sum(1 for row in grid for r in row if r == -1)
    flat = [r for row in grid for r in row if r != -1]
    found = [r for r in flat if r]
    return {
        "solv": round(sum(1 for r in found if r <= 3) / len(flat) * 100) if flat else 0,
        "arp": round(sum(found) / len(found), 1) if found else None,
        "atrp": round(sum(r or NOT_FOUND for r in flat) / len(flat), 1) if flat else None,
        "found": len(found), "points": len(flat), "failed": failed,
    }


def city_locations(client, cfg):
    """DataForSEO location names for the cities to check map pack / Google Maps from."""
    from fetch_ai_visibility import US_STATES
    out = []
    for loc in cfg.get("cities") or (client.get("geo") or {}).get("locations", []):
        if "," in loc:
            city, st = [p.strip() for p in loc.split(",", 1)]
            if st.upper() in US_STATES:
                out.append((loc, f"{city},{US_STATES[st.upper()]},United States"))
                continue
        out.append((loc, loc if "," in loc else f"{loc},United States"))
    return out[:5]


def fetch_client(client, cache, dry_run=False):
    from plans import has
    if not has(client, "local"):
        print(f"  ⏭  {client['name']} — the local map isn't in their plan")
        return False
    from plans import due
    if not due(client, ((cache.get(client["slug"]) or {}).get("runs") or [{}])[-1].get("date")):
        print(f"  ⏭  {client['name']} — monthly checks, not due yet")
        return False
    cfg = client.get("local_tracking") or {}
    slug = client["slug"]
    cfg.setdefault("business_name", client["name"])
    keywords = (cfg.get("keywords") or [])[:5]
    n = max(3, min(7, int(cfg.get("grid", 7)) | 1))  # same limits as grid_points
    spacing = float(cfg.get("spacing_miles") or (2 * float(cfg["radius_miles"]) / (n - 1) if cfg.get("radius_miles") else 1))
    radius = spacing * (n - 1) / 2
    modes = cfg.get("modes") or ["heat_map", "map_pack", "google_maps"]
    print(f"\n  📍 {client['name']} — {len(keywords)} keywords · {n}×{n} grid, {spacing:g} mi apart · {', '.join(modes)}")
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

    want_domain = domain_of(client.get("website", ""))
    run = {"date": datetime.date.today().isoformat(), "keywords": {}, "cities": {}}

    # Map pack + Google Maps, from each target city
    cities = city_locations(client, cfg)
    for label, loc in cities:
        run["cities"][label] = {}
        for kw in keywords:
            row = {}
            if "map_pack" in modes:
                try:
                    row["map_pack"] = map_pack(kw, loc, biz, cfg, want_domain)
                except Exception as e:
                    print(f"     map pack {kw} @ {label}: {str(e)[:80]}")
            if "google_maps" in modes:
                try:
                    res = maps_results(kw, {"location_name": loc}, biz, cfg, want_domain)
                    row["google_maps"] = {"rank": next((i + 1 for i, r in enumerate(res) if r["you"]), None),
                                          "top": [r["title"] for r in res[:3]]}
                except Exception as e:
                    print(f"     maps {kw} @ {label}: {str(e)[:80]}")
            run["cities"][label][kw] = row
        if cities:
            print(f"     ✓ {label}: map pack + Google Maps for {len(keywords)} keywords")

    # Heat map grid
    lat = cfg.get("lat") or (biz or {}).get("lat")
    lng = cfg.get("lng") or (biz or {}).get("lng")
    if "heat_map" in modes and lat and lng:
        points = grid_points(float(lat), float(lng), n, radius)
        zoom = zoom_for(radius)
        run.update({"center": [lat, lng], "radius_miles": radius, "spacing_miles": spacing, "points": points})
        for kw in keywords:
            businesses, index, grid, pts = [], {}, [], []
            for row in points:
                g_row, p_row = [], []
                for plat, plng in row:
                    try:
                        res = maps_results(kw, {"location_coordinate": f"{plat},{plng},{zoom}z"}, biz, cfg, want_domain)
                    except Exception as e:
                        print(f"     {kw} @ {plat},{plng}: {str(e)[:80]}")
                        res = None  # check failed: not the same as "not in the top 20"
                    ids = []
                    if res is None:
                        g_row.append(-1)
                        p_row.append([])
                        continue
                    for r in res:
                        key = r["place_id"] or norm(r["title"])
                        if key not in index:
                            index[key] = len(businesses)
                            businesses.append({k: r[k] for k in ("title", "rating", "reviews", "you")})
                        ids.append(index[key])
                    g_row.append(next((i + 1 for i, r in enumerate(res) if r["you"]), None))
                    p_row.append(ids)
                grid.append(g_row)
                pts.append(p_row)
            stats = keyword_stats(grid)
            center = pts[len(pts) // 2][len(pts) // 2] if pts else []
            run["keywords"][kw] = {"grid": grid, "pts": pts, "businesses": businesses,
                                   "center_top3": [businesses[i] for i in center[:3]], **stats}
            print(f"     {kw:<30} top-3 coverage {stats['solv']}% · avg rank {stats['atrp']} · found {stats['found']}/{stats['points']}")
    elif "heat_map" in modes:
        print("     No coordinates (add lat/lng to local_tracking) — skipping heat map")

    entry["runs"] = ([r for r in entry.get("runs", []) if r["date"] != run["date"]] + [run])[-26:]
    cache[slug] = entry
    return True


def run(slug_filter=None, dry_run=False):
    with open("clients.json") as f:
        clients = [c for c in json.load(f) if c.get("local_tracking") and not c.get("demo")]  # demo clients use demo_data.py
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

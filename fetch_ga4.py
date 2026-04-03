"""
fetch_ga4.py
─────────────────────────────────────────────────────────────────────────────
Fetches GA4 analytics data for all clients that have a ga4_id in clients.json.
Uses Windsor.ai GA4 connector (same as Google Ads data pipeline).

DATA PULLED PER CLIENT
  Sessions, users, bounce rate, engagement rate (30d + 7d)
  Landing pages — top 10 by sessions with avg time + engagement
  Geographic data — top cities and states by sessions
  Device breakdown — desktop / mobile / tablet
  Conversion events — each named event with count
  New vs returning users
  Daily session trend (30d)
  Channel breakdown — organic / paid / direct / social

OUTPUT
  ga4_cache.json — read by generate_reports.py

USAGE
  python fetch_ga4.py              # fetch all connected clients
  python fetch_ga4.py --slug pup-profile  # fetch one client
  python fetch_ga4.py --dry-run    # show what would be fetched
"""

import json
import os
import sys
import datetime
import argparse
import urllib.request
import urllib.parse

# ── LOAD CLIENTS ──────────────────────────────────────────────────────────────

with open("clients.json") as f:
    CLIENTS = json.load(f)

WINDSOR_API_KEY = os.environ.get("WINDSOR_API_KEY", "")
WINDSOR_BASE    = "https://connectors.windsor.ai/googleanalytics4"

# GA4 property IDs — update as Michael connects more
GA4_ACCOUNTS = {
    "pup-profile":       "477106491",
    "guardian-pet-trust": "482775564",
    "city-dogs-kitties": "490392378",
    "serenity-horse-rescue": "515161089",
    # Add more as connected:
    # "red-clay-ranch":   "XXXXXXXXX",
    # "straydog":         "XXXXXXXXX",
    # "ziva":             "XXXXXXXXX",
}

# ── WINDSOR GA4 FETCHER ───────────────────────────────────────────────────────

def fetch_ga4(account_id, fields, date_preset, filters=None):
    """Call Windsor GA4 connector."""
    if not WINDSOR_API_KEY:
        print("  No WINDSOR_API_KEY — skipping GA4 fetch")
        return []

    params = {
        "api_key":    WINDSOR_API_KEY,
        "connector":  "googleanalytics4",
        "accounts":   account_id,
        "date_preset": date_preset,
        "fields":     ",".join(fields),
    }
    if filters:
        params["filters"] = json.dumps(filters)

    url = f"https://connectors.windsor.ai/googleanalytics4?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read())
            return data.get("data", data) if isinstance(data, dict) else data
    except Exception as e:
        print(f"  GA4 fetch error: {e}")
        return []


def fetch_client_ga4(slug, account_id, dry_run=False):
    """Fetch all GA4 data points for one client."""
    print(f"  {slug} (GA4: {account_id})")

    if dry_run:
        print(f"    [DRY RUN] Would fetch GA4 data for {account_id}")
        return _empty_ga4()

    result = {}

    # ── 1. Overview — sessions, users, engagement (30d) ──
    print("    Fetching overview 30d...")
    overview_30d = fetch_ga4(account_id, [
        "sessions", "active_users", "newusers", "bounce_rate",
        "engagement_rate", "average_session_duration",
        "screen_page_views", "events_per_session",
        "conversions", "session_conversion_rate"
    ], "last_30dT")

    # ── 2. Overview (7d) ──
    print("    Fetching overview 7d...")
    overview_7d = fetch_ga4(account_id, [
        "sessions", "active_users", "newusers", "bounce_rate",
        "engagement_rate", "average_session_duration",
        "conversions", "session_conversion_rate"
    ], "last_7dT")

    result["overview_30d"] = _sum_rows(overview_30d)
    result["overview_7d"]  = _sum_rows(overview_7d)

    # ── 3. Daily sessions trend (30d) ──
    print("    Fetching daily trend...")
    daily = fetch_ga4(account_id, [
        "date", "sessions", "active_users", "conversions"
    ], "last_30dT")
    result["daily_30d"] = sorted(daily, key=lambda r: r.get("date", ""))

    # ── 4. Landing pages ──
    print("    Fetching landing pages...")
    pages = fetch_ga4(account_id, [
        "landing_page", "sessions", "active_users",
        "average_session_duration", "bounce_rate",
        "engagement_rate", "conversions"
    ], "last_7dT")
    # Sort by sessions, take top 10
    pages_sorted = sorted(
        [p for p in pages if p.get("landing_page") and p.get("sessions", 0) > 0],
        key=lambda p: p.get("sessions", 0), reverse=True
    )[:10]
    result["landing_pages"] = pages_sorted

    # ── 5. Geographic — cities ──
    print("    Fetching geo — cities...")
    cities = fetch_ga4(account_id, [
        "city", "region", "sessions", "active_users", "conversions"
    ], "last_7dT")
    cities_sorted = sorted(
        [c for c in cities if c.get("city") and c.get("city") != "(not set)"],
        key=lambda c: c.get("sessions", 0), reverse=True
    )[:15]
    result["cities"] = cities_sorted

    # ── 6. Geographic — states/regions ──
    print("    Fetching geo — states...")
    states = fetch_ga4(account_id, [
        "region", "sessions", "active_users"
    ], "last_7dT")
    states_sorted = sorted(
        [s for s in states if s.get("region") and s.get("region") != "(not set)"],
        key=lambda s: s.get("sessions", 0), reverse=True
    )[:10]
    result["states"] = states_sorted

    # ── 7. Device breakdown ──
    print("    Fetching devices...")
    devices = fetch_ga4(account_id, [
        "devicecategory", "sessions", "active_users",
        "bounce_rate", "conversions", "engagement_rate"
    ], "last_30dT")
    result["devices"] = devices

    # ── 8. Conversion events — all named events ──
    print("    Fetching conversion events...")
    events = fetch_ga4(account_id, [
        "event_name", "event_count", "conversions"
    ], "last_30dT")
    # Filter to meaningful events — exclude noise
    noise = {"session_start", "first_visit", "page_view", "user_engagement",
              "scroll", "click", "file_download", "video_start", "video_progress",
              "video_complete", "form_start"}
    meaningful_events = [
        e for e in events
        if e.get("event_name") and e.get("event_name") not in noise
        and e.get("event_count", 0) > 0
    ]
    all_events = sorted(events, key=lambda e: e.get("event_count", 0), reverse=True)[:20]
    meaningful_sorted = sorted(meaningful_events, key=lambda e: e.get("event_count", 0), reverse=True)
    result["events_all"]         = all_events
    result["events_meaningful"]  = meaningful_sorted

    # ── 9. New vs returning ──
    print("    Fetching new vs returning...")
    new_ret = fetch_ga4(account_id, [
        "new_vs_returning", "sessions", "active_users",
        "engagement_rate", "conversions"
    ], "last_30dT")
    result["new_vs_returning"] = new_ret

    # ── 10. Channel breakdown ──
    print("    Fetching channel breakdown...")
    channels = fetch_ga4(account_id, [
        "default_channel_group", "sessions", "active_users",
        "conversions", "engagement_rate"
    ], "last_30dT")
    channels_sorted = sorted(
        [c for c in channels if c.get("default_channel_group")],
        key=lambda c: c.get("sessions", 0), reverse=True
    )
    result["channels"] = channels_sorted

    # ── 11. Source / medium ──
    print("    Fetching source/medium...")
    sources = fetch_ga4(account_id, [
        "source_medium", "sessions", "conversions", "engagement_rate"
    ], "last_30dT")
    sources_sorted = sorted(
        [s for s in sources if s.get("source_medium")],
        key=lambda s: s.get("sessions", 0), reverse=True
    )[:10]
    result["sources"] = sources_sorted

    return result


def _sum_rows(rows):
    """Sum numeric fields across all rows."""
    if not rows:
        return {}
    totals = {}
    counts = {}
    avg_fields = {"bounce_rate", "engagement_rate", "average_session_duration",
                  "session_conversion_rate", "events_per_session"}
    for row in rows:
        for k, v in row.items():
            if isinstance(v, (int, float)):
                totals[k] = totals.get(k, 0) + v
                counts[k] = counts.get(k, 0) + 1
    # Average the rate fields
    for f in avg_fields:
        if f in totals and counts.get(f, 0) > 0:
            totals[f] = totals[f] / counts[f]
    return totals


def _empty_ga4():
    return {
        "overview_30d": {}, "overview_7d": {}, "daily_30d": [],
        "landing_pages": [], "cities": [], "states": [],
        "devices": [], "events_all": [], "events_meaningful": [],
        "new_vs_returning": [], "channels": [], "sources": []
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(slug_filter=None, dry_run=False):
    now = datetime.datetime.now().isoformat()
    print(f"\nFetch GA4 — {now}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}\n")

    # Load existing cache
    cache_path = "ga4_cache.json"
    cache = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    fetched = 0
    skipped = 0

    for client in CLIENTS:
        slug     = client["slug"]
        name     = client["name"]
        ga4_id   = client.get("ga4_id") or GA4_ACCOUNTS.get(slug)

        if slug_filter and slug != slug_filter:
            continue

        if not ga4_id:
            print(f"  ⏭  {name} — no GA4 property ID, skipping")
            skipped += 1
            continue

        print(f"\n  📊 {name}")
        data = fetch_client_ga4(slug, str(ga4_id), dry_run=dry_run)
        cache[slug] = {
            "ga4_id":     ga4_id,
            "client":     name,
            "fetched_at": now,
            **data
        }
        fetched += 1

    # Save cache
    cache["_meta"] = {
        "fetched_at":    now,
        "clients_fetched": fetched,
        "clients_skipped": skipped,
    }

    if not dry_run:
        with open(cache_path, "w") as f:
            json.dump(cache, f, indent=2, default=str)
        print(f"\n✓ ga4_cache.json saved — {fetched} clients fetched, {skipped} skipped")
    else:
        print(f"\n[DRY RUN] Would save ga4_cache.json — {fetched} clients, {skipped} skipped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch GA4 data for all clients")
    parser.add_argument("--slug",     help="Fetch one client only")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()
    run(slug_filter=args.slug, dry_run=args.dry_run)

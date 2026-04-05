"""
fetch_ga4.py
Fetches GA4 analytics data for all clients via Windsor GA4 connector.
Saves to ga4_cache.json keyed by client slug.
"""
import json, os, datetime, urllib.request, urllib.parse

with open("clients.json") as f:
    CLIENTS = json.load(f)

WINDSOR_API_KEY = os.environ.get("WINDSOR_API_KEY", "")
WINDSOR_BASE    = "https://connectors.windsor.ai/googleanalytics4"

# Map client slug to Windsor GA4 account ID
GA4_IDS = {c["slug"]: str(c["ga4_id"]) for c in CLIENTS if c.get("ga4_id")}

def fetch(account_id, fields, date_preset):
    if not WINDSOR_API_KEY:
        print("  No WINDSOR_API_KEY")
        return []
    params = {
        "api_key":     WINDSOR_API_KEY,
        "connector":   "googleanalytics4",
        "accounts":    account_id,
        "date_preset": date_preset,
        "fields":      ",".join(fields),
    }
    url = f"{WINDSOR_BASE}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.loads(r.read())
            return data.get("data", data) if isinstance(data, dict) else data
    except Exception as e:
        print(f"    Error: {e}")
        return []

def process_client(slug, ga4_id):
    print(f"  {slug} ({ga4_id})")

    # Overview 30d
    overview = fetch(ga4_id, [
        "sessions","active_users","newusers","bounce_rate",
        "engagement_rate","average_session_duration","conversions"
    ], "last_30dT")
    ov = overview[0] if overview else {}

    # Overview 7d
    overview_7d = fetch(ga4_id, [
        "sessions","active_users","conversions","engagement_rate"
    ], "last_7dT")
    ov7 = overview_7d[0] if overview_7d else {}

    # Landing pages 7d
    pages_raw = fetch(ga4_id, [
        "landing_page","sessions","average_session_duration",
        "engagement_rate","conversions","bounce_rate"
    ], "last_7dT")

    # Aggregate landing pages by page path
    page_map = {}
    for r in pages_raw:
        pg = r.get("landing_page","") or ""
        if not pg or pg in ["(not set)",""]:
            pg = "/"
        if pg not in page_map:
            page_map[pg] = {"sessions":0,"total_duration":0,"conversions":0,"engagement_sum":0,"count":0}
        page_map[pg]["sessions"]       += r.get("sessions",0) or 0
        page_map[pg]["total_duration"] += (r.get("average_session_duration",0) or 0) * (r.get("sessions",0) or 0)
        page_map[pg]["conversions"]    += r.get("conversions",0) or 0
        page_map[pg]["engagement_sum"] += (r.get("engagement_rate",0) or 0) * (r.get("sessions",0) or 0)
        page_map[pg]["count"]          += 1

    pages = []
    for pg, v in sorted(page_map.items(), key=lambda x: x[1]["sessions"], reverse=True)[:10]:
        sess = v["sessions"]
        pages.append({
            "landing_page":             pg,
            "sessions":                 sess,
            "average_session_duration": round(v["total_duration"]/max(sess,1),1),
            "engagement_rate":          round(v["engagement_sum"]/max(sess,1),3),
            "conversions":              v["conversions"],
        })

    # Geographic — states
    geo_raw = fetch(ga4_id, ["region","sessions","active_users"], "last_7dT")
    state_map = {}
    for r in geo_raw:
        region = r.get("region","") or ""
        if not region or region in ["(not set)",""]: continue
        state_map[region] = state_map.get(region,0) + (r.get("sessions",0) or 0)
    states = [{"region":k,"sessions":v} for k,v in sorted(state_map.items(), key=lambda x: x[1], reverse=True)[:10]]

    # Geographic — cities
    city_raw = fetch(ga4_id, ["city","region","sessions"], "last_7dT")
    city_map = {}
    for r in city_raw:
        city = r.get("city","") or ""
        region = r.get("region","") or ""
        if not city or city in ["(not set)",""]: continue
        key = f"{city}|{region}"
        city_map[key] = city_map.get(key,0) + (r.get("sessions",0) or 0)
    cities = [{"city":k.split("|")[0],"region":k.split("|")[1],"sessions":v}
              for k,v in sorted(city_map.items(), key=lambda x: x[1], reverse=True)[:15]]

    # Channels
    channels_raw = fetch(ga4_id, ["default_channel_group","sessions","conversions","engagement_rate"], "last_30dT")
    channels = sorted(
        [r for r in channels_raw if r.get("default_channel_group")],
        key=lambda r: r.get("sessions",0), reverse=True
    )

    print(f"    Sessions: {ov.get('sessions',0):.0f} | Pages: {len(pages)} | States: {len(states)} | Cities: {len(cities)}")

    return {
        "ga4_id":       ga4_id,
        "client":       slug,
        "fetched_at":   datetime.datetime.now().isoformat(),
        "overview_30d": ov,
        "overview_7d":  ov7,
        "landing_pages":pages,
        "states":       states,
        "cities":       cities,
        "channels":     channels,
    }

def run():
    print(f"\nFetch GA4 — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Clients with GA4: {len(GA4_IDS)}\n")

    cache_path = "ga4_cache.json"
    cache = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f: cache = json.load(f)
        except: cache = {}

    fetched = 0
    for slug, ga4_id in GA4_IDS.items():
        try:
            data = process_client(slug, ga4_id)
            cache[slug] = data
            fetched += 1
        except Exception as e:
            print(f"  Error processing {slug}: {e}")

    cache["_meta"] = {
        "fetched_at": datetime.datetime.now().isoformat(),
        "clients_fetched": fetched,
    }

    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2, default=str)
    print(f"\n✓ ga4_cache.json saved — {fetched} clients")

if __name__ == "__main__":
    run()

"""
fetch_gsc_clients.py
─────────────────────────────────────────────────────────────────────────────
Pull Google Search Console data for every client whose property the service
account can see, into gsc_clients_cache.json.

Same pattern as the internal Command Deck fetcher: the service account
(sap-reporting@sigma-lane-491921-k3.iam.gserviceaccount.com) is added as a
Restricted user on a client's Search Console property, and the client lights
up here automatically on the next run — nothing in this file needs editing.

Matching, in order:
  1. a client's explicit "gsc_property" field, if set (exact property string)
  2. the domain of their "website" field (sc-domain: properties match the bare
     domain and any subdomain; url-prefix properties match by host)

Clients WITHOUT access are listed under "missing" so we know who to ask, and
accessible properties that matched no client are listed under "unmatched" —
that means a client needs "website" or "gsc_property" filled in, and without
this report the data would be silently dropped.

ENV (either one)
  GSC_SERVICE_ACCOUNT_JSON   key file contents on one line (GitHub secret)
  GSC_SERVICE_ACCOUNT_FILE   path to the key file (local runs via vault)

Search Console data lags ~3 days, so the 28-day window ends there.
"""

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
LAG_DAYS = 3
WINDOW = 28
TOP_N = 25

with open(HERE / "clients.json") as f:
    CLIENTS = json.load(f)


def credentials():
    raw = os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        info = json.loads(raw)
    else:
        path = os.environ.get("GSC_SERVICE_ACCOUNT_FILE", "").strip()
        if not path:
            return None, "No GSC credentials (GSC_SERVICE_ACCOUNT_JSON or _FILE)."
        info = json.loads(Path(path).read_text(encoding="utf-8"))
    from google.oauth2 import service_account
    return service_account.Credentials.from_service_account_info(info, scopes=[SCOPE]), None


def token_for(creds):
    from google.auth.transport.requests import Request
    creds.refresh(Request())
    return creds.token


def api(token, path, payload=None):
    url = "https://searchconsole.googleapis.com/webmasters/v3/" + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read())


def query_rows(token, site, start, end, dimensions=None, limit=1):
    payload = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dimensions": dimensions or [],
        "rowLimit": limit,
    }
    encoded = urllib.parse.quote(site, safe="")
    return api(token, f"sites/{encoded}/searchAnalytics/query", payload).get("rows", [])


def client_domain(client) -> str:
    m = re.match(r"https?://(?:www\.)?([^/]+)", client.get("website", "") or "")
    return m.group(1).lower() if m else ""


def property_matches(site_url: str, domain: str) -> bool:
    """sc-domain:example.org matches example.org and any subdomain;
    https://host/ url-prefix properties match by host."""
    if not domain:
        return False
    if site_url.startswith("sc-domain:"):
        prop = site_url[len("sc-domain:"):].lower()
        return domain == prop or domain.endswith("." + prop)
    m = re.match(r"https?://(?:www\.)?([^/]+)", site_url)
    return bool(m) and m.group(1).lower() == domain


def summarise(rows):
    r = rows[0] if rows else {}
    clicks = int(r.get("clicks") or 0)
    imps = int(r.get("impressions") or 0)
    return {
        "clicks": clicks, "impressions": imps,
        "ctr_pct": round(clicks / imps * 100, 2) if imps else 0.0,
        "position": round(float(r.get("position") or 0), 1) if imps else None,
    }


def top(rows, key_name):
    # Explicit sort: clicks, then impressions — GSC ties at 0 clicks otherwise
    # come back alphabetically and read as a nonsense "top" list.
    ordered = sorted(rows, key=lambda r: (-(r.get("clicks") or 0), -(r.get("impressions") or 0)))
    return [
        {
            key_name: r["keys"][0],
            "clicks": int(r.get("clicks") or 0),
            "impressions": int(r.get("impressions") or 0),
            "position": round(float(r.get("position") or 0), 1),
        }
        for r in ordered[:TOP_N]
    ]


def run():
    creds, err = credentials()
    if err:
        print(f"SKIP: {err}")
        if not (HERE / "gsc_clients_cache.json").exists():
            (HERE / "gsc_clients_cache.json").write_text(
                json.dumps({"ok": False, "error": err, "clients": {}, "missing": []}),
                encoding="utf-8")
        return
    token = token_for(creds)

    end = date.today() - timedelta(days=LAG_DAYS)
    start = end - timedelta(days=WINDOW - 1)
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=WINDOW - 1)

    sites = api(token, "sites").get("siteEntry", [])
    print(f"Service account sees {len(sites)} properties; window {start} to {end}")

    out = {"fetched_at": date.today().isoformat(), "window": f"{start}/{end}",
           "clients": {}, "missing": [], "unmatched": []}
    claimed = set()

    for client in CLIENTS:
        slug = client["slug"]
        domain = client_domain(client)
        override = client.get("gsc_property")
        if override:
            prop = next((s["siteUrl"] for s in sites if s.get("siteUrl") == override), None)
        else:
            prop = next((s["siteUrl"] for s in sites if property_matches(s.get("siteUrl", ""), domain)), None)
        if not prop:
            out["missing"].append({"slug": slug, "domain": domain or "(no website in clients.json)"})
            continue
        claimed.add(prop)
        try:
            entry = {
                "property": prop,
                "now": summarise(query_rows(token, prop, start, end)),
                "prev": summarise(query_rows(token, prop, prev_start, prev_end)),
                "queries": top(query_rows(token, prop, start, end, ["query"], 1000), "query"),
                "pages": top(query_rows(token, prop, start, end, ["page"], 1000), "page"),
            }
            out["clients"][slug] = entry
            n = entry["now"]
            print(f"  {slug}: {n['clicks']} clicks, {n['impressions']} impressions ({prop})")
        except urllib.error.HTTPError as e:
            print(f"  {slug}: HTTP {e.code} on {prop}")
        except Exception as exc:
            print(f"  {slug}: {type(exc).__name__}: {exc}")

    out["unmatched"] = [s["siteUrl"] for s in sites
                        if s.get("siteUrl") and s["siteUrl"] not in claimed]

    (HERE / "gsc_clients_cache.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(f"\n{len(out['clients'])} clients with GSC data, {len(out['missing'])} awaiting access")
    if out["missing"]:
        print("Awaiting access: " + ", ".join(m["slug"] for m in out["missing"]))
    if out["unmatched"]:
        print("\nAccessible but matched to no client (set 'website' or 'gsc_property' "
              "in clients.json, otherwise this data is dropped):")
        for p in out["unmatched"]:
            print(f"  {p}")


if __name__ == "__main__":
    run()

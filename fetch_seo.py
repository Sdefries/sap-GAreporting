"""
fetch_seo.py
─────────────────────────────────────────────────────────────────────────────
Fetches Local SEO data for clients enrolled in SAP's Local SEO service.
Only runs for clients where local_seo_enrolled: true in clients.json.

DATA SOURCES
  Google Search Console API  — organic clicks, impressions, position, keywords
  PageSpeed Insights API     — Core Web Vitals, mobile + desktop scores (FREE)
  DataForSEO API             — local keyword rank tracking (~$0.002/check)
  Google Business Profile    — direction requests, calls, photo views (future)

OUTPUT
  seo_cache.json — read by generate_reports.py
  Only enrolled clients appear. Non-enrolled clients get nothing in reports.

COSTS
  Search Console  — free (same OAuth)
  PageSpeed       — free (no auth)
  DataForSEO      — ~$5-25/month total for all enrolled clients
  GBP             — free (same OAuth, future)

USAGE
  python fetch_seo.py                       # all enrolled clients
  python fetch_seo.py --slug red-clay-ranch # one client
  python fetch_seo.py --dry-run             # preview
  python fetch_seo.py --pagespeed-only      # just PageSpeed (always free)

ENV VARS
  GA_SERVICE_ACCOUNT     — service account JSON (same as GA4); add its email as a
                           user on each client's Search Console property
  PAGESPEED_API_KEY      — Google API key with PageSpeed Insights API enabled
  DATAFORSEO_LOGIN       — DataForSEO account login email
  DATAFORSEO_PASSWORD    — DataForSEO account password
"""

import json
import os
import sys
import datetime
import argparse
import urllib.request
import urllib.error
import time
import urllib.parse
import base64
import re

from fetch_ai_visibility import seo_location

# ── LOAD CLIENTS ──────────────────────────────────────────────────────────────

with open("clients.json") as f:
    ALL_CLIENTS = json.load(f)

# Only process enrolled clients
CLIENTS = [c for c in ALL_CLIENTS if c.get("local_seo_enrolled")]

DATAFORSEO_LOGIN    = os.environ.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")
DATAFORSEO_BASE     = "https://api.dataforseo.com/v3"
PAGESPEED_API_KEY   = os.environ.get("PAGESPEED_API_KEY", "")

print(f"SEO enrolled clients: {len(CLIENTS)} of {len(ALL_CLIENTS)}")
if PAGESPEED_API_KEY:
    print("PageSpeed API key: ✓ configured")
else:
    print("PageSpeed API key: not set (using anonymous quota)")


# ── PAGESPEED INSIGHTS ────────────────────────────────────────────────────────

def fetch_pagespeed(url, strategy="mobile"):
    """
    Returns Core Web Vitals and performance score.
    Uses API key if available for higher quota.
    strategy: 'mobile' or 'desktop'
    """
    if not url:
        return {}

    api_url = (
        f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        f"?url={urllib.parse.quote(url)}&strategy={strategy}"
        f"&category=performance&category=seo&category=best-practices&category=accessibility"
    )
    
    # Add API key if available
    if PAGESPEED_API_KEY:
        api_url += f"&key={PAGESPEED_API_KEY}"

    try:
        data = None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(api_url, timeout=60) as resp:
                    data = json.loads(resp.read())
                break
            except urllib.error.HTTPError as e:
                if e.code != 429 or attempt == 3:
                    raise
                wait = 15 * (attempt + 1)
                print(f"    PageSpeed rate-limited (429) — retrying in {wait}s"
                      + ("" if PAGESPEED_API_KEY else " (set PAGESPEED_API_KEY to avoid this)"))
                time.sleep(wait)

        cats     = data.get("lighthouseResult", {}).get("categories", {})
        audits   = data.get("lighthouseResult", {}).get("audits", {})
        lab_data = data.get("lighthouseResult", {}).get("audits", {})

        result = {
            "strategy":           strategy,
            "performance_score":  round((cats.get("performance", {}).get("score") or 0) * 100),
            "seo_score":          round((cats.get("seo", {}).get("score") or 0) * 100),
            "best_practices":     round((cats.get("best-practices", {}).get("score") or 0) * 100),
            "accessibility":      round((cats.get("accessibility", {}).get("score") or 0) * 100),
        }

        # Core Web Vitals
        lcp = audits.get("largest-contentful-paint", {})
        fid = audits.get("total-blocking-time", {})
        cls = audits.get("cumulative-layout-shift", {})
        fcp = audits.get("first-contentful-paint", {})
        tti = audits.get("interactive", {})
        spd = audits.get("speed-index", {})

        result["lcp"]          = lcp.get("displayValue", "—")
        result["lcp_score"]    = lcp.get("score")
        result["tbt"]          = fid.get("displayValue", "—")
        result["tbt_score"]    = fid.get("score")
        result["cls"]          = cls.get("displayValue", "—")
        result["cls_score"]    = cls.get("score")
        result["fcp"]          = fcp.get("displayValue", "—")
        result["fcp_score"]    = fcp.get("score")
        result["tti"]          = tti.get("displayValue", "—")
        result["speed_index"]  = spd.get("displayValue", "—")

        # Pass/fail overall
        result["cwv_pass"] = all([
            (lcp.get("score") or 0) >= 0.9,
            (fid.get("score") or 0) >= 0.9,
            (cls.get("score") or 0) >= 0.9,
        ])

        # Top opportunities
        opportunities = []
        for audit_id, audit in audits.items():
            if audit.get("score") is not None and audit.get("score") < 0.9:
                if audit.get("details", {}).get("type") in ["opportunity", "table"]:
                    savings = audit.get("details", {}).get("overallSavingsMs", 0)
                    if savings > 200:
                        opportunities.append({
                            "title":   audit.get("title", ""),
                            "savings": f"{savings:.0f}ms",
                            "score":   audit.get("score"),
                        })
        result["opportunities"] = sorted(
            opportunities, key=lambda o: o.get("score", 1)
        )[:5]

        return result

    except Exception as e:
        print(f"    PageSpeed error ({strategy}): {e}")
        return {"strategy": strategy, "performance_score": None, "error": str(e)}


# ── SEARCH CONSOLE ────────────────────────────────────────────────────────────

def fetch_search_console(property_url, days=30):
    """
    Pulls organic search performance from Google Search Console API.
    Credentials: see search_console_credentials().
    Falls back gracefully if not configured.
    """
    if not property_url:
        return {}

    try:
        creds = search_console_credentials()
        if creds is None:
            return {}
        end_date   = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days)

        headers = {
            "Authorization": f"Bearer {creds.token}",
            "Content-Type":  "application/json"
        }

        # Overall performance
        payload = json.dumps({
            "startDate":  str(start_date),
            "endDate":    str(end_date),
            "dimensions": [],
            "rowLimit":   1
        }).encode()

        # Search Console properties are either URL-prefix ("https://site.org/")
        # or domain ("sc-domain:site.org"); try what's configured, then both forms.
        host = urllib.parse.urlparse(property_url if "//" in property_url else "https://" + property_url).netloc
        bare = host[4:] if host.startswith("www.") else host
        candidates = list(dict.fromkeys([
            property_url, property_url.rstrip("/") + "/", f"sc-domain:{bare}",
            f"https://{host}/", f"https://www.{bare}/",
        ]))
        api_url, totals, last_err = None, None, None
        for prop in candidates:
            url = ("https://searchconsole.googleapis.com/webmasters/v3/sites/"
                   f"{urllib.parse.quote(prop, safe='')}/searchAnalytics/query")
            try:
                req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
                totals = json.loads(urllib.request.urlopen(req, timeout=20).read()).get("rows", [{}])[0]
                api_url = url
                print(f"    Search Console property: {prop}")
                break
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code not in (403, 404):
                    raise
        if api_url is None:
            print(f"    Search Console: no access to {property_url} ({last_err}). Add the GA service account "
                  "as a user on the property in Search Console → Settings → Users and permissions.")
            return {"error": "no_access"}

        result = {
            "clicks":      totals.get("clicks", 0),
            "impressions": totals.get("impressions", 0),
            "ctr":         round((totals.get("ctr") or 0) * 100, 2),
            "position":    round(totals.get("position") or 0, 1),
        }

        # Top keywords
        payload_kw = json.dumps({
            "startDate":  str(start_date),
            "endDate":    str(end_date),
            "dimensions": ["query"],
            "rowLimit":   20,
            "orderBy":    [{"fieldName": "clicks", "sortOrder": "DESCENDING"}]
        }).encode()

        req_kw  = urllib.request.Request(api_url, data=payload_kw, headers=headers, method="POST")
        resp_kw = urllib.request.urlopen(req_kw, timeout=15)
        kw_rows = json.loads(resp_kw.read()).get("rows", [])
        result["top_keywords"] = [
            {
                "query":       r["keys"][0],
                "clicks":      r.get("clicks", 0),
                "impressions": r.get("impressions", 0),
                "ctr":         round((r.get("ctr") or 0) * 100, 1),
                "position":    round(r.get("position") or 0, 1),
            }
            for r in kw_rows
        ]

        # Top pages by organic traffic
        payload_pg = json.dumps({
            "startDate":  str(start_date),
            "endDate":    str(end_date),
            "dimensions": ["page"],
            "rowLimit":   10,
            "orderBy":    [{"fieldName": "clicks", "sortOrder": "DESCENDING"}]
        }).encode()

        req_pg  = urllib.request.Request(api_url, data=payload_pg, headers=headers, method="POST")
        resp_pg = urllib.request.urlopen(req_pg, timeout=15)
        pg_rows = json.loads(resp_pg.read()).get("rows", [])
        result["top_pages"] = [
            {
                "page":        r["keys"][0],
                "clicks":      r.get("clicks", 0),
                "impressions": r.get("impressions", 0),
                "position":    round(r.get("position") or 0, 1),
            }
            for r in pg_rows
        ]

        # Opportunities — high impressions, low CTR
        payload_opp = json.dumps({
            "startDate":         str(start_date),
            "endDate":           str(end_date),
            "dimensions":        ["query"],
            "rowLimit":          50,
            "dimensionFilterGroups": [{
                "filters": [{
                    "dimension":  "query",
                    "operator":   "notContains",
                    "expression": "site:"
                }]
            }]
        }).encode()

        req_opp  = urllib.request.Request(api_url, data=payload_opp, headers=headers, method="POST")
        resp_opp = urllib.request.urlopen(req_opp, timeout=15)
        opp_rows = json.loads(resp_opp.read()).get("rows", [])

        opportunities = [
            {
                "query":       r["keys"][0],
                "impressions": r.get("impressions", 0),
                "clicks":      r.get("clicks", 0),
                "ctr":         round((r.get("ctr") or 0) * 100, 1),
                "position":    round(r.get("position") or 0, 1),
            }
            for r in opp_rows
            if r.get("impressions", 0) > 50 and (r.get("ctr") or 0) < 0.03
        ]
        result["opportunities"] = sorted(
            opportunities, key=lambda o: o.get("impressions", 0), reverse=True
        )[:10]

        print(f"    Search Console: {result['clicks']} clicks, {result['impressions']} impressions, pos {result['position']}")
        return result

    except ImportError:
        print("    google-auth not installed — pip install google-auth")
        return {}
    except Exception as e:
        print(f"    Search Console error: {e}")
        return {}


def search_console_credentials():
    """
    Credentials for the Search Console API, in order of preference:
      1. GA_SERVICE_ACCOUNT (the same service account fetch_ga4.py uses) — add its
         email as a user on each Search Console property.
      2. google-ads.yaml OAuth refresh token — only works if that token was
         granted the webmasters.readonly scope.
    """
    scope = ["https://www.googleapis.com/auth/webmasters.readonly"]
    try:
        from google.auth.transport.requests import Request
    except ImportError:
        print("    google-auth not installed — pip install google-auth")
        return None

    sa = os.environ.get("GA_SERVICE_ACCOUNT", "")
    if sa:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(json.loads(sa), scopes=scope)
        creds.refresh(Request())
        return creds

    if os.path.exists("google-ads.yaml"):
        import yaml
        from google.oauth2.credentials import Credentials
        with open("google-ads.yaml") as f:
            y = yaml.safe_load(f)
        creds = Credentials(token=None, refresh_token=y.get("refresh_token"),
                            token_uri="https://oauth2.googleapis.com/token",
                            client_id=y.get("client_id"), client_secret=y.get("client_secret"), scopes=scope)
        creds.refresh(Request())
        return creds

    print("    No Search Console credentials (set GA_SERVICE_ACCOUNT) — skipping")
    return None


# ── DATAFORSEO KEYWORD RANKINGS ───────────────────────────────────────────────

def _matches(domain, url):
    host = _domain(url)
    return bool(domain) and (host == domain or host.endswith("." + domain))


def _domain(url):
    host = urllib.parse.urlparse(url if "//" in (url or "") else "//" + (url or "")).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def fetch_keyword_rankings(keywords, domain, location="United States", competitors=None):
    """
    Track keyword rankings using DataForSEO SERP API.
    Cost: ~$0.002 per keyword check.

    One SERP call per keyword also gives us, for free:
      - competitor positions (for the Competitors view)
      - whether an AI Overview showed and whether it cited the client
      - "People also ask" questions (prompt ideas for AI tracking)
    """
    if not keywords or not domain:
        return []

    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        print("    No DataForSEO credentials — skipping keyword rankings")
        return []

    credentials = base64.b64encode(
        f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()
    ).decode()

    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type":  "application/json"
    }
    competitors = competitors or []

    results = []

    for keyword in keywords[:10]:  # cap at 10 per client per run
        task_body = {
            "keyword":              keyword,
            "location_name":        location,
            "language_code":        "en",
            "device":               "desktop",
            "os":                   "windows",
            "depth":                50,
            "calculate_rectangles": False,
            "load_async_ai_overview": True,
        }

        try:
            items = None
            for attempt_location in dict.fromkeys([location, "United States"]):
                task_body["location_name"] = attempt_location
                req = urllib.request.Request(
                    f"{DATAFORSEO_BASE}/serp/google/organic/live/advanced",
                    data=json.dumps([task_body]).encode(),
                    headers=headers,
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
                task = data.get("tasks", [{}])[0]
                if task.get("status_code") == 20000:
                    task_res = task.get("result", [{}])[0] if task.get("result") else {}
                    items = task_res.get("items") or []
                    break
                print(f"    DataForSEO task error ({attempt_location}): {task.get('status_message')}")
            if items is None:
                raise RuntimeError("DataForSEO task failed")

            # Find our domain in results
            position = None
            url = None
            in_local_pack = False
            snippet = None
            ai_overview = False
            ai_overview_cited = False
            paa = []
            comp_positions = {c["domain"]: None for c in competitors if c.get("domain")}
            top_domains = []          # page-one domains, for competitor suggestions
            ai_sources = []           # domains the AI Overview cites

            for item in items:
                item_type = item.get("type", "")

                # Local pack check
                if item_type == "local_pack":
                    if _matches(domain, item.get("url") or item.get("domain") or ""):
                        in_local_pack = True
                    for local_item in item.get("items", []) or []:
                        if _matches(domain, local_item.get("url") or ""):
                            in_local_pack = True

                if item_type == "ai_overview":
                    ai_overview = True
                    for ref in re.findall(r'"domain": "([^"]+)"', json.dumps(item)):
                        d = _domain(ref)
                        if d and d not in ai_sources:
                            ai_sources.append(d)
                    refs = json.dumps(item)
                    ai_overview_cited = bool(re.search(
                        r'"(?:url|domain)": "(?:https?://)?(?:www\.)?' + re.escape(domain), refs))

                if item_type == "people_also_ask":
                    paa += [el.get("title") for el in item.get("items") or [] if el.get("title")]

                # Organic result
                if item_type == "organic":
                    item_url = item.get("url") or ""
                    if (item.get("rank_group") or 99) <= 10 and _domain(item_url) not in top_domains:
                        top_domains.append(_domain(item_url))
                    if position is None and _matches(domain, item_url):
                        position = item.get("rank_group") or item.get("rank_absolute")
                        url      = item_url
                        snippet  = (item.get("description") or "")[:150]
                    for cd in comp_positions:
                        if comp_positions[cd] is None and _matches(cd, item_url):
                            comp_positions[cd] = item.get("rank_group") or item.get("rank_absolute")

            results.append({
                "keyword":           keyword,
                "position":          position,
                "url":               url,
                "in_local_pack":     in_local_pack,
                "snippet":           snippet,
                "ai_overview":       ai_overview,
                "ai_overview_cited": ai_overview_cited,
                "competitors":       comp_positions,
                "people_also_ask":   paa[:8],
                "top_domains":       top_domains,
                "ai_sources":        ai_sources[:10],
                "checked_at":        datetime.datetime.now().isoformat(),
            })
            print(f"    '{keyword}': position {position or 'not found'}" +
                  (" 🗺 local pack" if in_local_pack else "") +
                  (" 🤖 AI overview" + (" (cited)" if ai_overview_cited else "") if ai_overview else ""))

        except Exception as e:
            print(f"    DataForSEO error for '{keyword}': {e}")
            results.append({"keyword": keyword, "position": None, "error": str(e)})

    # Monthly US search volume for every tracked keyword (one call)
    try:
        req = urllib.request.Request(
            f"{DATAFORSEO_BASE}/keywords_data/google_ads/search_volume/live",
            data=json.dumps([{"keywords": keywords[:10], "location_name": "United States", "language_code": "en"}]).encode(),
            headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            task = json.loads(resp.read()).get("tasks", [{}])[0]
        vols = {(i.get("keyword") or "").lower(): i.get("search_volume") for i in (task.get("result") or [])}
        for r in results:
            r["volume"] = vols.get(r["keyword"].lower())
    except Exception as e:
        print(f"    Search volume lookup failed: {e}")

    return results


def rank_snapshot(rankings):
    """One point of ranking history: what the Classic SEO trend charts plot."""
    checked = [k for k in rankings if "error" not in k]
    if not checked:
        return None
    ranked = [k for k in checked if k.get("position")]
    return {
        "date":         datetime.date.today().isoformat(),
        "tracked":      len(checked),
        "top3":         len([k for k in ranked if k["position"] <= 3]),
        "top10":        len([k for k in ranked if k["position"] <= 10]),
        "top20":        len(ranked),
        "avg_position": round(sum(k["position"] for k in ranked) / len(ranked), 1) if ranked else None,
        "ai_cited":     len([k for k in checked if k.get("ai_overview_cited")]),
        "ai_shown":     len([k for k in checked if k.get("ai_overview")]),
        "positions":    {k["keyword"]: k.get("position") for k in checked},
        "competitors":  {
            cd: {k["keyword"]: (k.get("competitors") or {}).get(cd) for k in checked}
            for cd in {cd for k in checked for cd in (k.get("competitors") or {})}
        },
    }


# ── MAIN CLIENT FETCHER ───────────────────────────────────────────────────────

def fetch_client_seo(client, dry_run=False):
    name      = client["name"]
    slug      = client["slug"]
    website   = client.get("website", "")
    sc_prop   = client.get("search_console_property", website)
    keywords  = client.get("seo_keywords", [])
    domain    = website.replace("https://", "").replace("http://", "").split("/")[0]

    print(f"\n  🔍 {name}")

    if dry_run:
        print(f"    [DRY RUN] Would fetch SEO for {website}")
        return _empty_seo()

    result = {
        "client":     name,
        "slug":       slug,
        "website":    website,
        "location":   seo_location(client),
        "fetched_at": datetime.datetime.now().isoformat(),
    }

    # PageSpeed — always fetch (free)
    print("    Fetching PageSpeed mobile...")
    result["pagespeed_mobile"]  = fetch_pagespeed(website, "mobile")
    print("    Fetching PageSpeed desktop...")
    result["pagespeed_desktop"] = fetch_pagespeed(website, "desktop")

    # Search Console
    print("    Fetching Search Console...")
    result["search_console"] = fetch_search_console(sc_prop)

    # DataForSEO keyword rankings
    if keywords:
        print(f"    Fetching keyword rankings ({len(keywords)} keywords)...")
        result["keyword_rankings"] = fetch_keyword_rankings(
            keywords, domain.removeprefix("www."), seo_location(client),
            [{"name": c.get("name"), "domain": _domain(c.get("domain", ""))}
             for c in client.get("competitors", [])])
    else:
        print("    No seo_keywords defined — skipping rankings")
        result["keyword_rankings"] = []

    # Summary scores
    mob_score  = result["pagespeed_mobile"].get("performance_score")
    desk_score = result["pagespeed_desktop"].get("performance_score")
    sc         = result["search_console"]
    kw_ranked  = [k for k in result["keyword_rankings"] if k.get("position")]
    avg_pos    = (
        sum(k["position"] for k in kw_ranked) / len(kw_ranked)
        if kw_ranked else None
    )

    result["summary"] = {
        "pagespeed_mobile":   mob_score,
        "pagespeed_desktop":  desk_score,
        "cwv_pass":           result["pagespeed_mobile"].get("cwv_pass"),
        "organic_clicks_30d": sc.get("clicks", 0),
        "avg_position":       round(avg_pos, 1) if avg_pos else None,
        "keywords_tracked":   len(result["keyword_rankings"]),
        "keywords_ranked":    len(kw_ranked),
        "keywords_top10":     len([k for k in kw_ranked if k.get("position", 99) <= 10]),
        "in_local_pack":      any(k.get("in_local_pack") for k in result["keyword_rankings"]),
        "sc_impressions_30d": sc.get("impressions", 0),
        "sc_avg_position":    sc.get("position"),
    }

    return result


def _empty_seo():
    return {
        "pagespeed_mobile": {}, "pagespeed_desktop": {},
        "search_console": {}, "keyword_rankings": [], "summary": {}
    }


# ── RUN ───────────────────────────────────────────────────────────────────────

def run(slug_filter=None, dry_run=False, pagespeed_only=False):
    now = datetime.datetime.now().isoformat()
    print(f"\nFetch SEO — {now}")
    print(f"Enrolled clients: {len(CLIENTS)}")
    print(f"Mode: {'PAGESPEED ONLY' if pagespeed_only else 'FULL'}" +
          (" | DRY RUN" if dry_run else "") + "\n")

    if not CLIENTS:
        print("No clients have local_seo_enrolled: true in clients.json")
        print("Add 'local_seo_enrolled': true to a client to enable SEO tracking")
        return

    cache_path = "seo_cache.json"
    cache = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    fetched = 0
    for client in CLIENTS:
        slug = client["slug"]
        if slug_filter and slug != slug_filter:
            continue

        if pagespeed_only:
            website = client.get("website", "")
            print(f"\n  ⚡ {client['name']} — PageSpeed only")
            mob  = fetch_pagespeed(website, "mobile")
            desk = fetch_pagespeed(website, "desktop")
            if slug not in cache:
                cache[slug] = {}
            cache[slug]["pagespeed_mobile"]  = mob
            cache[slug]["pagespeed_desktop"] = desk
            cache[slug]["client"]  = client["name"]
            cache[slug]["website"] = website
        else:
            data = fetch_client_seo(client, dry_run=dry_run)
            # Ranking history powers the Classic SEO trend charts — carry it forward
            history = (cache.get(slug) or {}).get("rank_history", [])
            snap = rank_snapshot(data.get("keyword_rankings", [])) if not dry_run else None
            if snap:
                history = [h for h in history if h.get("date") != snap["date"]] + [snap]
            data["rank_history"] = history[-26:]
            cache[slug] = data

        fetched += 1

    cache["_meta"] = {
        "fetched_at": now,
        "clients_fetched": fetched,
        "pagespeed_only": pagespeed_only,
    }

    if not dry_run:
        with open(cache_path, "w") as f:
            json.dump(cache, f, indent=2, default=str)
        print(f"\n✓ seo_cache.json saved — {fetched} clients")
    else:
        print(f"\n[DRY RUN] Would save seo_cache.json — {fetched} clients")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch SEO data for enrolled clients")
    parser.add_argument("--slug",           help="One client only")
    parser.add_argument("--dry-run",        action="store_true")
    parser.add_argument("--pagespeed-only", action="store_true",
                        help="Only fetch PageSpeed scores (free, no auth needed)")
    args = parser.parse_args()
    run(
        slug_filter=args.slug,
        dry_run=args.dry_run,
        pagespeed_only=args.pagespeed_only
    )

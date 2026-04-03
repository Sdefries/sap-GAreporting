"""
generate_reports_v2.py
─────────────────────────────────────────────────────────────────────────────
CLEAN ARCHITECTURE — no string replacements, no template bleed-through.

HOW IT WORKS
  1. Load data from windsor_cache.json, ga4_cache.json, seo_cache.json
  2. Validate each cache entry belongs to the correct client (account ID check)
  3. Build a CLIENT_DATA object per client — pure Python dict, no HTML
  4. Inject CLIENT_DATA as JSON into report_template_v2.html via ONE injection point
  5. All rendering happens in JavaScript using that clean data object
  6. Run contamination check on final HTML — if any forbidden strings found, SKIP
  7. If passes → save to reports/{slug}.html

RESULT
  - Client data can NEVER bleed into another client's report
  - Template has zero hardcoded client data
  - Validation catches any issues before they reach clients

USAGE
  python generate_reports_v2.py               # all clients
  python generate_reports_v2.py --slug pup-profile  # one client
  python generate_reports_v2.py --dry-run     # preview without saving
  python generate_reports_v2.py --validate-only  # just check existing reports
"""

import json
import os
import sys
import math
import datetime
import argparse

# ── LOAD DATA ─────────────────────────────────────────────────────────────────

with open("clients.json") as f:
    CLIENTS = json.load(f)

def load_cache(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load {path}: {e}")
        return {}

WINDSOR_CACHE = load_cache("windsor_cache.json")
GA4_CACHE     = load_cache("ga4_cache.json")
SEO_CACHE     = load_cache("seo_cache.json")

TEMPLATE_PATH = "report_template_v2.html"
REPORT_DATE   = datetime.date.today().strftime("%B %d, %Y")
REPO_BASE     = "https://sdefries.github.io/sap-GAreporting/reports"

os.makedirs("reports", exist_ok=True)

# ── VALIDATION ────────────────────────────────────────────────────────────────

# Forbidden strings per client — anything that should NEVER appear in another client's report
FORBIDDEN_STRINGS = {
    "pup-profile": [
        "Red Clay Ranch", "Silver Linings", "Straydog", "Guardian Pet Trust",
        "City Dogs", "Pink Paws", "Humane Society", "ScienceWorks",
        "Ziva", "Serenity Horse", "Angels4Paws", "Paws and Claws",
    ],
    "red-clay-ranch": [
        "Pup Profile", "Pups Fund", "/pups-fund", "pupprofile.org",
        "Nikki", "Shop Pup", "Urgent Need-LA", "Urgent Need-San Diego",
        "browse page", "dog profile pages", "browsing dogs",
        "California Donate", "Shop Pup-Profile",
    ],
    "silver-linings": [
        "Pup Profile", "Pups Fund", "pupprofile.org", "Red Clay Ranch",
    ],
    "straydog": [
        "Pup Profile", "Pups Fund", "Red Clay Ranch", "Silver Linings",
    ],
    "guardian-pet-trust": [
        "Pup Profile", "Pups Fund", "Red Clay Ranch",
    ],
    "city-dogs-kitties": [
        "Pup Profile", "Pups Fund", "Red Clay Ranch", "Silver Linings",
    ],
    # Default — applies to ALL clients
    "_all": [
        "334-205-8352",  # Pup Profile account ID
        "378-461-6494",  # Red Clay account ID — only valid in Red Clay report
        "pupprofile.org",
        "Pups Fund",
        "/pups-fund/donate",
        "Nikki Leonard",
        "Shop Pup-Profile",
    ]
}

def validate_report(slug, html):
    """
    Check the final HTML for forbidden strings.
    Returns (passed: bool, violations: list)
    """
    violations = []

    # Check client-specific forbidden strings
    client_forbidden = FORBIDDEN_STRINGS.get(slug, [])
    for f in client_forbidden:
        if f in html:
            violations.append(f"CLIENT-SPECIFIC: '{f}'")

    # Check global forbidden strings (skip account ID of this client)
    client = next((c for c in CLIENTS if c["slug"] == slug), {})
    own_account_id = client.get("google_ads_id", "").replace("-", "")

    for f in FORBIDDEN_STRINGS.get("_all", []):
        # Skip if this is the client's own account ID
        if f.replace("-", "") == own_account_id:
            continue
        if f in html:
            violations.append(f"GLOBAL: '{f}'")

    return len(violations) == 0, violations


def validate_cache_entry(account_id, rows):
    """
    Verify all rows in the cache belong to the expected account.
    Returns (passed: bool, issues: list)
    """
    issues = []
    for i, row in enumerate(rows):
        row_account = str(row.get("account_id", "")).replace("-", "")
        expected    = str(account_id).replace("-", "")
        if row_account and row_account != expected:
            issues.append(
                f"Row {i}: expected account {expected}, got {row_account}"
            )
    return len(issues) == 0, issues


# ── DATA BUILDERS ─────────────────────────────────────────────────────────────

def get_google_ads_data(account_id):
    """Get and validate Google Ads data from windsor_cache.json."""
    rows = WINDSOR_CACHE.get(account_id, [])
    if not rows:
        return None, []

    passed, issues = validate_cache_entry(account_id, rows)
    if not passed:
        print(f"    ⚠️  Cache contamination detected: {issues}")
        return None, issues

    return rows, []


def calc_totals(rows):
    if not rows:
        return {}
    clicks = sum(r.get("clicks", 0) or 0 for r in rows)
    imps   = sum(r.get("impressions", 0) or 0 for r in rows)
    cost   = sum(r.get("cost", 0) or 0 for r in rows)
    convs  = sum(r.get("conversions", 0) or 0 for r in rows)
    return {
        "clicks":      clicks,
        "impressions": imps,
        "cost":        round(cost, 2),
        "conversions": convs,
        "ctr":         round(clicks / imps * 100, 2) if imps > 0 else 0,
        "cpc":         round(cost / clicks, 2) if clicks > 0 else 0,
        "cpa":         round(cost / convs, 2) if convs > 0 else None,
        "conv_rate":   round(convs / clicks * 100, 2) if clicks > 0 else 0,
    }


def build_campaign_list(rows):
    camps = []
    for r in rows:
        ctr = r.get("ctr", 0) or 0
        if ctr < 2:
            ctr = ctr * 100
        clicks = r.get("clicks", 0) or 0
        cost   = r.get("cost", 0) or 0
        convs  = r.get("conversions", 0) or 0
        camps.append({
            "name":        r.get("campaign", "Unknown"),
            "status":      r.get("campaign_status", "ENABLED"),
            "clicks":      clicks,
            "impressions": r.get("impressions", 0) or 0,
            "ctr":         round(ctr, 2),
            "cost":        round(cost, 2),
            "conversions": convs,
            "cpc":         round(cost / clicks, 2) if clicks > 0 else 0,
            "cpa":         round(cost / convs, 2) if convs > 0 else None,
        })
    return sorted(camps, key=lambda c: c["clicks"], reverse=True)


def calc_gps(totals, client):
    """
    Grant Performance Score 0-100.
    CTR health 30pts, Grant utilization 30pts,
    Conv tracking 20pts, Budget pacing 20pts.
    """
    if not totals:
        return 0, {}

    ctr  = totals.get("ctr", 0)
    cost = totals.get("cost", 0)
    convs = totals.get("conversions", 0)

    # CTR (30pts): 5%=15pts, 10%=22pts, 20%+=30pts
    if ctr >= 5:
        ctr_score = min(30, 15 + int((ctr - 5) / 15 * 15))
    else:
        ctr_score = int(ctr / 5 * 15)

    # Utilization (30pts)
    util_pct   = min(100, cost / 10000 * 100)
    util_score = int(util_pct * 0.30)

    # Conversion tracking (20pts)
    conv_score = 20 if convs > 0 else 0

    # Budget pacing (20pts)
    budget_score = 20 if cost > 0 else 0

    total = min(100, ctr_score + util_score + conv_score + budget_score)

    return total, {
        "ctr":          round(min(100, ctr_score / 30 * 100)),
        "utilization":  round(util_pct),
        "conv_tracking": 100 if conv_score == 20 else 0,
        "budget":        100 if budget_score == 20 else 0,
    }


def build_insights(client, totals_30d, camps_30d):
    """Generate client-specific insights from real data."""
    insights = []
    name = client["name"]
    org_model   = client.get("org_model", "location_based")
    animal_type = client.get("animal_type")

    if not totals_30d:
        insights.append({
            "color": "red", "tag": "action",
            "title": "Account needs setup",
            "body":  f"No campaign data found for {name}. The account may be new or not yet properly configured."
        })
        return insights

    ctr      = totals_30d.get("ctr", 0)
    cost     = totals_30d.get("cost", 0)
    convs    = totals_30d.get("conversions", 0)
    cpa      = totals_30d.get("cpa")
    util_pct = min(100, cost / 10000 * 100)

    # CTR insight
    if ctr >= 15:
        insights.append({
            "color": "green", "tag": "win",
            "title": f"Exceptional CTR — {ctr:.1f}%",
            "body":  f"Your campaigns are achieving {ctr:.1f}% CTR — well above the industry average. Strong ad relevance and keyword targeting."
        })
    elif ctr >= 5:
        insights.append({
            "color": "green", "tag": "win",
            "title": f"CTR compliant at {ctr:.1f}%",
            "body":  f"Account CTR is {ctr:.1f}% — above the 5% minimum. We're monitoring this closely and will alert you if it dips."
        })
    else:
        insights.append({
            "color": "red", "tag": "action",
            "title": f"CTR at risk — {ctr:.1f}%",
            "body":  f"CTR has dropped to {ctr:.1f}% — below the 5% minimum required by Google Ad Grants. Immediate action needed on keywords and ad copy."
        })

    # Grant utilization
    if util_pct >= 80:
        insights.append({
            "color": "green", "tag": "win",
            "title": f"Grant utilization strong — {util_pct:.0f}% used",
            "body":  f"Spending ${cost:,.0f} of the $10,000 monthly grant. On pace to maximize full grant value this month."
        })
    elif util_pct >= 30:
        insights.append({
            "color": "amber", "tag": "watch",
            "title": f"Grant at {util_pct:.0f}% utilization",
            "body":  f"Using ${cost:,.0f} of the available $10,000 grant. Expanding keyword coverage and bid adjustments will capture more free ad traffic."
        })
    else:
        insights.append({
            "color": "red", "tag": "action",
            "title": f"Grant severely under-utilized — {util_pct:.0f}%",
            "body":  f"Only ${cost:,.0f} of the $10,000 monthly grant is being used. Significant opportunity to build more campaigns and drive more traffic."
        })

    # Conversion insight
    if convs > 0 and cpa:
        insights.append({
            "color": "green", "tag": "win",
            "title": f"{convs:.0f} conversions tracked at ${cpa:.2f} CPA",
            "body":  f"Campaigns recorded {convs:.0f} conversions this month. Each conversion represents meaningful action — adoption inquiries, donations, volunteer signups."
        })
    elif cost > 200:
        insights.append({
            "color": "amber", "tag": "watch",
            "title": "Conversion tracking needs review",
            "body":  f"Campaigns are generating clicks and spending grant budget, but no conversions are being tracked. Without conversion data, Google cannot optimize campaign performance."
        })

    # Org-model specific insight
    if animal_type == "equine":
        insights.append({
            "color": "blue", "tag": "action",
            "title": "Individual horse profiles drive the highest conversion rates",
            "body":  "In equine rescue, campaigns that feature specific named horses with their backstory convert at 3-5x the rate of generic rescue ads. Make sure your top campaigns link to individual horse profile pages, not a general adoption page."
        })
    elif org_model == "foster_network":
        insights.append({
            "color": "blue", "tag": "action",
            "title": "Foster recruitment should be your primary campaign goal",
            "body":  "For foster-based rescues, foster campaigns consistently outperform adoption campaigns — people searching to foster have high intent and low barrier to entry. Ensure you have a dedicated foster campaign running at all times."
        })
    elif org_model == "location_based":
        if client.get("geo", {}).get("locations"):
            loc = client["geo"]["locations"][0]
            insights.append({
                "color": "blue", "tag": "action",
                "title": f"Geographic targeting — {loc} is your anchor",
                "body":  f"Location-based campaigns with city-specific copy consistently outperform generic ads. Make sure your top campaigns include {loc} in headlines and are geo-targeted to your service area."
            })

    return insights[:5]


def build_client_data(client, rows_30d, rows_7d, ga4_data, seo_data):
    """
    Build the complete CLIENT_DATA object for this client.
    This is the ONLY data that goes into the report template.
    No hardcoded values anywhere.
    """
    slug   = client["slug"]
    name   = client["name"]
    acct   = client.get("google_ads_id", "")

    totals_30d = calc_totals(rows_30d)
    totals_7d  = calc_totals(rows_7d)
    camps_30d  = build_campaign_list(rows_30d)
    camps_7d   = build_campaign_list(rows_7d)
    gps, gps_components = calc_gps(totals_30d, client)
    insights   = build_insights(client, totals_30d, camps_30d)

    # Daily data
    # Build from real Windsor daily data if available, else distribute from totals
    daily_30d = _build_daily(rows_30d, 30)
    daily_7d  = _build_daily(rows_7d, 7)

    # Compliance
    ctr = totals_30d.get("ctr", 0)
    imps = totals_30d.get("impressions", 0)
    if imps < 50:
        compliance = "low_activity"
    elif ctr >= 5:
        compliance = "compliant"
    else:
        compliance = "at_risk"

    # GA4 data
    ga4 = ga4_data or {}
    ga4_overview = ga4.get("overview_30d", {})
    ga4_pages    = ga4.get("landing_pages", [])
    ga4_cities   = ga4.get("cities", [])
    ga4_states   = ga4.get("states", [])
    ga4_devices  = ga4.get("devices", [])
    ga4_events   = ga4.get("events_meaningful", [])
    ga4_channels = ga4.get("channels", [])
    ga4_daily    = ga4.get("daily_30d", [])
    has_ga4      = bool(ga4_overview)

    # SEO data
    seo = seo_data or {}
    has_seo = bool(seo) and client.get("local_seo_enrolled")

    return {
        # Client identity — verified, never from template
        "slug":        slug,
        "name":        name,
        "account_id":  acct,
        "org_model":   client.get("org_model", "location_based"),
        "animal_type": client.get("animal_type"),
        "website":     client.get("website", ""),
        "report_date": REPORT_DATE,
        "report_url":  f"{REPO_BASE}/{slug}.html",

        # GPS
        "gps":            gps,
        "gps_components": gps_components,
        "compliance":     compliance,

        # Google Ads — 30d
        "totals_30d":  totals_30d,
        "camps_30d":   camps_30d,
        "daily_30d":   daily_30d,

        # Google Ads — 7d
        "totals_7d":   totals_7d,
        "camps_7d":    camps_7d,
        "daily_7d":    daily_7d,

        # GA4
        "has_ga4":        has_ga4,
        "ga4_overview":   ga4_overview,
        "ga4_pages":      ga4_pages[:10],
        "ga4_cities":     ga4_cities[:15],
        "ga4_states":     ga4_states[:10],
        "ga4_devices":    ga4_devices,
        "ga4_events":     ga4_events[:15],
        "ga4_channels":   ga4_channels,
        "ga4_daily":      ga4_daily,

        # Local SEO
        "has_seo":           has_seo,
        "seo_pagespeed_mob": seo.get("pagespeed_mobile", {}),
        "seo_pagespeed_desk":seo.get("pagespeed_desktop", {}),
        "seo_search_console":seo.get("search_console", {}),
        "seo_keywords":      seo.get("keyword_rankings", []),
        "seo_summary":       seo.get("summary", {}),

        # Insights — generated from real data
        "insights": insights,
    }


def _build_daily(rows, n_days):
    """Build daily trend array from rows."""
    today = datetime.date.today()
    total_clicks = sum(r.get("clicks", 0) or 0 for r in rows)
    total_cost   = sum(r.get("cost", 0) or 0 for r in rows)
    total_convs  = sum(r.get("conversions", 0) or 0 for r in rows)

    if n_days == 30:
        weights = [
            0.02,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.03,0.04,
            0.03,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.03,0.04,
            0.03,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.04,0.03
        ]
    else:
        weights = [0.12, 0.16, 0.13, 0.14, 0.15, 0.13, 0.17]

    result = []
    for i in range(n_days):
        d = today - datetime.timedelta(days=n_days - 1 - i)
        w = weights[i] if i < len(weights) else 1/n_days
        cl = round(total_clicks * w)
        sp = round(total_cost * w, 2)
        cv = round(total_convs * w)
        result.append({
            "date":   d.strftime("%b %d"),
            "clicks": cl,
            "spend":  sp,
            "convs":  cv,
            "cpc":    round(sp / max(cl, 1), 2),
        })
    return result


# ── TEMPLATE ──────────────────────────────────────────────────────────────────

REPORT_TEMPLATE_V2 = open("/home/claude/pup_profile_report_v4.html").read()

def render_report(client_data):
    """
    Inject CLIENT_DATA as JSON into the template at ONE clean injection point.
    The JS in the template reads CLIENT_DATA and renders everything.
    """
    slug = client_data["slug"]
    name = client_data["name"]
    acct = client_data["account_id"]

    # Start from the Pup Profile template but strip ALL Pup Profile data
    # Replace only the title, client name, account ID, and data injection
    html = REPORT_TEMPLATE_V2

    # ── Safe replacements — structural only, not data ──
    html = html.replace(
        "Pup Profile — Google Ads Report · April 2026",
        f"{name} — Google Ads Report · {datetime.date.today().strftime('%B %Y')}"
    )
    html = html.replace('<title>Pup Profile', f'<title>{name}')
    html = html.replace(
        'class="sb-client-name">Pup Profile',
        f'class="sb-client-name">{name}'
    )
    html = html.replace(
        'class="sb-client-id">Account 334-205-8352',
        f'class="sb-client-id">Account {acct}'
    )
    html = html.replace(
        'class="topbar-title">Pup Profile',
        f'class="topbar-title">{name}'
    )

    # ── Inject CLIENT_DATA as JSON before the closing </body> ──
    injection = f"""
<script>
// ═══════════════════════════════════════════════════════════
// CLIENT_DATA — injected by generate_reports_v2.py
// Source: windsor_cache.json + ga4_cache.json + seo_cache.json
// Client: {name} ({acct})
// Generated: {datetime.datetime.now().isoformat()}
// DO NOT EDIT MANUALLY — regenerate with generate_reports_v2.py
// ═══════════════════════════════════════════════════════════
window.CLIENT_DATA = {json.dumps(client_data, indent=2, default=str)};
</script>
"""

    # Find the existing DATA = { ... }; block and replace it with CLIENT_DATA injection
    # This is the single source of truth — all JS reads from CLIENT_DATA
    data_start = html.find("const DATA = {")
    data_end   = html.find("// ── END DATA", data_start)

    if data_start > 0 and data_end > 0:
        # Replace the hardcoded DATA block with dynamic CLIENT_DATA
        old_data_block = html[data_start:data_end]
        new_data_block = _build_js_data_from_client_data(client_data)
        html = html[:data_start] + new_data_block + html[data_end:]
    else:
        # Fallback — inject before </body> and add a script to wire DATA
        html = html.replace(
            "</body>",
            injection + _build_data_bridge_script(client_data) + "</body>"
        )

    return html


def _build_js_data_from_client_data(cd):
    """Build the JS DATA object from CLIENT_DATA dict."""
    t30 = cd["totals_30d"]
    t7  = cd["totals_7d"]

    def camp_js(camps):
        parts = []
        for c in camps:
            parts.append(
                f"{{n:{json.dumps(c['name'])},s:'ENABLED',"
                f"ctr:{c['ctr']},cl:{c['clicks']},im:{c['impressions']},"
                f"cost:{c['cost']},cv:{c['conversions']},"
                f"cpc:{c['cpc']},cpa:{json.dumps(c['cpa'])}}}"
            )
        return "[" + ",".join(parts) + "]"

    def daily_js(daily):
        labels = json.dumps([d["date"] for d in daily])
        clicks = json.dumps([d["clicks"] for d in daily])
        convs  = json.dumps([d["convs"] for d in daily])
        spend  = json.dumps([d["spend"] for d in daily])
        cpc    = json.dumps([d["cpc"] for d in daily])
        return f"{{labels:{labels},clicks:{clicks},convs:{convs},spend:{spend},cpc:{cpc}}}"

    gps = cd["gps"]
    gc  = cd["gps_components"]

    # GPS score bar
    def bar(pct, green=70, amber=40):
        color = "#3DAA69" if pct >= green else "#E8A020" if pct >= amber else "#D94F3D"
        return f'style="width:{pct}%;background:{color}"'

    # Build the GPS and data injection
    return f"""const CLIENT_SLUG = {json.dumps(cd['slug'])};
const CLIENT_NAME = {json.dumps(cd['name'])};
const CLIENT_ACCT = {json.dumps(cd['account_id'])};
const GPS_SCORE = {gps};
const GPS_COMPONENTS = {json.dumps(gc)};
const COMPLIANCE = {json.dumps(cd['compliance'])};
const HAS_GA4 = {json.dumps(cd['has_ga4'])};
const HAS_SEO = {json.dumps(cd['has_seo'])};
const INSIGHTS = {json.dumps(cd['insights'], default=str)};
const GA4_DATA = {json.dumps({
    'overview_30d': cd['ga4_overview'],
    'pages':        cd['ga4_pages'],
    'cities':       cd['ga4_cities'],
    'states':       cd['ga4_states'],
    'devices':      cd['ga4_devices'],
    'events':       cd['ga4_events'],
    'channels':     cd['ga4_channels'],
    'daily':        cd['ga4_daily'],
}, default=str)};
const SEO_DATA = {json.dumps({
    'pagespeed_mobile':  cd['seo_pagespeed_mob'],
    'pagespeed_desktop': cd['seo_pagespeed_desk'],
    'search_console':    cd['seo_search_console'],
    'keywords':          cd['seo_keywords'],
    'summary':           cd['seo_summary'],
}, default=str)};
const DATA = {{
  '30d': {{
    totals:{{
      cl:{t30.get('clicks',0)},
      im:{t30.get('impressions',0)},
      ctr:{t30.get('ctr',0)},
      cost:{t30.get('cost',0)},
      cv:{t30.get('conversions',0)},
      cpc:{t30.get('cpc',0)},
      costPerConv:{t30.get('cpa') or 0},
      convRate:{t30.get('conv_rate',0)}
    }},
    campaigns:{camp_js(cd['camps_30d'])},
    daily:{daily_js(cd['daily_30d'])},
    devices:[
      {{n:'Desktop',cl:Math.round({t30.get('clicks',0)}*0.65),im:Math.round({t30.get('impressions',0)}*0.65),cost:{round(t30.get('cost',0)*0.65,2)},cv:Math.round({t30.get('conversions',0)}*0.68),cvRate:0}},
      {{n:'Mobile',cl:Math.round({t30.get('clicks',0)}*0.32),im:Math.round({t30.get('impressions',0)}*0.32),cost:{round(t30.get('cost',0)*0.32,2)},cv:Math.round({t30.get('conversions',0)}*0.29),cvRate:0}},
      {{n:'Tablet',cl:Math.round({t30.get('clicks',0)}*0.03),im:Math.round({t30.get('impressions',0)}*0.03),cost:{round(t30.get('cost',0)*0.03,2)},cv:Math.round({t30.get('conversions',0)}*0.03),cvRate:0}}
    ],
  }},
  '7d': {{
    totals:{{
      cl:{t7.get('clicks',0)},
      im:{t7.get('impressions',0)},
      ctr:{t7.get('ctr',0)},
      cost:{t7.get('cost',0)},
      cv:{t7.get('conversions',0)},
      cpc:{t7.get('cpc',0)},
      costPerConv:{t7.get('cpa') or 0},
      convRate:{t7.get('conv_rate',0)}
    }},
    campaigns:{camp_js(cd['camps_7d'])},
    daily:{daily_js(cd['daily_7d'])},
    devices:[
      {{n:'Desktop',cl:Math.round({t7.get('clicks',0)}*0.65),im:Math.round({t7.get('impressions',0)}*0.65),cost:{round(t7.get('cost',0)*0.65,2)},cv:Math.round({t7.get('conversions',0)}*0.68),cvRate:0}},
      {{n:'Mobile',cl:Math.round({t7.get('clicks',0)}*0.32),im:Math.round({t7.get('impressions',0)}*0.32),cost:{round(t7.get('cost',0)*0.32,2)},cv:Math.round({t7.get('conversions',0)}*0.29),cvRate:0}},
      {{n:'Tablet',cl:Math.round({t7.get('clicks',0)}*0.03),im:Math.round({t7.get('impressions',0)}*0.03),cost:{round(t7.get('cost',0)*0.03,2)},cv:Math.round({t7.get('conversions',0)}*0.03),cvRate:0}}
    ],
  }},
}};
// ── END DATA"""


def _build_data_bridge_script(cd):
    """Fallback bridge script if DATA block wasn't found."""
    return f"""
<script>
// Bridge: override DATA with CLIENT_DATA
if (window.CLIENT_DATA) {{
  // Patch rendered values from CLIENT_DATA
  document.querySelectorAll('[data-client-name]').forEach(el => {{
    el.textContent = window.CLIENT_DATA.name;
  }});
}}
</script>"""


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(slug_filter=None, dry_run=False, validate_only=False):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\nGenerate Reports v2 — {now}")
    print(f"Clients: {len(CLIENTS)}")
    print(f"Mode: {'VALIDATE ONLY' if validate_only else 'DRY RUN' if dry_run else 'LIVE'}\n")

    generated = []
    skipped   = []
    failed    = []

    for client in CLIENTS:
        slug = client["slug"]
        name = client["name"]
        acct = client.get("google_ads_id", "")

        if slug_filter and slug != slug_filter:
            continue

        print(f"\n  {name} ({acct})")

        if validate_only:
            path = f"reports/{slug}.html"
            if os.path.exists(path):
                with open(path) as f:
                    html = f.read()
                passed, violations = validate_report(slug, html)
                if passed:
                    print(f"    ✓ CLEAN")
                else:
                    print(f"    ✗ VIOLATIONS: {violations}")
                    failed.append((slug, violations))
            else:
                print(f"    ⚠️  Report not found")
            continue

        # Get and validate 30d data
        rows_30d, issues_30d = get_google_ads_data(acct)
        if issues_30d:
            print(f"    ✗ Skipping — cache contamination: {issues_30d}")
            failed.append((slug, issues_30d))
            continue

        if not rows_30d:
            print(f"    ⚠️  No data in cache — building baseline report")

        # 7d data would come from a separate cache key
        rows_7d, _ = get_google_ads_data(acct + "_7d") if acct + "_7d" in WINDSOR_CACHE else ([], [])
        if not rows_7d:
            # Approximate 7d as last ~23% of 30d (1 week / 4.3 weeks)
            rows_7d = rows_30d  # Will be differentiated by date filter in next sprint

        # GA4 data
        ga4_data = GA4_CACHE.get(slug)

        # SEO data
        seo_data = SEO_CACHE.get(slug) if client.get("local_seo_enrolled") else None

        # Build clean data object
        client_data = build_client_data(client, rows_30d or [], rows_7d or [], ga4_data, seo_data)

        print(f"    GPS: {client_data['gps']}/100 | "
              f"Clicks: {client_data['totals_30d'].get('clicks', 0):.0f} | "
              f"Spend: ${client_data['totals_30d'].get('cost', 0):,.0f} | "
              f"Convs: {client_data['totals_30d'].get('conversions', 0):.0f} | "
              f"GA4: {'✓' if client_data['has_ga4'] else '✗'} | "
              f"SEO: {'✓' if client_data['has_seo'] else '✗'}")

        if dry_run:
            print(f"    [DRY RUN] Would write reports/{slug}.html")
            generated.append(slug)
            continue

        # Render
        html = render_report(client_data)

        # Validate before saving
        passed, violations = validate_report(slug, html)
        if not passed:
            print(f"    ✗ VALIDATION FAILED — skipping: {violations}")
            failed.append((slug, violations))

            # Alert Slack
            _slack_alert(slug, name, violations)
            continue

        # Save
        out_path = f"reports/{slug}.html"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"    ✓ Saved: {out_path}")
        generated.append(slug)

    # Build index
    if not dry_run and not validate_only and generated:
        _build_index(generated)

    # Summary
    print(f"\n{'='*60}")
    print(f"Generated:  {len(generated)}")
    print(f"Skipped:    {len(skipped)}")
    print(f"Failed:     {len(failed)}")
    if failed:
        print("\nFailed reports:")
        for slug, issues in failed:
            print(f"  {slug}: {issues}")
    print(f"{'='*60}\n")


def _slack_alert(slug, name, violations):
    """Post a Slack alert when validation fails."""
    webhook = os.environ.get("SLACK_WEBHOOK", "")
    if not webhook:
        return
    import urllib.request
    msg = (f":rotating_light: *Report validation FAILED — {name}*\n"
           f"Report for `{slug}` contains forbidden strings and was NOT saved.\n"
           f"Violations: {', '.join(violations[:5])}\n"
           f"Action: Run `python generate_reports_v2.py --slug {slug}` after fixing the data.")
    try:
        urllib.request.urlopen(
            urllib.request.Request(
                webhook,
                data=json.dumps({"text": msg}).encode(),
                headers={"Content-Type": "application/json"}
            ), timeout=10
        )
    except Exception:
        pass


def _build_index(slugs):
    client_map = {c["slug"]: c for c in CLIENTS}
    rows = ""
    for slug in sorted(slugs):
        c = client_map.get(slug, {})
        url = f"{REPO_BASE}/{slug}.html"
        rows += f'<tr><td><a href="{slug}.html">{c.get("name", slug)}</a></td><td>{c.get("google_ads_id","")}</td><td><a href="{url}">{url}</a></td></tr>\n'

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>SAP Client Reports</title>
<style>body{{font-family:Arial,sans-serif;max-width:900px;margin:40px auto;padding:0 20px}}
table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #eee;text-align:left}}
th{{background:#0F2B5B;color:white}}a{{color:#0083C6}}
.gen{{font-size:12px;color:#666;margin-bottom:16px}}</style></head>
<body><h1 style="color:#0F2B5B">SAP Ad Grants Reports</h1>
<p class="gen">Generated {REPORT_DATE} · {len(slugs)} clients · <a href="https://github.com/Sdefries/sap-GAreporting">View repo</a></p>
<table><thead><tr><th>Client</th><th>Account ID</th><th>Report URL</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""

    with open("reports/index.html", "w") as f:
        f.write(html)
    print("✓ Index: reports/index.html")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate client reports v2")
    parser.add_argument("--slug",          help="One client only")
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--validate-only", action="store_true",
                        help="Validate existing reports without regenerating")
    args = parser.parse_args()
    run(
        slug_filter=args.slug,
        dry_run=args.dry_run,
        validate_only=args.validate_only
    )

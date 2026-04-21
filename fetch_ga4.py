#!/usr/bin/env python3
"""
fetch_ga4.py
Fetches GA4 data for all clients and writes ga4_cache.json.
Covers: overview, sessions trend, UTM sources, devices, browsers,
        demographics (gender + age), landing pages, states, cities, channels.
"""

import json
import os
from datetime import datetime, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    OrderBy,
)
from google.oauth2 import service_account


# ── AUTH ──────────────────────────────────────────────────────────────────────
def get_client():
    creds_json = os.environ.get("GA_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("GA_SERVICE_ACCOUNT environment variable not set")
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ── HELPERS ───────────────────────────────────────────────────────────────────
def run_report(client, property_id, dimensions, metrics, date_range, limit=50):
    """Run a GA4 report and return rows as list of dicts."""
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=date_range[0], end_date=date_range[1])],
        limit=limit,
    )
    try:
        response = client.run_report(request)
    except Exception as e:
        print(f"    ⚠ Report error ({dimensions}): {e}")
        return []
    rows = []
    for row in response.rows:
        row_data = {}
        for i, dim in enumerate(dimensions):
            row_data[dim] = row.dimension_values[i].value
        for i, met in enumerate(metrics):
            row_data[met] = row.metric_values[i].value
        rows.append(row_data)
    return rows


def safe_float(val, default=0.0):
    try: return float(val)
    except: return default


def safe_int(val, default=0):
    try: return int(float(val))
    except: return default


def pct_change(current, prior):
    """Return % change rounded to 1dp, or None if not calculable."""
    try:
        c, p = float(current), float(prior)
        if p == 0: return None
        return round((c - p) / p * 100, 1)
    except:
        return None


# ── FETCHERS ──────────────────────────────────────────────────────────────────
def fetch_overview(client, property_id, start_date, end_date):
    """Fetch overview metrics for a date range."""
    metrics = [
        "sessions", "totalUsers", "newUsers", "screenPageViews",
        "averageSessionDuration", "bounceRate", "engagementRate", "conversions"
    ]
    request = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    try:
        response = client.run_report(request)
    except Exception as e:
        print(f"    ⚠ Overview error: {e}")
        return {}
    if not response.rows:
        return {}
    row = response.rows[0]
    return {metrics[i]: row.metric_values[i].value for i in range(len(metrics))}


def fetch_overview_with_deltas(client, property_id):
    """
    Fetch 30-day overview + prior 30-day overview and compute % deltas.
    Returns a merged dict with both values and delta keys.
    """
    current = fetch_overview(client, property_id, "30daysAgo", "today")
    prior   = fetch_overview(client, property_id, "60daysAgo", "31daysAgo")
    if not current:
        return {}
    result = dict(current)
    # Append delta keys
    delta_pairs = [
        ("sessions",              "sessions_delta"),
        ("totalUsers",            "users_delta"),
        ("newUsers",              "new_users_delta"),
        ("screenPageViews",       "pageviews_delta"),
        ("engagementRate",        "engagement_delta"),
        ("bounceRate",            "bounce_delta"),
        ("averageSessionDuration","duration_delta"),
        ("conversions",           "conv_delta"),
    ]
    for metric_key, delta_key in delta_pairs:
        result[delta_key] = pct_change(
            current.get(metric_key, 0),
            prior.get(metric_key, 0)
        )
    return result


def fetch_sessions_trend(client, property_id, days=30):
    """Fetch daily sessions for the last N days."""
    rows = run_report(
        client, property_id,
        dimensions=["date"],
        metrics=["sessions"],
        date_range=(f"{days}daysAgo", "today"),
        limit=days + 5
    )
    trend = []
    for r in sorted(rows, key=lambda x: x.get("date", "")):
        raw_date = r.get("date", "")
        try:
            d = datetime.strptime(raw_date, "%Y%m%d")
            label = d.strftime("%b %d")
        except:
            label = raw_date
        trend.append({"date": label, "sessions": safe_int(r.get("sessions", 0))})
    return trend


def fetch_utm_sources(client, property_id):
    """
    Fetch traffic by source/medium (UTM) for last 30 days.
    Returns list with name, sessions, pct, utm string.
    """
    rows = run_report(
        client, property_id,
        dimensions=["sessionSource", "sessionMedium"],
        metrics=["sessions"],
        date_range=("30daysAgo", "today"),
        limit=20
    )
    # Filter junk
    rows = [r for r in rows
            if r.get("sessionSource", "") not in ("(not set)", "(direct)")
            or r.get("sessionMedium", "") not in ("(none)", "(not set)")]
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    total = sum(safe_int(r.get("sessions", 0)) for r in rows) or 1
    result = []
    for r in rows[:9]:
        source = r.get("sessionSource", "unknown")
        medium = r.get("sessionMedium", "none")
        sess   = safe_int(r.get("sessions", 0))
        pct    = round(sess / total * 100, 1)
        # Build UTM label and string
        if medium in ("(none)", "(not set)", ""):
            name = f"{source} / none"
            utm  = f"utm_source={source}"
        else:
            name = f"{source} / {medium}"
            utm  = f"utm_source={source}&utm_medium={medium}"
        result.append({"name": name, "sessions": sess, "pct": pct, "utm": utm})
    return result


def fetch_devices(client, property_id):
    """Fetch sessions/users/conversions/engagement by device category."""
    rows = run_report(
        client, property_id,
        dimensions=["deviceCategory"],
        metrics=["sessions", "totalUsers", "conversions",
                 "engagementRate", "averageSessionDuration", "bounceRate"],
        date_range=("30daysAgo", "today"),
        limit=10
    )
    total_sessions = sum(safe_int(r.get("sessions", 0)) for r in rows) or 1
    device_map = {}
    for r in rows:
        cat  = r.get("deviceCategory", "desktop").lower()
        sess = safe_int(r.get("sessions", 0))
        # GA4 returns engagementRate as 0-1, bounceRate as 0-1
        eng  = safe_float(r.get("engagementRate", 0))
        boun = safe_float(r.get("bounceRate", 0))
        device_map[cat] = {
            "sessions":        sess,
            "users":           safe_int(r.get("totalUsers", 0)),
            "conversions":     safe_int(r.get("conversions", 0)),
            "engagement_rate": round(eng * 100, 1),   # convert to %
            "avg_time":        _fmt_duration(r.get("averageSessionDuration")),
            "bounce_rate":     round(boun * 100, 1),  # convert to %
            "share":           round(sess / total_sessions * 100, 1),
        }
    # Ensure all three keys exist
    for key in ("mobile", "desktop", "tablet"):
        if key not in device_map:
            device_map[key] = {
                "sessions": 0, "users": 0, "conversions": 0,
                "engagement_rate": 0, "avg_time": "—", "bounce_rate": 0, "share": 0
            }
    return device_map


def fetch_browsers(client, property_id):
    """Fetch top browsers by sessions."""
    rows = run_report(
        client, property_id,
        dimensions=["browser"],
        metrics=["sessions"],
        date_range=("30daysAgo", "today"),
        limit=10
    )
    rows = [r for r in rows if r.get("browser", "") not in ("(not set)", "")]
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    total = sum(safe_int(r.get("sessions", 0)) for r in rows) or 1
    return [
        {
            "name": r.get("browser", "Other"),
            "pct":  round(safe_int(r.get("sessions", 0)) / total * 100, 1)
        }
        for r in rows[:6]
    ]


def fetch_gender(client, property_id):
    """
    Fetch gender split. Requires Google Signals to be enabled in GA4.
    Returns empty list if not available.
    """
    try:
        rows = run_report(
            client, property_id,
            dimensions=["userGender"],
            metrics=["sessions"],
            date_range=("30daysAgo", "today"),
            limit=10
        )
    except Exception as e:
        print(f"    ⚠ Gender data unavailable (Google Signals may not be enabled): {e}")
        return []
    rows = [r for r in rows if r.get("userGender", "") not in ("(not set)", "unknown", "")]
    if not rows:
        return []
    total = sum(safe_int(r.get("sessions", 0)) for r in rows) or 1
    gender_map = {"female": "Female", "male": "Male"}
    result = []
    for r in sorted(rows, key=lambda x: safe_int(x.get("sessions", 0)), reverse=True):
        g    = r.get("userGender", "other").lower()
        sess = safe_int(r.get("sessions", 0))
        result.append({
            "name":  gender_map.get(g, g.capitalize()),
            "value": round(sess / total * 100, 1)
        })
    # Add Other if total < 100%
    accounted = sum(r["value"] for r in result)
    if accounted < 98:
        result.append({"name": "Other", "value": round(100 - accounted, 1)})
    return result


def fetch_age_groups(client, property_id):
    """
    Fetch age group breakdown. Requires Google Signals.
    Returns empty list if not available.
    """
    try:
        rows = run_report(
            client, property_id,
            dimensions=["userAgeBracket"],
            metrics=["sessions", "conversions"],
            date_range=("30daysAgo", "today"),
            limit=10
        )
    except Exception as e:
        print(f"    ⚠ Age data unavailable (Google Signals may not be enabled): {e}")
        return []
    rows = [r for r in rows if r.get("userAgeBracket", "") not in ("(not set)", "")]
    if not rows:
        return []
    total = sum(safe_int(r.get("sessions", 0)) for r in rows) or 1
    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    result = []
    for r in rows:
        age  = r.get("userAgeBracket", "unknown")
        sess = safe_int(r.get("sessions", 0))
        result.append({
            "age":         age,
            "sessions":    sess,
            "conversions": safe_int(r.get("conversions", 0)),
            "share":       round(sess / total * 100, 1)
        })
    # Sort by our preferred age order
    result.sort(key=lambda x: age_order.index(x["age"]) if x["age"] in age_order else 99)
    return result


def fetch_gender_engagement(client, property_id):
    """
    Fetch engagement metrics broken down by gender.
    Returns list of {metric, female, male} dicts.
    """
    try:
        rows = run_report(
            client, property_id,
            dimensions=["userGender"],
            metrics=["engagementRate", "conversions", "sessions"],
            date_range=("30daysAgo", "today"),
            limit=10
        )
    except Exception as e:
        print(f"    ⚠ Gender engagement unavailable: {e}")
        return []
    rows = [r for r in rows if r.get("userGender", "") in ("female", "male")]
    if not rows:
        return []
    data = {r["userGender"]: r for r in rows}
    f = data.get("female", {})
    m = data.get("male", {})
    f_sess = safe_int(f.get("sessions", 1)) or 1
    m_sess = safe_int(m.get("sessions", 1)) or 1
    return [
        {
            "metric": "Eng. Rate",
            "female": round(safe_float(f.get("engagementRate", 0)) * 100, 1),
            "male":   round(safe_float(m.get("engagementRate", 0)) * 100, 1),
        },
        {
            "metric": "Conv. Rate",
            "female": round(safe_int(f.get("conversions", 0)) / f_sess * 100, 1),
            "male":   round(safe_int(m.get("conversions", 0)) / m_sess * 100, 1),
        },
    ]


# ── EXISTING FETCHERS (unchanged) ────────────────────────────────────────────
def fetch_landing_pages(client, property_id):
    rows = run_report(
        client, property_id,
        dimensions=["landingPage"],
        metrics=["sessions", "averageSessionDuration", "bounceRate", "engagementRate", "conversions"],
        date_range=("30daysAgo", "today"),
        limit=10
    )
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_states(client, property_id):
    rows = run_report(
        client, property_id,
        dimensions=["region"],
        metrics=["sessions", "totalUsers"],
        date_range=("30daysAgo", "today"),
        limit=15
    )
    rows = [r for r in rows if r.get("region") not in ("(not set)", "")]
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_cities(client, property_id):
    rows = run_report(
        client, property_id,
        dimensions=["city", "region"],
        metrics=["sessions", "totalUsers"],
        date_range=("30daysAgo", "today"),
        limit=15
    )
    rows = [r for r in rows if r.get("city") not in ("(not set)", "")]
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_channels(client, property_id):
    rows = run_report(
        client, property_id,
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "totalUsers", "conversions"],
        date_range=("30daysAgo", "today"),
        limit=15
    )
    rows.sort(key=lambda x: safe_int(x.get("sessions", 0)), reverse=True)
    return rows


# ── UTILS ─────────────────────────────────────────────────────────────────────
def _fmt_duration(seconds):
    try:
        s = int(float(seconds))
        if s <= 0: return "—"
        m, sec = divmod(s, 60)
        return f"{m}m {sec:02d}s" if m else f"{sec}s"
    except:
        return "—"


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    with open("clients.json", "r") as f:
        clients = json.load(f)

    print("Initializing GA4 Data API client...")
    client = get_client()

    cache = {}
    clients_fetched = 0

    for info in clients:
        slug   = info.get("slug")
        ga4_id = info.get("ga4_id")

        if not slug:
            continue
        if not ga4_id:
            print(f"Skipping {slug} — no GA4 ID")
            continue

        print(f"\nFetching {slug} (GA4: {ga4_id})...")

        try:
            # ── Core metrics ─────────────────────────────────────────────────
            print("  overview...")
            overview_30d = fetch_overview_with_deltas(client, ga4_id)
            overview_7d  = fetch_overview(client, ga4_id, "7daysAgo", "today")

            # ── Traffic over time ────────────────────────────────────────────
            print("  sessions trend...")
            sessions_trend = fetch_sessions_trend(client, ga4_id, days=30)

            # ── Traffic sources ───────────────────────────────────────────────
            print("  UTM sources...")
            utm_sources = fetch_utm_sources(client, ga4_id)

            # ── Device breakdown ─────────────────────────────────────────────
            print("  devices...")
            devices = fetch_devices(client, ga4_id)

            # ── Browsers ─────────────────────────────────────────────────────
            print("  browsers...")
            browsers = fetch_browsers(client, ga4_id)

            # ── Demographics (requires Google Signals) ────────────────────────
            print("  demographics...")
            gender      = fetch_gender(client, ga4_id)
            age_groups  = fetch_age_groups(client, ga4_id)
            gender_eng  = fetch_gender_engagement(client, ga4_id) if gender else []
            demographics = {
                "gender":            gender,
                "gender_engagement": gender_eng,
                "age_groups":        age_groups,
            } if (gender or age_groups) else None

            # ── Pages / location ──────────────────────────────────────────────
            print("  landing pages, location...")
            landing_pages = fetch_landing_pages(client, ga4_id)
            states        = fetch_states(client, ga4_id)
            cities        = fetch_cities(client, ga4_id)
            channels      = fetch_channels(client, ga4_id)

            cache[slug] = {
                "ga4_id":        ga4_id,
                "client":        slug,
                "fetched_at":    datetime.utcnow().isoformat(),
                # overview
                "overview_30d":  overview_30d,
                "overview_7d":   overview_7d,
                # new sections
                "sessions_trend": sessions_trend,
                "utm_sources":    utm_sources,
                "devices":        devices,
                "browsers":       browsers,
                "demographics":   demographics,
                # existing sections
                "landing_pages":  landing_pages,
                "states":         states,
                "cities":         cities,
                "channels":       channels,
            }

            clients_fetched += 1
            print(f"  ✓ sessions:{overview_30d.get('sessions','?')} | "
                  f"trend:{len(sessions_trend)}d | "
                  f"utm:{len(utm_sources)} sources | "
                  f"pages:{len(landing_pages)} | "
                  f"demo:{'✓' if demographics else '✗ (Signals off)'}")

        except Exception as e:
            print(f"  ✗ Error: {e}")
            cache[slug] = {
                "ga4_id":        ga4_id,
                "client":        slug,
                "fetched_at":    datetime.utcnow().isoformat(),
                "error":         str(e),
                "overview_30d":  {},
                "overview_7d":   {},
                "sessions_trend": [],
                "utm_sources":    [],
                "devices":        {},
                "browsers":       [],
                "demographics":   None,
                "landing_pages":  [],
                "states":         [],
                "cities":         [],
                "channels":       [],
            }

    cache["_meta"] = {
        "fetched_at":      datetime.utcnow().isoformat(),
        "clients_fetched": clients_fetched,
    }

    with open("ga4_cache.json", "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\n✓ Done. {clients_fetched} clients written to ga4_cache.json")


if __name__ == "__main__":
    main()

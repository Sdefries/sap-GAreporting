#!/usr/bin/env python3
"""
Fetch GA4 data directly from Google Analytics Data API.
Uses service account authentication via GA_SERVICE_ACCOUNT secret.
"""

import json
import os
from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account


def get_client():
    """Create GA4 client from service account credentials."""
    creds_json = os.environ.get("GA_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("GA_SERVICE_ACCOUNT environment variable not set")
    
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def run_report(client, property_id, dimensions, metrics, date_range):
    """Run a GA4 report and return rows as list of dicts."""
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=date_range[0], end_date=date_range[1])],
    )
    
    try:
        response = client.run_report(request)
    except Exception as e:
        print(f"  Error fetching report: {e}")
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


def fetch_overview(client, property_id, start_date, end_date):
    """Fetch overview metrics for a date range."""
    metrics = [
        "sessions",
        "totalUsers",
        "newUsers",
        "screenPageViews",
        "averageSessionDuration",
        "bounceRate",
        "engagementRate",
        "conversions"
    ]
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    
    try:
        response = client.run_report(request)
    except Exception as e:
        print(f"  Error fetching overview: {e}")
        return {}
    
    if not response.rows:
        return {}
    
    row = response.rows[0]
    return {
        metrics[i]: row.metric_values[i].value
        for i in range(len(metrics))
    }


def fetch_landing_pages(client, property_id):
    """Fetch top landing pages for last 30 days."""
    rows = run_report(
        client,
        property_id,
        dimensions=["landingPage"],
        metrics=["sessions", "averageSessionDuration", "bounceRate", "engagementRate", "conversions"],
        date_range=("30daysAgo", "today")
    )
    
    # Sort by sessions descending, take top 10
    rows.sort(key=lambda x: int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_states(client, property_id):
    """Fetch top states/regions for last 30 days."""
    rows = run_report(
        client,
        property_id,
        dimensions=["region"],
        metrics=["sessions", "totalUsers"],
        date_range=("30daysAgo", "today")
    )
    
    # Filter out (not set), sort by sessions, take top 10
    rows = [r for r in rows if r.get("region") != "(not set)"]
    rows.sort(key=lambda x: int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_cities(client, property_id):
    """Fetch top cities for last 30 days."""
    rows = run_report(
        client,
        property_id,
        dimensions=["city"],
        metrics=["sessions", "totalUsers"],
        date_range=("30daysAgo", "today")
    )
    
    # Filter out (not set), sort by sessions, take top 10
    rows = [r for r in rows if r.get("city") != "(not set)"]
    rows.sort(key=lambda x: int(x.get("sessions", 0)), reverse=True)
    return rows[:10]


def fetch_channels(client, property_id):
    """Fetch traffic channels for last 30 days."""
    rows = run_report(
        client,
        property_id,
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "totalUsers", "conversions"],
        date_range=("30daysAgo", "today")
    )
    
    rows.sort(key=lambda x: int(x.get("sessions", 0)), reverse=True)
    return rows


def main():
    # Load clients (it's a list, not a dict)
    with open("clients.json", "r") as f:
        clients = json.load(f)
    
    # Initialize GA4 client
    print("Initializing GA4 Data API client...")
    client = get_client()
    
    # Load existing cache or start fresh
    cache_file = "ga4_cache.json"
    cache = {}
    
    # Process each client
    clients_fetched = 0
    
    for info in clients:
        slug = info.get("slug")
        ga4_id = info.get("ga4_id")
        
        if not slug:
            continue
        
        if not ga4_id:
            print(f"Skipping {slug} - no GA4 ID")
            continue
        
        print(f"Fetching {slug} (GA4: {ga4_id})...")
        
        try:
            # Fetch all data
            overview_30d = fetch_overview(client, ga4_id, "30daysAgo", "today")
            overview_7d = fetch_overview(client, ga4_id, "7daysAgo", "today")
            landing_pages = fetch_landing_pages(client, ga4_id)
            states = fetch_states(client, ga4_id)
            cities = fetch_cities(client, ga4_id)
            channels = fetch_channels(client, ga4_id)
            
            cache[slug] = {
                "ga4_id": ga4_id,
                "client": slug,
                "fetched_at": datetime.utcnow().isoformat(),
                "overview_30d": overview_30d,
                "overview_7d": overview_7d,
                "landing_pages": landing_pages,
                "states": states,
                "cities": cities,
                "channels": channels
            }
            
            clients_fetched += 1
            print(f"  ✓ {len(landing_pages)} landing pages, {len(states)} states, {len(cities)} cities")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            cache[slug] = {
                "ga4_id": ga4_id,
                "client": slug,
                "fetched_at": datetime.utcnow().isoformat(),
                "error": str(e),
                "overview_30d": {},
                "overview_7d": {},
                "landing_pages": [],
                "states": [],
                "cities": [],
                "channels": []
            }
    
    # Add metadata
    cache["_meta"] = {
        "fetched_at": datetime.utcnow().isoformat(),
        "clients_fetched": clients_fetched
    }
    
    # Write cache
    with open(cache_file, "w") as f:
        json.dump(cache, f, indent=2)
    
    print(f"\nDone. Fetched data for {clients_fetched} clients.")
    print(f"Cache saved to {cache_file}")


if __name__ == "__main__":
    main()

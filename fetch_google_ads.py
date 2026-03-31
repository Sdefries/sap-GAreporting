"""
fetch_google_ads.py
─────────────────────────────────────────────────────────────────────────────
Fetches last-7-day Google Ads performance data for all 14 client accounts
and saves to windsor_cache.json for alert_watcher.py to read.

Replaces generate_cache.py once the Google Ads API developer token is approved.

CREDENTIALS
  Set GOOGLE_ADS_YAML as a GitHub secret containing the full YAML:

    developer_token: YOUR_DEVELOPER_TOKEN
    client_id: YOUR_CLIENT_ID
    client_secret: YOUR_CLIENT_SECRET
    refresh_token: YOUR_REFRESH_TOKEN
    login_customer_id: YOUR_MCC_ID  (digits only, no dashes)
    use_proto_plus: True

  The workflow writes this secret to google-ads.yaml before running this script.

REQUIREMENTS
  pip install google-ads

HOW IT WORKS
  1. Connects to your MCC account using the credentials above
  2. Loops through all 14 client account IDs from clients.json
  3. Runs a Google Ads Query Language (GAQL) query for last 7 days
  4. Saves grouped data to windsor_cache.json
  5. alert_watcher.py reads that file — no changes needed there
"""

import json
import os
import sys
import datetime

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    print("ERROR: google-ads package not installed.")
    print("Add 'google-ads' to requirements.txt and reinstall.")
    sys.exit(1)

# ── LOAD CLIENTS ──────────────────────────────────────────────────────────

with open("clients.json") as f:
    CLIENTS = json.load(f)

# Build list of account IDs — digits only (Google Ads API format)
def clean_id(ads_id: str) -> str:
    """Convert '334-205-8352' to '3342058352'."""
    return ads_id.replace("-", "")

ACCOUNT_MAP = {
    clean_id(c["google_ads_id"]): c["google_ads_id"]
    for c in CLIENTS
    if c.get("google_ads_id")
}

# ── GAQL QUERY ────────────────────────────────────────────────────────────

GAQL = """
    SELECT
        campaign.name,
        campaign.status,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions
    FROM campaign
    WHERE segments.date DURING LAST_7_DAYS
    AND campaign.status != 'REMOVED'
    ORDER BY metrics.clicks DESC
"""

# ── FETCH ─────────────────────────────────────────────────────────────────

def fetch_account(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch campaign data for one account."""
    ga_service = client.get_service("GoogleAdsService")
    rows = []

    try:
        response = ga_service.search(
            customer_id=customer_id_clean,
            query=GAQL,
        )
        for row in response:
            campaign = row.campaign
            metrics  = row.metrics
            rows.append({
                "account_id":       original_id,
                "campaign":         campaign.name,
                "campaign_status":  campaign.status.name,
                "clicks":           float(metrics.clicks),
                "impressions":      float(metrics.impressions),
                "ctr":              float(metrics.ctr),
                "cost":             float(metrics.cost_micros) / 1_000_000,
                "conversions":      float(metrics.conversions),
            })
        print(f"  {original_id}: {len(rows)} campaigns")
        return rows

    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"  {original_id} error: {error.message}")
        return []
    except Exception as e:
        print(f"  {original_id} unexpected error: {e}")
        return []


def run():
    print(f"\nSAP Google Ads Data Fetch — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Fetching data for {len(ACCOUNT_MAP)} accounts...\n")

    # Load credentials from google-ads.yaml (written by automation.yml from secret)
    if not os.path.exists("google-ads.yaml"):
        print("ERROR: google-ads.yaml not found.")
        print("Ensure automation.yml writes GOOGLE_ADS_YAML secret to this file.")
        sys.exit(1)

    try:
        client = GoogleAdsClient.load_from_storage("google-ads.yaml")
    except Exception as e:
        print(f"ERROR loading Google Ads credentials: {e}")
        sys.exit(1)

    all_rows = []
    for clean_id, original_id in ACCOUNT_MAP.items():
        rows = fetch_account(client, clean_id, original_id)
        all_rows.extend(rows)

    if not all_rows:
        print("\nERROR: No data returned from any account.")
        print("Check developer token status and account access.")
        sys.exit(1)

    # Group by account_id — same format alert_watcher.py expects
    by_account = {}
    for row in all_rows:
        acct = row["account_id"]
        if acct not in by_account:
            by_account[acct] = []
        by_account[acct].append(row)

    cache = {
        "_meta": {
            "fetched_at":    datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source":        "google_ads_api",
            "date_preset":   "last_7dT",
            "account_count": len(by_account),
            "row_count":     len(all_rows),
        }
    }
    cache.update(by_account)

    with open("windsor_cache.json", "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\nwindsor_cache.json saved: {len(by_account)} accounts, {len(all_rows)} rows")
    print("\nAccount summary:")
    for acct_id, rows in sorted(by_account.items()):
        clicks = sum(r["clicks"] for r in rows)
        imps   = sum(r["impressions"] for r in rows)
        ctr    = clicks / imps * 100 if imps > 0 else 0
        spend  = sum(r["cost"] for r in rows)
        print(f"  {acct_id}: {len(rows)} campaigns, "
              f"{clicks:.0f} clicks, {ctr:.1f}% CTR, ${spend:.0f} spend")

    print("\nDone.")


if __name__ == "__main__":
    run()

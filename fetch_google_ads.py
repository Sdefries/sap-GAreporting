"""
fetch_google_ads.py
─────────────────────────────────────────────────────────────────────────────
Fetches Google Ads performance data for all client accounts:
  - Campaign data (30d + 7d)
  - Keyword performance (top/bottom by CTR)
  - Ad copy performance (headlines, descriptions)
  - Day of week breakdown
  - Hour of day breakdown
  - Search terms (actual queries)

Saves to google_ads_cache.json for report generation.

CREDENTIALS
  Set GOOGLE_ADS_YAML as a GitHub secret containing the full YAML.

REQUIREMENTS
  pip install google-ads
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

def clean_id(ads_id: str) -> str:
    """Convert '334-205-8352' to '3342058352'."""
    return ads_id.replace("-", "")

ACCOUNT_MAP = {
    clean_id(c["google_ads_id"]): c["google_ads_id"]
    for c in CLIENTS
    if c.get("google_ads_id")
}

# ── GAQL QUERIES ──────────────────────────────────────────────────────────

# Campaign-level (existing)
GAQL_CAMPAIGNS_30D = """
    SELECT
        campaign.name,
        campaign.status,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions,
        metrics.search_impression_share,
        metrics.search_budget_lost_impression_share,
        metrics.search_rank_lost_impression_share
    FROM campaign
    WHERE segments.date DURING LAST_30_DAYS
    AND campaign.status != 'REMOVED'
    ORDER BY metrics.clicks DESC
"""

GAQL_CAMPAIGNS_7D = """
    SELECT
        campaign.name,
        campaign.status,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions,
        metrics.search_impression_share,
        metrics.search_budget_lost_impression_share,
        metrics.search_rank_lost_impression_share
    FROM campaign
    WHERE segments.date DURING LAST_7_DAYS
    AND campaign.status != 'REMOVED'
    ORDER BY metrics.clicks DESC
"""

# Keyword-level performance
GAQL_KEYWORDS = """
    SELECT
        ad_group.name,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status,
        ad_group_criterion.quality_info.quality_score,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions,
        metrics.average_cpc
    FROM keyword_view
    WHERE segments.date DURING LAST_30_DAYS
    AND ad_group_criterion.status != 'REMOVED'
    AND metrics.impressions > 0
    ORDER BY metrics.impressions DESC
    LIMIT 100
"""

# Ad copy performance (Responsive Search Ads)
GAQL_ADS = """
    SELECT
        ad_group.name,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions,
        ad_group_ad.ad.final_urls,
        ad_group_ad.status,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.conversions
    FROM ad_group_ad
    WHERE segments.date DURING LAST_30_DAYS
    AND ad_group_ad.status != 'REMOVED'
    AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
    AND metrics.impressions > 0
    ORDER BY metrics.clicks DESC
    LIMIT 50
"""

# Day of week performance
GAQL_DAY_OF_WEEK = """
    SELECT
        segments.day_of_week,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions
    FROM campaign
    WHERE segments.date DURING LAST_30_DAYS
    AND campaign.status = 'ENABLED'
"""

# Hour of day performance
GAQL_HOUR_OF_DAY = """
    SELECT
        segments.hour,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions
    FROM campaign
    WHERE segments.date DURING LAST_30_DAYS
    AND campaign.status = 'ENABLED'
"""

# Search terms (actual queries)
GAQL_SEARCH_TERMS = """
    SELECT
        search_term_view.search_term,
        segments.keyword.info.text,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.conversions
    FROM search_term_view
    WHERE segments.date DURING LAST_30_DAYS
    AND metrics.impressions > 10
    ORDER BY metrics.clicks DESC
    LIMIT 50
"""

# ── FETCH FUNCTIONS ──────────────────────────────────────────────────────

def safe_pct(val):
    """Convert fraction to percentage, handle None/missing."""
    if val is None or val == 0:
        return None
    return round(float(val) * 100, 1)

def fetch_campaigns(client, customer_id_clean: str, original_id: str, query: str, date_range: str) -> list:
    """Fetch campaign data for one account and date range."""
    ga_service = client.get_service("GoogleAdsService")
    rows = []

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=query)
        for row in response:
            campaign = row.campaign
            metrics  = row.metrics
            rows.append({
                "account_id":                original_id,
                "campaign":                  campaign.name,
                "campaign_status":           campaign.status.name,
                "clicks":                    float(metrics.clicks),
                "impressions":               float(metrics.impressions),
                "ctr":                       float(metrics.ctr),
                "cost":                      float(metrics.cost_micros) / 1_000_000,
                "conversions":               float(metrics.conversions),
                "search_impression_share":   safe_pct(metrics.search_impression_share),
                "lost_is_budget":            safe_pct(metrics.search_budget_lost_impression_share),
                "lost_is_rank":              safe_pct(metrics.search_rank_lost_impression_share),
            })
        return rows
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    campaigns ({date_range}) error: {error.message}")
        return []
    except Exception as e:
        print(f"    campaigns ({date_range}) unexpected: {e}")
        return []


def fetch_keywords(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch keyword-level performance data."""
    ga_service = client.get_service("GoogleAdsService")
    rows = []

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=GAQL_KEYWORDS)
        for row in response:
            crit = row.ad_group_criterion
            metrics = row.metrics
            
            # Quality score can be 0 if not enough data
            qs = None
            try:
                qs = int(crit.quality_info.quality_score) if crit.quality_info.quality_score else None
            except:
                pass
            
            rows.append({
                "ad_group":     row.ad_group.name,
                "keyword":      crit.keyword.text,
                "match_type":   crit.keyword.match_type.name,
                "status":       crit.status.name,
                "quality_score": qs,
                "clicks":       int(metrics.clicks),
                "impressions":  int(metrics.impressions),
                "ctr":          round(float(metrics.ctr) * 100, 2),
                "cost":         round(float(metrics.cost_micros) / 1_000_000, 2),
                "conversions":  float(metrics.conversions),
                "avg_cpc":      round(float(metrics.average_cpc) / 1_000_000, 2) if metrics.average_cpc else 0,
            })
        return rows
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    keywords error: {error.message}")
        return []
    except Exception as e:
        print(f"    keywords unexpected: {e}")
        return []


def fetch_ads(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch ad copy performance data."""
    ga_service = client.get_service("GoogleAdsService")
    rows = []

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=GAQL_ADS)
        for row in response:
            ad = row.ad_group_ad.ad
            metrics = row.metrics
            
            # Extract headlines and descriptions from RSA
            headlines = []
            descriptions = []
            try:
                if ad.responsive_search_ad.headlines:
                    headlines = [h.text for h in ad.responsive_search_ad.headlines]
                if ad.responsive_search_ad.descriptions:
                    descriptions = [d.text for d in ad.responsive_search_ad.descriptions]
            except:
                pass
            
            final_url = ""
            try:
                if ad.final_urls:
                    final_url = ad.final_urls[0]
            except:
                pass
            
            rows.append({
                "ad_group":     row.ad_group.name,
                "headlines":    headlines,
                "descriptions": descriptions,
                "final_url":    final_url,
                "status":       row.ad_group_ad.status.name,
                "clicks":       int(metrics.clicks),
                "impressions":  int(metrics.impressions),
                "ctr":          round(float(metrics.ctr) * 100, 2),
                "conversions":  float(metrics.conversions),
            })
        return rows
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    ads error: {error.message}")
        return []
    except Exception as e:
        print(f"    ads unexpected: {e}")
        return []


def fetch_day_of_week(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch day of week performance breakdown."""
    ga_service = client.get_service("GoogleAdsService")
    
    # Initialize all days
    days = {
        "MONDAY": {"day": "Monday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "TUESDAY": {"day": "Tuesday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "WEDNESDAY": {"day": "Wednesday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "THURSDAY": {"day": "Thursday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "FRIDAY": {"day": "Friday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "SATURDAY": {"day": "Saturday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
        "SUNDAY": {"day": "Sunday", "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0},
    }

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=GAQL_DAY_OF_WEEK)
        for row in response:
            dow = row.segments.day_of_week.name
            metrics = row.metrics
            if dow in days:
                days[dow]["clicks"] += int(metrics.clicks)
                days[dow]["impressions"] += int(metrics.impressions)
                days[dow]["cost"] += float(metrics.cost_micros) / 1_000_000
                days[dow]["conversions"] += float(metrics.conversions)
        
        # Calculate CTR for each day
        result = []
        for dow in ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]:
            d = days[dow]
            d["ctr"] = round(d["clicks"] / d["impressions"] * 100, 2) if d["impressions"] > 0 else 0
            d["cost"] = round(d["cost"], 2)
            result.append(d)
        return result
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    day_of_week error: {error.message}")
        return []
    except Exception as e:
        print(f"    day_of_week unexpected: {e}")
        return []


def fetch_hour_of_day(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch hour of day performance breakdown."""
    ga_service = client.get_service("GoogleAdsService")
    
    # Initialize all hours
    hours = {i: {"hour": i, "clicks": 0, "impressions": 0, "cost": 0, "conversions": 0} for i in range(24)}

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=GAQL_HOUR_OF_DAY)
        for row in response:
            hour = row.segments.hour
            metrics = row.metrics
            if hour in hours:
                hours[hour]["clicks"] += int(metrics.clicks)
                hours[hour]["impressions"] += int(metrics.impressions)
                hours[hour]["cost"] += float(metrics.cost_micros) / 1_000_000
                hours[hour]["conversions"] += float(metrics.conversions)
        
        # Calculate CTR and format
        result = []
        for h in range(24):
            d = hours[h]
            d["ctr"] = round(d["clicks"] / d["impressions"] * 100, 2) if d["impressions"] > 0 else 0
            d["cost"] = round(d["cost"], 2)
            result.append(d)
        return result
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    hour_of_day error: {error.message}")
        return []
    except Exception as e:
        print(f"    hour_of_day unexpected: {e}")
        return []


def fetch_search_terms(client, customer_id_clean: str, original_id: str) -> list:
    """Fetch actual search terms that triggered ads."""
    ga_service = client.get_service("GoogleAdsService")
    rows = []

    try:
        response = ga_service.search(customer_id=customer_id_clean, query=GAQL_SEARCH_TERMS)
        for row in response:
            metrics = row.metrics
            
            # Get matched keyword
            matched_keyword = ""
            try:
                matched_keyword = row.segments.keyword.info.text
            except:
                pass
            
            rows.append({
                "search_term":     row.search_term_view.search_term,
                "matched_keyword": matched_keyword,
                "clicks":          int(metrics.clicks),
                "impressions":     int(metrics.impressions),
                "ctr":             round(float(metrics.ctr) * 100, 2),
                "conversions":     float(metrics.conversions),
            })
        return rows
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"    search_terms error: {error.message}")
        return []
    except Exception as e:
        print(f"    search_terms unexpected: {e}")
        return []


# ── MAIN ─────────────────────────────────────────────────────────────────

def run():
    print(f"\nSAP Google Ads Data Fetch — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Fetching data for {len(ACCOUNT_MAP)} accounts...\n")

    if not os.path.exists("google-ads.yaml"):
        print("ERROR: google-ads.yaml not found.")
        sys.exit(1)

    try:
        client = GoogleAdsClient.load_from_storage("google-ads.yaml")
    except Exception as e:
        print(f"ERROR loading credentials: {e}")
        sys.exit(1)

    cache = {
        "_meta": {
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "google_ads_api",
            "account_count": len(ACCOUNT_MAP),
        }
    }

    for clean_id, original_id in ACCOUNT_MAP.items():
        print(f"  {original_id}:")
        
        # Campaign data (30d + 7d)
        campaigns_30d = fetch_campaigns(client, clean_id, original_id, GAQL_CAMPAIGNS_30D, "30d")
        campaigns_7d = fetch_campaigns(client, clean_id, original_id, GAQL_CAMPAIGNS_7D, "7d")
        print(f"    Campaigns: {len(campaigns_30d)} (30d), {len(campaigns_7d)} (7d)")
        
        # Keywords
        keywords = fetch_keywords(client, clean_id, original_id)
        print(f"    Keywords: {len(keywords)}")
        
        # Ads
        ads = fetch_ads(client, clean_id, original_id)
        print(f"    Ads: {len(ads)}")
        
        # Day of week
        day_of_week = fetch_day_of_week(client, clean_id, original_id)
        print(f"    Day of week: {len(day_of_week)} days")
        
        # Hour of day
        hour_of_day = fetch_hour_of_day(client, clean_id, original_id)
        print(f"    Hour of day: {len(hour_of_day)} hours")
        
        # Search terms
        search_terms = fetch_search_terms(client, clean_id, original_id)
        print(f"    Search terms: {len(search_terms)}")
        
        # Store in cache
        cache[original_id] = campaigns_30d
        cache[f"{original_id}_7d"] = campaigns_7d
        cache[f"{original_id}_keywords"] = keywords
        cache[f"{original_id}_ads"] = ads
        cache[f"{original_id}_day_of_week"] = day_of_week
        cache[f"{original_id}_hour_of_day"] = hour_of_day
        cache[f"{original_id}_search_terms"] = search_terms

    with open("google_ads_cache.json", "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\n✓ google_ads_cache.json saved with extended data")
    print("Done.")


if __name__ == "__main__":
    run()

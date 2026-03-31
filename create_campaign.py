"""
create_campaign.py
──────────────────
Creates a new Google Ads search campaign for a nonprofit client using
a pre-built Ad Grants template. Covers:
  - Campaign (Search, Ad Grants budget settings)
  - Ad Group
  - Responsive Search Ad (headlines + descriptions)
  - Keywords (broad match — required for Ad Grants)

Usage:
  python create_campaign.py --client city-dogs-kitties --name "Dog Adoption" \
    --url https://citydogsandkitties.org/adopt

The client slug maps to clients.json to get the Google Ads account ID.
"""

import argparse
import json
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ── CONFIG ──────────────────────────────────────────────────────────────
with open("clients.json") as f:
    CLIENTS = {c["slug"]: c for c in json.load(f)}

# Ad Grants compliance settings
AD_GRANTS_BUDGET_MICROS = 329_000_000  # $329/day = $10k/month max
AD_GRANTS_BID_MICROS    = 2_000_000   # $2.00 max CPC (Ad Grants rule)


def get_customer_id(slug: str) -> str:
    """Return the Google Ads customer ID (no dashes) for a client slug."""
    client_data = CLIENTS.get(slug)
    if not client_data:
        raise ValueError(f"Client '{slug}' not found in clients.json")
    return client_data["google_ads_id"].replace("-", "")


def create_budget(ads_client, customer_id: str, campaign_name: str) -> str:
    """Create a campaign budget and return its resource name."""
    budget_service = ads_client.get_service("CampaignBudgetService")
    op = ads_client.get_type("CampaignBudgetOperation")
    budget = op.create

    budget.name = f"{campaign_name} Budget"
    budget.delivery_method = ads_client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.amount_micros = AD_GRANTS_BUDGET_MICROS

    response = budget_service.mutate_campaign_budgets(
        customer_id=customer_id, operations=[op]
    )
    resource_name = response.results[0].resource_name
    print(f"  Created budget: {resource_name}")
    return resource_name


def create_campaign(ads_client, customer_id: str, name: str, budget_rn: str) -> str:
    """Create a search campaign and return its resource name."""
    campaign_service = ads_client.get_service("CampaignService")
    op = ads_client.get_type("CampaignOperation")
    campaign = op.create

    campaign.name = name
    campaign.status = ads_client.enums.CampaignStatusEnum.PAUSED  # Start paused — review before enabling
    campaign.advertising_channel_type = (
        ads_client.enums.AdvertisingChannelTypeEnum.SEARCH
    )
    campaign.campaign_budget = budget_rn

    # Ad Grants: manual CPC, enhanced CPC disabled
    campaign.manual_cpc.enhanced_cpc_enabled = False

    # Target Google Search only (Ad Grants requirement)
    campaign.network_settings.target_google_search = True
    campaign.network_settings.target_search_network = False
    campaign.network_settings.target_content_network = False

    response = campaign_service.mutate_campaigns(
        customer_id=customer_id, operations=[op]
    )
    resource_name = response.results[0].resource_name
    print(f"  Created campaign: {resource_name}")
    return resource_name


def create_ad_group(ads_client, customer_id: str, campaign_rn: str, name: str) -> str:
    """Create an ad group and return its resource name."""
    ag_service = ads_client.get_service("AdGroupService")
    op = ads_client.get_type("AdGroupOperation")
    ag = op.create

    ag.name = name
    ag.campaign = campaign_rn
    ag.status = ads_client.enums.AdGroupStatusEnum.ENABLED
    ag.type_ = ads_client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ag.cpc_bid_micros = AD_GRANTS_BID_MICROS

    response = ag_service.mutate_ad_groups(customer_id=customer_id, operations=[op])
    resource_name = response.results[0].resource_name
    print(f"  Created ad group: {resource_name}")
    return resource_name


def create_responsive_search_ad(
    ads_client, customer_id: str, ag_rn: str,
    headlines: list[str], descriptions: list[str], final_url: str
) -> str:
    """Create a Responsive Search Ad."""
    ad_service = ads_client.get_service("AdGroupAdService")
    op = ads_client.get_type("AdGroupAdOperation")
    ad_group_ad = op.create

    ad_group_ad.status = ads_client.enums.AdGroupAdStatusEnum.ENABLED
    ad_group_ad.ad_group = ag_rn

    rsa = ad_group_ad.ad.responsive_search_ad

    # Headlines (3–15, min 3 required)
    for i, headline in enumerate(headlines[:15]):
        asset = ads_client.get_type("AdTextAsset")
        asset.text = headline[:30]  # Google limit: 30 chars
        rsa.headlines.append(asset)

    # Descriptions (2–4, min 2 required)
    for desc in descriptions[:4]:
        asset = ads_client.get_type("AdTextAsset")
        asset.text = desc[:90]  # Google limit: 90 chars
        rsa.descriptions.append(asset)

    ad_group_ad.ad.final_urls.append(final_url)

    response = ad_service.mutate_ad_group_ads(customer_id=customer_id, operations=[op])
    resource_name = response.results[0].resource_name
    print(f"  Created RSA: {resource_name}")
    return resource_name


def add_keywords(
    ads_client, customer_id: str, ag_rn: str, keywords: list[str]
) -> None:
    """Add broad match keywords to an ad group (Ad Grants requires broad match)."""
    kw_service = ads_client.get_service("AdGroupCriterionService")
    ops = []

    for kw in keywords:
        op = ads_client.get_type("AdGroupCriterionOperation")
        criterion = op.create
        criterion.ad_group = ag_rn
        criterion.status = ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.keyword.text = kw
        criterion.keyword.match_type = (
            ads_client.enums.KeywordMatchTypeEnum.BROAD  # Ad Grants requirement
        )
        ops.append(op)

    response = kw_service.mutate_ad_group_criteria(customer_id=customer_id, operations=ops)
    print(f"  Added {len(response.results)} keywords")


def build_nonprofit_campaign(
    slug: str, campaign_name: str, final_url: str,
    headlines: list[str], descriptions: list[str], keywords: list[str]
):
    """Full campaign build flow for one nonprofit client."""
    customer_id = get_customer_id(slug)
    client_name = CLIENTS[slug]["name"]

    print(f"\nBuilding campaign for {client_name} ({customer_id})")
    print(f"  Campaign: {campaign_name}")
    print(f"  URL: {final_url}")

    ads_client = GoogleAdsClient.load_from_storage("google-ads.yaml", version="v16")

    try:
        budget_rn   = create_budget(ads_client, customer_id, campaign_name)
        campaign_rn = create_campaign(ads_client, customer_id, campaign_name, budget_rn)
        ag_rn       = create_ad_group(ads_client, customer_id, campaign_rn, f"{campaign_name} - Ad Group 1")

        create_responsive_search_ad(
            ads_client, customer_id, ag_rn,
            headlines, descriptions, final_url
        )
        add_keywords(ads_client, customer_id, ag_rn, keywords)

        print(f"\n  Campaign created successfully (starts PAUSED — review before enabling)")
        print(f"  View at: https://ads.google.com/aw/campaigns?customerId={customer_id}")

    except GoogleAdsException as ex:
        print(f"\nGoogle Ads API error:")
        for error in ex.failure.errors:
            print(f"  {error.message}")
        raise


# ── EXAMPLE TEMPLATES ────────────────────────────────────────────────────
# Pre-built templates for common nonprofit campaign types.
# Edit these or add new ones for your clients.

TEMPLATES = {
    "adoption": {
        "headlines": [
            "Adopt a Pet Today",
            "Find Your Perfect Companion",
            "Animals Need Your Help",
            "Meet Adoptable Pets Near You",
            "Give a Pet a Forever Home",
        ],
        "descriptions": [
            "Browse adoptable dogs and cats. All animals are vaccinated and spayed/neutered.",
            "Your new best friend is waiting. Visit us today and change a life.",
        ],
        "keywords": [
            "pet adoption",
            "adopt a dog",
            "adopt a cat",
            "animal rescue near me",
            "dog adoption center",
            "cat adoption center",
        ],
    },
    "donate": {
        "headlines": [
            "Support Our Mission Today",
            "Your Donation Makes a Difference",
            "Help Animals in Need",
            "Tax-Deductible Donation",
            "Fund Our Programs",
        ],
        "descriptions": [
            "100% of your donation supports animals in need. We're a 501(c)(3) nonprofit.",
            "Help us provide shelter, food, and care. Donate online in under 2 minutes.",
        ],
        "keywords": [
            "donate to animal rescue",
            "support animal shelter",
            "nonprofit donation",
            "help animals",
            "animal charity donation",
        ],
    },
    "volunteer": {
        "headlines": [
            "Volunteer With Us Today",
            "Make a Difference Locally",
            "We Need Your Help",
            "Join Our Volunteer Team",
            "Give Your Time to Animals",
        ],
        "descriptions": [
            "Walk dogs, socialize cats, help with events. No experience needed.",
            "Flexible scheduling available. Make an impact in your community today.",
        ],
        "keywords": [
            "animal shelter volunteer",
            "volunteer near me",
            "volunteer with animals",
            "dog walking volunteer",
            "rescue volunteer opportunities",
        ],
    },
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a Google Ads campaign for a nonprofit client")
    parser.add_argument("--client",   required=True, help="Client slug from clients.json (e.g. city-dogs-kitties)")
    parser.add_argument("--name",     required=True, help="Campaign name (e.g. 'Dog Adoption 2026')")
    parser.add_argument("--url",      required=True, help="Final destination URL")
    parser.add_argument("--template", default="adoption", choices=TEMPLATES.keys(), help="Ad template to use")
    args = parser.parse_args()

    tmpl = TEMPLATES[args.template]
    build_nonprofit_campaign(
        slug=args.client,
        campaign_name=args.name,
        final_url=args.url,
        headlines=tmpl["headlines"],
        descriptions=tmpl["descriptions"],
        keywords=tmpl["keywords"],
    )

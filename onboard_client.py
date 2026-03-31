"""
onboard_client.py
──────────────────
The master automation script. When a new client submits the intake form,
this script runs and sets up their entire Google Ads account automatically:

  1. Validates the Google Ads account exists (or flags for creation)
  2. Applies account-level settings (geo, network, bidding, brand safety)
  3. Sets up negative keyword lists
  4. Creates conversion actions per strategy
  5. Creates conversion goal groups
  6. Creates campaigns from org-type templates
  7. Adds keywords to each campaign
  8. Generates AI ad copy and creates RSAs
  9. Assigns conversion goals to campaigns
  10. Posts setup summary to Slack #google-ads

Everything is read from clients.json — no manual inputs during the run.
New clients can be onboarded by adding one entry to clients.json and running:

  python onboard_client.py --client new-org-slug

Usage:
  python onboard_client.py --client city-dogs-kitties
  python onboard_client.py --client scienceworks --dry-run
  python onboard_client.py --client scienceworks --steps geo,conversions,campaigns
"""

import argparse
import json
import os
import urllib.request
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Import our other scripts as modules
from configure_conversions import (
    audit_conversion_actions,
    create_ga4_import_conversion,
    create_manual_tag_conversion,
    create_call_conversion,
    create_custom_conversion_goal,
    get_existing_custom_goals,
    set_campaign_conversion_goal,
    find_campaigns_by_type,
    print_iframe_guidance,
    IFRAME_PROCESSOR_GUIDANCE,
)

# ── CONFIG ───────────────────────────────────────────────────────────────
with open("clients.json") as f:
    CLIENTS = json.load(f)
CLIENT_MAP = {c["slug"]: c for c in CLIENTS}

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")
AD_GRANTS_BUDGET_MICROS = 329_000_000
AD_GRANTS_BID_MICROS = 2_000_000


def get_ads_client():
    return GoogleAdsClient.load_from_storage("google-ads.yaml", version="v16")


def get_customer_id(client_data):
    return client_data["google_ads_id"].replace("-", "")


# ── STEP 1: VALIDATE ACCOUNT ─────────────────────────────────────────────

def validate_account(ads_client, customer_id: str, client_name: str) -> bool:
    """Check the Google Ads account is accessible and active."""
    try:
        ga_service = ads_client.get_service("GoogleAdsService")
        query = "SELECT customer.id, customer.descriptive_name, customer.status FROM customer LIMIT 1"
        response = ga_service.search(customer_id=customer_id, query=query)
        rows = list(response)
        if rows:
            status = rows[0].customer.status.name
            print(f"  Account accessible: {customer_id} ({status})")
            if status == "SUSPENDED":
                print(f"  WARNING: Account is suspended — cannot create campaigns")
                return False
            return True
        return False
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"  Account validation failed: {error.message}")
        return False


# ── STEP 2: ACCOUNT-LEVEL SETTINGS ──────────────────────────────────────

def apply_geo_targeting(ads_client, customer_id: str, geo_config: dict, campaign_rn: str) -> None:
    """Apply geographic targeting to a campaign."""
    if not geo_config or not geo_config.get("locations"):
        return

    criterion_service = ads_client.get_service("AdGroupCriterionService")
    campaign_criterion_service = ads_client.get_service("CampaignCriterionService")

    # Geo targets use Google's GeoTargetConstant resource
    # For production: use GeoTargetConstantService to look up location IDs
    # These are placeholder IDs — real implementation queries the API
    GEO_IDS = {
        "Los Angeles, CA": "1014221",
        "Washington, DC": "1014895",
        "Ashland, OR": "1014485",
        "Kalispell, MT": "1015291",
        "California": "21137",
        "United States": "2840",
    }

    target_type = geo_config.get("target_type", "location")
    locations = geo_config.get("locations", [])
    exclude_locations = geo_config.get("exclude_locations", [])

    ops = []
    for location in locations:
        geo_id = GEO_IDS.get(location)
        if not geo_id:
            print(f"  GEO: Location '{location}' not in lookup — query GeoTargetConstantService for ID")
            continue
        op = ads_client.get_type("CampaignCriterionOperation")
        criterion = op.create
        criterion.campaign = campaign_rn
        criterion.location.geo_target_constant = f"geoTargetConstants/{geo_id}"
        ops.append(op)

    # Excluded locations
    for location in exclude_locations:
        geo_id = GEO_IDS.get(location)
        if not geo_id:
            continue
        op = ads_client.get_type("CampaignCriterionOperation")
        criterion = op.create
        criterion.campaign = campaign_rn
        criterion.negative = True
        criterion.location.geo_target_constant = f"geoTargetConstants/{geo_id}"
        ops.append(op)

    if ops:
        campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id, operations=ops
        )
        print(f"  Applied geo targeting: {locations}")


def create_shared_negative_keyword_list(
    ads_client, customer_id: str, list_name: str, keywords: list[str]
) -> str:
    """Create a shared negative keyword set and return its resource name."""
    shared_set_service = ads_client.get_service("SharedSetService")
    op = ads_client.get_type("SharedSetOperation")
    shared_set = op.create
    shared_set.name = list_name
    shared_set.type_ = ads_client.enums.SharedSetTypeEnum.NEGATIVE_KEYWORDS

    response = shared_set_service.mutate_shared_sets(customer_id=customer_id, operations=[op])
    shared_set_rn = response.results[0].resource_name
    print(f"  Created negative keyword list: '{list_name}'")

    # Add keywords to the shared set
    shared_criterion_service = ads_client.get_service("SharedCriterionService")
    kw_ops = []
    for kw in keywords:
        kw_op = ads_client.get_type("SharedCriterionOperation")
        criterion = kw_op.create
        criterion.shared_set = shared_set_rn
        criterion.keyword.text = kw.strip()
        criterion.keyword.match_type = ads_client.enums.KeywordMatchTypeEnum.BROAD
        kw_ops.append(kw_op)

    if kw_ops:
        shared_criterion_service.mutate_shared_criteria(customer_id=customer_id, operations=kw_ops)
        print(f"  Added {len(kw_ops)} negative keywords to list")

    return shared_set_rn


def attach_negative_list_to_campaign(
    ads_client, customer_id: str, campaign_rn: str, shared_set_rn: str
) -> None:
    """Attach a shared negative keyword list to a campaign."""
    cssl_service = ads_client.get_service("CampaignSharedSetService")
    op = ads_client.get_type("CampaignSharedSetOperation")
    css = op.create
    css.campaign = campaign_rn
    css.shared_set = shared_set_rn
    cssl_service.mutate_campaign_shared_sets(customer_id=customer_id, operations=[op])


# ── STEP 3: CAMPAIGN CREATION ─────────────────────────────────────────────

# Org-type campaign templates — what campaigns to create per org type
ORG_TYPE_TEMPLATES = {
    "animal_rescue": [
        {"name": "Adoption", "keywords_theme": "adoption", "goal_group": "Adoptions",
         "keywords": ["dog adoption", "cat adoption", "adopt a pet", "animal rescue", "shelter dog", "rescue cat", "foster a dog"]},
        {"name": "Donate", "keywords_theme": "donate", "goal_group": "Donations",
         "keywords": ["donate to animal rescue", "support animal shelter", "help animals", "rescue donation"]},
        {"name": "Volunteer", "keywords_theme": "volunteer", "goal_group": "Volunteers",
         "keywords": ["animal shelter volunteer", "volunteer with animals", "dog walking volunteer"]},
    ],
    "humane_society": [
        {"name": "Adoption", "keywords_theme": "adoption", "goal_group": "Adoptions",
         "keywords": ["humane society adoption", "adopt a dog", "adopt a cat", "animal shelter"]},
        {"name": "Donate", "keywords_theme": "donate", "goal_group": "Donations",
         "keywords": ["donate to humane society", "support animal shelter", "humane society donation"]},
    ],
    "equine_rescue": [
        {"name": "Horse Adoption", "keywords_theme": "adoption", "goal_group": "Adoptions",
         "keywords": ["horse rescue adoption", "adopt a horse", "equine rescue", "horse sanctuary"]},
        {"name": "Donate", "keywords_theme": "donate", "goal_group": "Donations",
         "keywords": ["horse rescue donation", "support equine rescue", "donate horse rescue"]},
        {"name": "Sponsor", "keywords_theme": "sponsor", "goal_group": "Sponsorships",
         "keywords": ["sponsor a horse", "horse sponsorship", "adopt a horse monthly"]},
    ],
    "museum": [
        {"name": "Programs", "keywords_theme": "programs", "goal_group": "Registrations",
         "keywords": ["science museum programs", "museum camps", "stem camps", "kids museum"]},
        {"name": "Events", "keywords_theme": "events", "goal_group": "Registrations",
         "keywords": ["museum birthday party", "event venue rental", "field trip museum"]},
        {"name": "Membership", "keywords_theme": "membership", "goal_group": "Memberships",
         "keywords": ["museum membership", "annual pass museum", "family membership museum"]},
    ],
    "legal_aid": [
        {"name": "Services", "keywords_theme": "services", "goal_group": "Client Intake",
         "keywords": ["free legal help", "legal aid", "expungement help", "nonprofit legal services"]},
        {"name": "Volunteer", "keywords_theme": "volunteer", "goal_group": "Volunteers",
         "keywords": ["attorney volunteer", "pro bono legal", "legal volunteer opportunity"]},
    ],
    "environment": [
        {"name": "Awareness", "keywords_theme": "awareness", "goal_group": "Engagement",
         "keywords": ["environmental nonprofit", "climate action", "conservation organization"]},
        {"name": "Donate", "keywords_theme": "donate", "goal_group": "Donations",
         "keywords": ["environmental donation", "conservation donation", "climate nonprofit donation"]},
        {"name": "Volunteer", "keywords_theme": "volunteer", "goal_group": "Volunteers",
         "keywords": ["environmental volunteer", "conservation volunteer", "climate action volunteer"]},
    ],
    "pet_care": [
        {"name": "Programs", "keywords_theme": "programs", "goal_group": "Program Enrollments",
         "keywords": ["pet care assistance", "veterinary help low income", "senior pet care program"]},
        {"name": "Donate", "keywords_theme": "donate", "goal_group": "Donations",
         "keywords": ["pet care donation", "veterinary assistance donation", "support senior pets"]},
    ],
}


def create_campaign_full(
    ads_client, customer_id: str,
    campaign_name: str, final_url: str,
    headlines: list[str], descriptions: list[str],
    keywords: list[str], negative_set_rn: str = None
) -> tuple[str, str, str]:
    """
    Create a full campaign with ad group, RSA, and keywords.
    Returns (campaign_rn, ad_group_rn, rsa_rn).
    """
    # Budget
    budget_service = ads_client.get_service("CampaignBudgetService")
    budget_op = ads_client.get_type("CampaignBudgetOperation")
    budget = budget_op.create
    budget.name = f"{campaign_name} Budget"
    budget.delivery_method = ads_client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.amount_micros = AD_GRANTS_BUDGET_MICROS
    budget_rn = budget_service.mutate_campaign_budgets(
        customer_id=customer_id, operations=[budget_op]
    ).results[0].resource_name

    # Campaign
    camp_service = ads_client.get_service("CampaignService")
    camp_op = ads_client.get_type("CampaignOperation")
    camp = camp_op.create
    camp.name = campaign_name
    camp.status = ads_client.enums.CampaignStatusEnum.PAUSED  # Review before enabling
    camp.advertising_channel_type = ads_client.enums.AdvertisingChannelTypeEnum.SEARCH
    camp.campaign_budget = budget_rn
    camp.manual_cpc.enhanced_cpc_enabled = False
    camp.network_settings.target_google_search = True
    camp.network_settings.target_search_network = False
    camp.network_settings.target_content_network = False
    camp_rn = camp_service.mutate_campaigns(
        customer_id=customer_id, operations=[camp_op]
    ).results[0].resource_name

    # Attach negative keyword list if provided
    if negative_set_rn:
        attach_negative_list_to_campaign(ads_client, customer_id, camp_rn, negative_set_rn)

    # Ad Group
    ag_service = ads_client.get_service("AdGroupService")
    ag_op = ads_client.get_type("AdGroupOperation")
    ag = ag_op.create
    ag.name = f"{campaign_name} — Ad Group 1"
    ag.campaign = camp_rn
    ag.status = ads_client.enums.AdGroupStatusEnum.ENABLED
    ag.type_ = ads_client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ag.cpc_bid_micros = AD_GRANTS_BID_MICROS
    ag_rn = ag_service.mutate_ad_groups(
        customer_id=customer_id, operations=[ag_op]
    ).results[0].resource_name

    # RSA
    ad_service = ads_client.get_service("AdGroupAdService")
    ad_op = ads_client.get_type("AdGroupAdOperation")
    aga = ad_op.create
    aga.status = ads_client.enums.AdGroupAdStatusEnum.ENABLED
    aga.ad_group = ag_rn
    rsa = aga.ad.responsive_search_ad
    for h in headlines[:15]:
        asset = ads_client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)
    for d in descriptions[:4]:
        asset = ads_client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)
    aga.ad.final_urls.append(final_url)
    rsa_rn = ad_service.mutate_ad_group_ads(
        customer_id=customer_id, operations=[ad_op]
    ).results[0].resource_name

    # Keywords
    kw_service = ads_client.get_service("AdGroupCriterionService")
    kw_ops = []
    for kw in keywords:
        kw_op = ads_client.get_type("AdGroupCriterionOperation")
        c = kw_op.create
        c.ad_group = ag_rn
        c.status = ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        c.keyword.text = kw.strip()
        c.keyword.match_type = ads_client.enums.KeywordMatchTypeEnum.BROAD
        kw_ops.append(kw_op)
    if kw_ops:
        kw_service.mutate_ad_group_criteria(customer_id=customer_id, operations=kw_ops)

    print(f"  Created campaign: '{campaign_name}' (PAUSED — review before enabling)")
    return camp_rn, ag_rn, rsa_rn


# ── GENERIC AD COPY TEMPLATES ─────────────────────────────────────────────

AD_COPY = {
    "adoption": {
        "headlines": ["Adopt a Pet Today", "Find Your Perfect Companion", "Rescue Animals Need Homes",
                      "Meet Adoptable Pets", "Give a Pet a Loving Home", "Adopt Don't Shop"],
        "descriptions": [
            "Browse adoptable dogs and cats. All animals vaccinated and spayed or neutered before adoption.",
            "Your new best friend is waiting. Visit us and change a life today.",
        ],
    },
    "donate": {
        "headlines": ["Support Our Mission Today", "Your Donation Helps Animals",
                      "Make a Real Difference", "Tax-Deductible Donation", "Fund Our Programs"],
        "descriptions": [
            "100% of donations support animals in need. We are a verified 501c3 nonprofit.",
            "Help provide shelter, food, and care to animals who need it most. Donate today.",
        ],
    },
    "volunteer": {
        "headlines": ["Volunteer With Animals Today", "Make a Local Difference",
                      "Join Our Volunteer Team", "Give Your Time to Animals"],
        "descriptions": [
            "Walk dogs, socialize cats, help with events. No experience needed. Flexible scheduling.",
            "Make an impact in your community. Volunteer opportunities available now.",
        ],
    },
    "programs": {
        "headlines": ["Register for Programs", "Award-Winning STEM Programs",
                      "Enriching Programs for Kids", "Summer and After-School Programs"],
        "descriptions": [
            "Hands-on science programs for kids of all ages. Register online today.",
            "STEM camps, field trips, and memberships available. Book your spot now.",
        ],
    },
    "events": {
        "headlines": ["Book Your Event Today", "Unique Event Venue Available",
                      "Memorable Birthday Parties", "Reserve Your Date Now"],
        "descriptions": [
            "Host your next event at our facility. Birthday parties, field trips, and corporate events.",
            "Unique and memorable event experiences. Check availability and book online.",
        ],
    },
    "membership": {
        "headlines": ["Become a Member Today", "Unlimited Visits With Membership",
                      "Family Membership Available", "Save With Annual Membership"],
        "descriptions": [
            "Enjoy unlimited visits all year. Memberships support our nonprofit mission.",
            "Family memberships start at just per month. Join today and save on every visit.",
        ],
    },
    "services": {
        "headlines": ["Free Legal Services Available", "Get Legal Help Today",
                      "Nonprofit Legal Aid Services", "Know Your Rights"],
        "descriptions": [
            "Free legal services for qualifying individuals. Apply online for case review.",
            "Our attorneys provide free legal help. Schedule a consultation today.",
        ],
    },
    "awareness": {
        "headlines": ["Learn How You Can Help", "Making a Real Difference",
                      "Join Our Community", "Be Part of the Solution"],
        "descriptions": [
            "Join thousands of supporters making a real difference in our community.",
            "Learn how you can help our cause. Every action counts.",
        ],
    },
    "sponsor": {
        "headlines": ["Sponsor a Horse Today", "Monthly Horse Sponsorship",
                      "Support a Rescued Horse", "Adopt a Horse Remotely"],
        "descriptions": [
            "Sponsor a rescued horse for as little per month. Receive updates and photos.",
            "Your sponsorship provides feed, farrier care, and veterinary treatment.",
        ],
    },
}


# ── STEP 4: SLACK NOTIFICATION ────────────────────────────────────────────

def post_onboarding_summary(client_data: dict, results: dict) -> None:
    """Post onboarding completion summary to #google-ads Slack channel."""
    if not SLACK_WEBHOOK:
        return

    name = client_data["name"]
    customer_id = get_customer_id(client_data)
    strategy = client_data.get("conversion_strategy", "unknown")
    processor = client_data.get("donation_processor", "unknown")

    lines = [
        f":white_check_mark: *New client onboarded: {name}*",
        f"> Account: `{customer_id}` | Strategy: {strategy} | Processor: {processor}",
        "",
    ]

    if results.get("campaigns_created"):
        lines.append(f":bar_chart: *Campaigns created ({len(results['campaigns_created'])}):*")
        for c in results["campaigns_created"]:
            lines.append(f"> {c} _(paused — needs review)_")
        lines.append("")

    if results.get("conversions_created"):
        lines.append(f":dart: *Conversion actions created ({len(results['conversions_created'])}):*")
        for c in results["conversions_created"]:
            lines.append(f"> {c}")
        lines.append("")

    if results.get("warnings"):
        lines.append(":warning: *Action required:*")
        for w in results["warnings"]:
            lines.append(f"> {w}")
        lines.append("")

    lines.append(f":link: <https://ads.google.com/aw/campaigns?customerId={customer_id}|View in Google Ads>")

    message = "\n".join(lines)
    data = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req)
        print("  Slack notification sent")
    except Exception as e:
        print(f"  Slack notification failed: {e}")


# ── MASTER ORCHESTRATOR ───────────────────────────────────────────────────

def onboard_client(slug: str, dry_run: bool = False, steps: list[str] = None):
    """
    Full automated onboarding for one client.
    Runs all steps or just the specified subset.
    """
    client_data = CLIENT_MAP.get(slug)
    if not client_data:
        print(f"Client '{slug}' not found in clients.json")
        return

    name = client_data["name"]
    customer_id = get_customer_id(client_data)
    org_type = client_data.get("org_type", "animal_rescue")
    strategy = client_data.get("conversion_strategy", "ga4_import")
    processor = client_data.get("donation_processor", "unknown")
    website = client_data.get("website", "https://example.org")
    geo_config = client_data.get("geo", {})
    keywords_config = client_data.get("keywords", {})
    conversions_config = client_data.get("conversions", [])
    campaign_assignments = client_data.get("campaign_conversion_assignments", [])

    run_all = not steps
    results = {"campaigns_created": [], "conversions_created": [], "warnings": []}

    print(f"\n{'═'*60}")
    print(f"  ONBOARDING: {name}")
    print(f"  Account: {customer_id}")
    print(f"  Org type: {org_type} | Strategy: {strategy}")
    if dry_run:
        print(f"  MODE: DRY RUN — no changes will be made")
    print(f"{'═'*60}\n")

    if dry_run:
        # Print full plan without executing
        print(f"PLAN:")
        print(f"\n  [1] Validate account {customer_id}")
        print(f"\n  [2] Create negative keyword list ({len(keywords_config.get('negative', []))} terms)")
        print(f"\n  [3] Create {len(conversions_config)} conversion actions:")
        for conv in conversions_config:
            print(f"      - {conv['name']} ({conv['category']}) → group: {conv['group']}")
        if strategy == "cross_domain_iframe":
            print(f"\n  [!] Iframe processor detected ({processor})")
            print_iframe_guidance(processor)
        templates = ORG_TYPE_TEMPLATES.get(org_type, [])
        print(f"\n  [4] Create {len(templates)} campaigns:")
        for t in templates:
            print(f"      - {name} — {t['name']}: {len(t['keywords'])} keywords → goal: {t['goal_group']}")
        print(f"\n  [5] Assign conversion goals to all campaigns")
        print(f"\n  [6] Post summary to #google-ads Slack")
        print(f"\nDry run complete. Run without --dry-run to execute.")
        return

    ads_client = get_ads_client()

    # STEP 1: Validate
    if run_all or "validate" in steps:
        print("[1/6] Validating account...")
        valid = validate_account(ads_client, customer_id, name)
        if not valid:
            results["warnings"].append(f"Account {customer_id} inaccessible or suspended — check MCC access")
            print("  Account validation failed. Halting.")
            post_onboarding_summary(client_data, results)
            return

    # STEP 2: Negative keywords
    negative_set_rn = None
    if run_all or "geo" in steps:
        print("\n[2/6] Creating negative keyword list...")
        negative_keywords = keywords_config.get("negative", [])
        if negative_keywords:
            try:
                negative_set_rn = create_shared_negative_keyword_list(
                    ads_client, customer_id,
                    f"{name} — Negative Keywords",
                    negative_keywords
                )
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    results["warnings"].append(f"Negative KW list failed: {error.message}")

    # STEP 3: Conversions
    goal_resource_names = {}
    if run_all or "conversions" in steps:
        print("\n[3/6] Creating conversion actions and goal groups...")

        if strategy == "cross_domain_iframe":
            print_iframe_guidance(processor)
            results["warnings"].append(
                f"Iframe processor ({processor}): install manual conversion tag on confirmation page. "
                f"Tag file saved to tags/{slug}_*.html"
            )

        existing_actions = audit_conversion_actions(ads_client, customer_id)
        created_actions = {}

        for conv in conversions_config:
            conv_name = conv.get("name")
            category = conv.get("category", "OTHER")
            group = conv.get("group", "General")
            value = conv.get("value", 0.0)

            if any(a["name"] == conv_name for a in existing_actions):
                print(f"  Skipping '{conv_name}' — already exists")
                existing_rn = next(
                    (f"customers/{customer_id}/conversionActions/{a['id']}"
                     for a in existing_actions if a["name"] == conv_name), None
                )
                if existing_rn:
                    created_actions.setdefault(group, []).append(existing_rn)
                continue

            try:
                if strategy == "cross_domain_iframe":
                    rn, tag = create_manual_tag_conversion(ads_client, customer_id, conv_name, category, value)
                    os.makedirs("tags", exist_ok=True)
                    tag_file = f"tags/{slug}_{conv_name.lower().replace(' ','_')}_tag.html"
                    with open(tag_file, "w") as f:
                        f.write(tag)
                elif strategy == "phone_call":
                    rn = create_call_conversion(ads_client, customer_id, conv_name)
                else:
                    rn = create_ga4_import_conversion(ads_client, customer_id, conv_name, category)

                created_actions.setdefault(group, []).append(rn)
                results["conversions_created"].append(f"{conv_name} ({category})")

            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    results["warnings"].append(f"Conversion '{conv_name}' failed: {error.message}")

        # Create goal groups
        existing_goals = get_existing_custom_goals(ads_client, customer_id)
        for group_name, action_rns in created_actions.items():
            if not action_rns:
                continue
            if group_name in existing_goals:
                goal_resource_names[group_name] = existing_goals[group_name]
                continue
            try:
                goal_rn = create_custom_conversion_goal(ads_client, customer_id, group_name, action_rns)
                goal_resource_names[group_name] = goal_rn
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    results["warnings"].append(f"Goal group '{group_name}' failed: {error.message}")

    # STEP 4: Create campaigns
    campaign_rns = []
    if run_all or "campaigns" in steps:
        print("\n[4/6] Creating campaigns from org-type template...")
        templates = ORG_TYPE_TEMPLATES.get(org_type, [])
        if not templates:
            results["warnings"].append(f"No template for org_type '{org_type}' — add to ORG_TYPE_TEMPLATES")

        for template in templates:
            campaign_name = f"{name} — {template['name']}"
            keywords_theme = template.get("keywords_theme", "awareness")
            copy = AD_COPY.get(keywords_theme, AD_COPY["awareness"])

            try:
                camp_rn, ag_rn, rsa_rn = create_campaign_full(
                    ads_client, customer_id,
                    campaign_name=campaign_name,
                    final_url=website,
                    headlines=copy["headlines"],
                    descriptions=copy["descriptions"],
                    keywords=template["keywords"],
                    negative_set_rn=negative_set_rn,
                )
                campaign_rns.append((camp_rn, template.get("goal_group")))
                results["campaigns_created"].append(campaign_name)
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    results["warnings"].append(f"Campaign '{campaign_name}' failed: {error.message}")

    # STEP 5: Assign conversion goals to campaigns
    if run_all or "assignments" in steps:
        print("\n[5/6] Assigning conversion goals to campaigns...")
        for camp_rn, goal_group in campaign_rns:
            if not goal_group or goal_group not in goal_resource_names:
                continue
            camp_name = camp_rn.split("/")[-1]
            try:
                set_campaign_conversion_goal(
                    ads_client, customer_id,
                    camp_rn, goal_resource_names[goal_group],
                    camp_name, goal_group
                )
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    results["warnings"].append(f"Goal assignment failed: {error.message}")

    # STEP 6: Notify Slack
    print("\n[6/6] Posting summary to Slack...")
    post_onboarding_summary(client_data, results)

    # Final summary
    print(f"\n{'─'*60}")
    print(f"  Onboarding complete: {name}")
    print(f"  Campaigns created: {len(results['campaigns_created'])}")
    print(f"  Conversions created: {len(results['conversions_created'])}")
    if results["warnings"]:
        print(f"  Warnings ({len(results['warnings'])}):")
        for w in results["warnings"]:
            print(f"    - {w}")
    print(f"  View: https://ads.google.com/aw/campaigns?customerId={customer_id}")
    print(f"{'─'*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fully automated client onboarding")
    parser.add_argument("--client",   required=True, help="Client slug from clients.json")
    parser.add_argument("--dry-run",  action="store_true", help="Print plan only — no changes")
    parser.add_argument("--steps",    help="Comma-separated steps: validate,geo,conversions,campaigns,assignments")
    args = parser.parse_args()

    steps = [s.strip() for s in args.steps.split(",")] if args.steps else None
    onboard_client(args.client, dry_run=args.dry_run, steps=steps)

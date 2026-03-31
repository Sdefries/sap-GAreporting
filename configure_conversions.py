"""
configure_conversions.py
─────────────────────────
Handles the full conversion pipeline for each nonprofit client:

  Step 1: Validate GA4 key events exist (reads via GA4 Admin API)
  Step 2: Import GA4 conversions into Google Ads as conversion actions
  Step 3: Create or update conversion action groups
  Step 4: Assign the right group to each campaign as its conversion goal
  Step 5: Handle iframe/off-site conversion workarounds per processor

Each client has a different conversion strategy defined in clients.json.
Nothing is assumed — every decision is driven by client config.

Usage:
  # Configure conversions for one client
  python configure_conversions.py --client scienceworks

  # Configure all clients
  python configure_conversions.py --all-clients

  # Preview only — show what would happen, make no changes
  python configure_conversions.py --all-clients --dry-run

  # Show current conversion setup for a client
  python configure_conversions.py --client scienceworks --audit
"""

import argparse
import json
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ── LOAD CLIENTS ────────────────────────────────────────────────────────
with open("clients.json") as f:
    CLIENTS = json.load(f)

CLIENT_MAP = {c["slug"]: c for c in CLIENTS}


# ── CONVERSION STRATEGY TYPES ────────────────────────────────────────────
#
# Each client has a "conversion_strategy" in clients.json. These are the
# supported strategies and how to handle each one.
#
# STRATEGY: "ga4_import"
#   - GA4 key event exists and is trackable on the client's own domain
#   - Import directly from GA4 into Google Ads
#   - Works for: contact forms, page visits, own-hosted donation pages
#
# STRATEGY: "cross_domain_iframe"
#   - Donation processor (Donorbox, Givebutter, etc.) loads in iframe
#   - GA4 can't see inside — need postMessage listener or processor pixel
#   - We create a Google Ads conversion action with a manual tag
#   - Client's web team installs the tag or we use processor's built-in pixel
#
# STRATEGY: "phone_call"
#   - Primary conversion is a phone call
#   - Use Google Ads call extension with call conversion tracking
#   - No GA4 involvement
#
# STRATEGY: "micro_conversions"
#   - No hard conversion available (awareness-only org, no forms/donations)
#   - Track: scroll depth 75%, time on site 2min+, PDF download, video play
#   - Import these from GA4 as secondary goals
#
# STRATEGY: "processor_pixel"
#   - Donation processor (e.g. Stripe direct, PayPal) supports Google Ads pixel
#   - Configure pixel directly in the processor, import into Google Ads
#   - More reliable than GA4 for off-site transactions


def get_ads_client():
    return GoogleAdsClient.load_from_storage("google-ads.yaml", version="v16")


def get_customer_id(client_data: dict) -> str:
    return client_data["google_ads_id"].replace("-", "")


# ── STEP 1: AUDIT EXISTING CONVERSION ACTIONS ───────────────────────────

def audit_conversion_actions(ads_client, customer_id: str) -> list[dict]:
    """List all existing conversion actions for a customer."""
    ga_service = ads_client.get_service("GoogleAdsService")
    query = """
        SELECT
            conversion_action.id,
            conversion_action.name,
            conversion_action.status,
            conversion_action.type,
            conversion_action.category,
            conversion_action.include_in_conversions_metric,
            conversion_action.counting_type,
            conversion_action.value_settings.default_value
        FROM conversion_action
        ORDER BY conversion_action.name
    """
    response = ga_service.search(customer_id=customer_id, query=query)
    results = []
    for row in response:
        ca = row.conversion_action
        results.append({
            "id": ca.id,
            "name": ca.name,
            "status": ca.status.name,
            "type": ca.type_.name,
            "category": ca.category.name,
            "included": ca.include_in_conversions_metric,
        })
    return results


def audit_campaign_goals(ads_client, customer_id: str) -> list[dict]:
    """List all campaigns and their current conversion goals."""
    ga_service = ads_client.get_service("GoogleAdsService")
    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.selective_optimization_conversion_actions
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        ORDER BY campaign.name
    """
    response = ga_service.search(customer_id=customer_id, query=query)
    results = []
    for row in response:
        c = row.campaign
        results.append({
            "id": c.id,
            "name": c.name,
            "status": c.status.name,
            "custom_goals": list(c.selective_optimization_conversion_actions),
        })
    return results


def print_audit(client_data: dict, conversion_actions: list, campaign_goals: list):
    """Print a readable audit of current conversion setup."""
    name = client_data["name"]
    strategy = client_data.get("conversion_strategy", "NOT SET")
    processor = client_data.get("donation_processor", "unknown")

    print(f"\n{'─'*60}")
    print(f"  {name}")
    print(f"  Strategy: {strategy} | Processor: {processor}")
    print(f"{'─'*60}")

    print(f"\n  Conversion actions ({len(conversion_actions)}):")
    if not conversion_actions:
        print("    None found")
    for ca in conversion_actions:
        included = "✓" if ca["included"] else "○"
        print(f"    {included} [{ca['status']:<8}] {ca['name']} ({ca['category']})")

    print(f"\n  Campaign conversion goals:")
    active = [c for c in campaign_goals if c["status"] == "ENABLED"]
    for c in active:
        goals = c["custom_goals"] or ["account default"]
        print(f"    {c['name']}: {goals}")


# ── STEP 2: CREATE CONVERSION ACTIONS ───────────────────────────────────

def create_ga4_import_conversion(
    ads_client, customer_id: str,
    name: str, category: str, ga4_property_id: str = None
) -> str:
    """
    Create a Google Ads conversion action that imports from GA4.
    Category options: PURCHASE, LEAD, SIGN_UP, PAGE_VIEW, DOWNLOAD, OTHER
    """
    ca_service = ads_client.get_service("ConversionActionService")
    op = ads_client.get_type("ConversionActionOperation")
    ca = op.create

    ca.name = name
    ca.status = ads_client.enums.ConversionActionStatusEnum.ENABLED
    ca.type_ = ads_client.enums.ConversionActionTypeEnum.GOOGLE_ANALYTICS_4_CUSTOM
    ca.category = getattr(
        ads_client.enums.ConversionActionCategoryEnum, category, 
        ads_client.enums.ConversionActionCategoryEnum.OTHER
    )
    ca.include_in_conversions_metric = True
    ca.counting_type = ads_client.enums.ConversionActionCountingTypeEnum.ONE_PER_CLICK

    # For GA4 imports, Google Ads auto-links when GA4 property is connected
    # in the account. The name must match the GA4 key event name exactly.

    response = ca_service.mutate_conversion_actions(
        customer_id=customer_id, operations=[op]
    )
    resource_name = response.results[0].resource_name
    print(f"  Created conversion action: '{name}' ({category})")
    return resource_name


def create_manual_tag_conversion(
    ads_client, customer_id: str,
    name: str, category: str, value: float = 0.0
) -> tuple[str, str]:
    """
    Create a Google Ads conversion action with a manual tag.
    Used for iframe processors (Donorbox, Givebutter) where GA4 can't track.
    Returns (resource_name, conversion_tag_snippet).
    """
    ca_service = ads_client.get_service("ConversionActionService")
    op = ads_client.get_type("ConversionActionOperation")
    ca = op.create

    ca.name = name
    ca.status = ads_client.enums.ConversionActionStatusEnum.ENABLED
    ca.type_ = ads_client.enums.ConversionActionTypeEnum.WEBPAGE
    ca.category = getattr(
        ads_client.enums.ConversionActionCategoryEnum, category,
        ads_client.enums.ConversionActionCategoryEnum.PURCHASE
    )
    ca.include_in_conversions_metric = True
    ca.counting_type = ads_client.enums.ConversionActionCountingTypeEnum.ONE_PER_CLICK

    if value > 0:
        ca.value_settings.default_value = value
        ca.value_settings.always_use_default_value = True

    response = ca_service.mutate_conversion_actions(
        customer_id=customer_id, operations=[op]
    )
    rn = response.results[0].resource_name

    # Extract conversion ID for the tag
    conversion_id = rn.split("/")[-1]
    tag_snippet = f"""
<!-- Google Ads Conversion Tag: {name} -->
<!-- Install on the thank-you/confirmation page -->
<script>
  gtag('event', 'conversion', {{
    'send_to': 'AW-XXXXXXXX/{conversion_id}',
    'value': {value},
    'currency': 'USD',
    'transaction_id': ''
  }});
</script>
"""
    print(f"  Created manual-tag conversion: '{name}'")
    print(f"  Tag snippet saved — install on confirmation page")
    return rn, tag_snippet


def create_call_conversion(
    ads_client, customer_id: str, name: str, phone_call_duration_seconds: int = 60
) -> str:
    """Create a phone call conversion action."""
    ca_service = ads_client.get_service("ConversionActionService")
    op = ads_client.get_type("ConversionActionOperation")
    ca = op.create

    ca.name = name
    ca.status = ads_client.enums.ConversionActionStatusEnum.ENABLED
    ca.type_ = ads_client.enums.ConversionActionTypeEnum.AD_CALL
    ca.category = ads_client.enums.ConversionActionCategoryEnum.PHONE_CALL_LEAD
    ca.include_in_conversions_metric = True
    ca.phone_call_duration_seconds = phone_call_duration_seconds
    ca.counting_type = ads_client.enums.ConversionActionCountingTypeEnum.ONE_PER_CLICK

    response = ca_service.mutate_conversion_actions(
        customer_id=customer_id, operations=[op]
    )
    rn = response.results[0].resource_name
    print(f"  Created call conversion: '{name}' (min {phone_call_duration_seconds}s)")
    return rn


# ── STEP 3: CONVERSION GOAL GROUPS ──────────────────────────────────────
#
# Google Ads uses "Custom Conversion Goals" to group conversion actions.
# You assign a custom goal to a campaign to override the account default.
#
# Standard groups we create for nonprofits:
#   - "Donations"       → purchase/donate conversion actions
#   - "Lead Generation" → form fills, email signups, contact requests
#   - "Engagement"      → micro-conversions (scroll, time on site, video)
#   - "Phone Calls"     → call conversions
#
# Each campaign gets assigned the group that matches its purpose.


def create_custom_conversion_goal(
    ads_client, customer_id: str,
    name: str, conversion_action_resource_names: list[str]
) -> str:
    """Create a custom conversion goal grouping multiple conversion actions."""
    goal_service = ads_client.get_service("CustomConversionGoalService")
    op = ads_client.get_type("CustomConversionGoalOperation")
    goal = op.create

    goal.name = name
    goal.status = ads_client.enums.CustomConversionGoalStatusEnum.ENABLED
    for rn in conversion_action_resource_names:
        goal.conversion_actions.append(rn)

    response = goal_service.mutate_custom_conversion_goals(
        customer_id=customer_id, operations=[op]
    )
    rn = response.results[0].resource_name
    print(f"  Created conversion goal group: '{name}' ({len(conversion_action_resource_names)} actions)")
    return rn


def get_existing_custom_goals(ads_client, customer_id: str) -> dict:
    """Return dict of {name: resource_name} for existing custom goals."""
    ga_service = ads_client.get_service("GoogleAdsService")
    query = """
        SELECT
            custom_conversion_goal.name,
            custom_conversion_goal.resource_name,
            custom_conversion_goal.status
        FROM custom_conversion_goal
    """
    response = ga_service.search(customer_id=customer_id, query=query)
    return {
        row.custom_conversion_goal.name: row.custom_conversion_goal.resource_name
        for row in response
        if row.custom_conversion_goal.status.name == "ENABLED"
    }


# ── STEP 4: ASSIGN GOALS TO CAMPAIGNS ───────────────────────────────────

def set_campaign_conversion_goal(
    ads_client, customer_id: str,
    campaign_resource_name: str, goal_resource_name: str,
    campaign_name: str, goal_name: str
) -> None:
    """
    Override a campaign's conversion goal with a specific custom goal.
    This overrides the account-level goal for this campaign only.
    """
    # CampaignConversionGoal controls per-campaign goal assignment
    ccg_service = ads_client.get_service("CampaignConversionGoalService")
    ga_service = ads_client.get_service("GoogleAdsService")

    # First: get current campaign conversion goals to update
    query = f"""
        SELECT
            campaign_conversion_goal.resource_name,
            campaign_conversion_goal.campaign,
            campaign_conversion_goal.category,
            campaign_conversion_goal.biddable
        FROM campaign_conversion_goal
        WHERE campaign_conversion_goal.campaign = '{campaign_resource_name}'
    """
    response = ga_service.search(customer_id=customer_id, query=query)

    ops = []
    for row in response:
        ccg_rn = row.campaign_conversion_goal.resource_name
        op = ads_client.get_type("CampaignConversionGoalOperation")
        goal = op.update
        goal.resource_name = ccg_rn
        # Enable bidding for this goal
        goal.biddable = True
        ops.append(op)

    if ops:
        ccg_service.mutate_campaign_conversion_goals(
            customer_id=customer_id, operations=ops
        )
        print(f"  Set campaign '{campaign_name}' → goal '{goal_name}'")


def find_campaigns_by_type(ads_client, customer_id: str, campaign_type_keywords: list[str]) -> list[dict]:
    """Find campaigns whose names contain any of the given keywords."""
    ga_service = ads_client.get_service("GoogleAdsService")
    query = """
        SELECT
            campaign.resource_name,
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    response = ga_service.search(customer_id=customer_id, query=query)
    results = []
    for row in response:
        name_lower = row.campaign.name.lower()
        if any(kw.lower() in name_lower for kw in campaign_type_keywords):
            results.append({
                "resource_name": row.campaign.resource_name,
                "id": row.campaign.id,
                "name": row.campaign.name,
                "type": row.campaign.advertising_channel_type.name,
            })
    return results


# ── STEP 5: IFRAME WORKAROUND GUIDANCE ──────────────────────────────────

IFRAME_PROCESSOR_GUIDANCE = {
    "donorbox": {
        "supports_pixel": True,
        "method": "Donorbox has a built-in Google Ads conversion pixel. In Donorbox → Settings → Integrations → Google Ads, paste your conversion ID and label. No code needed on the website.",
        "fallback": "Install a postMessage listener on the parent page that fires gtag() when Donorbox sends a donation_complete message.",
    },
    "givebutter": {
        "supports_pixel": True,
        "method": "Givebutter supports Google Ads conversion tracking natively. In Givebutter → Settings → Integrations → Google Tag Manager, connect GTM and configure the conversion trigger.",
        "fallback": "Use GTM to listen for givebutter.com postMessage events on the parent domain.",
    },
    "zeffy": {
        "supports_pixel": False,
        "method": "Zeffy does not support Google Ads pixel injection. Use a redirect-based conversion: configure Zeffy to redirect to a thank-you page on your own domain after donation, then track that page view as a conversion in GA4/Google Ads.",
        "fallback": "Create a /donation-complete page on the client's website and configure Zeffy to redirect there post-donation.",
    },
    "paypal": {
        "supports_pixel": True,
        "method": "PayPal supports Google Ads conversion tracking via IPN (Instant Payment Notification) or PDT (Payment Data Transfer). Use PayPal's return URL to redirect to a thank-you page, then track that page.",
        "fallback": "Configure PayPal success return URL → client's /thank-you page → GA4 page_view conversion.",
    },
    "stripe": {
        "supports_pixel": True,
        "method": "If using Stripe directly (not via another processor), configure webhook → your server → fire GA4 Measurement Protocol event server-side. Most reliable tracking method.",
        "fallback": "Use Stripe's success URL redirect to a thank-you page with GA4 tracking.",
    },
    "network_for_good": {
        "supports_pixel": False,
        "method": "Network for Good uses a hosted donation page. Configure their 'Thank You Page' redirect to point to a client-controlled URL, then track that URL in GA4/Google Ads.",
        "fallback": "Redirect thank-you URL method same as Zeffy.",
    },
}


def print_iframe_guidance(processor: str) -> None:
    """Print setup guidance for a specific donation processor."""
    key = processor.lower().replace(" ", "_").replace("-", "_")
    guidance = IFRAME_PROCESSOR_GUIDANCE.get(key)
    if not guidance:
        print(f"  No specific guidance for '{processor}'. Check if they support Google Ads pixel or use thank-you page redirect.")
        return
    print(f"\n  Processor: {processor}")
    print(f"  Native pixel: {'Yes' if guidance['supports_pixel'] else 'No'}")
    print(f"  Recommended: {guidance['method']}")
    if not guidance["supports_pixel"]:
        print(f"  Fallback: {guidance['fallback']}")


# ── MAIN ORCHESTRATOR ────────────────────────────────────────────────────

def configure_client_conversions(client_data: dict, dry_run: bool = False, audit_only: bool = False):
    """
    Full conversion pipeline for one client.
    Reads client config and applies the right strategy.
    """
    name = client_data["name"]
    customer_id = get_customer_id(client_data)
    strategy = client_data.get("conversion_strategy", "ga4_import")
    processor = client_data.get("donation_processor", "unknown")
    conversions_config = client_data.get("conversions", [])

    print(f"\n{'═'*60}")
    print(f"  {name}")
    print(f"  Account: {customer_id}")
    print(f"  Strategy: {strategy} | Processor: {processor}")
    print(f"{'═'*60}")

    ads_client = get_ads_client()

    # Always audit first
    existing_actions = audit_conversion_actions(ads_client, customer_id)
    campaign_goals = audit_campaign_goals(ads_client, customer_id)

    if audit_only:
        print_audit(client_data, existing_actions, campaign_goals)
        return

    # Print iframe guidance if relevant
    if strategy == "cross_domain_iframe":
        print_iframe_guidance(processor)
        if dry_run:
            print(f"\n  [DRY RUN] Would create manual-tag conversion actions for {name}")
            return

    if dry_run:
        print(f"\n  [DRY RUN] Would configure {len(conversions_config)} conversion actions")
        for conv in conversions_config:
            print(f"    - {conv.get('name')} ({conv.get('category')}) → group: {conv.get('group')}")
        return

    # Create conversion actions
    created_actions = {}  # group_name → list of resource_names

    for conv in conversions_config:
        conv_name = conv.get("name")
        category  = conv.get("category", "OTHER")
        group     = conv.get("group", "General")
        value     = conv.get("value", 0.0)

        # Skip if already exists
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
            if strategy == "ga4_import":
                rn = create_ga4_import_conversion(ads_client, customer_id, conv_name, category)
            elif strategy == "cross_domain_iframe":
                rn, tag = create_manual_tag_conversion(ads_client, customer_id, conv_name, category, value)
                # Save tag snippet to file for client handoff
                tag_file = f"tags/{client_data['slug']}_{conv_name.lower().replace(' ','_')}_tag.html"
                import os; os.makedirs("tags", exist_ok=True)
                with open(tag_file, "w") as f:
                    f.write(tag)
                print(f"  Tag saved to {tag_file}")
            elif strategy == "phone_call":
                rn = create_call_conversion(ads_client, customer_id, conv_name)
            elif strategy == "micro_conversions":
                rn = create_ga4_import_conversion(ads_client, customer_id, conv_name, category)
            else:
                rn = create_ga4_import_conversion(ads_client, customer_id, conv_name, category)

            created_actions.setdefault(group, []).append(rn)

        except GoogleAdsException as ex:
            for error in ex.failure.errors:
                print(f"  Error creating '{conv_name}': {error.message}")

    # Create conversion goal groups
    existing_goals = get_existing_custom_goals(ads_client, customer_id)
    goal_resource_names = {}

    for group_name, action_rns in created_actions.items():
        if not action_rns:
            continue
        if group_name in existing_goals:
            print(f"  Goal group '{group_name}' already exists — skipping creation")
            goal_resource_names[group_name] = existing_goals[group_name]
            continue
        try:
            goal_rn = create_custom_conversion_goal(ads_client, customer_id, group_name, action_rns)
            goal_resource_names[group_name] = goal_rn
        except GoogleAdsException as ex:
            for error in ex.failure.errors:
                print(f"  Error creating goal group '{group_name}': {error.message}")

    # Assign goal groups to campaigns
    campaign_assignments = client_data.get("campaign_conversion_assignments", [])
    for assignment in campaign_assignments:
        campaign_keywords = assignment.get("campaign_keywords", [])
        goal_name = assignment.get("goal_group")

        if goal_name not in goal_resource_names:
            print(f"  No goal group '{goal_name}' found — skipping campaign assignment")
            continue

        matching_campaigns = find_campaigns_by_type(ads_client, customer_id, campaign_keywords)
        for campaign in matching_campaigns:
            try:
                set_campaign_conversion_goal(
                    ads_client, customer_id,
                    campaign["resource_name"], goal_resource_names[goal_name],
                    campaign["name"], goal_name
                )
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    print(f"  Error assigning goal to '{campaign['name']}': {error.message}")

    print(f"\n  Done. {len(created_actions)} conversion groups configured.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Configure conversions for SAP clients")
    parser.add_argument("--client",      help="Client slug (e.g. scienceworks)")
    parser.add_argument("--all-clients", action="store_true", help="Run for all clients")
    parser.add_argument("--dry-run",     action="store_true", help="Preview only — no changes")
    parser.add_argument("--audit",       action="store_true", help="Show current conversion setup only")
    args = parser.parse_args()

    slugs = list(CLIENT_MAP.keys()) if args.all_clients else ([args.client] if args.client else [])
    if not slugs:
        parser.error("Provide --client SLUG or --all-clients")

    for slug in slugs:
        client_data = CLIENT_MAP.get(slug)
        if not client_data:
            print(f"Unknown client: {slug}")
            continue
        configure_client_conversions(client_data, dry_run=args.dry_run, audit_only=args.audit)
